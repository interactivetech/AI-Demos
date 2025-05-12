from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, RepeatVector, TimeDistributed, Dense
import tensorflow as tf
import mlflow
import mlflow.tensorflow
import os
import json
import re
import numpy as np
import matplotlib.pyplot as plt
from minio import Minio
from minio.error import S3Error
from mlflow.models.signature import infer_signature


print("tensorflow.__version__: ", tf.__version__)
print("mlflow.__version__: ", mlflow.__version__)

print("MLflow Tracking URI:", mlflow.get_tracking_uri())
print("MLFLOW_TRACKING_TOKEN:", os.getenv("MLFLOW_TRACKING_TOKEN"))

try:
    with open('/var/run/secrets/mlflow/mlflow-token', "r") as file:
        token = file.read().strip()
except Exception as e:
    print(f"Error reading MLflow auth token: {e}")
    raise
os.environ['MLFLOW_TRACKING_TOKEN'] = token
print("--MLFLOW_TRACKING_TOKEN:", os.getenv("MLFLOW_TRACKING_TOKEN"))

# Set tracking URI
mlflow.set_tracking_uri("http://mlflow.mlflow.svc.cluster.local:5000")

# Inject bearer token into all MLflow HTTP requests
try:
    with open('/var/run/secrets/mlflow/mlflow-token', "r") as file:
        token = file.read().strip()

    from mlflow.utils.rest_utils import http_request
    def authenticated_request(*args, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        return http_request(*args, headers=headers, **kwargs)
    mlflow.utils.rest_utils.http_request = authenticated_request
    print("✅ MLflow token injected into HTTP requests")
except Exception as e:
    print("❌ Failed to configure MLflow token:", e)
    raise

def upload_folder(client, bucket, local_folder, prefix):
    for root, dirs, files in os.walk(local_folder):
        for fname in files:
            path = os.path.join(root, fname)
            rel = os.path.relpath(path, local_folder)
            obj = f"{prefix.rstrip('/')}/{rel.replace(os.sep, '/')}"
            try:
                client.fput_object(bucket, obj, path)
                print('Uploaded', obj)
            except S3Error as e:
                print('Upload error', e)


def train_and_export_model(
    cleaned_log_filename='merged_cleaned.log',
    source_bucket='clean-logs',
    target_bucket='models2',
    minio_client=None):

    tmp_dir = '/tmp/model_pipeline'
    os.makedirs(tmp_dir, exist_ok=True)
    export_dir = os.path.join(tmp_dir, 'anomaly_detection/0001')
    os.makedirs(os.path.join(export_dir, 'assets'), exist_ok=True)

    # Read MinIO credentials from env
    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')

    if not all([endpoint, access_key, secret_key]):
        raise ValueError("Missing MinIO credentials in environment variables.")

    if minio_client is None:
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=True
        )

    # Download log
    download_path = os.path.join(tmp_dir, 'downloaded-server.log')
    try:
        minio_client.fget_object(source_bucket, cleaned_log_filename, download_path)
        print(f"📥 Downloaded '{cleaned_log_filename}' to '{download_path}'")
    except S3Error as err:
        print("Download error:", err)

    # Parse response times
    times = []
    pattern = re.compile(r'response_time=(\d+\.\d+)ms')
    with open(download_path) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                times.append(float(m.group(1)))
    data = np.array(times)

    window_size = 10
    windows = np.array([data[i:i+window_size] for i in range(len(data)-window_size)])
    windows = windows[..., np.newaxis]

    split = int(0.8 * len(windows))
    train_data = windows[:split]
    test_data = windows[split:]
    print('Train shape:', train_data.shape, 'Test shape:', test_data.shape)

    if mlflow.active_run():
        mlflow.end_run()
    mlflow.tensorflow.autolog()

    with mlflow.start_run(run_name="lstm_autoencoder_anomaly"):
        inp = Input(shape=(window_size, 1))
        enc = LSTM(16, activation='relu')(inp)
        dec = RepeatVector(window_size)(enc)
        dec = LSTM(16, activation='relu', return_sequences=True)(dec)
        out = TimeDistributed(Dense(1))(dec)
        autoencoder = Model(inp, out)
        autoencoder.compile(optimizer='adam', loss='mse')

        history = autoencoder.fit(
            train_data, train_data,
            validation_data=(test_data, test_data),
            epochs=5,
            batch_size=32
        )

    recon_train = autoencoder.predict(train_data)
    errors_train = np.mean((recon_train - train_data)**2, axis=(1,2))
    threshold = errors_train.mean() + 2 * errors_train.std()

    recon_test = autoencoder.predict(test_data)
    errors_test = np.mean((recon_test - test_data)**2, axis=(1,2))

    mlflow.log_metric("threshold", threshold)
    mlflow.log_metric("mean_test_error", errors_test.mean())
    mlflow.log_metric("max_test_error", errors_test.max())

    # Save plots
    plt.figure()
    plt.hist(errors_test, bins=50)
    plt.axvline(threshold, linestyle='--')
    plt.title('Reconstruction Error Distribution (Test)')
    plt.savefig(os.path.join(export_dir, 'assets', 'error_hist.png'))

    plt.figure()
    plt.plot(errors_test)
    plt.axhline(threshold, linestyle='--')
    plt.title('Reconstruction Error Over Time')
    plt.savefig(os.path.join(export_dir, 'assets', 'error_time.png'))

    # Export model
    dummy = tf.random.normal([1, window_size, 1])
    autoencoder(dummy)

    archive = tf.keras.export.ExportArchive()
    archive.track(autoencoder)

    @tf.function(input_signature=[tf.TensorSpec([None, window_size, 1], tf.float32, name='inputs')])
    def serve_fn(inputs):
        recon = autoencoder(inputs)
        score = tf.reduce_mean(tf.square(recon - inputs), axis=[1,2])
        return {'anomaly_score': score}

    archive.add_endpoint('serving_default', serve_fn)
    archive.add_variable_collection('variables', autoencoder.variables)
    archive.write_out(export_dir)
    print('✅ Model exported to', export_dir)

    if mlflow.active_run():
        mlflow.end_run()

    with mlflow.start_run(run_name="register_exported_model") as run:
        example_input = tf.convert_to_tensor(train_data[:1])
        example_output = autoencoder(example_input)
        signature = infer_signature(example_input.numpy(), example_output.numpy())

        mlflow.tensorflow.log_model(
            model=autoencoder,
            artifact_path="model",
            signature=signature,
            registered_model_name="AnomalyDetectionModel"
        )

        metadata = {
            "run_id": run.info.run_id,
            "artifact_uri": mlflow.get_artifact_uri("model"),
            "registered_model_name": "AnomalyDetectionModel"
        }

        metadata_path = os.path.join(tmp_dir, "anomaly_detection", "model_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        print("📦 Metadata written to", metadata_path)

    # Upload everything to MinIO
    upload_folder(minio_client, target_bucket, os.path.join(tmp_dir, 'anomaly_detection'), 'anomaly_detection/')

# Run the training
train_and_export_model(
    cleaned_log_filename='merged_cleaned.log',
    source_bucket='clean-logs',
    target_bucket='models2',
    minio_client=None)