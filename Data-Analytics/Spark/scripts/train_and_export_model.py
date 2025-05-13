import os
import json
import re
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, RepeatVector, TimeDistributed, Dense
from mlflow.models.signature import infer_signature
import mlflow
import mlflow.tensorflow
from minio import Minio
from minio.error import S3Error

# --- Setup ---
os.environ["MPLCONFIGDIR"] = "/tmp/mpl_config"  # Fix matplotlib cache dir issue
WORK_DIR = "/tmp/model_pipeline"
os.makedirs(WORK_DIR, exist_ok=True)

print("tensorflow.__version__:", tf.__version__)
print("mlflow.__version__:", mlflow.__version__)

mlflow.set_tracking_uri("http://mlflow.mlflow.svc.cluster.local:5000")
print("MLflow Tracking URI:", mlflow.get_tracking_uri())

# --- Load MLflow Token from Volume Mount ---
try:
    with open("/var/run/secrets/mlflow/mlflow-token", "r") as f:
        token = f.read().strip()
        os.environ["MLFLOW_TRACKING_TOKEN"] = token
        print("✅ MLflow token injected into environment")
except Exception as e:
    raise RuntimeError(f"❌ Failed to read MLflow token: {e}")
os.environ['MLFLOW_TRACKING_INSECURE_TLS']='true'
os.environ['MLFLOW_S3_IGNORE_TLS']='true'
os.environ['MLFLOW_S3_ENDPOINT_URL']='http://local-s3-service.ezdata-system.svc.cluster.local:30000'
# --- Helper: Upload a folder to MinIO ---
def upload_folder(client, bucket, local_folder, prefix):
    for root, dirs, files in os.walk(local_folder):
        for fname in files:
            path = os.path.join(root, fname)
            rel = os.path.relpath(path, local_folder)
            obj = f"{prefix.rstrip('/')}/{rel.replace(os.sep, '/')}"
            try:
                client.fput_object(bucket, obj, path)
                print(f"🆙 Uploaded: {obj}")
            except S3Error as e:
                print("⚠️ Upload error:", e)

# --- Main training and export function ---
def train_and_export_model(
    cleaned_log_filename="merged_cleaned.log",
    source_bucket="clean-logs",
    target_bucket="models2",
    minio_client=None,
):

    # --- Init MinIO ---
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not all([endpoint, access_key, secret_key]):
        raise ValueError("Missing MinIO credentials in environment variables.")

    if minio_client is None:
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=True
        )

    # --- Download cleaned log file ---
    local_log_path = os.path.join(WORK_DIR, "downloaded-server.log")
    try:
        minio_client.fget_object(source_bucket, cleaned_log_filename, local_log_path)
        print(f"📥 Downloaded '{cleaned_log_filename}' to '{local_log_path}'")
    except S3Error as err:
        raise RuntimeError(f"❌ Download error from MinIO: {err}")

    # --- Parse response times ---
    times = []
    pattern = re.compile(r'response_time=(\d+\.\d+)ms')
    with open(local_log_path) as f:
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
    print(f"🧪 Train shape: {train_data.shape}, Test shape: {test_data.shape}")

    if mlflow.active_run():
        mlflow.end_run()
    mlflow.tensorflow.autolog()

    # --- Train Model ---
    with mlflow.start_run(run_name="lstm_autoencoder_anomaly"):
        inp = Input(shape=(window_size, 1))
        enc = LSTM(16, activation='relu')(inp)
        dec = RepeatVector(window_size)(enc)
        dec = LSTM(16, activation='relu', return_sequences=True)(dec)
        out = TimeDistributed(Dense(1))(dec)
        autoencoder = Model(inp, out)
        autoencoder.compile(optimizer='adam', loss='mse')

        autoencoder.fit(
            train_data, train_data,
            validation_data=(test_data, test_data),
            epochs=5,
            batch_size=32
        )

    # --- Evaluate and log plots ---
    recon_train = autoencoder.predict(train_data)
    errors_train = np.mean((recon_train - train_data) ** 2, axis=(1, 2))
    threshold = errors_train.mean() + 2 * errors_train.std()

    recon_test = autoencoder.predict(test_data)
    errors_test = np.mean((recon_test - test_data) ** 2, axis=(1, 2))

    mlflow.log_metric("threshold", threshold)
    mlflow.log_metric("mean_test_error", errors_test.mean())
    mlflow.log_metric("max_test_error", errors_test.max())

    plot_dir = os.path.join(WORK_DIR, "anomaly_detection/0001/assets")
    os.makedirs(plot_dir, exist_ok=True)

    # Histogram
    plt.figure()
    plt.hist(errors_test, bins=50)
    plt.axvline(threshold, linestyle='--')
    plt.title("Reconstruction Error Distribution (Test)")
    plt.savefig(os.path.join(plot_dir, "error_hist.png"))

    # Error over time
    plt.figure()
    plt.plot(errors_test)
    plt.axhline(threshold, linestyle='--')
    plt.title("Reconstruction Error Over Time")
    plt.savefig(os.path.join(plot_dir, "error_time.png"))

    # --- Export TF SavedModel ---
    export_dir = os.path.join(WORK_DIR, "anomaly_detection/0001")
    dummy = tf.random.normal([1, window_size, 1])
    autoencoder(dummy)  # ensure model is built

    archive = tf.keras.export.ExportArchive()
    archive.track(autoencoder)

    @tf.function(input_signature=[tf.TensorSpec([None, window_size, 1], tf.float32, name="inputs")])
    def serve_fn(inputs):
        recon = autoencoder(inputs)
        score = tf.reduce_mean(tf.square(recon - inputs), axis=[1, 2])
        return {"anomaly_score": score}

    archive.add_endpoint("serving_default", serve_fn)
    archive.add_variable_collection("variables", autoencoder.variables)
    archive.write_out(export_dir)
    print(f"📦 Model exported to {export_dir}")

    # --- Register in MLflow ---
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

        meta_path = os.path.join(WORK_DIR, "anomaly_detection/model_metadata.json")
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2)

        print("📄 Metadata written:", meta_path)

    # --- Upload full model folder to MinIO ---
    upload_folder(minio_client, target_bucket, os.path.join(WORK_DIR, "anomaly_detection"), "anomaly_detection/")


# --- Run if script is main ---
if __name__ == "__main__":
    train_and_export_model(
        cleaned_log_filename="merged_cleaned.log",
        source_bucket="clean-logs",
        target_bucket="models2"
    )
