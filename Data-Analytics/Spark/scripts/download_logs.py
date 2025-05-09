from minio import Minio
from minio.error import S3Error
import os

endpoint = os.getenv('MINIO_ENDPOINT', 'minio-api.ingress.pcai0108.sv11.hpecolo.net')
access_key = os.getenv('MINIO_ACCESS_KEY', '4JCA5L2jOci5eacIW24i')
secret_key = os.getenv('MINIO_SECRET_KEY', '8ksfKLEoFOWcXAOGpq4oIRun96S9bvo0c6xOyxUA')
secure_conn = True

# Save location inside shared volume
download_path = '/mounts/shared-volume/shared/airflow_demo/downloaded-server.log'

client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=secure_conn
)

bucket_name = "logs"
object_name = "server.log"

try:
    client.fget_object(bucket_name, object_name, download_path)
    print(f"Downloaded '{bucket_name}/{object_name}' to '{download_path}'.")
except S3Error as err:
    print("Download error:", err)
