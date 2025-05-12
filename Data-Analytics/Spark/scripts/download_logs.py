import os
import sys
import subprocess
import importlib
# --- Dependency Check and Installation ---
package_name = "minio"
try:
    # Try importing the package to see if it exists
    importlib.import_module(package_name)
    print(f"'{package_name}' package already installed.")
except ImportError:
    print(f"'{package_name}' package not found. Attempting installation...")
    try:
        # Use check_call to ensure pip command runs successfully
        # sys.executable ensures pip is called for the correct python version
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"Successfully installed '{package_name}'.")
        # Need to import again after installation
        importlib.import_module(package_name)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install '{package_name}'. Error: {e}")
        print("Please ensure the base image has 'pip' and network access.")
        sys.exit(1) # Exit if installation fails
    except ImportError:
        # This shouldn't happen if check_call succeeded, but as a safeguard
        print(f"ERROR: '{package_name}' installed but still cannot be imported.")
        sys.exit(1)


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
