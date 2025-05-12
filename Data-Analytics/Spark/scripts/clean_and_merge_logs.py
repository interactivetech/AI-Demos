import os
import re
from minio import Minio
from minio.error import S3Error

def merge_and_clean_logs(
    log_filenames,
    cleaned_log_filename='cleaned_server.log',
    source_bucket='raw-logs',
    target_bucket='clean-logs',
    local_dir='.',
    minio_client=None
):
    """
    Merges and cleans logs, uploads cleaned file to target bucket, and returns response times.
    """
    # Read credentials from environment (do NOT hardcode)
    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')

    if not all([endpoint, access_key, secret_key]):
        raise ValueError("Missing MinIO credentials in environment variables.")

    # Initialize MinIO client if not passed
    if minio_client is None:
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=True
        )

    # Ensure target bucket exists
    if not minio_client.bucket_exists(target_bucket):
        minio_client.make_bucket(target_bucket)
        print(f"📦 Created bucket '{target_bucket}'")

    pattern = re.compile(r'response_time=(\d+\.\d+)ms')
    response_times = []
    clean_lines = []

    for fname in log_filenames:
        local_path = os.path.join(local_dir, fname)
        try:
            minio_client.fget_object(source_bucket, fname, local_path)
            print(f"📥 Downloaded {fname} from '{source_bucket}'")
        except S3Error as e:
            print(f"⚠️ Failed to download {fname}: {e}")
            continue

        with open(local_path) as f:
            for line in f:
                if "status=404" in line:
                    continue
                clean_lines.append(line)
                match = pattern.search(line)
                if match:
                    response_times.append(float(match.group(1)))

    # Write cleaned file
    cleaned_path = os.path.join(local_dir, cleaned_log_filename)
    with open(cleaned_path, 'w') as f:
        f.writelines(clean_lines)

    # Upload to target bucket
    try:
        minio_client.fput_object(target_bucket, cleaned_log_filename, cleaned_path)
        print(f"📤 Uploaded cleaned log to '{target_bucket}/{cleaned_log_filename}'")
    except S3Error as e:
        print("⚠️ Upload failed:", e)

    print(f"✅ Cleaned log written with {len(clean_lines)} entries")
    return response_times
