```bash
docker buildx build \
  --platform linux/amd64 \
  -t mendeza/python3.10-slim-airflow:latest \
  --push \
  .
```