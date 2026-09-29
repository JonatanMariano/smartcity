FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto /app/proto
RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/smartcity.proto
COPY common /app/common
COPY processing/ /app/
CMD ["python","-u","main.py"]
