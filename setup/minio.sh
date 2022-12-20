# Minio
sudo docker run --name minio -d -p 9000:9000 -p 9001:9001 -v "$PWD/datalake:/data" minio/minio server /data --console-address ":9001"

http://127.0.0.1:9001/login

minioadmin
