docker compose up -d --build
docker exec -it python_api python api.py
docker exec -it python_productor python producer.py
docker compose down --rmi all
docker compose up -d --scale python-client=3
docker exec -ti CONTAINER_ID python emit_logs.py