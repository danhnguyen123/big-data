# Project from Data team for backend job

git source:

- Asynchronous Tasks with FastAPI and Celery

# Step to deploy

Install Redis & Postgresql

docker run --name redis -p 6379:6379 -d redis
docker run --name postgresql -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -v /home/danhnguyen/postgres-dir:/var/lib/postgresql/data -d postgres

Create new user airflow
docker exec -it airflow-airflow-webserver-1 airflow db init
docker exec -it airflow-airflow-webserver-1 airflow users create --username admin --firstname Peter --lastname Parker --role Admin --email peter.parker@advanger.com

Spin up the containers:

```sh
$ docker-compose up -d --build
```
