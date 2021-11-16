from celery import Celery


# The hardcoded Redis host `redisservice` is a hostname created for the Redis
# container in the Docker network, defined in the docker-compose.yml.
# This is unfortunately specific to the currend Docker-compose setup and will
# not work if we want the Redis backend to be located elsewhere.
app = Celery(
    "asyncworker",
    backend="redis://redisservice",
    broker="redis://redisservice",
)
app.autodiscover_tasks(["asyncworker.tasks"])
