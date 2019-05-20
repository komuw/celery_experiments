import celery
import prometheus_client

"""
run as:
    1. python cel.py
    2. celery worker -A cel:celery_obj --concurrency=1 --pool=gevent --loglevel=DEBUG
"""


# broker
CELERY_BROKER_URL = "amqp://guest:guest@localhost:7777//"
celery_obj = celery.Celery(broker=CELERY_BROKER_URL)


registry = prometheus_client.CollectorRegistry()
metric_counter = prometheus_client.Counter(
    name="number_of_tasks",
    documentation="number of tasks processed.",
    labelnames=["task_name"],
    registry=registry,
)


@celery_obj.task(name="adder", rate_limit="25/s")
def adder(a, b):
    """
    task that adds two numbers.
    when it is ran by celery workers; it also keeps metrics of number of times it was executed.

    You can find the rate of execution with a query like:
      rate(number_of_tasks_total{task_name="adder"}[30s])
    """
    res = a + b
    print("res: ", res)

    metric_counter.labels(task_name="adder").inc()  # Increment by 1
    prometheus_client.push_to_gateway("localhost:9091", job="adder_task", registry=registry)

    return res


if __name__ == "__main__":
    # publish some tasks
    for i in range(1, 1000):
        adder.delay(a=45, b=i)

    print("celery_obj.conf.BROKER_URL", celery_obj.conf.BROKER_URL)
    print("celery_obj.conf.conf.BROKER_TRANSPORT", celery_obj.conf.BROKER_TRANSPORT)
    print("!!! adder messages enqueued !!!")
