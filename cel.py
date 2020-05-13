import time
import celery

from limiter import limit


"""
run as:
    1. python cel.py
    2. celery worker -A cel:celery_obj --concurrency=1 --loglevel=INFO
"""


# broker
CELERY_BROKER_URL = "amqp://guest:guest@localhost:7777//"
celery_obj = celery.Celery(broker=CELERY_BROKER_URL)


my_limiter = limit.Limiter(
    max_sample_size=1_000,
    min_sample_size=1,
    sampling_period=3 * 60,  # seconds
    breach_latency=700,  # milliSeconds
    breach_error_percent=5,  # percent
    max_rate_limit=12,  # reqs/sec
    noteworthy_exceptions=[BlockingIOError, TimeoutError],
    control_celery_rate_limit_interval=3,  # seconds,
    # ADDITION
    celery_app=celery_obj,
    task_name="my_adder_task_name",
)


@celery_obj.task(name="my_adder_task_name", rate_limit="1/m")
def adder(a, b):
    res = None
    error = None
    start = time.monotonic()
    try:
        res = a + b
        print("res: ", res)
    except Exception as e:
        error = e
    finally:
        end = time.monotonic()
        latency = end - start

        if 400 < res < 410:
            latency = 801
        if 700 < res < 705:
            latency = 1023

        my_limiter.updateAndGetRateLimit(latency=latency, error=None)

        return res


if __name__ == "__main__":
    # publish some tasks
    for i in range(1_000):
        adder.delay(a=i, b=3)
        # adder.delay(a=124, b=i)
        # adder.delay(a=i, b=26)
    print("celery_obj.conf.BROKER_URL", celery_obj.conf.BROKER_URL)
    print("celery_obj.conf.conf.BROKER_TRANSPORT", celery_obj.conf.BROKER_TRANSPORT)
    print("!!! adder messages enqueued !!!")
