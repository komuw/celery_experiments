import celery

import requests

"""
run as:
    1. python cel.py
    2. celery worker -A cel:celery_obj --concurrency=1 --loglevel=DEBUG --pool=gevent
"""


celery_obj = celery.Celery(broker="amqp://guest:guest@localhost:5672//")


@celery_obj.task(name="do_request", rate_limit="1/m")
def do_request():
    """
    a task whose rate_limit is set at 1task per minute.
    """
    url = "https://httpbin.org/delay/2"
    res = requests.get(url, timeout=180)
    print("res: ", res)


if __name__ == "__main__":
    # publish some tasks
    # and ascertain that they are only executed by the asyncio workers at the set rate_limit
    do_request.delay()
    do_request.delay()
    do_request.delay()
    print("!!! do_request messages enqueued !!!")
