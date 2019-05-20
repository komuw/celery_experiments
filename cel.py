import celery


"""
run as:
    1. python cel.py
    2. celery worker -A cel:celery_obj --concurrency=1 --loglevel=DEBUG
"""


# broker
CELERY_BROKER_URL = "amqp://guest:guest@localhost:7777//"
celery_obj = celery.Celery(broker=CELERY_BROKER_URL)


@celery_obj.task(name="adder")
def adder(a, b):
    res = a + b
    print("res: ", res)
    return res


if __name__ == "__main__":
    # publish some tasks
    adder.delay(a=45, b=141)
    adder.delay(a=124, b=51)
    adder.delay(a=252, b=26)
    print("celery_obj.conf.BROKER_URL", celery_obj.conf.BROKER_URL)
    print("celery_obj.conf.conf.BROKER_TRANSPORT", celery_obj.conf.BROKER_TRANSPORT)
    print("!!! adder messages enqueued !!!")
