import sys
import redis
import time
import celery


"""
run as:
    1. python cel.py
    2. celery worker -A cel:celery_obj --concurrency=20 --pool=gevent --loglevel=DEBUG
"""


# broker
CELERY_BROKER_URL = "amqp://guest:guest@localhost:7777//"
celery_obj = celery.Celery(broker=CELERY_BROKER_URL)


class DoubleExecution(Exception):
    """
    raised when/if celery tries to execute the same task more than once
    """

    pass


redis_instance = redis.Redis(host="localhost", port=6379, db=0)


@celery_obj.task(name="adder", bind=True)
def adder(self, a, b):
    print("\n\t task_id: ", self.request.id)

    if redis_instance.get(name=self.request.id):
        err_msg = "task: {0} with arguments: a={1} b={2} has already been executed".format(
            self.request.id, a, b
        )
        print("\n\t {0} \n\n".format(err_msg))
        sys.exit(77)
        raise DoubleExecution(err_msg)
    else:
        redis_instance.set(
            name=self.request.id, value="{0}.executed".format(self.request.id), ex=432000
        )  # expires in 5days

    res = a + b
    print("res: ", res)

    # time.sleep(1.2)
    return res


if __name__ == "__main__":
    # publish some tasks
    for i in range(1, 10000000):
        adder.delay(a=45, b=i)
    print("celery_obj.conf.BROKER_URL", celery_obj.conf.BROKER_URL)
    print("celery_obj.conf.conf.BROKER_TRANSPORT", celery_obj.conf.BROKER_TRANSPORT)
    print("!!! adder messages enqueued !!!")
