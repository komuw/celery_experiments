import os
import sys

import redis
import celery
import requests

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


@celery_obj.task(name="network_task", bind=True)
def network_task(self):
    """
    task that performs network call
    """
    pid = os.getpid()
    print("\n\t task_id={0}. pid={1}".format(self.request.id, pid))

    if redis_instance.get(name=self.request.id):
        err_msg = "TASK: {0} HAS ALREADY BEEN EXECUTED".format(self.request.id)
        print("\n\t {0} \n\n".format(err_msg))
        sys.exit(77)
        os.kill(pid, 9)

        raise DoubleExecution(err_msg)
    else:
        redis_instance.set(
            name=self.request.id, value="{0}.executed".format(self.request.id), ex=432000
        )  # expires in 5days

    latency = 2  # seconds
    res = requests.get("https://httpbin.org/delay/{0}".format(latency))
    print("res: ", res)
    return res


################################ SYNC ACK PATCH ###########################################
from celery.worker.request import Request

# make acknowledgments to be synchronous instead of using promise
def new_acknowledge(self):
    """Acknowledge task."""
    if not self.acknowledged:
        self.message.ack()
        self.acknowledged = True


Request.acknowledge = new_acknowledge
################################ SYNC ACK PATCH ###########################################

########################################################################################################################################
# 1.
# /home/komuw/mystuff/celery_experiments/.venv/lib/python2.7/site-packages/celery/worker/strategy.py
#    info('Received task: %s', req)
# req.acknowledge
#  <bound method Request.acknowledge of <Request: network_task[c95fa6c4-3da4-4f17-90b0-e34f8b925ca3] () {}>>

# 2.
# /home/komuw/mystuff/celery_experiments/.venv/lib/python2.7/site-packages/celery/worker/request.py
#    def acknowledge(self)

# 3.
# (a) `celery.worker.request.Request.acknowledge` is called by
# (b) `celery.worker.request.Request.on_accepted` which is called by
# (c) `celery.worker.request.create_request_cls.Request.execute_using_pool` which is called by
# (d) `celery.worker.strategy.default` which is called by
# (e) `celery.utils.imports.instantiate` which is called by
# (f) `celery.app.task.start_strategy`
# a,b,c are called for every task executed
# d,e,f are called once at start-up

# 4.
# https://stackoverflow.com/questions/2654113/how-to-get-the-callers-method-name-in-the-called-method
# import inspect
# curframe = inspect.currentframe()
# calframe = inspect.getouterframes(curframe, 2)
# print("\n\t `default` called by, caller name: path={0} func={1} \n".format(calframe[1][1], calframe[1][3]))

########################################################################################################################################

# To reproduce;
# 1. queue one task.
# `python cel.py`
# 2. introduce latency when making acknowledgement to rabbitmq
#  ie add latency in the method `celery.worker.request.Request.acknowledge`
# 3. run celery workers
#  `celery worker -A cel:celery_obj --concurrency=2 --pool=gevent --loglevel=DEBUG`
# 4. simulate deployment
# `docker-compose restart --timeout 5 rabbitmq1`
# 5. boom

if __name__ == "__main__":
    # publish some tasks
    # for _ in range(1, 1000000):
    network_task.delay()
    print("celery_obj.conf.BROKER_URL", celery_obj.conf.BROKER_URL)
    print("celery_obj.conf.conf.BROKER_TRANSPORT", celery_obj.conf.BROKER_TRANSPORT)
    print("!!! network_task messages enqueued !!!")
