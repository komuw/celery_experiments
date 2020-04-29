import redis
import celery
import prometheus_client


"""
run as:
    1. python cel.py
    2. celery worker -A cel:celery_obj --concurrency=20 --pool=gevent --loglevel=DEBUG
"""


# broker
CELERY_BROKER_URL = "amqp://guest:guest@localhost:5672//"
celery_obj = celery.Celery(broker=CELERY_BROKER_URL)


registry = prometheus_client.CollectorRegistry()
metric_counter = prometheus_client.Counter(
    name="number_of_tasks",
    documentation="number of tasks processed.",
    labelnames=["task_name"],
    registry=registry,
)


redis_instance = redis.Redis(host="localhost", port=6379, db=0)


@celery_obj.task(name="adder", rate_limit="1/s")
def adder(a, b):
    """
    task that adds two numbers.
    when it is ran by celery workers; it also keeps metrics of number of times it was executed.

    You can find the rate of execution with a query like:
      rate(number_of_tasks_total{task_name="adder"}[120s])  # find task processing rate(tasks per second) over the past 30seconds
    """
    res = a + b
    print("\t res: ", res)

    if res > 1000:
        redis_instance.incr("failed")
    else:
        redis_instance.incr("succeeded")

    metric_counter.labels(task_name="adder").inc()  # Increment by 1
    prometheus_client.push_to_gateway("localhost:9091", job="adder_task", registry=registry)

    print("\n\t `adder` called. \n")
    return res


################################ CUSTOM RATELIMITER ###########################################
# Instead of using this interface which can be problematic, 
# you could use `celery.app.control.Control.rate_limit` 
# https://docs.celeryproject.org/en/4.4.2/reference/celery.app.control.html#celery.app.control.Control.rate_limit

class KomuCustomRateLimiter:
    def __init__(self):
        """
        This rateLimiter;
        1. fetches error rates and success rates from a redis instance.
        2. it uses that to calculate the current failure rate
        3. If the percentage failure rate is above a certain percentage;
            (a) it inhibits the celery worker from processing anymore requests
        
        class Consumer:
            # celery/worker/consumer/consumer.py
            def reset_rate_limits(self):
                self.task_buckets.update(
                    (n, self.bucket_for_task(t)) for n, t in items(self.app.tasks)
                )
                # use custom ratelimiter for the `adder` task
                self.task_buckets['adder'] = KomuCustomRateLimiter()
        """
        import redis
        from collections import deque

        self.redis_instance = redis.Redis(host="localhost", port=6379, db=0)
        self.retry_count = 0

        self.contents = deque()  # celery is dependent on this

    def add(self, item):
        # celery is dependent on this
        self.contents.append(item)

    def pop(self):
        # celery is dependent on this
        return self.contents.popleft()

    def clear_pending(self):
        # celery is dependent on this
        self.contents.clear()
        self.retry_count = 0

    def _calculate_failure_rate(self):
        number_success = (
            float(self.redis_instance.get("succeeded"))
            if self.redis_instance.get("succeeded")
            else 1.00
        )  # eliminate division by zero
        number_failures = (
            float(self.redis_instance.get("failed")) if self.redis_instance.get("failed") else 1.00
        )

        failure_rate = (number_failures / (number_failures + number_success)) * 100
        print("\n\t failure_rate:: {0}\n".format(failure_rate))
        return failure_rate

    def can_consume(self, tokens=1):
        """
        Returns:
            bool
        """
        _can_consume = True
        if self._calculate_failure_rate() > 30:
            _can_consume = False

        print(
            "\n\t `KomuCustomRateLimiter.can_consume` called. _can_consume={0}\n".format(
                _can_consume
            )
        )
        return _can_consume

    @staticmethod
    def _retry_after(current_retries):
        """
        returns the number of seconds to retry after.
        retries will happen in this sequence;
        0.5min, 1min, 2min, 4min, 8min, 16min, 32min, 16min, 16min, 16min ...
        """
        import random

        if current_retries < 0:
            current_retries = 0

        jitter = random.randint(10, 30)  # 10sec-30sec
        if current_retries in [0, 1]:
            return float(10.0)  # 10sec
        elif current_retries == 2:
            return float(20.0)
        elif current_retries >= 6:
            return float((2 * 60) + jitter)  # 2 minutes + jitter
        else:
            return float((20 * (2 ** current_retries)) + jitter)

    def expected_time(self, tokens=1):
        """Return estimated time of token availability.

        Returns:
            float: the time in seconds.
        """
        # TODO: add more logic

        if self._calculate_failure_rate() > 30:
            self.retry_count += 1
            _time = self._retry_after(self.retry_count)  # 0.99 # do exponetial back-off
        else:
            self.retry_count = 0
            _time = 0.00

        print("\n\t `KomuCustomRateLimiter.expected_time` called. _time={0}\n".format(_time))
        return _time  # seconds


from celery.worker.consumer.consumer import Consumer


def _patched_reset_rate_limits(celery_Consumer_obj):
    """
    patch celery to accept a custom ratelimiter
    """
    celery_Consumer_obj.task_buckets.update(
        (n, KomuCustomRateLimiter()) for n, t in celery_obj.tasks.items()
    )
    print("\n\t `_patched_reset_rate_limits` called. \n")
    print()


# patch celery to use custom ratelimiter
Consumer.reset_rate_limits = _patched_reset_rate_limits
################################ CUSTOM RATELIMITER ###########################################

if __name__ == "__main__":
    # publish 10K tasks
    for i in range(1, 10000):
        adder.delay(a=45, b=i)

    print("celery_obj.conf.BROKER_URL", celery_obj.conf.BROKER_URL)
    print("celery_obj.conf.conf.BROKER_TRANSPORT", celery_obj.conf.BROKER_TRANSPORT)
    print("!!! adder messages enqueued !!!")
