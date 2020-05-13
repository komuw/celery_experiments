import math
import time
import enum
import typing
import random
import collections


@enum.unique
class _State(enum.Enum):
    SLOW_START: int = 1
    CONGESTION_AVOIDANCE: int = 2
    CONGESTION_DETECTION: int = 3


class _Latency:
    def __init__(
        self, duration: float,
    ):
        self.duration = duration
        self.at = time.monotonic()

    def __gt__(self, other):
        if isinstance(other, _Latency):
            if self.duration > other.duration:
                return True
            else:
                return False
        else:
            if self.duration > other:
                return True
            else:
                return False

    def __ge__(self, other):
        if isinstance(other, _Latency):
            return self.duration >= other.duration
        else:
            return self.duration >= other

    def __lt__(self, other):
        if isinstance(other, _Latency):
            if self.duration < other.duration:
                return True
            else:
                return False
        else:
            if self.duration < other:
                return True
            else:
                return False

    def __mul__(self, other):
        if isinstance(other, _Latency):
            # eg, _Latency(45) * _Latency(3.7)
            return self.duration * other.duration
        else:
            # eg, _Latency(45) * 7.8
            return self.duration * other


class _Error:
    def __init__(
        self, e: typing.Union[None, Exception],
    ):
        self.e = e
        self.at = time.monotonic()


class Limiter:
    """
    It is important that tasks start with a very low rate limit by default.
    This can be done by setting `task_default_rate_limit`[1] 
    or
        @celery_app.task(name="my_adder_task_name", rate_limit="0.02/s")
        def adder(a, b):
            res = a + b
            return res
            
    Error Motto:
      If this limiter is to err, it should err on the side of letting more requests flow rather than less
    
    References:
    1. https://docs.celeryproject.org/en/stable/userguide/configuration.html#std:setting-task_default_rate_limit
    """

    def __init__(
        self,
        max_sample_size: int,
        min_sample_size: int,
        sampling_period: int,
        breach_latency: int,
        breach_error_percent: int,
        max_rate_limit: int,
        noteworthy_exceptions: typing.List[Exception],
        control_celery_rate_limit_interval: int,
        # TODO: document this plus type hints
        celery_app,
        task_name,
    ) -> None:
        """
        Parameters:
            max_sample_size: the maximum number of past requests to use when making decisions.
            min_sample_size: the minimum number of past requests that have to be available, in the last `sampling_period` seconds, for the limiter to do any limiting.
                             if there are fewer requests than this in the last `sampling_period` seconds, then this limiter fails open.
            sampling_period: the duration (in seconds) over which we will calculate the percentage error rate and latency.
            breach_latency: the p99 latency(in milliSeconds) at which the rateLimiter starts to control rate downwards.
            breach_error_percent: the percentage error/failre (in percent) at which the rateLimiter starts to control rate downwards.
            max_rate_limit: the maximum rate(in requests/second) that we can send requests at.
            noteworthy_exceptions: list of Exceptions, that should contribute towards `breach_error_percent`
            control_celery_rate_limit_interval: the interval(in seconds) at which to call celery to control its rate limit.
        """
        if max_sample_size <= 0:
            raise ValueError("`max_sample_size` cannot be zero or negative.")
        if max_sample_size > 100_000:
            # control memory size
            max_sample_size = 100_000
        self.latency_queue: typing.Deque[_Latency] = collections.deque(maxlen=max_sample_size)
        # TODO: what if error is None? One way to think of it is that, that request succded
        self.errors_queue: typing.Deque[_Error] = collections.deque(maxlen=max_sample_size)
        # Deques are thread-safe.

        if min_sample_size <= 0:
            raise ValueError("`min_sample_size` cannot be zero or negative.")
        self.min_sample_size = min_sample_size

        if sampling_period <= 0:
            raise ValueError("`sampling_period` cannot be zero or negative.")
        self.sampling_period = sampling_period

        if breach_latency <= 0:
            raise ValueError("`breach_latency` cannot be zero or negative.")
        self.BREACH_LATENCY = breach_latency  # milliSeconds

        if breach_error_percent <= 0:
            raise ValueError("`breach_error_percent` cannot be zero or negative.")
        self.BREACH_ERROR_RATE = breach_error_percent  # percent

        # requests/sec
        self.initial_rate_limit: float = round(1 / 60, 2)  # ie, 1req/min
        self.effective_rate_limit: float = self.initial_rate_limit
        if max_rate_limit <= 0:
            raise ValueError("`max_rate_limit` cannot be zero or negative.")
        self.MAX_RATE_LIMIT: float = max_rate_limit  # requests/second

        if not isinstance(noteworthy_exceptions, list):
            raise ValueError("`noteworthy_exceptions` should be a list of Exceptions.")
        if not noteworthy_exceptions:
            raise ValueError("`noteworthy_exceptions` should not be an empty list")
        self.list_of_noteworthy_exceptions = tuple(noteworthy_exceptions)

        if control_celery_rate_limit_interval <= 0:
            raise ValueError("`control_celery_rate_limit_interval` cannot be zero or negative.")
        # jitter is an extra precaution so that not all workers are rushing to change the
        # limit of the same task at the exact same time.
        # TODO:
        # - test what would happen if there is a race condition in updating rate limit of same task.
        # - document the results.
        # - maybe also bake it into a testcase
        # one way to carry out the experiment is to spawn multiple(1000) threads in python
        # that each call the celery ratelimit api.
        jitter = random.randint(3, 8)
        self.control_celery_rate_limit_interval = control_celery_rate_limit_interval + jitter

        self.current_state = _State.SLOW_START
        self.updated_celery_at: float = time.monotonic()

        self.celery_app = celery_app
        self.task_name = task_name

    @staticmethod
    def _percentile(N: list, percent: float) -> None:
        """
        # http://code.activestate.com/recipes/511478-finding-the-percentile-of-the-values/

        Find the percentile of a list of values.

        @parameter N - is a list of values.
        @parameter percent - a float value from 0.0 to 1.0.

        @return - the percentile of the values

        Examples:
          _percentile(range(10), 0.25) # 2.25
          _percentile([5,6,7,8,9,0,1,2,3,4], 0.25) # 2.25
        """
        if not N:
            return 0
        N = sorted(N)

        k = (len(N) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return N[int(k)]
        d0 = N[int(f)] * (c - k)
        d1 = N[int(c)] * (k - f)
        return d0 + d1

    def _get_p99_latency(self) -> float:
        """
        We only consider latencies of requests that happened in the last `self.sampling_period` seconds.
        The service we are talking to may have been having a bad time 10hrs ago, but now it is okay;
        we do not want the bad latencies it had 10hrs ago to be factored in. If however that is the behaviour
        you actually want, intially the limiter with a `sampling_period` longer than 10hrs.

        We also only make a decision if there were at least `min_sample_size` requests in the last `sampling_period` seconds.
        If there are not, then we assume p99 latency was 0.0
        """
        _hold = []
        now = time.monotonic()
        for lat in self.latency_queue:
            # only consider latencies of requests that happened
            # in the last `self.sampling_period` seconds.
            time_elapsed = now - lat.at
            if time_elapsed < self.sampling_period:
                _hold.append(lat.duration)

        if len(_hold) < self.min_sample_size:
            # the number of requests in the last `sampling_period` seconds is less than
            # is neccessary to make a decision
            return 0.0

        return self._percentile(_hold, 0.99)

    def _get_error_rate(self) -> int:
        # (number_of_exceptions/total_requests_made) * 100

        successes = []
        failures = []
        now = time.monotonic()
        for item in self.errors_queue:
            time_elapsed = now - item.at
            if time_elapsed < self.sampling_period:
                if isinstance(item.e, self.list_of_noteworthy_exceptions):
                    failures.append(1)
                else:
                    # be conservative, and assume everything else is success
                    # remember: the error motto
                    successes.append(1)

        total_reqs = sum(successes + failures)
        if total_reqs <= 0:
            return 0

        if total_reqs < self.min_sample_size:
            # the number of requests in the last `sampling_period` seconds is less than
            # is neccessary to make a decision
            return 0

        error_rate = (sum(failures) / total_reqs) * 100
        return error_rate

    def _slow_start(self) -> float:
        """
        Slow Start Phase : exponential increment – In this phase after every RTT the congestion window size increments exponentially.
        """
        if self._get_p99_latency() == 0:
            # this is initialization of the class isinstance.
            # Because that is the only time that latency can ever be zero
            self.effective_rate_limit = self.initial_rate_limit
        elif (self._get_p99_latency() >= self.BREACH_LATENCY) or (
            self._get_error_rate() >= self.BREACH_ERROR_RATE
        ):
            self.current_state = _State.CONGESTION_DETECTION
            self.effective_rate_limit = self.initial_rate_limit
        else:
            new_limit = self.effective_rate_limit + (
                self.MAX_RATE_LIMIT / 100
            )  # It would require 100 consecutive requests for effective_rate_limit == MAX_RATE_LIMIT
            self.effective_rate_limit = round(float(min(new_limit, self.MAX_RATE_LIMIT)), 2)

        return self.effective_rate_limit

    def _congestion_detection(self) -> float:
        self.current_state = _State.SLOW_START
        self.effective_rate_limit = self.initial_rate_limit
        return self.effective_rate_limit

    def _limit(self) -> float:
        if self.current_state == _State.SLOW_START:
            return self._slow_start()
        if self.current_state == _State.CONGESTION_DETECTION:
            return self._congestion_detection()
        else:
            # remember error motto
            return self.MAX_RATE_LIMIT

    def _update(self, latency: float, error: typing.Union[None, Exception]) -> None:
        self.latency_queue.append(_Latency(duration=latency))
        self.errors_queue.append(_Error(e=error))

    def _call_celery_rate_limit(self, rate_limit: float):
        # TODO: implement according to:
        # https://github.com/celery/celery/blob/4.1/celery/app/control.py#L240-L260

        # TODO: confirm value that celery accepts.
        # https://docs.celeryproject.org/en/stable/userguide/tasks.html#Task.rate_limit says that
        # it can be int or float or string
        #
        # whereas https://docs.celeryproject.org/en/stable/reference/celery.app.control.html#celery.app.control.Control.rate_limit
        # says of only int or string

        now = time.monotonic()
        time_since_updating_celery = now - self.updated_celery_at
        if time_since_updating_celery >= self.control_celery_rate_limit_interval:
            self.updated_celery_at = now  # important

            rate_limit_per_sec = "{0:.2f}/s".format(rate_limit)
            log_data = {
                "event": "call_celery_rate_limit",
                "task_name": self.task_name,
                "current_rate_limit": rate_limit_per_sec,
                "max_rate_limit": "{0}/s".format(self.MAX_RATE_LIMIT),
                "current_error_rate": "{0}%".format(self._get_error_rate()),
                "breach_error_rate": "{0}%".format(self.BREACH_ERROR_RATE),
                "current_p99_latency": "{0:.2f}ms".format(self._get_p99_latency()),
                "breach_latency": "{0}ms".format(self.BREACH_LATENCY),
                "control_celery_rate_limit_interval": "{0:.2f}s".format(
                    self.control_celery_rate_limit_interval
                ),
                "time_since_updating_celery": "{0:.2f}s".format(time_since_updating_celery),
            }
            print("\n\n\t {0}\n\n".format(log_data))
            self.celery_app.control.rate_limit(
                task_name=self.task_name, rate_limit=rate_limit_per_sec,
                timeout=0.5# seconds
            )
        else:
            return

    def updateAndGetRateLimit(self, latency: float, error: typing.Union[None, Exception]) -> float:
        try:
            self._update(latency=latency, error=error)

            # we want limit to be called for every request.
            # this ensures that we always get the uptodate limits
            new_rate_limit = self._limit()

            self._call_celery_rate_limit(rate_limit=new_rate_limit)

            return new_rate_limit
        except Exception:
            # TODO: log
            return self.MAX_RATE_LIMIT


import threading, multiprocessing


class TEST_RATELIMIT_RACE_CONDITION:
    """
    https://docs.python.org/3.6/library/multiprocessing.html
    """

    def __init__(self, celery_app, task_name):
        self.celery_app = celery_app
        self.task_name = task_name
        self.race_condition_tester_rate_limit_per_sec = "0.67/s"

    def _test_ratelimit_race_condition(self):
        self.celery_app.control.rate_limit(
            task_name=self.task_name, rate_limit=self.race_condition_tester_rate_limit_per_sec
        )
        msg = "_test_ratelimit_race_condition called END."
        print(msg)
        return msg

    def _multiprocessing(self):
        num_processes = 100
        with multiprocessing.Pool(num_processes) as p:
            # launching multiple evaluations asynchronously *may* use more processes
            multiple_results = [
                p.apply_async(self._test_ratelimit_race_condition, ()) for i in range(num_processes)
            ]
            get_all = [res.get(timeout=0.5) for res in multiple_results]
            print(get_all)

    def start(self):
        self._multiprocessing()
