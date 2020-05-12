# do not to pollute the global namespace.
# see: https://python-packaging.readthedocs.io/en/latest/testing.html
import time
from unittest import TestCase, mock

import limit


class TestLimit(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_limit.TestLimit.test_something

    use: http://fooplot.com/ for plotting
    """

    def setUp(self):
        self.max_sample_size = 1_000
        self.min_sample_size = 1
        self.sampling_period = 15 * 60  # seconds
        self.breach_latency = 700  # milliSeconds
        self.breach_error_percent = 5  # percent
        self.max_rate_limit = 20  # requests/second
        self.noteworthy_exceptions = [BlockingIOError, TimeoutError]
        self.control_celery_rate_limit_interval = 2 * 60  # 2minutes

        self.lt = self._create_new_limiter()

        self.IN_consequential_latency = self.lt.BREACH_LATENCY / 10
        self.Consequential_latency = self.lt.BREACH_LATENCY * 3

    def _create_new_limiter(self):
        return limit.Limiter(
            max_sample_size=self.max_sample_size,
            min_sample_size=self.min_sample_size,
            sampling_period=self.sampling_period,
            breach_latency=self.breach_latency,
            breach_error_percent=self.breach_error_percent,
            max_rate_limit=self.max_rate_limit,
            noteworthy_exceptions=self.noteworthy_exceptions,
            control_celery_rate_limit_interval=self.control_celery_rate_limit_interval,
        )

    def test_initialization_validations(self):
        with self.assertRaises(ValueError) as raised_exception:
            self.lt = limit.Limiter(
                max_sample_size=self.max_sample_size,
                min_sample_size=self.min_sample_size,
                sampling_period=self.sampling_period,
                breach_latency=self.breach_latency,
                breach_error_percent=self.breach_error_percent,
                max_rate_limit=self.max_rate_limit,
                noteworthy_exceptions=[],
                control_celery_rate_limit_interval=self.control_celery_rate_limit_interval,
            )
        self.assertIn(
            "`noteworthy_exceptions` should not be an empty list", str(raised_exception.exception)
        )

    def test_initial_conditions(self):
        self.assertEqual(self.lt._get_error_rate(), 0)
        self.assertEqual(self.lt._get_p99_latency(), 0)
        self.assertEqual(
            self.lt._limit(), self.lt.initial_rate_limit,
        )

    def test_slow_start(self):
        # start with inconsequential latency
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        self.assertEqual(self.lt._limit(), 0.22)
        self.assertEqual(self.lt._limit(), 0.42)
        self.assertEqual(self.lt._limit(), 0.62)

    def test_slow_start_to_max(self):
        # start with inconsequential latency
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        for _ in range(0, 100):
            self.lt._limit()

        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

    def test_congestion_detection(self):
        # start with inconsequential latency
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

        # now, tank the latency
        self.lt._update(latency=self.Consequential_latency, error=None)
        self.assertEqual(
            self.lt._limit(), self.lt.initial_rate_limit,
        )
        self.assertEqual(
            self.lt._limit(), self.lt.initial_rate_limit,
        )

    def test_congestion_detection_then_normalncy(self):
        # start with inconsequential latency
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

        # now, tank the latency
        for _ in range(0, 5):
            self.lt._update(latency=self.Consequential_latency, error=None)
        self.assertEqual(
            self.lt._limit(), self.lt.initial_rate_limit,
        )

        # now, the service we are talking to improves
        for _ in range(0, 1000):
            self.lt._update(latency=self.IN_consequential_latency, error=None)
        self.assertEqual(self.lt._limit(), 0.02)
        self.assertEqual(self.lt._limit(), 0.22)
        self.assertEqual(self.lt._limit(), 0.42)
        self.assertEqual(self.lt._limit(), 0.62)

        # eventually the rate limit should be able to go to max_rate_limit
        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(
            self.lt._limit(), self.lt.MAX_RATE_LIMIT,
        )

    def test_congestion_detection_then_normalncy_using_errors(self):
        """
        this is just like the previous test,
        except it uses exceptions and not latency to achieve the same thing.
        """
        # start with inconsequential latency
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

        # now, tank the errors. ie, cause 50% error rate
        self.lt._update(
            latency=self.IN_consequential_latency, error=self.noteworthy_exceptions[0]("boom!")
        )
        self.assertEqual(self.lt._get_error_rate(), 50.0)
        self.assertEqual(self.lt._limit(), self.lt.initial_rate_limit)

        # now, the service we are talking to improves
        for _ in range(0, 100):
            self.lt._update(latency=self.IN_consequential_latency, error=None)
        self.assertEqual(self.lt._limit(), 0.02)
        self.assertEqual(self.lt._limit(), 0.22)
        self.assertEqual(self.lt._limit(), 0.42)
        self.assertEqual(self.lt._limit(), 0.62)

        # eventually the rate limit should be able to go to max_rate_limit
        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

    def test_noteworthy_exceptions_do_count(self):
        # start with inconsequential error
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

        # now, tank the errors. ie, cause 50% error rate
        self.lt._update(
            latency=self.IN_consequential_latency, error=self.noteworthy_exceptions[0]("boom!")
        )
        self.assertEqual(self.lt._get_error_rate(), 50.0)
        self.assertEqual(self.lt._limit(), self.lt.initial_rate_limit)
        self.assertEqual(self.lt._limit(), self.lt.initial_rate_limit)

    def test_UN_noteworthy_exceptions_do_NOT_count(self):
        """
        this is just like the previous test,
        except we are using errors that are not in the noteworthy_exceptions list.
        """
        # start with inconsequential error
        self.lt._update(latency=self.IN_consequential_latency, error=None)

        for _ in range(0, 100):
            self.lt._limit()
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)

        class SomeException(Exception):
            pass

        self.assertNotIn(SomeException, self.noteworthy_exceptions)
        # now, tank the errors.
        self.lt._update(
            latency=self.IN_consequential_latency, error=SomeException("some exception!")
        )
        self.assertEqual(self.lt._get_error_rate(), 0.0)
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)
        self.assertEqual(self.lt._limit(), self.lt.MAX_RATE_LIMIT)  # rate is unaffected

    def test_public_api_resilience_to_internal_errors(self):
        """
        test that the public API succeds even if there are internal errors
        """
        self.lt.latency_queue = None
        self.lt.errors_queue = None
        new_limit = self.lt.updateAndGetRateLimit(latency=self.IN_consequential_latency, error=None)
        self.assertEqual(new_limit, self.lt.MAX_RATE_LIMIT)

        with mock.patch("limit.Limiter._get_p99_latency") as mock_get_p99_latency:
            mock_get_p99_latency.side_effect = KeyError("boom!")

            brand_new_lt = self._create_new_limiter()  # since `self.lt` has been spoilt above

            new_limit = brand_new_lt.updateAndGetRateLimit(
                latency=self.IN_consequential_latency, error=None
            )
            self.assertEqual(new_limit, brand_new_lt.MAX_RATE_LIMIT)

    def test_percentiles(self):
        self.assertEqual(self.lt._percentile(range(10), 0.25), 2.25)
        self.assertEqual(self.lt._percentile([5, 6, 7, 8, 9, 0, 1, 2, 3, 4], 0.25), 2.25)
        self.assertEqual(self.lt._percentile(range(1000), 0.99), 989.01)


class TestLatency(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_limit.TestLatency.test_something
    """

    def setUp(self):
        self.max_sample_size = 1_000
        self.min_sample_size = 1
        self.sampling_period = 15 * 60  # seconds
        self.breach_latency = 700  # milliSeconds
        self.breach_error_percent = 5  # percent
        self.max_rate_limit = 20  # requests/second
        self.noteworthy_exceptions = [BlockingIOError, TimeoutError]
        self.control_celery_rate_limit_interval = 2 * 60  # 2minutes
        self.lt = limit.Limiter(
            max_sample_size=self.max_sample_size,
            min_sample_size=self.min_sample_size,
            sampling_period=self.sampling_period,
            breach_latency=self.breach_latency,
            breach_error_percent=self.breach_error_percent,
            max_rate_limit=self.max_rate_limit,
            noteworthy_exceptions=self.noteworthy_exceptions,
            control_celery_rate_limit_interval=self.control_celery_rate_limit_interval,
        )

    def test_initial_conditions(self):
        self.assertEqual(self.lt._get_p99_latency(), 0)

    def test_new_latencies_count(self):
        for i in range(10):
            self.lt._update(latency=i, error=None)
        self.assertEqual(self.lt._get_p99_latency(), 8.91)

    def test_old_latencies_do_NOT_count(self):
        """
        latencies for requests that are older than `sampling_period` seconds do not count.
        """
        now = time.monotonic()
        old = now - (self.sampling_period * 2)
        for i in range(10):
            lat = limit._Latency(duration=i)
            lat.at = old
            self.lt.latency_queue.append(lat)

        self.assertEqual(self.lt._get_p99_latency(), 0.0)
        self.assertEqual(self.lt._get_p99_latency(), 0.0)

    def test_Latency_operator_overloading(self):
        # test __gt__ overloading
        self.assertTrue(limit._Latency(45) > limit._Latency(12))
        self.assertTrue(limit._Latency(45) > 4)
        self.assertTrue(limit._Latency(45) > 0.9)

        # test __ge__ overloading
        self.assertTrue(limit._Latency(45) >= limit._Latency(12))
        self.assertTrue(limit._Latency(45) >= 4)
        self.assertTrue(limit._Latency(45) >= 0.9)
        self.assertTrue(limit._Latency(45) >= 45)

        # test __lt__ overloading
        self.assertTrue(limit._Latency(12) < limit._Latency(19))
        self.assertTrue(limit._Latency(12) < 40)
        self.assertTrue(limit._Latency(12) < 23.90)

        # test __mul__ overloading
        self.assertEqual(limit._Latency(12) * limit._Latency(19), 228)
        self.assertEqual(limit._Latency(12) * 3, 36)
        self.assertEqual(limit._Latency(12) * 1.23, 14.76)


class TestError(TestCase):
    """
    run tests as:
        python -m unittest discover -v -s .
    run one testcase as:
        python -m unittest -v tests.test_limit.TestError.test_something
    """

    def setUp(self):
        self.max_sample_size = 1_000
        self.min_sample_size = 1
        self.sampling_period = 15 * 60  # seconds
        self.breach_latency = 700  # milliSeconds
        self.breach_error_percent = 5  # percent
        self.max_rate_limit = 20  # requests/second
        self.noteworthy_exceptions = [BlockingIOError, TimeoutError]
        self.control_celery_rate_limit_interval = 2 * 60
        self.lt = limit.Limiter(
            max_sample_size=self.max_sample_size,
            min_sample_size=self.min_sample_size,
            sampling_period=self.sampling_period,
            breach_latency=self.breach_latency,
            breach_error_percent=self.breach_error_percent,
            max_rate_limit=self.max_rate_limit,
            noteworthy_exceptions=self.noteworthy_exceptions,
            control_celery_rate_limit_interval=self.control_celery_rate_limit_interval,
        )

    def test_initial_conditions(self):
        self.assertEqual(self.lt._get_error_rate(), 0)

    def test_new_errors_count(self):
        for i in range(10):
            self.lt._update(latency=i, error=self.noteworthy_exceptions[1]("something bad!!"))
        self.assertEqual(self.lt._get_error_rate(), 100.0)

    def test_old_errors_do_NOT_count(self):
        """
        erors for requests that are older than `sampling_period` seconds do not count.
        """
        now = time.monotonic()
        old = now - (self.sampling_period * 2)
        for i in range(10):
            e = limit._Error(e=self.noteworthy_exceptions[1]("something bad {0}!!".format(i)))
            e.at = old
            self.lt.errors_queue.append(e)

        self.assertEqual(self.lt._get_error_rate(), 0.0)
        self.assertEqual(self.lt._get_error_rate(), 0.0)
