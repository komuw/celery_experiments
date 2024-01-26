### usage:


#### 1. Branch: [broker-change](https://github.com/komuw/celery_experiments/tree/broker-change)   
Show how to change celery broker at runtime.    
This can even enable you to run with more than one broker concurrently.
usage:  
```bash
1. docker-compose up
2. python cel.py # queue tasks
3. celery worker -A cel:celery_v3_obj --concurrency=1 --loglevel=DEBUG # run workers
4. celery worker -A cel:celery_v4_obj --concurrency=1 --loglevel=DEBUG
```         


#### 2. Branch: [asyncio-ratelimit](https://github.com/komuw/celery_experiments/tree/asyncio-ratelimit)  
Ascertain that celery is still able to maintain the set ratelimits even in asyncio mode.
usage:  
```bash
1. docker-compose up
2. python cel.py # queue tasks
3. celery worker -A cel:celery_obj --concurrency=20 --pool=gevent --loglevel=DEBUG # run workers
```        


#### 3. Branch: [adaptive-rate-limits](https://github.com/komuw/celery_experiments/tree/adaptive-rate-limits)  
Patch celery to use a custom adaptive rate limiter.  
usage:  
```bash
1. docker-compose up
2. python cel.py # queue tasks
3. celery worker -A cel:celery_obj --concurrency=20 --pool=gevent --loglevel=DEBUG # run workers
```      


#### 4. Branch: [issues/4426](https://github.com/komuw/celery_experiments/tree/issues/4426)  
Patch celery to prevent [issues/4426](https://github.com/celery/celery/issues/4426), `Task is executed twice when the worker restarts/deployments`  
usage:  
```bash
1. docker-compose up
2. python cel.py # queue tasks
3. celery worker -A cel:celery_obj --concurrency=2 --pool=gevent --loglevel=DEBUG # run workers
```       



#### 5. Branch: [custom-rate-limiter](https://github.com/komuw/celery_experiments/tree/custom-rate-limiter)   
Create a custom rate limiter that is insipired by TCP congestion control algorithms.
```bash
1. docker-compose up
2. python cel.py # queue tasks
3. celery worker -A cel:celery_obj --concurrency=200 --pool=gevent --loglevel=INFO # run workers
```

#### 6. Also look into Additive-increase/Multiplicative-decrease(AIMD). 
[https://www.youtube.com/watch?v=PiVFygc7B50 ](https://youtu.be/PiVFygc7B50?t=838)
```go
// There's a problem with the following algos; they have no co-ordination between process. So you can't know the true global capacity.
// But that is actually a feature(not a bug).

func aimd() {
	var limit = 1.0

	err := doSomething()
	if err == nil {
		limit = limit + 1
	} else {
		limit = limit * 0.9
	}
}

func aimdSlowStart() {
	var limit = 1.0
	var slowStart = 100.0

	err := doSomething()
	if err == nil {
		if limit >= slowStart {
			limit = limit + (1 / limit)
		} else {
			limit = limit + 1
		}
	} else {
		limit = limit * 0.9
	}
}

func aimdSlowerStart() {
	var limit = 1.0
	var slowStart = 99_999_999.0

	err := doSomething()
	if err == nil {
		if limit >= slowStart {
			limit = limit + (1 / limit)
		} else {
			limit = limit + 1
		}
	} else {
		prev, next := limit, limit*0.9
		slowStart = (prev + next) / 2
		limit = next
	}
}

func aimdSlowerStartWithScaleBack() {
	var limit = 1.0
	var used = 1.0
	var slowStart = 99_999_999.0

	used++
	err := doSomething()
	used--

	if err == nil {
		if limit >= slowStart {
			limit = limit + (1 / limit)
		} else if limit < used {
			limit = used
		} else {
			limit = limit + 1
		}
	} else {
		prev, next := limit, limit*0.9
		slowStart = (prev + next) / 2
		limit = next
	}
}
```
