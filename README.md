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


#### 2. Branch: [adaptive-rate-limits](https://github.com/komuw/celery_experiments/tree/adaptive-rate-limits)  
Patch celery to use a custom adaptive rate limiter.  
usage:  
```bash
1. docker-compose up
2. python cel.py # queue tasks
3. celery worker -A cel:celery_obj --concurrency=20 --pool=gevent --loglevel=DEBUG # run workers
```      

