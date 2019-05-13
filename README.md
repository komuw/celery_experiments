### usage:


#### Branch: [asyncio-ratelimit](https://github.com/komuw/celery_experiments/tree/asyncio-ratelimit)  
Ascertain that celery is still able to maintain the set ratelimits even in asyncio mode.

1. run `docker-compose up`, this will start a rabbitmq broker.    

2. queue tasks by running 
```sh
python cel.py
```

3. run workers 
```sh
celery worker -A cel:celery_obj --concurrency=1 --loglevel=DEBUG --pool=gevent
```
