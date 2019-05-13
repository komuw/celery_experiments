### usage:

#### Branch: [broker-change](https://github.com/komuw/celery_experiments/tree/broker-change)   
Show how to change celery broker at runtime.    
This can even enable you to run with more than one broker concurrently.


1. run `docker-compose up`, this will start two rabbitmq brokers.    

2. queue tasks by running 
```sh
python cel.py
```

3. run one brokers workers 
```sh
celery worker -A cel:celery_v3_obj --concurrency=1 --loglevel=DEBUG
```

4. in another terminal run the other brokers workers   
```sh
celery worker -A cel:celery_v4_obj --concurrency=1 --loglevel=DEBUG
````
