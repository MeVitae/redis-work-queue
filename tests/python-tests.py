import sys
import json
from time import sleep

import redis

sys.path.append('../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue

if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")
    
host = sys.argv[1].split(":")

if len(host) == 2:
    db = redis.Redis(host=host[0], port=host[1])
else:
    db = redis.Redis(host=host[0])

python_results_key = KeyPrefix("results:python:")
shared_results_key = KeyPrefix("results:shared:")

python_queue = WorkQueue(KeyPrefix("python_jobs"))
shared_queue = WorkQueue(KeyPrefix("shared_jobs"))

python_job_counter = 0
shared_job_counter = 0

shared = False
while True:
    shared = not shared
    if shared:
        shared_job_counter += 1

        # First, try to get a job from the shared job queue
        block = shared_job_counter%5 == 0
        print("Leasing shared with block = {}".format(block))
        job = shared_queue.lease(db, 2, timeout=1, block=block)
        # If there was no job, continue.
        # Also, if we get 'unlucky', crash while completing the job.
        if job is None or shared_job_counter % 7 == 0:
            print("Dropping job: {}".format(job))
            continue

        # Parse the data
        data = job.data_json()
        # Generate the response
        result = {
            'a': data['a'],
            'sum': data['a'] + data['b'],
            'prod': data['a'] * data['b'],
            'worker': 'python',
        }
        result_json = json.dumps(result)
        print("Result:", result_json)
        # Pretend it takes us a while to compute the result
        # Sometimes this will take too long and we'll timeout
        if shared_job_counter % 12 == 0:
            sleep(shared_job_counter % 4)

        # Store the result
        db.set(shared_results_key.of(job.id()), result_json)

        # Complete the job unless we're 'unlucky' and crash again
        if shared_job_counter % 29 != 0:
            print("Completing")
            shared_queue.complete(db, job)
        else:
            print("Dropping")
    else:
        python_job_counter += 1

        # First, try to get a job from the python job queue
        block = shared_job_counter%6 == 0
        print("Leasing python with block = {}".format(block))
        job = python_queue.lease(db, 1, timeout=2, block=block)
        # If there was no job, continue.
        # Also, if we get 'unlucky', crash while completing the job.
        if job is None or python_job_counter % 7 == 0:
            print("Dropping job: {}".format(job))
            continue

        # Check the data is a sinle byte
        data = job.data()
        assert len(data) == 1
        # Generate the response
        result = (data[0]*3) % 256
        print("Result:", result)
        # Pretend it takes us a while to compute the result
        # Sometimes this will take too long and we'll timeout
        if python_job_counter % 25 == 0:
            sleep(python_job_counter % 20)

        # Store the result
        db.set(python_results_key.of(job.id()), bytes([result]))

        # Complete the job unless we're 'unlucky' and crash again
        if python_job_counter % 29 != 0:
            print("Completing")
            # If we succesfully completed the result, create two new shared jobs.
            if python_queue.complete(db, job):
                print("Spawning shared jobs")
                shared_queue.add_item(db, Item.from_json_data({
                    'a': 13,
                    'b': result,
                }))
                shared_queue.add_item(db, Item.from_json_data({
                    'a': 17,
                    'b': result,
                }))
        else:
            print("Dropping")