import sys
import json
from time import sleep

import redis

sys.path.append('../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue

if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")
db = redis.Redis(host=sys.argv[1])

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
        job = shared_queue.lease(db, 2, timeout=4)
        # If there was no job, continue.
        # Also, if we get 'unlucky', crash while completing the job.
        if job is None or shared_job_counter % 7 == 0:
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
        # Pretend it takes us a while to compute the result
        # Sometimes this will take too long and we'll timeout
        sleep(shared_job_counter % 7)

        # Store the result
        db.set(shared_results_key.of(job.id()), result_json)

        # Complete the job unless we're 'unlucky' and crash again
        if shared_job_counter % 29 != 0:
            shared_queue.complete(db, job)
    else:
        python_job_counter += 1

        # First, try to get a job from the python job queue
        job = python_queue.lease(db, 1, timeout=2)
        # If there was no job, continue.
        # Also, if we get 'unlucky', crash while completing the job.
        if job is None or python_job_counter % 7 == 0:
            continue

        # Check the data is a sinle byte
        data = job.data()
        assert len(data) == 1
        # Generate the response
        result = (data[0]*3) % 256
        # Pretend it takes us a while to compute the result
        # Sometimes this will take too long and we'll timeout
        if python_job_counter % 11 == 0:
            sleep(python_job_counter % 20)

        # Store the result
        db.set(python_results_key.of(job.id()), bytes([result]))

        # Complete the job unless we're 'unlucky' and crash again
        if python_job_counter % 29 != 0:
            # If we succesfully completed the result, create two new shared jobs.
            if python_queue.complete(db, job):
                shared_queue.add_item(db, Item.from_json_data({
                    'a': 13,
                    'b': result,
                }))
                shared_queue.add_item(db, Item.from_json_data({
                    'a': 17,
                    'b': result,
                }))
