import sys
import json
from time import sleep
import random

import redis

sys.path.append('../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue


if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")

host = sys.argv[1].split(":")
db = redis.Redis(host=host[0], port=int(host[1]) if len(host) > 1 else 6379)

print("Starter running python duplicated items test.")

key_prefix = KeyPrefix('Duplicate-Items-Test')
work_queue = WorkQueue(key_prefix)
passed = 0
not_passed = 0
number_of_tests = 150
number_of_random_possible_items = 232
items = []

for i in range(1, number_of_tests + 1):
    item_data1 = str(
        random.randint(0, number_of_random_possible_items - 1)
    )
    item_data2 = str(
        random.randint(0, number_of_random_possible_items - 1)
    )
    item1 = Item(bytes(item_data1, 'utf-8'), item_data1)
    item2 = Item(bytes(item_data2, 'utf-8'), item_data2)
    items.append(item1)
    items.append(item2)

    result1 = work_queue.add_item_atomically(db, item1)
    result2 = work_queue.add_item_atomically(db, item2)

    if result1 is None:
        passed += 1
    else:
        not_passed += 1

    if result2 is None:
        passed += 1
    else:
        not_passed += 1

    if random.randint(0, 10) < 3:
        work_queue.lease(db, 1, False, 4)

    if random.randint(0, 10) < 5:
        to_remove = items.pop()
        work_queue.complete(db, to_remove)

assert (
    not_passed + passed == number_of_tests * 2
), f"The number of passed items ({passed}) with the number of not passed items ({not_passed}) should be equal to number of tests * 2: {number_of_tests * 2}"

main_queue_key = key_prefix.of(':queue')
processing_key = key_prefix.of(':processing')
main_items = db.lrange(main_queue_key, 0, -1)
processing_items = db.lrange(processing_key, 0, -1)
for item in main_items:
    if item in processing_items:
        print(item)
        raise ValueError("Found duplicated item from main queue inside processing queue")

for item in processing_items:
    if item in main_items:
        raise ValueError("Found duplicated item from processing queue inside main queue")


print("Python duplicated items test finished succesfully.")

python_results_key = KeyPrefix("results:python:")
shared_results_key = KeyPrefix("results:shared:")

python_queue = WorkQueue(KeyPrefix("python_jobs"))
shared_queue = WorkQueue(KeyPrefix("shared_jobs"))
python_job_counter = 0
shared_job_counter = 0
Duplicate_test_finish = False

shared = False
while True:
    shared = not shared
    if shared:
        shared_job_counter += 1

        # First, try to get a job from the shared job queue
        block = shared_job_counter % 5 == 0
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
        block = shared_job_counter % 6 == 0
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
