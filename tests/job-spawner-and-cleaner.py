import sys
import json
from time import sleep

import redis

sys.path.append('../python')
from redis_work_queue import KeyPrefix, Item, WorkQueue


if len(sys.argv) < 2:
    raise Exception("first command line argument must be redis host")
host = sys.argv[1].split(":")
queue_list_names = sys.argv[2].split(" ")

db = redis.Redis(host=host[0], port=int(host[1]) if len(host) > 1 else 6379)
if len(db.keys("*")) > 0:
    raise Exception("redis database isn't clean")

shared_queue = WorkQueue(KeyPrefix("shared_jobs"))
queue_list = list(map(
    lambda name: WorkQueue(KeyPrefix(name)),
    queue_list_names,
))

counter = 0
doom_counter = 0
revived = False

while doom_counter < 20:
    if counter < 256:
        # Spawn 256 initial jobs in each queue
        for queue in queue_list:
            queue.add_item(db, Item(bytes([counter])))
    elif counter % 2 == 0:
        # Every other tick just log how much work is left
        for queue in queue_list:
            print(queue._main_queue_key)
            print((queue.queue_len(db), queue.processing(db)))

        print(shared_queue._main_queue_key)
        print((shared_queue.queue_len(db), shared_queue.processing(db)))
        sleep(0.5)
    elif counter == 501:
        # After a little bit, add more jobs.
        print("More jobs!!")
        for n in range(0, 256):
            n = n % 256
            for queue in queue_list:
                queue.add_item(db, Item(bytes([n])))
    elif doom_counter > 10 and not revived:
        # After everything settles down, add more jobs
        print("Even more jobs!!")
        revived = True
        for n in range(0, 256):
            for queue in queue_list:
                queue.add_item(db, Item(bytes([n])))
    else:
        # Otherwise, clean!
        print("Cleaning")
        for queue in queue_list:
            queue.light_clean(db)
        shared_queue.light_clean(db)
    # The `doom_counter` counts the number of consecutive times all the lengths are 0.
    doom_counter = doom_counter + 1 if all(map(
        lambda queue: queue.queue_len(db) == 0 and queue.processing(db) == 0,
        queue_list + [shared_queue],
    )) else 0
    counter += 1

# These are the results are still expecting, when a result is found, it's removed from these lists.
expecting_dict_config = {
    "python_jobs": {
        "expecting share": [13, 17],
        "expected": [(n * 3) % 256 for n in range(0, 256*3)],
        "result_name": "results:python:"
    },
    "rust_jobs": {
        "expecting share": [3, 5],
        "expected": [(n * 7) % 256 for n in range(0, 256*3)],
        "result_name": "results:rust:"
    },
    "go_jobs": {
        "expecting share": [7, 11],
        "expected": [(n * 5) % 256 for n in range(0, 256*3)],
        "result_name": "results:go:"
    },
    "node_jobs": {
        "expecting share": [17, 21],
        "expected": [(n * 17) % 256 for n in range(0, 256*3)],
        "result_name": "results:node:"
    },
    "dotnet_jobs": {
        "expecting share": [19, 23],
        "expected": [(n * 11) % 256 for n in range(0, 256*3)],
        "result_name": "results:dotnet:"
    }
}


for queue_name in queue_list_names[:]:
    if queue_name not in expecting_dict_config:
        queue_list_names.remove(queue_name)


keys_to_delete = []

for queue_name in expecting_dict_config:
    if queue_name not in queue_list_names:
        keys_to_delete.append(queue_name)

for queue_name in keys_to_delete:
    del expecting_dict_config[queue_name]

expecting_shared = []
for expecting_config in expecting_dict_config.values():
    expecting_shared += [
        (a + b, a * b) for a in expecting_config["expecting share"]
                       for b in expecting_config["expected"]
    ]


shared_counts = {}

for key in db.keys("*"):
    key = key.decode('utf-8')
    found_first = False
    for result in expecting_dict_config.values():
        name = result["result_name"]
        if key.find(name) == 0:
            results = db.get(key)
            assert results is not None
            assert len(results) == 1
            result["expected"].remove(results[0])
            found_first = True
            break
    if not found_first:
        if key.find('results:shared:') == 0:
            result = db.get(key)
            assert result is not None
            if isinstance(result, bytes):
                result = result.decode('utf-8')
            result = json.loads(result)
            worker = result['worker']
            if worker in shared_counts:
                shared_counts[worker] += 1
            else:
                shared_counts[worker] = 1
            expecting_shared.remove((result['sum'], result['prod']))
            pass
        else:
            raise Exception('found unexpected key: ' + key)

updated_names = []
for name in queue_list_names:
    updated_names.append(name.replace("_jobs", ""))

total_count_keys = 0
for key in shared_counts.keys():
    total_count_keys += shared_counts[key]

maximum_allowed = total_count_keys/len(shared_counts)*1.2

print("Maximum number of job counts:", maximum_allowed)

for key in shared_counts.keys():
    assert key in updated_names
    # Check that it's fairly well balanced
    print(key, "Job counts:", shared_counts[key])
    assert shared_counts[key] < maximum_allowed
