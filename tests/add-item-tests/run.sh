#!/bin/bash
set -e

echo --- Single Threaded Tests ---

echo ----------- Python ----------
echo ---------- add_item ---------
echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
# There won't be duplicate items from other workers, so this will fail.
! python3 ./python-tests.py localhost --add-item
# The previous worker didn't complete, so the queue shouldn't contain everything.
! python3 ./check.py localhost
kill $REDIS_PID
wait $REDIS_PID

echo ------ add_unique_item ------
echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
# No checks, so this must pass
python3 ./python-tests.py localhost --add-unique-item
# There will be duplicates, so check should fail
! python3 ./check.py localhost
kill $REDIS_PID
wait $REDIS_PID


echo ---- Duel Threaded Tests ----

echo ----------- Python ----------
echo ------ add_unique_item ------
echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
# No checks, so this should succeed.
python3 ./python-tests.py localhost --add-unique-item &
FIRST_THREAD_PID=$!
# No checks, so this should succeed.
python3 ./python-tests.py localhost --add-unique-item
wait $FIRST_THREAD_PID
# There will be duplicates, so check should fail
! python3 ./check.py localhost
kill $REDIS_PID
wait $REDIS_PID

echo ---------- add_item ---------
echo -e 'save ""\nappendonly no' | redis-server - &
REDIS_PID=$!
# There will be jobs from the other worker, so this should pass checks.
python3 ./python-tests.py localhost --add-item &
FIRST_THREAD_PID=$!
# There will be jobs from the other worker, so this should pass checks.
python3 ./python-tests.py localhost --add-item
wait $FIRST_THREAD_PID
# And there should be no duplicate items!
python3 ./check.py localhost
kill $REDIS_PID
wait $REDIS_PID
