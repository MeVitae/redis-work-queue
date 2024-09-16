import uuid
from redis import Redis
from redis.client import Pipeline

from redis_work_queue import Item
from redis_work_queue import KeyPrefix


class WorkQueue(object):
    """A work queue backed by a redis database"""

    def __init__(self, name: KeyPrefix):
        self._session = uuid.uuid4().hex
        self._main_queue_key = name.of(':queue')
        self._processing_key = name.of(':processing')
        self._lease_key = KeyPrefix.concat(name, ':lease:')
        self._item_data_key = KeyPrefix.concat(name, ':item:')

    def add_item(self, db: Redis, item: Item) -> bool:
        """Add an item to the work queue.

        If an item with the same ID already exists, this item is not added, and False is returned. Otherwise, if the item is added True is returned.

        If you know the item ID is unique, and not already in the queue, use the optimised `add_unique_item` method instead.
        """
        if db.setnx(self._item_data_key.of(item.id()), item.data()) != 0:
            db.lpush(self._main_queue_key, item.id())
            return True
        return False

    def add_unique_item_to_pipeline(self, pipeline: Pipeline, item: Item) -> None:
        """Add an item, which is known to have an ID not already in the queue, to the work queue. This adds the redis commands onto the pipeline passed.

        Use `WorkQueue.add_unique_item` if you don't want to pass a pipeline directly.
        """
        # Add the item data
        # NOTE: it's important that the data is added first, otherwise someone before the data is
        # ready
        pipeline.set(self._item_data_key.of(item.id()), item.data())
        # Then add the id to the work queue
        pipeline.lpush(self._main_queue_key, item.id())

    def add_unique_item(self, db: Redis, item: Item) -> None:
        """Add an item, which is known to have an ID not already in the queue, to the work queue.

        This creates a pipeline and executes it on the database.
        """
        pipeline = db.pipeline()
        self.add_unique_item_to_pipeline(pipeline, item)
        pipeline.execute()

    def queue_len(self, db: Redis) -> int:
        """Return the length of the work queue (not including items being processed, see
        `WorkQueue.processing`)."""
        return db.llen(self._main_queue_key)

    def processing(self, db: Redis) -> int:
        """Return the number of items being processed."""
        return db.llen(self._processing_key)

    def lease(self, db: Redis, lease_secs: int, block=True, timeout=0) -> Item | None:
        """Request a work lease the work queue. This should be called by a worker to get work to
        complete. When completed, the `complete` method should be called.

        If `block` is true, the function will return either when a job is leased or after `timeout`
        if `timeout` isn't 0. (If `timeout` isn't 0, this may return earlier, with `None` in some
        race cases).

        If the job is not completed before the end of `lease_duration`, another worker may pick up
        the same job. It is not a problem if a job is marked as `done` more than once.

        If you've not already done it, it's worth reading [the documentation on leasing
        items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).
        """
        while True:
            # First, to get an item, we try to move an item from the main queue to the processing list.
            if block:
                maybe_item_id: bytes | str | None = db.brpoplpush(
                    self._main_queue_key,
                    self._processing_key,
                    timeout=timeout,
                )
            else:
                maybe_item_id: bytes | str | None = db.rpoplpush(
                    self._main_queue_key,
                    self._processing_key,
                )

            if maybe_item_id is None:
                return None

            # Make sure the item id is a string
            if isinstance(maybe_item_id, bytes):
                item_id: str = maybe_item_id.decode('utf-8')
            elif isinstance(maybe_item_id, str):
                item_id: str = maybe_item_id
            else:
                raise Exception("item id from work queue not bytes or string")

            # If we got an item, fetch the associated data.
            data: bytes | None = db.get(self._item_data_key.of(item_id))
            if data is None:
                if block and timeout == 0:
                    continue
                return None

            # Setup the lease item.
            # NOTE: Racing for a lease is ok.
            db.setex(self._lease_key.of(item_id), lease_secs, self._session)
            return Item(data, id=item_id)

    def complete(self, db: Redis, item: Item) -> bool:
        """Marks a job as completed and remove it from the work queue. After `complete` has been
        called (and returns `true`), no workers will receive this job again.

        `complete` returns a boolean indicating if *the job has been removed* **and** *this worker
        was the first worker to call `complete`*. So, while lease might give the same job to
        multiple workers, complete will return `true` for only one worker."""
        item_id = item.id()
        item_del_result, _, _ = db.pipeline() \
            .delete(self._item_data_key.of(item_id)) \
            .lrem(self._processing_key, 0, item.id()) \
            .delete(self._lease_key.of(item_id)) \
            .execute()
        return item_del_result is not None and item_del_result != 0

    def _lease_exists(self, db: Redis, item_id: str) -> bool:
        """True iff a lease on 'item_id' exists."""
        return db.exists(self._lease_key.of(item_id)) != 0

    def light_clean(self, db: Redis) -> None:
        # A light clean only checks items in the processing queue
        processing: list[bytes | str] = db.lrange(
            self._processing_key, 0, -1,
        )
        for item_id in processing:
            item_id = _value_str(item_id)
		    # If there's no lease for the item, then it should be reset.
            if not self._lease_exists(db, item_id):
                # We also check the item actually exists before pushing it back to the main queue
                if db.exists(self._item_data_key.of(item_id)):
                    print(item_id, 'has no lease, it will be reset')
                    db.pipeline() \
                        .lrem(self._processing_key, 0, item_id) \
                        .lpush(self._main_queue_key, item_id) \
                        .execute()
                else:
                    print(item_id, 'was in the processing queue but does not exist')
                    db.lrem(self._processing_key, 0, item_id)

    def deep_clean(self, db: Redis) -> None:
        # A deep clean checks all data keys
        res: tuple[list[bytes | str], list[bytes | str]] = db.pipeline() \
                .keys(self._item_data_key.of('*')) \
                .lrange(self._main_queue_key, 0, -1) \
                .execute()
        item_data_keys, main_queue = res
        main_queue = list(map(_value_str, main_queue))
        for item_data_key in item_data_keys:
            item_id = _value_str(item_data_key)[len(self._item_data_key.prefix):]
            # If the item isn't in the queue, and there's no lease for the item, then it should be
            # reset.
            if item_id not in main_queue and not self._lease_exists(db, item_id):
                print(item_id, 'has no lease, it will be reset')
                db.pipeline() \
                    .lrem(self._processing_key, 0, item_id) \
                    .lpush(self._main_queue_key, item_id) \
                    .execute()


def _value_str(value: bytes | str) -> str:
    if isinstance(value, bytes):
        return value.decode('utf-8')
    assert isinstance(value, str)
    return value


__version__ = "0.1.2"
