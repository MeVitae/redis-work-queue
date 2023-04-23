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
        self._cleaning_key = name.of(':cleaning')
        self._lease_key = KeyPrefix.concat(name, ':leased_by_session:')
        self._item_data_key = KeyPrefix.concat(name, ':item:')

    def add_item_to_pipeline(self, pipeline: Pipeline, item: Item) -> None:
        """Add an item to the work queue. This adds the redis commands onto the pipeline passed.

        Use `WorkQueue.add_item` if you don't want to pass a pipeline directly.
        """
        # Add the item data
        # NOTE: it's important that the data is added first, otherwise someone before the data is
        # ready
        pipeline.set(self._item_data_key.of(item.id()), item.data())
        # Then add the id to the work queue
        pipeline.lpush(self._main_queue_key, item.id())

    def add_item(self, db: Redis, item: Item) -> None:
        """Add an item to the work queue.

        This creates a pipeline and executes it on the database.
        """
        pipeline = db.pipeline()
        self.add_item_to_pipeline(pipeline, item)
        pipeline.execute()

    def queue_len(self, db: Redis) -> int:
        """Return the length of the work queue (not including items being processed, see
        `WorkQueue.processing`)."""
        return db.llen(self._main_queue_key)

    def processing(self, db: Redis) -> int:
        """Return the number of items being processed."""
        return db.llen(self._processing_key)

    def light_clean(self, db: Redis) -> None:
        processing: list[bytes | str] = db.lrange(
            self._processing_key, 0, -1)
        for item_id in processing:
            if isinstance(item_id, bytes):
                item_id = item_id.decode('utf-8')
            # If the lease key is not present for an item (it expired or was never created because
            # the client crashed before creating it) then move the item back to the main queue so
            # others can work on it.
            if not self._lease_exists(db, item_id):
                print(item_id, 'has no lease')
                # While working on an item, we store it in the cleaning list. If we ever crash, we
                # come back and check these items.
                db.lpush(self._cleaning_key, item_id)
                removed = int(db.lrem(self._processing_key, 0, item_id))
                if removed > 0:
                    db.lpush(self._main_queue_key, item_id)
                    print(item_id, 'was still in the processing queue, it was reset')
                else:
                    print(item_id, 'was no longer in the processing queue')
                db.lrem(self._cleaning_key, 0, item_id)

        # Now we check the
        forgot: list[bytes | str] = db.lrange(self._cleaning_key, 0, -1)
        for item_id in forgot:
            if isinstance(item_id, bytes):
                item_id = item_id.decode('utf-8')
            print(item_id, 'was forgotten in clean')
            if not self._lease_exists(db, item_id) and \
                    db.lpos(self._main_queue_key, item_id) is None and \
                    db.lpos(self._processing_key, item_id) is None:
                # FIXME: this introcudes a race
                # maybe not anymore
                # no, it still does, what if the job has been completed?
                db.lpush(self._main_queue_key, item_id)
                print(item_id, 'was not in any queue, it was reset')
            db.lrem(self._cleaning_key, 0, item_id)

    def _lease_exists(self, db: Redis, item_id: str) -> bool:
        """True iff a lease on 'item_id' exists."""
        return db.exists(self._lease_key.of(item_id)) != 0

    def lease(self, db: Redis, lease_secs: int, block=True, timeout=0) -> Item | None:
        """Request a work lease the work queue. This should be called by a worker to get work to
        complete. When completed, the `complete` method should be called.

        If `block` is true, the function will return either when a job is leased or after `timeout`
        if `timeout` isn't 0.

        If the job is not completed before the end of `lease_duration`, another worker may pick up
        the same job. It is not a problem if a job is marked as `done` more than once.

        If you've not already done it, it's worth reading [the documentation on leasing
        items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).
        """
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
            data = bytes()

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
        removed = int(db.lrem(self._processing_key, 0, item.id()))
        # Only complete the work if it was still in the processing queue
        if removed == 0:
            return False
        item_id = item.id()
        # TODO: The cleaner should also handle these... :(
        db.pipeline() \
            .delete(self._item_data_key.of(item_id)) \
            .delete(self._lease_key.of(item_id)) \
            .execute()
        return True


__version__ = "0.1.2"
