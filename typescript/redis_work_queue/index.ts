import Redis, { Pipeline } from 'ioredis';
const KeyPrefix = require("./KeyPrefix");
const { Item } = require("./Item");
const { v4 : uuidv4 } = require('uuid');

export class WorkQueue {
  /**
   * A work queue backed by a redis database
   */
  private session: string;
  private mainQueueKey: string;
  private processingKey: string;
  private cleaningKey: string;
  private leaseKey: string;
  private itemDataKey: string;

  constructor(name: typeof KeyPrefix) {
    this.mainQueueKey = name.of(':queue');
    this.processingKey = name.of(':processing');
    this.cleaningKey = name.of(':cleaning');
    this.session = uuidv4();
    this.leaseKey = KeyPrefix.concat(':leased_by_session:');
    this.itemDataKey = KeyPrefix.concat(':item:');
  }

  addItemToPipeline(pipeline: Pipeline, item: typeof Item): void {
    /**
     * Add an item to the work queue. This adds the redis commands onto the pipeline passed.
     * Use `WorkQueue.addItem` if you don't want to pass a pipeline directly.
     * Add the item data
     * NOTE: it's important that the data is added first, otherwise someone before the data is
     * ready
     */
    const itemId = item.Id();
    pipeline.set(this.itemDataKey + itemId, item.Data());
    // Then add the id to the work queue
    pipeline.lpush(this.mainQueueKey, itemId);
  }

  addItem(db: Redis, item: typeof Item): void {
    // Add an item to the work queue.
    // This creates a pipeline and executes it on the database.
    const pipeline = db.pipeline() as unknown as Pipeline;
    this.addItemToPipeline(pipeline, item);
    pipeline.exec();
  }

  queueLen(db: Redis): Promise<number> {
    // Return the length of the work queue (not including items being processed, see `WorkQueue.processing()`).
    return db.llen(this.mainQueueKey);
  }

  processing(db: Redis) {
    // Return the number of items being processed.
    return db.llen(this.processingKey);
  }

  _lease_exists(db: Redis, itemId: string): Promise<boolean> {
    return db.exists(KeyPrefix(this.leaseKey).of(itemId)).then(exists => exists !== 0);
  }
  

  async lease(db: Redis, leaseSecs: number, block = true, timeout = 0): Promise<typeof Item> {
    /**
     * Request a work lease the work queue. This should be called by a worker to get work to
     * complete. When completed, the `complete` method should be called.
     *
     * If `block` is true, the function will return either when a job is leased or after `timeout`
     * if `timeout` isn't 0.
     *
     * If the job is not completed before the end of `lease_duration`, another worker may pick up
     * the same job. It is not a problem if a job is marked as `done` more than once.
     *
     * If you've not already done it, it's worth reading [the documentation on leasing
     * items](https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item).
     */
    let maybeItemId: Buffer | string | null;
    let itemId: string;
    // First, to get an item, we try to move an item from the main queue to the processing list.
    if (block) {
      maybeItemId = await db.brpoplpush(this.mainQueueKey, this.processingKey, timeout);
    } else {
      maybeItemId = await db.rpoplpush(this.mainQueueKey, this.processingKey);
    }

    if (maybeItemId == null) {
      return undefined;
    }

    // Make sure the item id is a string
    if (Buffer.isBuffer(maybeItemId)) {
      itemId = maybeItemId.toString('utf-8');
    } else if (typeof maybeItemId === 'string') {
      itemId = maybeItemId;
    } else {
      throw new Error("item id from work queue not bytes or string");
    }

    // If we got an item, fetch the associated data.
    let data: Buffer | string | null = await db.get(KeyPrefix(this.itemDataKey).of(itemId));

    if (data === null) {
      data = Buffer.from([]);
    }

    /**
     * Setup the lease item.
     * NOTE: Racing for a lease is ok.
     */
    await db.setex(KeyPrefix(this.leaseKey).of(itemId), timeout, this.session);

    return new Item(data, itemId);
  }

  async lightClean(db: Redis) {
    const processing: Array<Buffer | string> = await db.lrange(this.processingKey, 0, -1);

    for (let itemId of processing) {
      if (Buffer.isBuffer(itemId)) {
        itemId = itemId.toString('utf-8');
      }

      // If the lease key is not present for an item (it expired or was never created because
      // the client crashed before creating it), then move the item back to the main queue so
      // others can work on it.
      if (!this._lease_exists(db, itemId)) {
        console.log(`${itemId} has no lease`);
        // While working on an item, we store it in the cleaning list. If we ever crash, we
        // come back and check these items.
        await db.lpush(this.cleaningKey, itemId);
        let removed = Number(db.lrem(this.processingKey, 0, itemId));

        if (removed > 0) {
          await db.lpush(this.processingKey, 0, itemId);
          console.log(`${itemId} was still in the processing queue, it was reset`);
        } else {
          console.log(`${itemId} was no longer in the processing queue`);
        }

        await db.lrem(this.cleaningKey, 0, itemId);
      }
    }

    // Now we check the
    const forgot: Array<Buffer | string> = await db.lrange(this.cleaningKey, 0, -1);

    for (let itemId of forgot) {
      if (Buffer.isBuffer(itemId)) {
        itemId = itemId.toString('utf-8');
      }
      console.log(`${itemId} was forgotten in clean`);
      const leaseExists: boolean = await this._lease_exists(db, itemId);
      const isItemInMainQueue: boolean | null = await db.lpos(this.mainQueueKey, itemId)!== 0;
      const isItemInProcessing: boolean | null = await db.lpos(this.processingKey, itemId)!== 0;
      if (!leaseExists && isItemInMainQueue === null && isItemInProcessing === null) {
        /**
         * FIXME: this introduces a race
         * maybe not anymore
         * no, it still does, what if the job has been completed?
         */
        await db.lpush(this.mainQueueKey, itemId);
        console.log(`${itemId} was still not in any queue, it was returned to the main queue`);
      } else if (!leaseExists) {
        console.log(`${itemId} is not leased, still in main queue or still in processing queue`);
      }
      await db.lrem(this.cleaningKey, 0, itemId);
    }
  }

  async clean(db: Redis) {
    const processing: Array<Buffer | string> = await db.lrange(this.processingKey, 0, -1);

    for (let itemId of processing) {
      if (Buffer.isBuffer(itemId)) {
        itemId = itemId.toString('utf-8');
      }

      // If the lease key is not present for an item (it expired or was never created because
      // the client crashed before creating it), then move the item back to the main queue so
      // others can work on it.
      if (!this._lease_exists(db, itemId)) {
        console.log(`${itemId} has no lease`);
        // While working on an item, we store it in the cleaning list. If we ever crash, we
        // come back and check these items.
        await db.lpush(this.cleaningKey, itemId);
        let removed = Number(db.lrem(this.processingKey, 0, itemId));

        if (removed > 0) {
          await db.lpush(this.processingKey, 0, itemId);
          console.log(`${itemId} was still in the processing queue, it was reset`);
        } else {
          console.log(`${itemId} was no longer in the processing queue`);
        }

        await db.lrem(this.cleaningKey, 0, itemId);
      }
    }

    // Now we check the
    const forgot: Array<Buffer | string> = await db.lrange(this.cleaningKey, 0, -1);

    for (let itemId of forgot) {
      if (Buffer.isBuffer(itemId)) {
        itemId = itemId.toString('utf-8');
      }
      console.log(`${itemId} was forgotten in clean`);
      const leaseExists: boolean = await this._lease_exists(db, itemId);
      const isItemInMainQueue: boolean | null = await db.lpos(this.mainQueueKey, itemId)!== 0;
      const isItemInProcessing: boolean | null = await db.lpos(this.processingKey, itemId)!== 0;
      if (!leaseExists && isItemInMainQueue === null && isItemInProcessing === null) {
        /**
         * FIXME: this introduces a race
         * maybe not anymore
         * no, it still does, what if the job has been completed?
         */
        await db.lpush(this.mainQueueKey, itemId);
        console.log(`${itemId} was still not in any queue, it was returned to the main queue`);
      } else if (!leaseExists) {
        console.log(`${itemId} is not leased, still in main queue or still in processing queue`);
      }
      await db.lrem(this.cleaningKey, 0, itemId);
    }
  }



  async complete(db:Redis,item: typeof Item): Promise<boolean> {  
    /**
     * Marks a job as completed and remove it from the work queue. After `complete` has been
     * called (and returns `true`), no workers will receive this job again.

     * `complete` returns a boolean indicating if *the job has been removed* **and** *this worker
     * was the first worker to call `complete`*. So, while lease might give the same job to
     * multiple workers, complete will return `true` for only one worker.
     */

    const removed = await db.lrem(this.processingKey, 0, item.id());
  
    if (removed === 0) {
      return false;
    }
    const pipeline = db.pipeline();
    pipeline.del(KeyPrefix(this.itemDataKey).of(item.id()));
    pipeline.del(KeyPrefix(this.leaseKey).of(item.id()));
    await pipeline.exec();

    return true;
  }
  
}

