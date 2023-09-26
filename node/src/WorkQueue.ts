/**
 * @module WorkQueue
 * @description A work queue backed by a redis database.
 */

import Redis, {ChainableCommander} from 'ioredis';
import {v4 as uuidv4} from 'uuid';
import {Item} from './Item';
import {KeyPrefix} from './KeyPrefix';

export {KeyPrefix, Item};

/**
 * A work queue backed by a redis database.
 */
export class WorkQueue {
  private session: string;
  private mainQueueKey: string;
  private processingKey: string;
  private cleaningKey: string;
  private leaseKey: KeyPrefix;
  private itemDataKey: KeyPrefix;

  /**
   * @param {KeyPrefix} name This is the prefix created for the WorkQueue.
   */
  constructor(name: KeyPrefix) {
    this.mainQueueKey = name.of(':queue');
    this.processingKey = name.of(':processing');
    this.cleaningKey = name.of(':cleaning');
    this.session = uuidv4();
    this.leaseKey = name.concat(':leased_by_session:');
    this.itemDataKey = name.concat(':item:');
  }

  /**
   * Add an item to the work queue. This adds the redis commands onto the pipeline passed.
   *
   * Use `WorkQueue.addItem` if you don't want to pass a pipeline directly.
   *
   * @param {Pipeline} pipeline The pipeline that the commands to add the item will be pushed to.
   * @param {Item} item The Item to be added.
   */
  addItemToPipeline(pipeline: ChainableCommander, item: Item) {
    // NOTE: it's important that the data is added first, otherwise someone before the data is ready.
    pipeline.set(this.itemDataKey.of(item.id), item.data);
    // Then add the id to the work queue
    pipeline.lpush(this.mainQueueKey, item.id);
  }

  /**
   * Add an item to the work queue. See `addNewItem` to avoid adding duplicate items.
   *
   * This creates a pipeline and executes it on the database.
   *
   * @param {Redis} db The Redis Connection.
   * @param item The item to be added.
   */
  async addItem(db: Redis, item: Item): Promise<void> {
    const pipeline = db.pipeline();
    this.addItemToPipeline(pipeline, item);
    await pipeline.exec();
  }

  /**
   * Adds an item to the work queue only if an item with the same ID doesn't already exist.
   *
   * This method uses WATCH to add the item atomically. The db client passed must not be used by
   * anything else while this method is running.
   *
   * Returns a boolean indicating if the item was added or not. The item is only not added if it
   * already exists or if an error (other than a transaction error, which triggers a retry) occurs.
   *
   * @param {Redis} db The Redis Connection, this must not be used by anything else while this method is running.
   * @param item The item that will be added, only if an item doesn't already exist with the same ID.
   * @returns {boolean} returns false if already in queue or true if the item is successfully added.
   */
  async addNewItem(db: Redis, item: Item): Promise<boolean> {
      for (;;) {
        try {
          await db.watch(this.mainQueueKey, this.processingKey);
  
          const pipeNew = db.pipeline();
          pipeNew.lpos(this.processingKey, item.id);
          pipeNew.lpos(this.mainQueueKey, item.id);
          const pipeResult = await pipeNew.exec();
          if (pipeResult && (pipeResult[0][1] !== null || pipeResult[1][1] !== null)) {
            // If the item already exists in either mainQueue or processingKey, don't add it.
            return false;
          }

          const pipeEx = db.multi();
          this.addItemToPipeline(pipeEx, item);
          let results = await pipeEx.exec()
          if (results && results[0][1] === "OK") {
            return true;
          }
        } finally {
          db.unwatch();
        }
      }
  }

  /**
   * Return the length of the work queue (not including items being processed, see
   * `WorkQueue.processing` or `WorkQueue.counts` to get both).
   *
   * @param {Redis} db The Redis Connection.
   * @returns {Promise<number>} Return the length of the work queue (not including items being processed, see `WorkQueue.processing()`).
   */
  queueLen(db: Redis): Promise<number> {
    return db.llen(this.mainQueueKey);
  }

  /**
   * This is used to get the number of items currently being processed.
   *
   * @param {Redis} db The Redis Connection.
   * @returns {Promise<number>} The number of items being processed.
   */
  processing(db: Redis): Promise<number> {
    return db.llen(this.processingKey);
  }

  /**
   * Returns the queue length, and number of items currently being processed, atomically.
   * 
   * @param {Redis} db The Redis Connection.
   * @returns {Promise<[number, number]>} Return the length of main queue and processing queue, respectively.
   */
  async counts(db: Redis): Promise<[number, number]> {
    const multi = db.multi();
    multi.llen(this.mainQueueKey);
    multi.llen(this.processingKey);
    const result = (await multi.exec()) as Array<[Error | null, number]>;
    const queueLength = result[0][1];
    const processingLength = result[1][1];
    return [queueLength, processingLength];
  }

  /**
   * This method can be used to check if a Lease Exists or not for a itemId.
   *
   * @param {Redis} db The Redis Connection.
   * @param {string} itemId The itemId of the item you want to check if it has a lease.
   * @returns {Promise<boolean>}
   */
  async leaseExists(db: Redis, itemId: string): Promise<boolean> {
    const exists = await db.exists(this.leaseKey.of(itemId));
    return exists !== 0;
  }

  /**
   * Request a work lease from the work queue. This should be called by a worker to get work to complete.
   * When completed, the `complete` method should be called.
   *
   * If `block` is true, the function will return either when a job is leased or after `timeout` seconds if `timeout` isn't 0.
   * If the job is not completed before the end of `leaseDuration`, another worker may pick up the same job.
   * It is not a problem if a job is marked as `done` more than once.
   *
   * If you haven't already, it's worth reading the documentation on leasing items:
   * https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item
   *
   * @param {Redis} db The Redis Connection.
   * @param {number} leaseSecs The number of seconds that the lease should hold.
   * @param {boolean} block Is a block or not, default is true.
   * @param {number} timeout The number of seconds the lease will time out at.
   * @returns {Promise<Item>} Returns a new lease Item.
   *
   * Process:
   * First, to get an item, we try to move an item from the main queue to the processing list.
   * Then we setup the lease item.
   */
  async lease(
    db: Redis,
    leaseSecs: number,
    block: boolean = true,
    timeout: number = 1
  ): Promise<Item | null> {
    let maybeItemId: string | null = null;

    // Try to move an item from the main queue to the processing list.
    if (block) {
      maybeItemId = await db.brpoplpush(
        this.mainQueueKey,
        this.processingKey,
        timeout
      );
    } else {
      maybeItemId = await db.rpoplpush(this.mainQueueKey, this.processingKey);
    }

    if (maybeItemId == null) {
      return null;
    }

    const itemId = maybeItemId;

    let data: Buffer | null = await db.getBuffer(this.itemDataKey.of(itemId));

    if (data == null) {
      data = Buffer.alloc(0);
    }

    // Setup the lease item.
    await db.setex(this.leaseKey.of(itemId), leaseSecs, this.session);
    return new Item(data, itemId);
  }

  /**
   * Moves items from the processing Queue to the Main Queue if the lease key is missing.
   * This can be used in case worker dies or crashes and item is hold onto the processing, this allows the item to be moved onto another worker.
   *
   * @param {Redis} db The Redis connection.
   *
   * Process Explenation:
   * If the lease key is not present for an item (it expired or was never created because the client crashed before creating it), then move the item back to the main queue so others can work on it.
   * While working on an item, we store it in the cleaning list. If we ever crash, we come back and check these items.
   */
  async lightClean(db: Redis) {
    const processing: Array<string> = await db.lrange(
      this.processingKey,
      0,
      -1
    );
    for (const itemId of processing) {
      if (!(await this.leaseExists(db, itemId))) {
        await db.lpush(this.cleaningKey, itemId);
        const removed = await db.lrem(this.processingKey, 0, itemId);
        if (removed > 0) {
          await db.lpush(this.mainQueueKey, 0, itemId);
        }
        await db.lrem(this.cleaningKey, 0, itemId);
      }
    }

    const forgot: Array<string> = await db.lrange(this.cleaningKey, 0, -1);
    for (const itemId of forgot) {
      const leaseExists: boolean = await this.leaseExists(db, itemId);
      if (
        !leaseExists &&
        (await db.lpos(this.mainQueueKey, itemId)) == null &&
        (await db.lpos(this.processingKey, itemId)) == null
      ) {
        /**
         * FIXME: this introduces a race
         * maybe not anymore
         * no, it still does, what if the job has been completed?
         */
        await db.lpush(this.mainQueueKey, itemId);
      }
      await db.lrem(this.cleaningKey, 0, itemId);
    }
  }

  /**
   * Marks a job as completed and remove it from the work queue.
   *
   * @param {Redis} db The Redis connection.
   * @param {Item} item The Item which the processing got completed
   * @returns {boolean} returns a boolean indicating if *the job has been removed* **and** *this worker was the first worker to call `complete`*. So, while lease might give the same job to multiple workers, complete will return `true` for only one worker.
   */
  async complete(db: Redis, item: Item): Promise<boolean> {
    const removed = await db.lrem(this.processingKey, 0, item.id);
    if (removed === 0) {
      return false;
    }

    const pipeline = db.pipeline();
    pipeline.del(this.itemDataKey.of(item.id));
    pipeline.del(this.leaseKey.of(item.id));
    await pipeline.exec();

    return true;
  }
}
