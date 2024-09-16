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
  private leaseKey: KeyPrefix;
  private itemDataKey: KeyPrefix;

  /**
   * @param {KeyPrefix} name This is the prefix created for the WorkQueue.
   */
  constructor(name: KeyPrefix) {
    this.mainQueueKey = name.of(':queue');
    this.processingKey = name.of(':processing');
    this.session = uuidv4();
    this.leaseKey = name.concat(':lease:');
    this.itemDataKey = name.concat(':item:');
  }

  /**
   * Add an item to the work queue.
   *
   * If an item with the same ID already exists, this item is not added, and `false` is returned.
   * Otherwise, if the item is added `true` is returned.
   *
   * If you know the item ID is unique, and not already in the queue, use the optimised
   * `WorkQueue.add_unique_item` instead.
   *
   * @param {Redis} db - The Redis Connection.
   * @param {Item} item - The item to be added..
   * @returns {Promise<bool>} - A boolean indicating if the item was added.
   */
  async addItem(db: Redis, item: Item): Promise<boolean> {
    const added = await db.setnx(this.itemDataKey.of(item.id), item.data) > 0;
    if (added) await db.lpush(this.mainQueueKey, item.id);
    return added;
  }

  /**
   * Add an item, which is known to have an ID not already in the queue, to the work queue. This
   * adds the redis commands onto the pipeline passed.
   * 
   * Use `WorkQueue.add_unique_item` if you don't want to pass a pipeline directly.
   *
   * @param {Pipeline} pipeline - The pipeline that the data will be executed.
   * @param {Item} item - The Item to be added.
   */
  addUniqueItemToPipeline(pipeline: ChainableCommander, item: Item) {
    // NOTE: it's important that the data is added first, otherwise someone before the data is ready.
    pipeline.set(this.itemDataKey.of(item.id), item.data);
    // Then add the id to the work queue
    pipeline.lpush(this.mainQueueKey, item.id);
  }

  /**
   * Add an item, which is known to have an ID not already in the queue, to the work queue.
   *
   * This creates a pipeline and executes it on the database.
   *
   * @param {Redis} db - The Redis Connection.
   * @param {Item} item - The item to be added.
   */
  async addUniqueItem(db: Redis, item: Item): Promise<void> {
    const pipeline = db.pipeline();
    this.addUniqueItemToPipeline(pipeline, item);
    await pipeline.exec();
  }

  /**
   * Return the length of the work queue (not including items being processed, see
   * `WorkQueue.processing`).
   *
   * @param {Redis} db - The Redis Connection.
   * @returns {Promise<number>} The length of the work queue (not including items being processed).
   */
  queueLen(db: Redis): Promise<number> {
    return db.llen(this.mainQueueKey);
  }

  /**
   * Return the number of items being processed.
   *
   * @param {Redis} db - The Redis Connection.
   * @returns {Promise<number>} The number of items being processed.
   */
  processing(db: Redis): Promise<number> {
    return db.llen(this.processingKey);
  }

  /**
   * Request a work lease from the work queue. This should be called by a worker to get work to
   * complete.  When completed, the `complete` method should be called.
   *
   * If `block` is true, the function will return either when a job is leased or after `timeout`
   * seconds if `timeout` isn't 0.
   *
   * If the job is not completed before the end of `leaseDuration`, another worker may pick up the same job.
   *
   * It is not a problem if a job is marked as `done` more than once.
   *
   * If you haven't already, it's worth reading the documentation on leasing items:
   * https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item
   *
   * @param {Redis} db - The Redis Connection.
   * @param {number} leaseSecs - The number of seconds that the lease should hold.
   * @param {boolean} block - If false, the method will return immediately if there is no item.
   * @param {number} timeout - The number of seconds the lease will time out at.
   * @returns {Promise<Item>} The item to process!
   */
  async lease(
    db: Redis,
    leaseSecs: number,
    block = true,
    timeout = 0,
  ): Promise<Item | null> {
    for (;;) {
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
        if (block && timeout === 0) continue;
        return null;
      }

      // Setup the lease item.
      await db.setex(this.leaseKey.of(itemId), leaseSecs, this.session);
      return new Item(data, itemId);
    }
  }

  /**
   * Marks a job as completed and remove it from the work queue. After `complete` has been called
   * (and returns `true`), no workers will receive this job again.
   *
   * @param {Redis} db - The Redis connection.
   * @param {Item} item - The Item to complete.
   * @returns {boolean} a boolean indicating if *the job has been removed* **and** *this worker was the first worker to call `complete`*. So, while lease might give the same job to multiple workers, complete will return `true` for only one worker.
   */
  async complete(db: Redis, item: Item): Promise<boolean> {
    const results = await db.pipeline()
        .del(this.itemDataKey.of(item.id))
        .lrem(this.processingKey, 0, item.id)
        .del(this.leaseKey.of(item.id))
        .exec();
    return Boolean(results && results[0][1]);
  }

  /**
   * Check if a lease exists for an `itemId`.
   *
   * @param {Redis} db - The Redis Connection.
   * @param {string} itemId
   * @returns {Promise<boolean>} A boolean indicating if a lease exists for the item.
   */
  private async leaseExists(db: Redis, itemId: string): Promise<boolean> {
    const exists = await db.exists(this.leaseKey.of(itemId));
    return exists > 0;
  }

  /**
   * Check if an item exists with `itemId`.
   *
   * @param {Redis} db - The Redis Connection.
   * @param {string} itemId
   * @returns {Promise<boolean>} A boolean indicating if the item exists.
   */
  private async itemExists(db: Redis, itemId: string): Promise<boolean> {
    const exists = await db.exists(this.leaseKey.of(itemId));
    return exists > 0;
  }

  async lightClean(db: Redis) {
    const processing: Array<string> = await db.lrange(
      this.processingKey,
      0, -1,
    );
    for (const itemId of processing) {
      // If there's no lease for the item, then it should be reset.
      if (!(await this.leaseExists(db, itemId))) {
        // We also check that the item actually exists before pushing it back to the main queue
        if (await this.itemExists(db, itemId)) {
          console.log(itemId, "has no lease, it will be reset");
          await db.pipeline()
            .lrem(this.processingKey, 0, itemId)
            .lpush(this.mainQueueKey, itemId)
            .exec();
        } else {
            console.log(itemId, "was in the processing queue but does not exist");
            await db.lrem(this.processingKey, 0, itemId);
        }
      }
    }
  }

  async deepClean(db: Redis) {
    // A deep clean checks all data keys
    const itemResults = await db.pipeline()
      .keys(this.itemDataKey.of("*"))
      .lrange(this.mainQueueKey, 0, -1)
      .exec();
    if (!itemResults) throw new Error("pipeline did not return results");
    const [[_err_a, itemDataKeys], [_err_b, mainQueueUntyped]] = itemResults;
    const mainQueue = mainQueueUntyped as string[];
    for (const itemDataKey of itemDataKeys as string[]) {
      const itemId = itemDataKey.slice(this.itemDataKey.prefix.length);
      // If the item isn't in the queue, and there's no lease for the item, then it should be reset.
      if (!mainQueue.includes(itemId) && !(await this.leaseExists(db, itemId))) {
        console.log(itemId, "has no lease, it will be reset");
        await db.pipeline()
          .lrem(this.processingKey, 0, itemId)
          .lpush(this.mainQueueKey, itemId)
          .exec();
      }
    }
  }
}
