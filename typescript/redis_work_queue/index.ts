import Redis, { Pipeline } from "ioredis";
const { KeyPrefix } = require("./KeyPrefix");
const { Item } = require("./Item");
const { v4 : uuidv4 } = require("uuid");

export class WorkQueue {
  /**
   * A work queue backed by a redis database
   */
  private session: string;
  private mainQueueKey: string;
  private processingKey: string;
  private cleaningKey: string;
  private leaseKey: typeof KeyPrefix;
  private itemDataKey: typeof KeyPrefix;

  constructor(name: typeof KeyPrefix) {
    this.mainQueueKey = name.of(':queue');
    this.processingKey = name.of(':processing');
    this.cleaningKey = name.of(':cleaning');
    this.session = uuidv4();
    //console.log(this.session)
    this.leaseKey = KeyPrefix.concat(name, ':leased_by_session:');
    this.itemDataKey = KeyPrefix.concat(name, ':item:');
  }

  async addItemToPipeline(pipeline: Pipeline, item: typeof Item) {
    /**
     * Add an item to the work queue. This adds the redis commands onto the pipeline passed.
     * Use `WorkQueue.addItem` if you don't want to pass a pipeline directly.
     * Add the item data
     * NOTE: it's important that the data is added first, otherwise someone before the data is
     * ready
     */
    const itemId = item.Id();
    pipeline.set(this.itemDataKey.of(itemId), item.Data());
    // Then add the id to the work queue
    pipeline.lpush(this.mainQueueKey, itemId);
    await pipeline.exec();
  }

  addItem(db: Redis, item: typeof Item): void {
    // Add an item to the work queue.
    // This creates a pipeline and executes it on the database.
    const pipeline = db.pipeline() as unknown as Pipeline;
    this.addItemToPipeline(pipeline, item);
    
  }

  queueLen(db: Redis): Promise<number> {
    // Return the length of the work queue (not including items being processed, see `WorkQueue.processing()`).
    return db.llen(this.mainQueueKey);
  }

  processing(db: Redis) {
    // Return the number of items being processed.
    return db.llen(this.processingKey);
  }

  async leaseExists(db: Redis, itemId: string): Promise<boolean> {
    console.log(this.leaseKey.of(itemId),"-- lease key")
    return await db.exists(this.leaseKey.of(itemId)).then(exists => {console.log(exists,"--exists");return exists !== 0 });
  }
  


  async lease(db:Redis, leaseSecs:number, block = true, timeout = 1): Promise<typeof Item> {
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
     */
    //const processing: Array<Buffer | string> = await db.lrange(this.processingKey, 0, -1);

    ////console.log(processing)
  
    let maybeItemId = null;
    let itemId;
  
    // First, to get an item, we try to move an item from the main queue to the processing list.
    if (block) {
      maybeItemId = await db.brpoplpush(this.mainQueueKey, this.processingKey, timeout);
    } else {
      maybeItemId = await db.rpoplpush(this.mainQueueKey, this.processingKey);
    }
    
    if (maybeItemId == null) {
      return null;
    }

    if (Buffer.isBuffer(maybeItemId)) {
        maybeItemId = maybeItemId.toString('utf-8');
    }
  
    itemId = maybeItemId;
    //console.log(itemId)
    let data: Buffer | null = await db.getBuffer(this.itemDataKey.of(itemId));


    if (data == null) {
      data = Buffer.alloc(0);
    }
    // Setup the lease item.
    await db.setex(this.leaseKey.of(itemId), leaseSecs, this.session);
    const item = new Item(data, itemId)
    return item;
  }

  
  async lightClean(db: Redis) {
    const processing: Array<string> = await db.lrange(this.processingKey, 0, -1);
    
    for (let itemId of processing) {
      
      // If the lease key is not present for an item (it expired or was never created because
      // the client crashed before creating it), then move the item back to the main queue so
      // others can work on it.
      console.log(itemId,"-- Item ID")
      console.log(await this.leaseExists(db, itemId),"-- Lease")
      if (!await this.leaseExists(db, itemId)) {

        //console.log(`${itemId} has no lease`);
        // While working on an item, we store it in the cleaning list. If we ever crash, we
        // come back and check these items.
        await db.lpush(this.cleaningKey, itemId);
        let removed = await db.lrem(this.processingKey, 0, itemId);

        if (removed > 0) {
          await db.lpush(this.mainQueueKey, 0, itemId);
          //console.log(`${itemId} was still in the processing queue, it was reset`);
        } else {
          //console.log(`${itemId} was no longer in the processing queue`);
        }

        await db.lrem(this.cleaningKey, 0, itemId);
      }
    }

    // Now we check the
    const forgot: Array<string> = await db.lrange(this.cleaningKey, 0, -1);

    for (let itemId of forgot) {
      //console.log(`${itemId} was forgotten in clean`);
      const leaseExists: boolean = await this.leaseExists(db, itemId);
       if (!leaseExists && await db.lpos(this.mainQueueKey, itemId) == null && await db.lpos(this.processingKey, itemId) == null) {
        /**
         * FIXME: this introduces a race
         * maybe not anymore
         * no, it still does, what if the job has been completed?
         */
        await db.lpush(this.mainQueueKey, itemId);
        //console.log(`${itemId} was still not in any queue, it was returned to the main queue`);
      } else if (!leaseExists) {
        //console.log(`${itemId} is not leased, still in main queue or still in processing queue`);
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
      if (!this.leaseExists(db, itemId)) {
        //console.log(`${itemId} has no lease`);
        // While working on an item, we store it in the cleaning list. If we ever crash, we
        // come back and check these items.
        await db.lpush(this.cleaningKey, itemId);
        let removed = Number(db.lrem(this.processingKey, 0, itemId));

        if (removed > 0) {
          await db.lpush(this.processingKey, 0, itemId);
          //console.log(`${itemId} was still in the processing queue, it was reset`);
        } else {
          //console.log(`${itemId} was no longer in the processing queue`);
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
      //console.log(`${itemId} was forgotten in clean`);
      const leaseExists: boolean = await this.leaseExists(db, itemId);
      const isItemInMainQueue: boolean | null = await db.lpos(this.mainQueueKey, itemId)!== 0;
      const isItemInProcessing: boolean | null = await db.lpos(this.processingKey, itemId)!== 0;
      if (!leaseExists && isItemInMainQueue === null && isItemInProcessing === null) {
        /**
         * FIXME: this introduces a race
         * maybe not anymore
         * no, it still does, what if the job has been completed?
         */
        await db.lpush(this.mainQueueKey, itemId);
        //console.log(`${itemId} was still not in any queue, it was returned to the main queue`);
      } else if (!leaseExists) {
        //console.log(`${itemId} is not leased, still in main queue or still in processing queue`);
      }
      await db.lrem(this.cleaningKey, 0, itemId);
    }
  }



  async complete(db:Redis,item: typeof Item) {  
    /**
     * Marks a job as completed and remove it from the work queue. After `complete` has been
     * called (and returns `true`), no workers will receive this job again.

     * `complete` returns a boolean indicating if *the job has been removed* **and** *this worker
     * was the first worker to call `complete`*. So, while lease might give the same job to
     * multiple workers, complete will return `true` for only one worker.
     */

    const removed = await db.lrem(this.processingKey, 0, item.Id());
    //console.log(removed+"-removed")
    if (removed === 0) {
      return false;
    }
    const pipeline = db.pipeline();
    pipeline.del(this.itemDataKey.of(item.Id()));
    pipeline.del(this.leaseKey.of(item.Id()));
    //console.log(await pipeline.exec());
    await pipeline.exec()
    return true;
  }
  
}

