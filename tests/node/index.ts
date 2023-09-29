import Redis from 'ioredis';
import { Item } from '../../node/src/Item';
import { KeyPrefix } from '../../node/src/KeyPrefix';
import { WorkQueue } from '../../node/src/WorkQueue';

const redisHost: string = process.argv[2];
const db: Redis = new Redis(redisHost);
const nodeResultsKey: KeyPrefix = new KeyPrefix('results:node:');
const sharedResultsKey: KeyPrefix = new KeyPrefix('results:shared:');
const nodeQueueKeyPrefix: KeyPrefix = new KeyPrefix('node_jobs');
const sharedQueueKeyPrefix: KeyPrefix = new KeyPrefix('shared_jobs');
const nodeQueue: WorkQueue = new WorkQueue(nodeQueueKeyPrefix);
const sharedQueue: WorkQueue = new WorkQueue(sharedQueueKeyPrefix);
let nodeJobCounter: number = 0;
let sharedJobCounter: number = 0;
let shared: boolean = true;
let redisConnections: Redis[] = [];

for (let i = 0; i < 5; i++) {
  redisConnections.push(new Redis(redisHost));
}

async function addNewItemWithSeep(db: Redis, item: Item, workQueue: WorkQueue, processingKey: string, mainKey: string): Promise<boolean> {
  while (true) {
    try {
      await db.watch(mainKey, processingKey);

      const isItemInProcessingKey = await db.lpos(processingKey, item.id);
      const isItemInMainQueueKey = await db.lpos(mainKey, item.id);
      if (isItemInProcessingKey !== null || isItemInMainQueueKey !== null) {
        console.log("Item already exists, not added", item.id);
        await db.unwatch();
        return false;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 100));;
      const transaction = db.multi();
      workQueue.addItemToPipeline(transaction, item);
      const results = await transaction.exec();

      if (!results) {
        console.log("Transaction failed, item not added", item.id);
        await db.unwatch();
      }
      console.log("Item added successfully", item.id);
      return true
    } catch (e) {
      console.log("Error", e);
    } finally {
      await db.unwatch();
    }
  }
  return false;
}

async function main() {
  while (true) {
    shared = !shared;
    if (shared) {
      sharedJobCounter += 1;

      // First, try to get a job from the shared job queue
      const block = sharedJobCounter % 5 === 0;
      const job = await sharedQueue.lease(db, 1, block, 4);
      // If there was no job, continue.
      // Also, if we get 'unlucky', crash while completing the job.
      if (!job || sharedJobCounter % 7 === 0) {
        continue;
      }

      // Parse the job
      const data = job.dataJson();
      // Generate response.
      const result = {
        a: data.a,
        sum: data.a + data.b,
        prod: data.a * data.b,
        worker: 'node',
      };
      const resultJson = JSON.stringify(result);

      // Pretend it takes us a while to compute the result
      // Sometimes this will take too long and we'll timeout
      if (sharedJobCounter % 12 === 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, (sharedJobCounter % 4) * 1000))
      }

      // Store result
      await db.set(sharedResultsKey.of(job.id), resultJson);

      // Complete Job
      await sharedQueue.complete(db, job);
    } else {
      nodeJobCounter += 1;
      // First, try to get a job from the python job queue
      const block = nodeJobCounter % 6 === 0;
      const job = await nodeQueue.lease(db, 1, block, 4);
      // If there was no job, continue.
      // Also, if we get 'unlucky', crash while completing the job.
      if (job === null || nodeJobCounter % 7 === 0) {
        continue;
      }

      // Generate result
      const data: any = job.data;
      const result = (data[0] * 17) % 256;

      /*
      * Pretend it takes us a while to compute the result
      * Sometimes this will take too long and we'll timeout
      */
      if (nodeJobCounter % 25 === 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, (nodeJobCounter % 20) * 1000));
      }

      // Store result
      await db.set(nodeResultsKey.of(job.id), Buffer.from([result]));
      // Complete the job unless we're 'unlucky' and crash again
      if (nodeJobCounter % 29 !== 0) {
        if (await nodeQueue.complete(db, job)) {
          let promisees: Promise<any>[] = [];
          let item = new Item(
            JSON.stringify({
              a: 17,
              b: result,
            }));
          for (const redis of redisConnections) {
            if (Math.random() < 0.9) {
              promisees.push(sharedQueue.addNewItem(
                redis,
                item
              ));
            } else {
              promisees.push(addNewItemWithSeep(redis, item, sharedQueue, sharedResultsKey.of('":processing"'), sharedResultsKey.of(":queue")));
            }
          }
          await Promise.all(promisees);
          promisees = [];
          let item2 = new Item(
            JSON.stringify({
              a: 21,
              b: result,
            }));
          for (const redis of redisConnections) {
            if (Math.random() < 0.9) {
              promisees.push(sharedQueue.addNewItem(
                redis,
                item2,
              ));
            } else {
              promisees.push(addNewItemWithSeep(redis, item2, sharedQueue, sharedResultsKey.of(":processing"), sharedResultsKey.of(":queue")));
            }
          }
          await Promise.all(promisees);

        }
      }
    }
  }
}

main()