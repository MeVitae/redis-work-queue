import Redis from 'ioredis';
import {Item} from '../../node/src/Item';
import {KeyPrefix} from '../../node/src/KeyPrefix';
import {WorkQueue} from '../../node/src/WorkQueue';

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
        await sleep((sharedJobCounter % 4)*1000);
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
        await sleep((nodeJobCounter % 20)*1000);
      }

      // Store result
      await db.set(nodeResultsKey.of(job.id), Buffer.from([result]));
      // Complete the job unless we're 'unlucky' and crash again
      if (nodeJobCounter % 29 !== 0) {
        if (await nodeQueue.complete(db, job)) {
          await sharedQueue.addItem(
              db,
              new Item(
                  JSON.stringify({
                    a: 17,
                    b: result,
                  }),
              ),
          );
          await sharedQueue.addItem(
              db,
              new Item(
                  JSON.stringify({
                    a: 21,
                    b: result,
                  }),
              ),
          );
        }
      }
    }
  }
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
}

async function testDuplicateItems() {
  const keyPrefix = new KeyPrefix('Duplicate-Items-Test');
  const workQueue = new WorkQueue(keyPrefix);
  let passed = 0;
  let notPassed = 0;
  const numberOfTests = 3500;
  const numberOfRandomPossibleItems = 232;
  const items: Item[] = [];

  for (let i = 1; i <= numberOfTests; i++) {
    const itemData1 = Math.floor(
      Math.random() * numberOfRandomPossibleItems
    ).toString();
    const itemData2 = Math.floor(
      Math.random() * numberOfRandomPossibleItems
    ).toString();
    const item1 = await new Item(Buffer.from(itemData1), itemData1);
    const item2 = await new Item(Buffer.from(itemData2), itemData2);
    items.push(item1);
    items.push(item2);

    const result1 = await workQueue.addAtomicItem(db, item1);
    const result2 = await workQueue.addAtomicItem(db, item2);

    if (result1 === undefined) {
      passed += 1;
    } else {
      notPassed += 1;
    }

    if (result2 === undefined) {
      passed += 1;
    } else {
      notPassed += 1;
    }

    if (Math.floor(Math.random() * 11) < 3) {
      await workQueue.lease(db, 1, false, 4);
    }

    if (Math.floor(Math.random() * 11) < 5) {
      const toRemove: Item = items.pop() as Item;
      await workQueue.complete(db, toRemove);
    }
    //console.log(await workQueue.getQueueLengths(redis))
  }

  console.assert(
    notPassed + passed === numberOfTests * 2,
    'The number of passed items (',
    passed,
    ') with the number of not passed items (',
    notPassed,
    ') should be equal to number of tests * 2:',
    numberOfTests * 2
  );

  const mainQueueKey = keyPrefix.of(':queue');
  const processingKey = keyPrefix.of(':processing');
  const mainItems = await db.lrange(mainQueueKey, 0, -1);
  const processingItems = await db.lrange(processingKey, 0, -1);

  for (let i = 0; i < mainItems.length; i++) {
    console.assert(
      !processingItems.includes(mainItems[i]),
      'Found duplicated item from main queue inside processing queue'
    );
  }

  for (let i = 0; i < processingItems.length; i++) {
    console.assert(
      !mainItems.includes(processingItems[i]),
      'Found duplicated item from processing queue inside main queue'
    );
  }
}
main()
testDuplicateItems();
