import Redis from 'ioredis';
import { Item } from './Item';
import { WorkQueue } from './index';
import { KeyPrefix } from './KeyPrefix';

async function sleep(ms: number): Promise<void> {
  return await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
let error:number=0;

const redisHost = process.argv[2];

// Create a Redis client
const db = new Redis(redisHost);
  


const typeScriptResultsKey = new KeyPrefix('results:typeScript:');
const sharedResultsKey = new KeyPrefix('results:shared:');

const typeScriptQueueKeyPrefix = new KeyPrefix('typeScript_jobs');
const sharedQueueKeyPrefix = new KeyPrefix('shared_jobs');

const typeScriptQueue = new WorkQueue(typeScriptQueueKeyPrefix);
const sharedQueue = new WorkQueue(sharedQueueKeyPrefix);

let typeScriptJobCounter = 0;
let sharedJobCounter = 0;

let shared = true;

async function main() {
      let timeToProcess :number=0
      shared = !shared;
      //console.log(shared)
      if (shared) {
        sharedJobCounter += 1;
  
        const block = sharedJobCounter % 5 === 0;
        //console.log(`Leasing shared with block = ${block}`);
        const job = await sharedQueue.lease(db, 1, block, 4);
      console.log(job)
      if (job === null) {
        //console.log('No shared job available');
        //continue;
      }

      //console.log(job);
      const data = job.dataJson();
      const result = {
        a: data.a,
        sum: data.a + data.b,
        prod: data.a * data.b,
        worker: 'typeScript',
      };
      const resultJson = JSON.stringify(result);
      console.log('Result 1:', resultJson);

      if (sharedJobCounter % 12 === 0) {
        timeToProcess = (sharedJobCounter % 4)*1000;
      }

      await db.set(sharedResultsKey.of(job.Id()), resultJson);


      await sharedQueue.complete(db, job);
   
    } else {
        typeScriptJobCounter += 1;
        
        const block = typeScriptJobCounter % 6 === 0;
        //console.log(`Leasing typeScript with block = ${block}`);
        const job = await typeScriptQueue.lease(db, 1, block, 4);
        
      if (job === null) {
        //console.log('No typeScript job available');
        //continue;
      }

      //console.log(job);
      const data = job.dataJson();
      console.log("------"+JSON.stringify(data))
      const result = (data.a * 3) % 256;
      
      console.log('Result 2:', result+"\n"+data);

      if (typeScriptJobCounter % 25 === 0) {
        timeToProcess = (sharedJobCounter % 20)*1000;
     
      }

      await db.set(typeScriptResultsKey.of(job.Id()), Buffer.from([result]));

      if (typeScriptJobCounter % 29 !== 0) {
        //console.log('Completing');

        if (await typeScriptQueue.complete(db, job)) {
          //console.log('Spawning shared jobs');
          await sharedQueue.addItem(
            db,
            new Item(
              JSON.stringify({
                a: 13,
                b: result,
              })
            )
          );
          await sharedQueue.addItem(
            db,
            new Item(
              JSON.stringify({
                a: 17,
                b: result,
              })
            )
          );
        }
      } else {
        //console.log('Dropping');
      }
    }
    setTimeout(function() {

        main();  
              
    }, timeToProcess)
  }


main();
