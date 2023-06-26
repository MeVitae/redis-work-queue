
const Redis = require('ioredis');
const KeyPrefix = require("./KeyPrefix")

const redisCon = new Redis('redis://default:eqThIleBUDGMARxY1mtZtCKCYKDXlqFC@redis-14357.c59.eu-west-1-2.ec2.cloud.redislabs.com:14357');
const { Item } = require("./Item");
const { v4 : uuidv4 } = require('uuid');

class WorkQueue {
  private session: string;
  private main_queue_key: string;
  private processing_key: string;
  private cleaning_key: string;
  private lease_key: string;
  private item_data_key:string;
  constructor(name: typeof KeyPrefix) {
    this.main_queue_key = name.of(':queue');
    this.processing_key = name.of(':processing');
    this.cleaning_key = name.of(':cleaning');
    this.session = uuidv4();
    this.lease_key = KeyPrefix.concat(name, ':leased_by_session:')
    this.item_data_key = KeyPrefix.concat(name, ':item:')
  }
  addItemToPipeline(pipeline: Redis.Pipeline, item:typeof Item) {
    const itemId = item.Id();
    pipeline.set(this.item_data_key + itemId, item.Data());
    pipeline.lpush(this.main_queue_key, itemId);
  }
  addItem(db:typeof Redis,item:typeof Item){
    const pipeline = db.pipeline();
    this.addItemToPipeline(pipeline,item)
    pipeline.exec();
  }
  async queueLen(db:typeof Redis):Promise<number> {
    return await db.llen(this.main_queue_key);
  }
  async processing(db:typeof Redis){
    return await db.llen(this.processing_key);
  }
  async _lease_exists(db:typeof Redis,itemId:string):Promise<boolean>{
    const exists = await db.exists(KeyPrefix(this.lease_key).of(itemId));
    return exists !== 0;
  }
  async lease(db:typeof Redis,leaseSecs:number,block=true,timeout=0):Promise<typeof Item>{
    let maybeItemId : Buffer |string|undefined;
    let itemId:string;
    if (block){
        maybeItemId  = await db.brpoplpush(
            this.main_queue_key,
            this.processing_key,
            timeout
          );
    
    }else{
        maybeItemId  = await db.brpoplpush(
            this.main_queue_key,
            this.processing_key,
          );
    }
    if (maybeItemId== undefined){
        return undefined
    }
    if (Buffer.isBuffer(maybeItemId)) {
        itemId = maybeItemId.toString('utf-8');
      } else if (typeof maybeItemId === 'string') {
        itemId = maybeItemId;
      } else {
        throw new Error("item id from work queue not bytes or string");
      }
      let data: Buffer | null = await db.get(KeyPrefix(this.item_data_key).of(itemId));
      if (data === null) {
        data = Buffer.from([]);
      }
      await db.setex(KeyPrefix(this.lease_key).of(itemId), timeout, this.session);

      return new Item(data, itemId);
  }
  async lightClean(db:typeof Redis){
    const processing: Array<Buffer | string> = await db.lrange(this.processing_key, 0, -1);
    for (let item_id of processing) {
        if (Buffer.isBuffer(item_id)) {
            item_id = item_id.toString('utf-8');
          }
        if (!this._lease_exists(db,item_id)){
            console.log(`${item_id} has no lease`)
            await db.lpush(this.cleaning_key,item_id)
            let removed = Number(db.lrem(this.processing_key,0,item_id))
            if (removed>0){
                await db.lpush(this.processing_key,0,item_id)
                console.log(`${item_id} was still in the processing queue, it was reset`)
            }else{
                console.log(`${item_id} was no longer in the processing queue`)
            }
            await db.lrem(this.cleaning_key,0,item_id)
        }
          
      }
    
      const forgot: Array<Buffer | string> = await db.lrange(this.cleaning_key, 0, -1);
      for (let item_id of forgot){
        if (Buffer.isBuffer(item_id)) {
            item_id = item_id.toString('utf-8');
        }
        console.log(`${item_id} was forgotten in clean`)
        const leaseExists: boolean = await this._lease_exists(db, item_id);
        const isItemInMainQueue: boolean | null = await db.lpos(this.main_queue_key, item_id);
        const isItemInProcessing: boolean | null = await db.lpos(this.processing_key, item_id);
        if (!leaseExists && isItemInMainQueue === null && isItemInProcessing === null) {
            await db.lpush(this.main_queue_key, item_id)
            console.log(`${item_id} 'was not in any queue, it was reset`)
        }
        await db.lrem(this.cleaning_key, 0, item_id)
      }
  }
  async complete(db:typeof Redis,item:typeof Item): Promise<boolean>{
    const removed: number = await db.lrem(this.processing_key, 0, item.Id());

    if (removed === 0) {
      return false;
    }

    const item_id: string = item.Id();
    await db.pipeline()
      .delete(this._item_data_key.of(item_id))
      .delete(this._lease_key.of(item_id))
      .exec();

    return true;
  }
}
 

  const item = new Item(JSON.stringify({ "asdasd": "asdasd", "asda1sd": "asd" }));
  console.log(item.Data().toString('utf-8'));
  




