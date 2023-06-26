const { v4 : uuidv4 } = require('uuid');
type ItemData = {
    [key: string]: any;
  };

export class Item{
    private dict: ItemData = {};
    private data: Buffer | string;

    private id: string;
    constructor(data: string | Buffer, id: string | null = null) {

        if (typeof data === 'string') {
          this.data = Buffer.from(data, 'utf-8');
        } else if (!(data instanceof Buffer)) {
          this.data = Buffer.from(data);
        } else {
          this.data = data;
        }
    
        if (id === null) {
          this.id = uuidv4();
        } else if (typeof id !== 'string') {
          this.id = String(id);
        } else {
          this.id = id;
        }
        this.dict[this.id] = this.data;
      }

    static fromDict(loaded: ItemData): Item {
        let id: string | null = null;
        if ('id' in loaded) {
          id = loaded['id'];
        }
        return new Item(loaded['data'], id);
      }
    static parse(string:string){
        return JSON.parse(string);
    }
    
    static fromJsonData(data:string,id: string | null = null){
        return new Item(JSON.stringify(data), id);
    }
    Data(){
        return this.data
    }
    DataJson(): any {
    let jsonString: string;
    if (typeof this.data === 'string') {
        jsonString = this.data;
    } else {
        jsonString = this.data.toString('utf-8');
    }
    return JSON.parse(jsonString);
    }

    Id():string{
        return this.id
    }
      
}

