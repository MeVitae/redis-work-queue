const { v4: uuidv4 } = require('uuid');

type ItemData = {
  [key: string]: any;
};

export class Item {
  // An item for a work queue. Each item has an ID and associated data.

  private data: Buffer | string;
  private id: string;

  constructor(data: string | Buffer, id?: string) {
    /**
     * Args:
     * data (bytes or str): Data to associate with this item, strings will be converted to bytes.
     * id (str | null): ID of the Item, if null, a new (random UUID) ID is generated.
     */
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

  
  }

  static fromDict(loaded: ItemData): Item {
    // Create an `Item` from a dictionary containing 'data' and, optionally, 'id'.
    let id: string | undefined = undefined;
    if ('id' in loaded) {
      id = loaded['id'];
    }
    return new Item(loaded['data'], id);
  }

  static parse(string: string) {
    // Parse an `Item` from JSON. The JSON structure should be an object with a 'data' key and, optionally, an 'id' key.
    return JSON.parse(string);
  }

  static fromJsonData(data: string, id?: string) {
    //Generate an item where the associated data is the JSON string of `data`.
    return new Item(JSON.stringify(data), id);
  }

  Data() {
    // Get the data associated with this item.
    return this.data;
  }

  dataJson(): any {
    // Get the data associated with this item, parsed as JSON.
    let jsonString: string;
    if (typeof this.data === 'string') {
      jsonString = this.data;
    } else {
      jsonString = this.data.toString('utf-8');
    }
    return JSON.parse(jsonString);
  }

  Id(): string {
    // Get the ID of the item.
    return this.id;
  }
}
