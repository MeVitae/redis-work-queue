export class KeyPrefix {

    /**
     A string which should be prefixed to an identifier to generate a database key.

    ### Example

    ```typeScript
    const cv_key = new KeyPrefix("cv:");

    const cv_id = "abcdef-123456";
    console.assert(cv_key.of(cv_id) === "cv:abcdef-123456");

    // You could use this to fetch something from a database, for example:
    const cv_info = db.get(cv_key.of(cv_id));
    ```
     */
    private prefix: string;
  
    constructor(prefix: string) {
      this.prefix = prefix;
    }
  
    of(name: string): string {
        //Returns the result of prefixing `self` onto `name`.
      return this.prefix + name;
    }
  
    static concat(prefix: KeyPrefix, name: string): KeyPrefix {
    //Returns the result of prefixing `self` onto `name` as a new `KeyPrefix`.
      return new KeyPrefix(prefix.of(name));
    }
  }
  