export class KeyPrefix {
    /**
     * A string which should be prefixed to an identifier to generate a database key.
     *
     * ### Example
     *
     * ```typescript
     * const cvKey = new KeyPrefix("cv:");
     *
     * const cvId = "abcdef-123456";
     * console.assert(cvKey.of(cvId) === "cv:abcdef-123456");
     *
     * // You could use this to fetch something from a database, for example:
     * const cvInfo = db.get(cv_key.of(cvId));
     * ```
     */
  
    private prefix: string;
  
    constructor(prefix: string) {
      this.prefix = prefix;
    }
  
    of(name: string): string {
      // Returns the result of prefixing `self` onto `name`.
      return this.prefix + name;
    }
  
    concat(name: string): KeyPrefix {
      // Returns the result of prefixing `self` onto `name` as a new `KeyPrefix`.
      return new KeyPrefix(this.of(name));
    }
  }
  