/**
 * @module KeyPrefix
 * @description A string which should be prefixed to an identifier to generate a database key.
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
export class KeyPrefix {
  /**
   * KeyPrefix instance prefix.
   */
  private prefix: string;
  
  /**
   * This creates a new instance with the prefix passed.
   * 
   * @param {string} prefix 
   */
  constructor(prefix: string) {
    this.prefix = prefix;
  }
  
  /**
   * This creates the prefixing based on the name and the instance prefix.
   * 
   * @param {string} name The name of the wanted prefix.
   * @returns {string} Result of prefixing `self` onto `name`.
   */
  of(name: string): string {
    return this.prefix + name;
  }
  
  /**
   * Creates a new instance of KeyPrefix based of the current instance and the name passed through.
   * 
   * @param {string} name The name of the wanted prefix.
   * @returns {KeyPrefix} The result of prefixing `self` onto `name` as a new `KeyPrefix`.
   */
  concat(name: string): KeyPrefix {
    return new KeyPrefix(this.of(name));
  }

  /**
   * Creates a new
   * 
   * @param {KeyPrefix} prefix The already created prefix instance.
   * @param {string} name The name of the wanted prefix.
   * @returns {KeyPrefix} This return a new KeyPrefix based on the prefix and Name.
   */
  static concat(prefix: KeyPrefix, name: string): KeyPrefix {
    return new KeyPrefix(prefix.of(name));
  }
}
  
