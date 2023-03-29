package workqueue

// KeyPrefix is a string which should be prefixed to an identifier to generate a database key.
//
// # Example
//
//	cv_key := KeyPrefix("cv:")
//	// ...
//	cv_id := "abcdef-123456"
//	// Now, cv_id == "cv:abcdef-123456"
//	// It can be used to access an item in a database, for example:
//	cv_info := db.get(cv_key.of(cv_id))
type KeyPrefix string

// Of returns the result of prefixing prefix onto name.
func (prefix KeyPrefix) Of(name string) string {
	return string(prefix) + name
}

// Concat other onto prefix and return the result as a KeyPrefix.
func (prefix KeyPrefix) Concat(other string) KeyPrefix {
	return KeyPrefix(prefix.Of(other))
}
