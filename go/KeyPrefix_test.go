package workqueue

import "testing"

func TestKeyPrefix(t *testing.T) {
	prefix := KeyPrefix("abc")
	anotherPrefix := prefix.Concat("123")
	finalPrefix := KeyPrefix("abc123")
	if anotherPrefix != finalPrefix {
		t.Error(`anotherPrefix != finalPrefix`)
	}
	if prefix == anotherPrefix {
		t.Error(`prefix == anotherPrefix`)
	}
	if prefix.Of("bar") != "abcbar" {
		t.Error(`prefix.Of("bar") != "abcbar"`)
	}
	if string(prefix.Concat("foo")) != "abcfoo" {
		t.Error(`string(prefix.Concat("foo")) != "abcfoo"`)
	}
	if prefix.Of("foo") != "abcfoo" {
		t.Error(`prefix.Of("foo") != "abcfoo"`)
	}
	if prefix.Concat("foo").Of("bar") != "abcfoobar" {
		t.Error(`prefix.Concat("foo").Of("bar") != "abcfoobar"`)
	}
}
