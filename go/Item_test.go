package workqueue

import (
	"bytes"
	"testing"
)

func unwrap[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func unwrapToPointer[T any](value T, err error) *T {
	if err != nil {
		panic(err)
	}
	return &value
}

func TestItemJson(t *testing.T) {
	type Test struct {
		N uint   `json:"n"`
		S string `json:"s"`
	}

	testFoo := Test{7, "foo"}
	testBar := Test{8, "bar"}
	testBaz := Test{0, "baz"}

	testItemFoo := unwrap(NewItemFromJSONData(testFoo))
	if testFoo != unwrap(ItemDataJson[Test](&testItemFoo)) {
		t.Error("foo not formatted and reparsed")
	}

	testItemBar := unwrap(NewItemFromJSONData(testBar))
	if len(testItemBar.ID) != len("00112233-4455-6677-8899-aabbccddeeff") {
		t.Error("bar ID length not correct")
	}
	if unwrap(ItemDataJson[Test](&testItemBar)) != testBar {
		t.Error("bar data didn't format or parse correctly")
	}

	testItemBaz := NewItem([]byte("{\"s\":\"baz\"}"))
	if len(testItemBaz.ID) != len("00112233-4455-6677-8899-aabbccddeeff") {
		t.Error("baz ID length not correct")
	}
	if testItemBar.ID == testItemBaz.ID {
		t.Error("bar and baz items have the same ID")
	}
	if bytes.Equal(testItemBar.Data, testItemBaz.Data) {
		t.Error("bar and baz items have the same data")
	}
	if bytes.Equal(testItemFoo.Data, testItemBar.Data) {
		t.Error("foo and bar items have the same data")
	}
	if unwrap(ItemDataJson[Test](&testItemBaz)) != testBaz {
		t.Error("baz data didn't format or parse correctly")
	}
}
