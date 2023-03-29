package workqueue

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Item for a work queue. Each item has an ID and associated data.
type Item struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
}

// NewItem creates a new item with a random ID (a UUID).
func NewItem(data []byte) Item {
	return Item{
		ID:   uuid.NewString(),
		Data: data,
	}
}

// NewItemFromJSONData creates a new item with a random ID (a UUID). The data is the result of json.Marshal(data).
func NewItemFromJSONData(data any) (Item, error) {
	dataBytes, err := json.Marshal(data)
	return NewItem(dataBytes), err
}

// ParseJsonData parses the item's data, as JSON, to v.
func (item *Item) ParseJsonData(v any) error {
	return json.Unmarshal(item.Data, v)
}

// ItemDataJson returns the data from an item, parsed as JSON to type T.
func ItemDataJson[T any](item *Item) (data T, err error) {
	err = item.ParseJsonData(&data)
	return
}
