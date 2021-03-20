package view

import (
	"encoding/json"
	"reflect"
)

type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
}

type jsonCodec struct {
	t reflect.Type
}

func NewJSONCodec(v interface{}) Codec {
	return &jsonCodec{reflect.TypeOf(v).Elem()}
}

func (c *jsonCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *jsonCodec) Decode(data []byte) (interface{}, error) {
	value := reflect.New(c.t).Interface()
	return value, json.Unmarshal(data, value)
}
