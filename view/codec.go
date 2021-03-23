package view

import (
	"encoding/json"
	"reflect"
)

type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
}

type MustCodec interface {
	Codec

	MustEncode(value interface{}) []byte
	MustDecode(data []byte) interface{}
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

type mustCodecImpl struct {
	Codec
}

func NewMustCodec(codec Codec) MustCodec {
	return &mustCodecImpl{codec}
}

func (c *mustCodecImpl) MustEncode(value interface{}) []byte {
	data, err := c.Encode(value)
	if err != nil {
		panic(err)
	}

	return data
}

func (c *mustCodecImpl) MustDecode(data []byte) interface{} {
	value, err := c.Decode(data)
	if err != nil {
		panic(err)
	}

	return value
}
