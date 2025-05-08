package transport

import (
	"encoding/json"
	"errors"

	"github.com/c12s/hyparview/data"
)

func serialize(msg data.Message) ([]byte, error) {
	typeByte := byte(msg.Type)
	typeBytes := []byte{typeByte}
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, err
	}
	return append(typeBytes, payloadBytes...), nil
}

func Deserialize(msgSerialized []byte, payload any) (data.Message, error) {
	if len(msgSerialized) == 0 {
		return data.Message{}, errors.New("message empty")
	}
	msgType := data.MessageType(int8(msgSerialized[0]))
	var err error = nil
	if payload != nil {
		err = json.Unmarshal(msgSerialized[1:], payload)
	}
	return data.Message{
		Type:    msgType,
		Payload: payload,
	}, err
}
