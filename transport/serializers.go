package transport

import (
	"errors"

	"github.com/c12s/hyparview/data"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func Serialize(msg data.Message) ([]byte, error) {
	typeByte := byte(msg.Type)
	typeBytes := []byte{typeByte}
	var payloadBytes []byte
	var err error = nil
	switch payload := msg.Payload.(type) {
	case []byte:
		payloadBytes = payload
	default:
		payloadBytes, err = json.Marshal(msg.Payload)
	}
	if err != nil {
		return nil, err
	}
	return append(typeBytes, payloadBytes...), nil
}

func GetMsgType(msgBytes []byte) data.MessageType {
	if len(msgBytes) == 0 {
		return data.UNKNOWN
	}
	return data.MessageType(int8(msgBytes[0]))
	// msgType := data.MessageType(int8(msgBytes[0]))
	// if !slices.Contains(data.KnownMsgTypes(), msgType) {
	// 	return data.UNKNOWN
	// }
	// return msgType
}

func GetPayload(msgBytes []byte) ([]byte, error) {
	if len(msgBytes) == 0 {
		return nil, errors.New("empty message")
	}
	return msgBytes[1:], nil
}

func Deserialize(payloadBytes []byte, payload any) error {
	return json.Unmarshal(payloadBytes, payload)
}
