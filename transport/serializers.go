package transport

import (
	"bytes"
	"errors"
	"sync"

	"github.com/c12s/hyparview/data"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigFastest

var bufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

func Serialize(msg data.Message) ([]byte, error) {
	typeByte := byte(msg.Type)

	var payloadBytes []byte

	switch payload := msg.Payload.(type) {
	case []byte:
		payloadBytes = payload
	default:
		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset() // clear previous content
		defer bufferPool.Put(buf)

		stream := jsoniter.NewStream(jsoniter.ConfigFastest, buf, 512)
		stream.WriteVal(payload)
		stream.Flush()
		payloadBytes = buf.Bytes()
	}

	// Prepend type byte
	result := make([]byte, 1+len(payloadBytes))
	result[0] = typeByte
	copy(result[1:], payloadBytes)

	return result, nil
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
