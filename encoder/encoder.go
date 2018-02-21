package encoder

import (
	"encoding/json"

	"github.com/cloudfoundry/sonde-go/events"
)

// JSONEncoder is implemtented sarama.Encoder interface.
// It transforms protobuf data to JSON data.
type JSONEncoder struct {
	Event   *events.Envelope
	encoded []byte
	err     error
}

// Encode returns json encoded data. If any, returns error.
func (j *JSONEncoder) Encode() ([]byte, error) {
	j.encode()
	return j.encoded, j.err
}

// Length returns length json encoded data.
func (j JSONEncoder) Length() int {
	j.encode()
	return len(j.encoded)
}

// encode encodes data to json.
func (j *JSONEncoder) encode() {
	if j.encoded == nil && j.err == nil {
		j.encoded, j.err = json.Marshal(j.Event)
	}
}
