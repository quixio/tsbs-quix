package jsonlines

import (
	"encoding/json"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"io"
)

type Serializer struct{}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	message := map[string]interface{}{
		"timestamp":   p.Timestamp().UnixNano(),
		"measurement": string(p.MeasurementName()),
		"tags":        map[string]interface{}{},
		"fields":      map[string]interface{}{},
	}

	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i := range tagKeys {
		message["tags"].(map[string]interface{})[string(tagKeys[i])] = tagValues[i]
	}

	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()
	for i := range fieldKeys {
		switch v := fieldValues[i].(type) {
		case int64, int32, int16, int8, float64, float32, string:
			message["fields"].(map[string]interface{})[string(fieldKeys[i])] = v
		default:
			message["fields"].(map[string]interface{})[string(fieldKeys[i])] = fmt.Sprintf("%v", v)
		}
	}

	// Marshal the JSON line
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error serializing JSON line: %w", err)
	}

	// Write to the output stream
	_, err = w.Write(append(data, '\n'))
	return err
}