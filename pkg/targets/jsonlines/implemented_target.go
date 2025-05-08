package jsonlines

import (
    "encoding/json"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
    "time"
)

type JSONLinesTarget struct {
    // Any necessary fields for the target (e.g., output stream, etc.)
}

func NewTarget() targets.ImplementedTarget {
    return &JSONLinesTarget{}
}

// Implement the necessary methods for the ImplementedTarget interface
func (t *JSONLinesTarget) WritePoint(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) error {
    // Convert the data into JSON Lines format
    record := map[string]interface{}{
        "measurement": measurement,
        "tags":        tags,
        "fields":      fields,
        "timestamp":   timestamp,
    }

    // Serialize to JSON and write to the desired output (e.g., stdout or file)
    recordBytes, err := json.Marshal(record)
    if err != nil {
        return err
    }

    // Output the serialized JSON line (you can adjust to write to a file or Kafka)
    // This is just an example of writing to stdout
    println(string(recordBytes))
    return nil
}

func (t *JSONLinesTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
    return
}

func (t *JSONLinesTarget) TargetName() string {
	return constants.FormatJSONLines
}

func (t *JSONLinesTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *JSONLinesTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}

