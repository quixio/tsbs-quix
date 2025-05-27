package kafkaquix

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
    "time"
)

type KafkaQuixTarget struct {
}

func NewTarget() targets.ImplementedTarget {
    return &KafkaQuixTarget{}
}

func (t *KafkaQuixTarget) WritePoint(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) error {
    panic("not implemented")
}

func (t *KafkaQuixTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
}

func (t *KafkaQuixTarget) TargetName() string {
	return constants.FormatKafkaQuix
}

func (t *KafkaQuixTarget) Serializer() serialize.PointSerializer {
    panic("not implemented")
}

func (t *KafkaQuixTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
