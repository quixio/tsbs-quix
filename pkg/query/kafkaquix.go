package query

import (
	"fmt"
	"sync"
)

// KafkaQuix encodes a KafkaQuix request. This will be serialized for use
// by the tsbs_run_queries_cratedb program.
type KafkaQuix struct {
	HumanLabel       []byte
	HumanDescription []byte

	HttpQuery []byte
	id       uint64
}

var KafkaQuixPool = sync.Pool{
	New: func() interface{} {
		return &KafkaQuix{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			HttpQuery:        make([]byte, 0, 1024),
		}
	},
}

func NewKafkaQuix() *KafkaQuix {
	return KafkaQuixPool.Get().(*KafkaQuix)
}

func (q *KafkaQuix) GetID() uint64 {
	return q.id
}

func (q *KafkaQuix) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *KafkaQuix) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s",
		q.HumanLabel, q.HumanDescription, q.HttpQuery)
}

func (q *KafkaQuix) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *KafkaQuix) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *KafkaQuix) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.HttpQuery = q.HttpQuery[:0]
	KafkaQuixPool.Put(q)
}
