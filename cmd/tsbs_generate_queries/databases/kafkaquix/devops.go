package kafkaquix

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces KafkaQuix-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}


// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	metrics := strings.Join(devops.GetAllCPUMetrics(), ",")
	interval := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	httpQuery := fmt.Sprintf(
		"/cpu/max-all?hosts=%s&metrics=%s&start=%s&end=%s",
		strings.Join(hosts, ","),
		metrics,
		interval.StartString(),
		interval.EndString(),
)

	humanLabel := devops.GetMaxAllLabel("KafkaQuix", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, httpQuery, interval.StartUnixNano(), interval.EndUnixNano())
}
