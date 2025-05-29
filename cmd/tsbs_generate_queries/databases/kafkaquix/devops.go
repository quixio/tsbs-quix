package kafkaquix

import (
	"fmt"
	"time"
	"encoding/json"

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


type QueryBody struct {
	Hostnames 	[]string 	`json:"hostnames"`
	Metrics		[]string	`json:"metrics"`
	Begin		string		`json:"timestamp_begin"`
	End			string		`json:"timestamp_end"`
}


func (qb QueryBody) ToBytes() []byte {
    b, err := json.Marshal(qb)
    if err != nil {
        panic(err)
    }
    return b
}


// GroupByTime selects the MAX for a single metric under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE
// 		(hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// 	AND time >= '$HOUR_START'
// 	AND time < '$HOUR_END'
// GROUP BY minute
// ORDER BY minute ASC
//
// Resultsets:
// single-groupby-1-1-12
// single-groupby-1-1-1
// single-groupby-1-8-1
// single-groupby-5-1-12
// single-groupby-5-1-1
// single-groupby-5-8-1
func (d *Devops) GroupByTime(qi query.Query, nhosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	hosts, err := d.GetRandomHosts(nhosts)
	if err != nil {
		panic(err)
	}
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	q := QueryBody{
		Hostnames: hosts,
		Metrics: metrics,
		Begin: interval.StartString(),
		End: interval.EndString(),
	}

	humanLabel := fmt.Sprintf("KafkaQuix max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "/single-groupby", &q, interval.StartUnixNano(), interval.EndUnixNano())
}


// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, timeRange time.Duration) {
	metrics := devops.GetAllCPUMetrics()
	interval := d.Interval.MustRandWindow(timeRange)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	q := QueryBody{
		Hostnames: hosts,
		Metrics: metrics,
		Begin: interval.StartString(),
		End: interval.EndString(),
	}

	humanLabel := devops.GetMaxAllLabel("KafkaQuix", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "/max-all", &q, interval.StartUnixNano(), interval.EndUnixNano())
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour, hostname
//
// Resultsets:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	q := QueryBody{
		Hostnames: []string{}, // translates to all hostnames
		Metrics: metrics,
		Begin: interval.StartString(),
		End: interval.EndString(),
	}

	humanLabel := devops.GetDoubleGroupByLabel("KafkaQuix", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "/double-groupby", &q, interval.StartUnixNano(), interval.EndUnixNano())}


// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
//
// Resultsets:
// high-cpu-1
// high-cpu-all
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	var hostnames []string
	if nHosts > 0 {
		var err error
		hostnames, err = d.GetRandomHosts(nHosts)
		panicIfErr(err)
	}

	q := QueryBody{
		Hostnames: hostnames,
		Metrics: devops.GetAllCPUMetrics(),
		Begin: interval.StartString(),
		End: interval.EndString(),
	}

	humanLabel, err := devops.GetHighCPULabel("KafkaQuix", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s - %s", humanLabel, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, "/high-cpu", &q, interval.StartUnixNano(), interval.EndUnixNano())
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "KafkaQuix last event per hostname"
	humanDesc := humanLabel + ": cpu"

	q := QueryBody{
		Hostnames: []string{},
		Metrics: []string{},
		Begin: "",
		End: "",
	}
	d.fillInQuery(qi, humanLabel, humanDesc, "/lastpoint", &q, 0, 0)
}