package minmax

import (
	"reflect"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

type Rate struct {
	cache         map[uint64]aggregate
	Suffix        string
	Metrics       []string
	RateFields    []string
	BitRateFields []string
}

func NewRate() telegraf.Aggregator {
	mm := &Rate{}
	mm.Reset()
	return mm
}

type aggregate struct {
	fields map[string]last
	name   string
	tags   map[string]string
}

type last struct {
	val       float64
	timestamp int64
	rate      int64
}

var sampleConfig = `
  ## General Aggregator Arguments:
  ## The period on which to flush & clear the aggregator.
  period = "30s"
  ## If true, the original metric will be dropped by the
  ## aggregator and will not get sent to the output plugins.
  drop_original = true
  ## the suffix of field to rename,
  ## example: the field named "in", would be renamed "in_rate"
  #suffix="_rate"
  ## metrics to filter
  #metrics = ["snmp"]
  ## fields to rate algorithm, (curr_val - last_val) / (curr_time - last_time)
  #rate_fields = ["in_pkts","out_pkts"]
  ## fields to bit rate algorithm, (curr_val - last_val) / (curr_time - last_time) * 8
  #bitrate_fields = ["in","out"]
`

func (m *Rate) SampleConfig() string {
	return sampleConfig
}

func (m *Rate) Description() string {
	return "Calc the rate of each metric passing through."
}

func (m *Rate) IsRateMetric(obj interface{}) bool {
	targetValue := reflect.ValueOf(m.Metrics)
	for i := 0; i < targetValue.Len(); i++ {
		if targetValue.Index(i).Interface() == obj {
			return true
		}
	}
	return false
}

func (m *Rate) IsRateField(obj interface{}) bool {
	targetValue := reflect.ValueOf(m.RateFields)
	for i := 0; i < targetValue.Len(); i++ {
		if targetValue.Index(i).Interface() == obj {
			return true
		}
	}
	return false
}

func (m *Rate) IsBitRateField(obj interface{}) bool {
	targetValue := reflect.ValueOf(m.BitRateFields)
	for i := 0; i < targetValue.Len(); i++ {
		if targetValue.Index(i).Interface() == obj {
			return true
		}
	}
	return false
}

func (m *Rate) Add(in telegraf.Metric) {
	if !m.IsRateMetric(in.Name()) {
		return
	}
	id := in.HashID()
	ts := time.Now().Unix()
	if _, ok := m.cache[id]; !ok {
		// hit an uncached metric, create caches for first time:
		a := aggregate{
			name:   in.Name(),
			tags:   in.Tags(),
			fields: make(map[string]last),
		}

		for k, v := range in.Fields() {
			if m.IsBitRateField(k) || m.IsRateField(k) {
				if fv, ok := convert(v); ok {
					a.fields[k] = last{
						val:       fv,
						timestamp: ts,
						rate:      0,
					}
				}
			}
		}
		m.cache[id] = a
	} else {
		for k, v := range in.Fields() {
			if m.IsBitRateField(k) || m.IsRateField(k) {
				if fv, ok := convert(v); ok {
					if _, ok := m.cache[id].fields[k]; !ok {
						// hit an uncached field of a cached metric
						//fmt.Println("not in cached fields: ", id, ",", k, ",", fv)
						m.cache[id].fields[k] = last{
							val:       fv,
							timestamp: ts,
							rate:      0,
						}
						continue
					}
					ld := m.cache[id].fields[k]
					var rate float64
					period := ts - ld.timestamp

					if period < 2 {
						rate = float64(ld.rate)
					} else {
						if fv >= ld.val {
							rate = (fv - ld.val) / float64(period)
							if m.IsBitRateField(k) {
								rate = rate * 8
							}
						} else {
							rate = float64(ld.rate)
						}
					}
					//tag, _ := in.GetTag("if_name")
					//fmt.Println("calc rate,", "tag: ", tag, " k: ", k, "v: ", v, "last_data_time: ", ld.timestamp, "last_data_v: ", ld.val, "rate: ", rate)
					m.cache[id].fields[k] = last{
						val:       fv,
						timestamp: ts,
						rate:      int64(rate),
					}
				}
			}
		}
	}
}

func (m *Rate) Push(acc telegraf.Accumulator) {
	for _, aggregate := range m.cache {
		fields := map[string]interface{}{}
		for k, v := range aggregate.fields {
			fields[k+m.Suffix] = v.rate
			//fmt.Println("name: ", aggregate.name, "tags: ", aggregate.tags, "field: ", k+"_rate", "value: ", v.rate)
		}
		acc.AddFields(aggregate.name, fields, aggregate.tags)
	}
}

func (m *Rate) Reset() {
	if m.cache == nil {
		//fmt.Println("Invoke reset")
		m.cache = make(map[uint64]aggregate)
	}
}

func convert(in interface{}) (float64, bool) {
	switch v := in.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func init() {
	//fmt.Println("Invoke init")
	aggregators.Add("rate", func() telegraf.Aggregator {
		return NewRate()
	})
}
