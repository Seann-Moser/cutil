package cachec

import (
	"context"
	"fmt"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

type CacheStatus string
type CacheCmd string

const (
	CacheCmdSET    = CacheCmd("SET")
	CacheCmdGET    = CacheCmd("GET")
	CacheCmdDELETE = CacheCmd("DELETE")

	CacheStatusFOUND   = CacheStatus("FOUND")
	CacheStatusOK      = CacheStatus("OK")
	CacheStatusMISSING = CacheStatus("MISSING")
	CacheStatusERR     = CacheStatus("ERR")
)

type CacheTags struct {
	CacheName string
	instance  string
	Name      tag.Key
	Status    tag.Key
	Cmd       tag.Key
	Latency   *stats.Int64Measure
}

func NewCacheTags(cacheName string, instance string) CacheTags {
	tags := CacheTags{
		CacheName: cacheName,
		instance:  instance,
		Name:      tag.MustNewKey(fmt.Sprintf("%s_cache_name", cacheName)),
		Status:    tag.MustNewKey(fmt.Sprintf("%s_cache_status", cacheName)),
		Cmd:       tag.MustNewKey(fmt.Sprintf("%s_cache_cmd", cacheName)),
		Latency:   stats.Int64(fmt.Sprintf("%s.cache/latency", cacheName), "latency of calls in milliseconds", stats.UnitMilliseconds),
	}
	_ = tags.RegisterAllViews()
	return tags
}

func (c *CacheTags) RegisterAllViews() error {
	return view.Register(c.Views()...)
}

func (c *CacheTags) Views() []*view.View {
	formatedViewName := fmt.Sprintf("%s.cache/client", c.CacheName)

	latencyView := &view.View{
		Name:        formatedViewName + "/latency",
		Description: "The distribution of latency of various calls in milliseconds",
		Measure:     c.Latency,
		Aggregation: view.Distribution(
			0.0,
			0.001,
			0.005,
			0.01,
			0.05,
			0.1,
			0.5,
			1.0,
			1.5,
			2.0,
			2.5,
			5.0,
			10.0,
			25.0,
			50.0,
			100.0,
			200.0,
			400.0,
			600.0,
			800.0,
			1000.0,
			1500.0,
			2000.0,
			2500.0,
			5000.0,
			10000.0,
			20000.0,
			40000.0,
			100000.0,
			200000.0,
			500000.0,
		),
		TagKeys: []tag.Key{c.Cmd, c.Status, c.Name},
	}

	callsView := &view.View{
		Name:        formatedViewName + "/calls",
		Description: "The number of various calls of methods",
		Measure:     c.Latency,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{c.Cmd, c.Status, c.Name},
	}

	return []*view.View{latencyView, callsView}
}

type Status func(err error) CacheStatus

func (c *CacheTags) record(ctx context.Context, cmd CacheCmd, status Status) func(err error) {
	var startTime = time.Now()
	return func(err error) {
		var (
			timeSpentMs = time.Since(startTime).Milliseconds()
			tags        = []tag.Mutator{
				tag.Insert(c.Name, c.instance),
				tag.Insert(c.Cmd, string(cmd)),
				tag.Insert(c.Status, string(status(err))),
			}
		)
		_ = stats.RecordWithTags(ctx, tags, c.Latency.M(timeSpentMs))
	}
}
