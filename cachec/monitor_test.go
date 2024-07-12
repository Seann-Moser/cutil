package cachec

import (
	"context"
	"github.com/orijtech/gomemcache/memcache"
	"github.com/patrickmn/go-cache"
	"sync"
	"testing"
	"time"
)

type CacheTestMonitor struct {
	Key            string
	Group          string
	Func           func(ctx context.Context) (string, error)
	Expected       string
	IsExpectingErr bool
}

func TestMonitor(t *testing.T) {
	ctx := context.Background()
	GlobalCacheMonitor = NewMonitor()
	workers := 20
	cacheFunctions := []*CacheTestMonitor{
		{
			Key:      "default1",
			Group:    "default",
			Expected: time.Now().Add(time.Hour).String(),
		},
		{
			Key:      "default2",
			Group:    "default",
			Expected: time.Now().Add(time.Minute).String(),
		},
		{
			Key:      "default3",
			Group:    "default",
			Expected: time.Now().Add(time.Minute).String(),
		},
		{
			Key:            "default1",
			Group:          "default",
			Expected:       time.Now().Add(time.Hour).String(),
			IsExpectingErr: true,
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(workers)

	c := NewTieredCache(nil, NewGoCache(cache.New(1*time.Minute, time.Minute), 1*time.Minute, ""), NewMemcache(memcache.New(""), 1*time.Minute, "", false))
	ctx = ContextWithCache(ctx, c)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for _, cacheFunction := range cacheFunctions {
				e, err := GetSet[string](ctx, 1*time.Minute, cacheFunction.Group, cacheFunction.Key, NewD(cacheFunction))
				if err != nil {
					t.Errorf("failed getting cache %s %s Expected:%s", cacheFunction.Group, cacheFunction.Key, cacheFunction.Expected)
				}
				if e != cacheFunction.Expected {
					if !cacheFunction.IsExpectingErr {
						t.Errorf("failed getting cache %s %s Actual:%s Expected:%s", cacheFunction.Group, cacheFunction.Key, e, cacheFunction.Expected)
					}
				} //else if cacheFunction.IsExpectingErr {
				//	t.Errorf("should have failed failed getting cache %s %s Actual:%s", cacheFunction.Group, cacheFunction.Key, e)
				//}

			}
			_ = GlobalCacheMonitor.DeleteCache(ctx, "default")
		}()
	}

	wg.Wait()
}

func NewD(c *CacheTestMonitor) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		return c.Expected, nil
	}
}
