package cachec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/patrickmn/go-cache"
)

var _ Cache = &GoCache{}

type GoCache struct {
	defaultDuration time.Duration
	cacher          *cache.Cache
	cacheTags       CacheTags
}

func (c *GoCache) GetName() string {
	return fmt.Sprintf("GOCACHE_%s", c.cacheTags.instance)
}

func (c *GoCache) GetParentCaches() map[string]Cache {
	return map[string]Cache{}
}

func GoCacheFlags(prefix string) *pflag.FlagSet {
	fs := pflag.NewFlagSet(prefix+"gocache", pflag.ExitOnError)
	fs.Duration(prefix+"gocache-default-duration", 5*time.Minute, "")
	fs.Duration(prefix+"gocache-cleanup-duration", 1*time.Minute, "")

	return fs
}
func NewGoCacheFromFlags(prefix string) *GoCache {
	return NewGoCache(cache.New(viper.GetDuration(prefix+"gocache-cleanup-duration"), viper.GetDuration(prefix+"gocache-default-duration")), viper.GetDuration(prefix+"gocache-default-duration"), prefix)
}

func NewGoCache(cacher *cache.Cache, defaultDuration time.Duration, instance string) *GoCache {
	return &GoCache{
		cacher:          cacher,
		defaultDuration: defaultDuration,
		cacheTags:       NewCacheTags("go-cache", instance),
	}
}

func (c *GoCache) DeleteKey(ctx context.Context, key string) error {
	c.cacher.Delete(key)
	return nil
}

func (c *GoCache) Ping(ctx context.Context) error {
	return nil
}

func (c *GoCache) Close() {

}
func (c *GoCache) SetCacheWithExpiration(ctx context.Context, cacheTimeout time.Duration, group, key string, item interface{}) error {
	var err error
	s := c.cacheTags.record(ctx, CacheCmdSET, func(err error) CacheStatus {
		if errors.Is(err, ErrCacheMiss) {
			return CacheStatusMISSING
		}
		if err != nil {
			return CacheStatusERR
		}
		return CacheStatusOK
	})

	defer func() {
		s(err)
	}()

	c.cacher.Set(key, item, cacheTimeout)
	return nil
}

func (c *GoCache) SetCache(ctx context.Context, group, key string, item interface{}) error {
	return c.SetCacheWithExpiration(ctx, c.defaultDuration, group, key, item)
}

func (c *GoCache) GetCache(ctx context.Context, group, key string) ([]byte, error) {
	var cacheErr error
	s := c.cacheTags.record(ctx, CacheCmdGET, func(err error) CacheStatus {
		if errors.Is(err, ErrCacheMiss) {
			return CacheStatusMISSING
		}
		if err != nil {
			return CacheStatusERR
		}
		return CacheStatusFOUND
	})
	defer func() {
		s(cacheErr)
	}()
	if data, found := c.cacher.Get(key); !found {
		cacheErr = ErrCacheMiss
		return nil, ErrCacheMiss
	} else {
		switch v := data.(type) {
		case string:
			return []byte(v), nil
		default:
			b, err := json.Marshal(data)
			if err != nil {
				cacheErr = err
				return nil, err
			}
			return b, nil
		}
	}
}
