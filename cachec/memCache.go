package cachec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/orijtech/gomemcache/memcache"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var _ Cache = &MemCache{}

type MemCache struct {
	memcacheClient  *memcache.Client
	defaultDuration time.Duration
	cacheTags       CacheTags
	enabled         bool
}

func (c *MemCache) GetParentCaches() map[string]Cache {
	return map[string]Cache{}
}

func MemcacheFlags(prefix string) *pflag.FlagSet {
	fs := pflag.NewFlagSet(prefix+"memcache", pflag.ExitOnError)
	fs.StringSlice(prefix+"memcache-addrs", []string{}, "")
	fs.Bool(prefix+"memcache-enabled", false, "")
	fs.Duration(prefix+"memcache-default-duration", 1*time.Minute, "")
	return fs
}
func NewMemcacheFromFlags(prefix string) *MemCache {
	return NewMemcache(memcache.New(viper.GetStringSlice(prefix+"memcache-addrs")...), viper.GetDuration(prefix+"memcache-default-duration"), prefix, viper.GetBool(prefix+"memcache-enabled"))
}

func NewMemcache(cacher *memcache.Client, defaultDuration time.Duration, instance string, enabled bool) *MemCache {
	return &MemCache{
		memcacheClient:  cacher,
		defaultDuration: defaultDuration,
		cacheTags:       NewCacheTags("memcache", instance),
		enabled:         enabled,
	}
}

func (c *MemCache) GetName() string {
	return fmt.Sprintf("MEMCACHE_%s", c.cacheTags.instance)
}

func (c *MemCache) Close() {

}

func (c *MemCache) Ping(ctx context.Context) error {
	return nil
}

func (c *MemCache) DeleteKey(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}
	return c.memcacheClient.Delete(ctx, key)
}

func (c *MemCache) SetCache(ctx context.Context, group, key string, item interface{}) error {
	if !c.enabled {
		return nil
	}
	return c.SetCacheWithExpiration(ctx, c.defaultDuration, group, key, item)
}

func (c *MemCache) SetCacheWithExpiration(ctx context.Context, cacheTimeout time.Duration, group, key string, item interface{}) error {
	if !c.enabled {
		return nil
	}
	var cacheErr error
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
		s(cacheErr)
	}()
	data, err := json.Marshal(item)
	if err != nil {
		cacheErr = err
		return err
	}
	cacheErr = c.memcacheClient.Set(ctx, &memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: int32(cacheTimeout.Seconds()),
	})
	return cacheErr
}

func (c *MemCache) GetCache(ctx context.Context, group, key string) ([]byte, error) {
	if !c.enabled {
		return nil, ErrCacheMiss
	}
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

	it, err := c.memcacheClient.Get(ctx, key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		cacheErr = ErrCacheMiss
		return nil, ErrCacheMiss
	}
	if err != nil {
		cacheErr = err
		return nil, err
	}
	return it.Value, nil
}
