package cachec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	redis "github.com/Seann-Moser/ociredis"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var _ Cache = &RedisCache{}

type RedisCache struct {
	cacher          *redis.Client
	defaultDuration time.Duration
	cacheTags       CacheTags
	enabled         bool
}

func (c *RedisCache) GetParentCaches() map[string]Cache {
	return map[string]Cache{}
}

func RedisFlags(prefix string) *pflag.FlagSet {
	fs := pflag.NewFlagSet(prefix+"redis", pflag.ExitOnError)
	fs.String(prefix+"redis-addr", "", "")
	fs.String(prefix+"redis-pass", "", "")
	fs.Bool(prefix+"redis-enabled", false, "")
	fs.String(prefix+"redis-instance", "default", "")
	fs.Duration(prefix+"redis-cleanup-duration", 1*time.Minute, "")

	return fs
}
func NewRedisCacheFromFlags(ctx context.Context, prefix string) *RedisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     viper.GetString(prefix + "redis-addr"),
		Password: viper.GetString(prefix + "redis-pass"),
		Context:  ctx,
	})

	return NewRedisCache(rdb, viper.GetDuration(prefix+"redis-cleanup-duration"), viper.GetString(prefix+"redis-instance"), viper.GetBool(prefix+"redis-enabled"))
}

func NewRedisCache(cacher *redis.Client, defaultDuration time.Duration, instance string, enabled bool) *RedisCache {

	return &RedisCache{
		cacher:          cacher,
		defaultDuration: defaultDuration,
		cacheTags:       NewCacheTags("redis", instance),
		enabled:         enabled,
	}
}
func (c *RedisCache) Close() {
	_ = c.cacher.Close()
}
func (c *RedisCache) GetName() string {
	return fmt.Sprintf("REDISCACHE_%s", c.cacheTags.instance)
}
func (c *RedisCache) DeleteKey(ctx context.Context, key string) error {
	return c.cacher.Del(key).Err()
}
func (c *RedisCache) SetCacheWithExpiration(ctx context.Context, cacheTimeout time.Duration, group, key string, item interface{}) error {
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
		cacheErr = ErrCacheMiss
		return err
	}
	localClient := c.cacher.WithContext(ctx)
	stats := localClient.Set(key, data, cacheTimeout)
	cacheErr = stats.Err()
	return stats.Err()
}

func (c *RedisCache) SetCache(ctx context.Context, group, key string, item interface{}) error {
	return c.SetCacheWithExpiration(ctx, c.defaultDuration, group, key, item)
}

func (c *RedisCache) GetCache(ctx context.Context, group, key string) ([]byte, error) {
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

	localClient := c.cacher.WithContext(ctx)
	data, err := localClient.Get(key).Bytes()
	if err != nil {
		cacheErr = err
		return nil, err
	}
	if len(data) == 0 {
		cacheErr = ErrCacheMiss
		return nil, ErrCacheMiss
	}
	return data, nil
}

func (c *RedisCache) Ping(ctx context.Context) error {
	localClient := c.cacher.WithContext(ctx)
	return localClient.Ping().Err()
}
