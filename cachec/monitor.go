package cachec

import (
	"context"
	"fmt"
	"github.com/Seann-Moser/cutil/logc"
	"github.com/google/uuid"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var GlobalCacheMonitor CacheMonitor = NewMonitor()

const GroupPrefix = "[CTX_CACHE_GROUP]"

type CacheMonitor interface {
	AddGroupKeys(ctx context.Context, group string, newKeys ...string) error
	HasGroupKeyBeenUpdated(ctx context.Context, group string) bool
	GetGroupKeys(ctx context.Context, group string) (map[string]struct{}, error)
	DeleteCache(ctx context.Context, group string) error
	UpdateCache(ctx context.Context, group string, key string) error
	WaitForTransaction(ctx context.Context, group string, read bool)
	StartTransaction(ctx context.Context, group string, duration time.Duration, read bool) (string, context.Context, context.CancelFunc)
	EndTransaction(ctx context.Context, id string, read bool)
}

type CacheMonitorImpl struct {
	cleanDuration time.Duration
	Mutex         *sync.RWMutex
	groupKeys     map[string]int64

	txMutex            *sync.RWMutex
	transactionMonitor map[string]*TransactionMonitor
}

type TransactionMonitor struct {
	Group string
	Mutex *sync.RWMutex
}

func NewMonitor() CacheMonitor {
	return &CacheMonitorImpl{
		cleanDuration:      time.Minute,
		groupKeys:          make(map[string]int64),
		Mutex:              &sync.RWMutex{},
		txMutex:            &sync.RWMutex{},
		transactionMonitor: make(map[string]*TransactionMonitor),
	}
}

func (c *CacheMonitorImpl) UpdateCache(ctx context.Context, group string, key string) error {
	err := c.AddGroupKeys(ctx, group, key)
	if err != nil {
		return err
	}
	now := time.Now()
	groupKey := fmt.Sprintf("%s_%s_updated", GroupPrefix, group)
	c.setGroupKeys(groupKey, now.Unix())
	err = SetWithExpiration[int64](ctx, 60*time.Minute, GroupPrefix, groupKey, now.Unix())
	if err != nil {
		return err
	}
	logc.Debug(ctx, "setting cache", zap.String("group", group), zap.String("key", key))
	return nil
}

func (c *CacheMonitorImpl) DeleteCache(ctx context.Context, group string) error {
	keys, err := c.GetGroupKeys(ctx, group)
	if err != nil {
		return err
	}
	for k := range keys {
		err = multierr.Combine(err, DeleteKey(ctx, k))
	}
	return err
}

func (c *CacheMonitorImpl) GetGroupKeys(ctx context.Context, group string) (map[string]struct{}, error) {
	key := fmt.Sprintf("%s_%s_keys", GroupPrefix, group)
	keys, err := Get[map[string]struct{}](ctx, GroupPrefix, key)
	var foundKeys map[string]struct{}
	if err != nil {
		return nil, err
	} else {
		foundKeys = *keys
	}
	return foundKeys, nil
}

func (c *CacheMonitorImpl) AddGroupKeys(ctx context.Context, group string, newKeys ...string) error {
	if len(newKeys) == 0 {
		return nil
	}
	key := fmt.Sprintf("%s_%s_keys", GroupPrefix, group)
	keys, err := Get[map[string]struct{}](ctx, GroupPrefix, key)
	var foundKeys map[string]struct{}
	if err != nil || keys == nil {
		foundKeys = map[string]struct{}{}
	} else {
		foundKeys = *keys
	}
	if foundKeys == nil {
		foundKeys = map[string]struct{}{}
	}
	for _, k := range newKeys {
		if foundKeys == nil {
			return nil
		}
		foundKeys[k] = struct{}{}
	}
	return SetWithExpiration[map[string]struct{}](ctx, 60*time.Minute, GroupPrefix, key, foundKeys)
}

// HasGroupKeyBeenUpdated is the time does not match then the key value has been updated, if it has been updated invalidate all cache
func (c *CacheMonitorImpl) HasGroupKeyBeenUpdated(ctx context.Context, group string) bool {
	if group == GroupPrefix {
		return false
	}
	key := fmt.Sprintf("%s_%s_updated", GroupPrefix, group)
	lastUpdated, err := Get[int64](ctx, GroupPrefix, key)
	if err != nil {
		logc.Debug(ctx, "failed getting last updated group", zap.Error(err))
		err = SetWithExpiration[int64](ctx, 60*time.Minute, GroupPrefix, key, time.Now().Unix())
		if err != nil {
			return true
		}
		return true
	}
	for _, c := range GetCacheFromContext(ctx).GetParentCaches() {
		i, err := GetFromCache[int64](ctx, c, GroupPrefix, key)
		if err != nil || *i != *lastUpdated {
			if i != nil {
				logc.Debug(ctx, "last updated does not match", zap.Int64("lastUpdated", *lastUpdated), zap.Int64("cache", *i), zap.Error(err))
			}
			return true
		}
	}
	if c.findGroupKey(key, *lastUpdated) {
		return false
	}
	logc.Debug(ctx, "last updated does not match group", zap.Int64("last_updated", *lastUpdated))
	c.setGroupKeys(key, *lastUpdated)
	return true
}

func (c *CacheMonitorImpl) setGroupKeys(key string, lastUpdated int64) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.groupKeys[key] = lastUpdated
}

func (c *CacheMonitorImpl) findGroupKey(key string, lastUpdated int64) bool {
	c.Mutex.RLock()
	defer c.Mutex.RUnlock()
	if v, found := c.groupKeys[key]; found && v == lastUpdated {
		return true
	}
	return false
}

func (c *CacheMonitorImpl) WaitForTransaction(ctx context.Context, group string, read bool) {
	t, ok := c.transactionMonitor[group]
	if !ok {
		return
	}
	unlockChan := make(chan struct{})

	go func() {
		if read {
			t.Mutex.RLock()
			defer t.Mutex.RUnlock()
		} else {
			t.Mutex.Lock()
			defer t.Mutex.Unlock()
		}
		close(unlockChan)
	}()

	select {
	case <-unlockChan:
		// Mutex was unlocked
		return
	case <-ctx.Done():
		// Context was finished
		return
	}
}
func (c *CacheMonitorImpl) StartTransaction(ctx context.Context, group string, duration time.Duration, read bool) (string, context.Context, context.CancelFunc) {
	k := fmt.Sprintf("%s_%s", group, uuid.New().String())
	c.txMutex.Lock()
	defer c.txMutex.Unlock()
	if _, found := c.transactionMonitor[group]; !found {
		c.transactionMonitor[group] = &TransactionMonitor{
			Group: group,
			Mutex: &sync.RWMutex{},
		}
	}

	if duration == 0 {
		if read {
			c.transactionMonitor[group].Mutex.RLock()
		} else {
			c.transactionMonitor[group].Mutex.Lock()
		}
		return k, ctx, func() {
		}
	}
	tm, cf := context.WithTimeout(ctx, duration)
	if read {
		c.transactionMonitor[group].Mutex.RLock()
	} else {
		c.transactionMonitor[group].Mutex.Lock()
	}
	return k, tm, cf
}

func (c *CacheMonitorImpl) EndTransaction(ctx context.Context, group string, read bool) {
	t, ok := c.transactionMonitor[group]
	if !ok {
		return
	}
	if read {
		t.Mutex.RUnlock()
	} else {
		t.Mutex.Unlock()
		t.Mutex.Lock()
	}
}
