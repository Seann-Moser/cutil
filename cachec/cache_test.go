package cachec

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
)

type cacheTestCase struct {
	Name           string
	Cache          Cache
	Key            string
	Group          string
	Value          string
	ExpectedOutput string
	ExpectedErr    error
}

func TestTieredCache(t *testing.T) {
	testCases := []cacheTestCase{
		{
			Name:           "go cache",
			Cache:          NewGoCache(cache.New(time.Minute, time.Minute), time.Minute, ""),
			Key:            "test_cache",
			Value:          "test",
			ExpectedOutput: "test",
			ExpectedErr:    nil,
		},
		{
			Name:           "go cache",
			Cache:          NewGoCache(cache.New(time.Minute, time.Minute), time.Minute, ""),
			Key:            "test_cache_fail",
			Value:          "",
			ExpectedOutput: "",
			ExpectedErr:    ErrCacheMiss,
		},
		{
			Name:           "go cache tiered",
			Cache:          NewTieredCache(nil, NewGoCache(cache.New(time.Minute, time.Minute), time.Minute, "")),
			Key:            "test_cache_fail",
			Value:          "",
			ExpectedOutput: "",
			ExpectedErr:    ErrCacheMiss,
		},
		{
			Name:  "go cache tiered",
			Cache: NewTieredCache(nil, NewGoCache(cache.New(time.Minute, time.Minute), time.Minute, "")),
			Key:   "test_cache",
			Value: "test",

			ExpectedOutput: "test",
		},
	}
	GlobalCacheMonitor = NewMonitor()
	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Value != "" {
				err := tc.Cache.SetCache(ctx, tc.Group, tc.Key, tc.Value)
				if err != nil {
					t.Errorf("failed setting cache:%s", err.Error())
					return
				}
			}
			value, err := tc.Cache.GetCache(ctx, tc.Group, tc.Key)
			if err != nil && !errors.Is(err, tc.ExpectedErr) {
				t.Errorf("failed getting cache:%s", err.Error())
				return
			}
			if tc.ExpectedErr != nil {
				return
			}
			if string(value) != tc.ExpectedOutput {
				t.Errorf("does not match expected output: %s != %s", tc.ExpectedOutput, string(value))
			}

			err = Set[string](ctx, tc.Group, tc.Key, tc.Value)
			if err != nil {
				t.Errorf("failed setting cache:%s", err.Error())
				return
			}

		})
	}

}

func TestGetKey(t *testing.T) {
	tests := []struct {
		name     string
		keys     []string
		expected string
	}{
		{
			name:     "Test with string type",
			keys:     []string{"key1", "key2"},
			expected: GetMD5Hash(fmt.Sprintf("%s_%s", "string", "key1_key2")),
		},
		{
			name:     "Test with int type",
			keys:     []string{"key1", "key2"},
			expected: GetMD5Hash(fmt.Sprintf("%s_%s", "int", "key1_key2")),
		},
		{
			name:     "Test with struct type",
			keys:     []string{"key1", "key2"},
			expected: GetMD5Hash(fmt.Sprintf("%s_%s", "ResponseData", "key1_key2")),
		},
		{
			name:     "Test with empty keys",
			keys:     []string{},
			expected: GetMD5Hash(fmt.Sprintf("%s_%s", "string", "")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			switch tt.name {
			case "Test with string type":
				result = GetKey[string](tt.keys...)
			case "Test with int type":
				result = GetKey[int](tt.keys...)
			case "Test with struct type":
				result = GetKey[ResponseData](tt.keys...)
			case "Test with empty keys":
				result = GetKey[string](tt.keys...)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetType(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "Test with nil",
			input:    nil,
			expected: "nil",
		},
		{
			name:     "Test with int",
			input:    123,
			expected: "int",
		},
		{
			name:     "Test with string",
			input:    "test",
			expected: "string",
		},
		{
			name:     "Test with struct",
			input:    ResponseData{},
			expected: "ResponseData",
		},
		{
			name:     "Test with pointer to struct",
			input:    &ResponseData{},
			expected: "ResponseData",
		},
		{
			name:     "Test with slice of pointers to struct",
			input:    []*ResponseData{},
			expected: "[]*cachec.ResponseData",
		},
		{
			name:     "Test with map",
			input:    map[string]int{},
			expected: "map[string]int",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSet(t *testing.T) {
	mockCache := NewGoCacheFromFlags("")
	mockCacheMonitor := NewMonitor()
	GlobalCacheMonitor = mockCacheMonitor

	ctx := ContextWithCache(context.Background(), mockCache)
	group := "testGroup"
	key := "testKey"
	data := "testData"
	//wrappedData := Wrapper[string]{Data: data}
	//cacheKey := GetKey[string](group, key)

	//mockCache.On("SetCache", ctx, group, cacheKey, wrappedData).Return(nil)
	//mockCacheMonitor.On("UpdateCache", ctx, group, key).Return(nil)

	err := Set(ctx, group, key, data)
	assert.NoError(t, err)

	//mockCache.AssertExpectations(t)
	//mockCacheMonitor.AssertExpectations(t)
}

func TestSet_GroupPrefix(t *testing.T) {
	mockCache := NewGoCacheFromFlags("")
	mockCacheMonitor := NewMonitor()
	GlobalCacheMonitor = mockCacheMonitor

	ctx := ContextWithCache(context.Background(), mockCache)
	group := GroupPrefix
	key := "testKey"
	data := "testData"
	//wrappedData := Wrapper[string]{Data: data}
	//cacheKey := GetKey[string](group, key)

	//mockCache.On("SetCache", ctx, group, cacheKey, wrappedData).Return(nil)

	err := Set(ctx, group, key, data)
	assert.NoError(t, err)

	//mockCache.AssertExpectations(t)
	//mockCacheMonitor.AssertNotCalled(t, "UpdateCache")
}

func TestDelete(t *testing.T) {
	mockCache := NewGoCacheFromFlags("")
	mockCacheMonitor := NewMonitor()
	GlobalCacheMonitor = mockCacheMonitor
	ctx := ContextWithCache(context.Background(), mockCache)
	group := "testGroup"
	key := "testKey"
	//cacheKey := GetKey[string](group, key)

	//mockCache.On("DeleteKey", ctx, cacheKey).Return(nil)

	err := Delete[string](ctx, group, key)
	assert.NoError(t, err)

	//mockCache.AssertExpectations(t)
}

func TestDeleteKey(t *testing.T) {
	mockCache := NewGoCacheFromFlags("")
	ctx := ContextWithCache(context.Background(), mockCache)
	key := "testKey"

	err := DeleteKey(ctx, key)
	assert.NoError(t, err)

}
