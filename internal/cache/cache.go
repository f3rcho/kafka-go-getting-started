package cache

import (
	"time"

	"github.com/patrickmn/go-cache"
)

type EventCache struct {
	cache *cache.Cache
}

func NewEventCache(defaultExpiration, cleanupInterval time.Duration) *EventCache {
	return &EventCache{
		cache: cache.New(defaultExpiration, cleanupInterval),
	}
}

func (c *EventCache) Set(key string, value interface{}) {
	c.cache.Set(key, value, cache.DefaultExpiration)
}

func (c *EventCache) Get(key string) (interface{}, bool) {
	return c.cache.Get(key)
}

func (c *EventCache) Delete(key string) {
	c.cache.Delete(key)
}
