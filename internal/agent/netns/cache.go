package netns

import (
	"context"
	"os"
	"sync"
)

// Cache memoises resolutions and drops an entry as soon as its namespace file
// is gone. A sandbox recreate produces a new netns path, so the stale entry is
// detected on the next use rather than being trusted indefinitely.
type Cache struct {
	inner Resolver

	mu sync.Mutex
	m  map[string]Handle
}

// NewCache wraps a resolver.
func NewCache(inner Resolver) *Cache {
	return &Cache{inner: inner, m: make(map[string]Handle)}
}

// Resolve returns a cached handle when its netns still exists, otherwise it
// re-resolves through the runtime.
func (c *Cache) Resolve(ctx context.Context, podUID string) (Handle, error) {
	c.mu.Lock()
	h, ok := c.m[podUID]
	c.mu.Unlock()

	if ok {
		if _, err := os.Stat(h.Path); err == nil {
			return h, nil
		}
		c.Forget(podUID)
	}

	h, err := c.inner.Resolve(ctx, podUID)
	if err != nil {
		return Handle{}, err
	}
	c.mu.Lock()
	c.m[podUID] = h
	c.mu.Unlock()
	return h, nil
}

// Forget drops a Pod's cached handle, e.g. once the Pod is gone.
func (c *Cache) Forget(podUID string) {
	c.mu.Lock()
	delete(c.m, podUID)
	c.mu.Unlock()
}

// Len reports how many handles are cached.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}
