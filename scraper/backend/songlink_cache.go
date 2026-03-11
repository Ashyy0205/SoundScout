package backend

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// SongLinkCache is a concurrency-safe, disk-backed cache that maps Spotify track IDs to
// their resolved AllPlatformURLs. A cache hit skips the mandatory 7-second song.link
// rate-limit gap entirely, making repeated imports of known tracks near-instant.
type SongLinkCache struct {
	mu      sync.RWMutex
	entries map[string]*AllPlatformURLs
	path    string
}

// NewSongLinkCache creates (or loads from disk) a cache at the given file path.
// If the file does not exist the cache starts empty; a file is created on the first Put.
func NewSongLinkCache(path string) *SongLinkCache {
	c := &SongLinkCache{
		entries: make(map[string]*AllPlatformURLs),
		path:    path,
	}
	c.load()
	return c
}

// Get returns the cached AllPlatformURLs for the given Spotify track ID.
// Returns (nil, false) on a cache miss.
func (c *SongLinkCache) Get(spotifyID string) (*AllPlatformURLs, bool) {
	if spotifyID == "" {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[spotifyID]
	return e, ok
}

// Put stores platform URLs for a Spotify track ID and asynchronously saves the cache to disk.
func (c *SongLinkCache) Put(spotifyID string, urls *AllPlatformURLs) {
	if spotifyID == "" || urls == nil {
		return
	}
	c.mu.Lock()
	c.entries[spotifyID] = urls
	c.mu.Unlock()
	go c.save() // fire-and-forget — never blocks the caller
}

// Len returns the total number of cached entries.
func (c *SongLinkCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// load reads the JSON cache file from disk. Silently ignored if the file is absent.
func (c *SongLinkCache) load() {
	if c.path == "" {
		return
	}
	f, err := os.Open(c.path)
	if err != nil {
		return // new or missing — not an error
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&c.entries); err != nil {
		fmt.Fprintf(os.Stderr, "[songlink-cache] failed to decode cache at %s: %v — starting empty\n", c.path, err)
		c.entries = make(map[string]*AllPlatformURLs)
	}
}

// save atomically writes the cache to disk (write-to-temp then rename).
func (c *SongLinkCache) save() {
	if c.path == "" {
		return
	}
	c.mu.RLock()
	data, err := json.Marshal(c.entries)
	c.mu.RUnlock()
	if err != nil {
		return
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, c.path)
}
