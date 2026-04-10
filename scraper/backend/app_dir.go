package backend

import (
	"fmt"
	"os"
)

// EnsureAppDir returns the persistent data directory used by the scraper for caches and
// databases.  In Docker (IS_DOCKER env var set) it is /config; locally it is the current
// working directory.  The directory is created if it does not already exist.
func EnsureAppDir() (string, error) {
	var dir string
	if os.Getenv("IS_DOCKER") != "" {
		dir = "/config"
	} else {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create app dir %q: %w", dir, err)
	}
	return dir, nil
}
