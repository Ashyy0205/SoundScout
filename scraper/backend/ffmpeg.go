package backend

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func ValidateExecutable(path string) error {
	cleanedPath := filepath.Clean(path)
	if cleanedPath == "" {
		return fmt.Errorf("empty path")
	}

	if !filepath.IsAbs(cleanedPath) {
		return fmt.Errorf("path must be absolute: %s", path)
	}

	info, err := os.Stat(cleanedPath)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if info.IsDir() {
		return fmt.Errorf("path is a directory: %s", path)
	}

	if runtime.GOOS != "windows" {
		if info.Mode()&0111 == 0 {
			return fmt.Errorf("file is not executable: %s", path)
		}
	}

	base := filepath.Base(cleanedPath)
	validNames := map[string]bool{
		"ffmpeg":      true,
		"ffmpeg.exe":  true,
		"ffprobe":     true,
		"ffprobe.exe": true,
	}
	if !validNames[base] {
		return fmt.Errorf("invalid executable name: %s", base)
	}

	return nil
}

func GetFFmpegDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	return filepath.Join(homeDir, ".scraper"), nil
}

func GetFFmpegPath() (string, error) {
	ffmpegName := "ffmpeg"
	if runtime.GOOS == "windows" {
		ffmpegName = "ffmpeg.exe"
	}

	// 1. Check the dedicated app directory (~/.scraper/).
	if ffmpegDir, err := GetFFmpegDir(); err == nil {
		candidate := filepath.Join(ffmpegDir, ffmpegName)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// 2. Fall back to PATH (e.g. system-installed ffmpeg).
	if pathBin, err := exec.LookPath(ffmpegName); err == nil {
		return pathBin, nil
	}

	return "", fmt.Errorf("ffmpeg not found: place ffmpeg in ~/.scraper/ or ensure it is on PATH")
}

func GetFFprobePath() (string, error) {
	ffprobeName := "ffprobe"
	if runtime.GOOS == "windows" {
		ffprobeName = "ffprobe.exe"
	}

	// 1. Check the dedicated app directory.
	if ffmpegDir, err := GetFFmpegDir(); err == nil {
		candidate := filepath.Join(ffmpegDir, ffprobeName)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// 2. Fall back to PATH.
	if pathBin, err := exec.LookPath(ffprobeName); err == nil {
		return pathBin, nil
	}

	return "", fmt.Errorf("ffprobe not found in app directory or PATH")
}
