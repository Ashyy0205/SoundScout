package backend

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

const acoustidAPIBase = "https://api.acoustid.org/v2"

var acoustidHTTPClient = &http.Client{Timeout: 15 * time.Second}

// acoustidResult is the relevant subset of an AcoustID lookup response.
type acoustidResult struct {
	// ID is the AcoustID fingerprint UUID (not a MusicBrainz ID).
	ID string
	// Recordings holds the MusicBrainz recording candidates returned by AcoustID.
	Recordings []mbRecordingCandidate
}

type mbRecordingCandidate struct {
	ID      string // MusicBrainz recording MBID
	Artists []struct {
		ID string
	}
	Releasegroups []struct {
		Releases []struct {
			ID string
		}
	}
}

// fingerprintFile generates an AcoustID fingerprint from the audio file at
// filePath using ffmpeg's built-in chromaprint muxer.  It returns the
// fingerprint string and the duration in seconds, or an error.
//
// ffmpeg must be available on PATH and compiled with chromaprint support.
func fingerprintFile(filePath string) (fingerprint string, duration int, err error) {
	ffmpegBin, err := GetFFmpegPath()
	if err != nil {
		return "", 0, fmt.Errorf("acoustid: ffmpeg not found: %w", err)
	}
	// -f chromaprint -fp_format raw writes only the raw base64 fingerprint bytes.
	cmd := exec.Command(
		ffmpegBin,
		"-hide_banner", "-loglevel", "error",
		"-i", filePath,
		"-f", "chromaprint", "-fp_format", "raw",
		"pipe:1",
	)
	out, err := cmd.Output()
	if err != nil {
		return "", 0, fmt.Errorf("acoustid: ffmpeg fingerprint failed: %w", err)
	}
	fp := strings.TrimSpace(string(out))
	if fp == "" {
		return "", 0, fmt.Errorf("acoustid: empty fingerprint from ffmpeg")
	}

	// Obtain duration via ffprobe so we can pass it to AcoustID.
	dur, err := probeDuration(filePath)
	if err != nil {
		return "", 0, err
	}
	return fp, dur, nil
}

// probeDuration returns the audio duration in whole seconds using ffprobe.
func probeDuration(filePath string) (int, error) {
	ffprobeBin, err := GetFFprobePath()
	if err != nil {
		return 0, fmt.Errorf("acoustid: ffprobe not found: %w", err)
	}
	cmd := exec.Command(
		ffprobeBin,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filePath,
	)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("acoustid: ffprobe duration failed: %w", err)
	}
	raw := strings.TrimSpace(string(out))
	var secs float64
	if _, err := fmt.Sscanf(raw, "%f", &secs); err != nil {
		return 0, fmt.Errorf("acoustid: could not parse duration %q", raw)
	}
	return int(secs), nil
}

// LookupAcoustID queries the AcoustID service for the given fingerprint+duration
// pair and returns the best MBRecording candidate.
//
// apiKey must come from the ACOUSTID_API_KEY environment variable.
// Returns nil, nil (not an error) when the API key is absent or no match is found.
func LookupAcoustID(filePath string) (*MBRecording, error) {
	apiKey := os.Getenv("ACOUSTID_API_KEY")
	if apiKey == "" {
		return nil, nil // AcoustID disabled — no key configured
	}

	fp, dur, err := fingerprintFile(filePath)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf(
		"%s/lookup?client=%s&duration=%d&fingerprint=%s&meta=recordings+releasegroups+compress",
		acoustidAPIBase,
		url.QueryEscape(apiKey),
		dur,
		url.QueryEscape(fp),
	)

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", mbUserAgent) // reuse SoundScout user-agent

	resp, err := acoustidHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("acoustid: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return parseAcoustIDResponse(body)
}

// parseAcoustIDResponse picks the best MBRecording from an AcoustID JSON response.
func parseAcoustIDResponse(body []byte) (*MBRecording, error) {
	var envelope struct {
		Status  string `json:"status"`
		Results []struct {
			Score      float64 `json:"score"`
			Recordings []struct {
				ID      string `json:"id"`
				Artists []struct {
					ID string `json:"id"`
				} `json:"artists"`
				Releasegroups []struct {
					Releases []struct {
						ID string `json:"id"`
					} `json:"releases"`
				} `json:"releasegroups"`
			} `json:"recordings"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("acoustid: JSON parse error: %w", err)
	}
	if envelope.Status != "ok" {
		return nil, fmt.Errorf("acoustid: status=%s", envelope.Status)
	}

	// Find the highest-scoring result that has at least one recording with a MBID.
	for _, result := range envelope.Results {
		if result.Score < 0.5 {
			continue // too low confidence
		}
		for _, rec := range result.Recordings {
			if rec.ID == "" {
				continue
			}
			out := &MBRecording{TrackID: rec.ID}
			if len(rec.Artists) > 0 {
				out.ArtistID = rec.Artists[0].ID
			}
			if len(rec.Releasegroups) > 0 && len(rec.Releasegroups[0].Releases) > 0 {
				out.AlbumID = rec.Releasegroups[0].Releases[0].ID
			}
			return out, nil
		}
	}
	return nil, nil
}

// reISRC matches a valid 12-character ISRC (letters+digits, no hyphens).
var reISRC = regexp.MustCompile(`^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$`)

// isValidISRC reports whether s is a normalised (no hyphens, uppercase) ISRC.
func isValidISRC(s string) bool {
	return reISRC.MatchString(strings.ToUpper(strings.ReplaceAll(s, "-", "")))
}
