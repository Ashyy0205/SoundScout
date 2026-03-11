package backend

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type SongLinkClient struct {
	client           *http.Client
	lastAPICallTime  time.Time
	apiCallCount     int
	apiCallResetTime time.Time
}

type SongLinkURLs struct {
	TidalURL  string `json:"tidal_url"`
	AmazonURL string `json:"amazon_url"`
}

type TrackAvailability struct {
	SpotifyID string `json:"spotify_id"`
	Tidal     bool   `json:"tidal"`
	Amazon    bool   `json:"amazon"`
	Qobuz     bool   `json:"qobuz"`
	TidalURL  string `json:"tidal_url,omitempty"`
	AmazonURL string `json:"amazon_url,omitempty"`
	QobuzURL  string `json:"qobuz_url,omitempty"`
}

func NewSongLinkClient() *SongLinkClient {
	return &SongLinkClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		apiCallResetTime: time.Now(),
	}
}

func (s *SongLinkClient) GetAllURLsFromSpotify(spotifyTrackID string) (*SongLinkURLs, error) {

	now := time.Now()
	if now.Sub(s.apiCallResetTime) >= time.Minute {
		s.apiCallCount = 0
		s.apiCallResetTime = now
	}

	if s.apiCallCount >= 9 {
		waitTime := time.Minute - now.Sub(s.apiCallResetTime)
		if waitTime > 0 {
			fmt.Fprintf(os.Stderr, "Rate limit reached, waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
			s.apiCallCount = 0
			s.apiCallResetTime = time.Now()
		}
	}

	if !s.lastAPICallTime.IsZero() {
		timeSinceLastCall := now.Sub(s.lastAPICallTime)
		minDelay := 6 * time.Second
		if timeSinceLastCall < minDelay {
			waitTime := minDelay - timeSinceLastCall
			fmt.Fprintf(os.Stderr, "Rate limiting: waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
		}
	}

	spotifyBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL3RyYWNrLw==")
	spotifyURL := fmt.Sprintf("%s%s", string(spotifyBase), spotifyTrackID)

	apiBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9hcGkuc29uZy5saW5rL3YxLWFscGhhLjEvbGlua3M/dXJsPQ==")
	apiURL := fmt.Sprintf("%s%s", string(apiBase), url.QueryEscape(spotifyURL))

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	fmt.Fprintln(os.Stderr, "Getting streaming URLs from song.link...")

	maxRetries := 3
	var resp *http.Response
	for i := 0; i < maxRetries; i++ {
		resp, err = s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to get URLs: %w", err)
		}

		s.lastAPICallTime = time.Now()
		s.apiCallCount++

		if resp.StatusCode == 429 {
			resp.Body.Close()
			if i < maxRetries-1 {
				waitTime := 15 * time.Second
				fmt.Fprintf(os.Stderr, "Rate limited by API, waiting %v before retry...\n", waitTime)
				time.Sleep(waitTime)
				continue
			}
			return nil, fmt.Errorf("API rate limit exceeded after %d retries", maxRetries)
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		break
	}
	defer resp.Body.Close()

	var songLinkResp struct {
		LinksByPlatform map[string]struct {
			URL string `json:"url"`
		} `json:"linksByPlatform"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if len(body) == 0 {
		return nil, fmt.Errorf("API returned empty response")
	}

	if err := json.Unmarshal(body, &songLinkResp); err != nil {

		bodyStr := string(body)
		if len(bodyStr) > 200 {
			bodyStr = bodyStr[:200] + "..."
		}
		return nil, fmt.Errorf("failed to decode response: %w (response: %s)", err, bodyStr)
	}

	urls := &SongLinkURLs{}

	if tidalLink, ok := songLinkResp.LinksByPlatform["tidal"]; ok && tidalLink.URL != "" {
		urls.TidalURL = tidalLink.URL
		fmt.Fprintf(os.Stderr, "✓ Tidal URL found\n")
	}

	if amazonLink, ok := songLinkResp.LinksByPlatform["amazonMusic"]; ok && amazonLink.URL != "" {
		amazonURL := amazonLink.URL

		if len(amazonURL) > 0 {
			urls.AmazonURL = amazonURL
			fmt.Fprintf(os.Stderr, "✓ Amazon URL found\n")
		}
	}

	if urls.TidalURL == "" && urls.AmazonURL == "" {
		return nil, fmt.Errorf("no streaming URLs found")
	}

	return urls, nil
}

func (s *SongLinkClient) CheckTrackAvailability(spotifyTrackID string, isrc string) (*TrackAvailability, error) {

	now := time.Now()
	if now.Sub(s.apiCallResetTime) >= time.Minute {
		s.apiCallCount = 0
		s.apiCallResetTime = now
	}

	if s.apiCallCount >= 9 {
		waitTime := time.Minute - now.Sub(s.apiCallResetTime)
		if waitTime > 0 {
			fmt.Fprintf(os.Stderr, "Rate limit reached, waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
			s.apiCallCount = 0
			s.apiCallResetTime = time.Now()
		}
	}

	if !s.lastAPICallTime.IsZero() {
		timeSinceLastCall := now.Sub(s.lastAPICallTime)
		minDelay := 6 * time.Second
		if timeSinceLastCall < minDelay {
			waitTime := minDelay - timeSinceLastCall
			fmt.Fprintf(os.Stderr, "Rate limiting: waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
		}
	}

	spotifyBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL3RyYWNrLw==")
	spotifyURL := fmt.Sprintf("%s%s", string(spotifyBase), spotifyTrackID)

	apiBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9hcGkuc29uZy5saW5rL3YxLWFscGhhLjEvbGlua3M/dXJsPQ==")
	apiURL := fmt.Sprintf("%s%s", string(apiBase), url.QueryEscape(spotifyURL))

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Checking availability for track: %s\n", spotifyTrackID)

	maxRetries := 3
	var resp *http.Response
	for i := 0; i < maxRetries; i++ {
		resp, err = s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to check availability: %w", err)
		}

		s.lastAPICallTime = time.Now()
		s.apiCallCount++

		if resp.StatusCode == 429 {
			resp.Body.Close()
			if i < maxRetries-1 {
				waitTime := 15 * time.Second
				fmt.Fprintf(os.Stderr, "Rate limited by API, waiting %v before retry...\n", waitTime)
				time.Sleep(waitTime)
				continue
			}
			return nil, fmt.Errorf("API rate limit exceeded after %d retries", maxRetries)
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		break
	}
	defer resp.Body.Close()

	var songLinkResp struct {
		LinksByPlatform map[string]struct {
			URL string `json:"url"`
		} `json:"linksByPlatform"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if len(body) == 0 {
		return nil, fmt.Errorf("API returned empty response")
	}

	if err := json.Unmarshal(body, &songLinkResp); err != nil {

		bodyStr := string(body)
		if len(bodyStr) > 200 {
			bodyStr = bodyStr[:200] + "..."
		}
		return nil, fmt.Errorf("failed to decode response: %w (response: %s)", err, bodyStr)
	}

	availability := &TrackAvailability{
		SpotifyID: spotifyTrackID,
	}

	if tidalLink, ok := songLinkResp.LinksByPlatform["tidal"]; ok && tidalLink.URL != "" {
		availability.Tidal = true
		availability.TidalURL = tidalLink.URL
	}

	if amazonLink, ok := songLinkResp.LinksByPlatform["amazonMusic"]; ok && amazonLink.URL != "" {
		availability.Amazon = true
		availability.AmazonURL = amazonLink.URL
	}

	if deezerLink, ok := songLinkResp.LinksByPlatform["deezer"]; ok && deezerLink.URL != "" {
		deezerURL := deezerLink.URL

		deezerISRC, err := GetDeezerISRC(deezerURL)
		if err == nil && deezerISRC != "" {
			qobuzAvailable := checkQobuzAvailability(deezerISRC)
			availability.Qobuz = qobuzAvailable
		}
	}

	return availability, nil
}

func checkQobuzAvailability(isrc string) bool {
	client := &http.Client{Timeout: 10 * time.Second}
	appID := "798273057"

	apiBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly93d3cucW9idXouY29tL2FwaS5qc29uLzAuMi90cmFjay9zZWFyY2g/cXVlcnk9")
	searchURL := fmt.Sprintf("%s%s&limit=1&app_id=%s", string(apiBase), isrc, appID)

	resp, err := client.Get(searchURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}

	var searchResp struct {
		Tracks struct {
			Total int `json:"total"`
		} `json:"tracks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return false
	}

	return searchResp.Tracks.Total > 0
}

func (s *SongLinkClient) GetDeezerURLFromSpotify(spotifyTrackID string) (string, error) {

	now := time.Now()
	if now.Sub(s.apiCallResetTime) >= time.Minute {
		s.apiCallCount = 0
		s.apiCallResetTime = now
	}

	if s.apiCallCount >= 9 {
		waitTime := time.Minute - now.Sub(s.apiCallResetTime)
		if waitTime > 0 {
			fmt.Fprintf(os.Stderr, "Rate limit reached, waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
			s.apiCallCount = 0
			s.apiCallResetTime = time.Now()
		}
	}

	if !s.lastAPICallTime.IsZero() {
		timeSinceLastCall := now.Sub(s.lastAPICallTime)
		minDelay := 6 * time.Second
		if timeSinceLastCall < minDelay {
			waitTime := minDelay - timeSinceLastCall
			fmt.Fprintf(os.Stderr, "Rate limiting: waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
		}
	}

	spotifyBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL3RyYWNrLw==")
	spotifyURL := fmt.Sprintf("%s%s", string(spotifyBase), spotifyTrackID)

	apiBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9hcGkuc29uZy5saW5rL3YxLWFscGhhLjEvbGlua3M/dXJsPQ==")
	apiURL := fmt.Sprintf("%s%s", string(apiBase), url.QueryEscape(spotifyURL))

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	fmt.Fprintln(os.Stderr, "Getting Deezer URL from song.link...")

	maxRetries := 3
	var resp *http.Response
	for i := 0; i < maxRetries; i++ {
		resp, err = s.client.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to get Deezer URL: %w", err)
		}

		s.lastAPICallTime = time.Now()
		s.apiCallCount++

		if resp.StatusCode == 429 {
			resp.Body.Close()
			if i < maxRetries-1 {
				waitTime := 15 * time.Second
				fmt.Fprintf(os.Stderr, "Rate limited by API, waiting %v before retry...\n", waitTime)
				time.Sleep(waitTime)
				continue
			}
			return "", fmt.Errorf("API rate limit exceeded after %d retries", maxRetries)
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			return "", fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		break
	}
	defer resp.Body.Close()

	var songLinkResp struct {
		LinksByPlatform map[string]struct {
			URL string `json:"url"`
		} `json:"linksByPlatform"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&songLinkResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	deezerLink, ok := songLinkResp.LinksByPlatform["deezer"]
	if !ok || deezerLink.URL == "" {
		return "", fmt.Errorf("deezer link not found")
	}

	deezerURL := deezerLink.URL
	fmt.Fprintf(os.Stderr, "Found Deezer URL: %s\n", deezerURL)
	return deezerURL, nil
}

func GetDeezerISRC(deezerURL string) (string, error) {

	var trackID string
	if strings.Contains(deezerURL, "/track/") {
		parts := strings.Split(deezerURL, "/track/")
		if len(parts) > 1 {
			trackID = strings.Split(parts[1], "?")[0]
			trackID = strings.TrimSpace(trackID)
		}
	}

	if trackID == "" {
		return "", fmt.Errorf("could not extract track ID from Deezer URL: %s", deezerURL)
	}

	apiURL := fmt.Sprintf("https://api.deezer.com/track/%s", trackID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", fmt.Errorf("failed to call Deezer API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("Deezer API returned status %d", resp.StatusCode)
	}

	var deezerTrack struct {
		ID    int64  `json:"id"`
		ISRC  string `json:"isrc"`
		Title string `json:"title"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&deezerTrack); err != nil {
		return "", fmt.Errorf("failed to decode Deezer API response: %w", err)
	}

	if deezerTrack.ISRC == "" {
		return "", fmt.Errorf("ISRC not found in Deezer API response for track %s", trackID)
	}

	fmt.Fprintf(os.Stderr, "Found ISRC from Deezer: %s (track: %s)\n", deezerTrack.ISRC, deezerTrack.Title)
	return deezerTrack.ISRC, nil
}

// AllPlatformURLs holds streaming URLs for every available platform, resolved in a single
// song.link API call. DeezerISRC is fetched via the free Deezer public API (not rate-limited)
// and is used as the lookup key for Qobuz.
type AllPlatformURLs struct {
	TidalURL   string
	AmazonURL  string
	DeezerURL  string
	DeezerISRC string // fetched via Deezer public API — enables Qobuz downloads
}

// GetAllPlatformURLs resolves every available streaming platform URL for a Spotify track ID
// using a *single* song.link API call and returns them together in AllPlatformURLs.
// It then makes a supplementary (non-rate-limited) Deezer public API call to retrieve the ISRC
// required for Qobuz. This eliminates the 2–3 redundant song.link calls that were previously
// made by each individual service downloader.
func (s *SongLinkClient) GetAllPlatformURLs(spotifyTrackID string) (*AllPlatformURLs, error) {
	// --- Shared rate limiting (same thresholds as the other methods) ---
	now := time.Now()
	if now.Sub(s.apiCallResetTime) >= time.Minute {
		s.apiCallCount = 0
		s.apiCallResetTime = now
	}
	if s.apiCallCount >= 9 {
		waitTime := time.Minute - now.Sub(s.apiCallResetTime)
		if waitTime > 0 {
			fmt.Fprintf(os.Stderr, "Rate limit reached, waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
			s.apiCallCount = 0
			s.apiCallResetTime = time.Now()
		}
	}
	if !s.lastAPICallTime.IsZero() {
		timeSinceLastCall := now.Sub(s.lastAPICallTime)
		minDelay := 6 * time.Second
		if timeSinceLastCall < minDelay {
			waitTime := minDelay - timeSinceLastCall
			fmt.Fprintf(os.Stderr, "Rate limiting: waiting %v...\n", waitTime.Round(time.Second))
			time.Sleep(waitTime)
		}
	}

	spotifyBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL3RyYWNrLw==")
	spotifyURL := fmt.Sprintf("%s%s", string(spotifyBase), spotifyTrackID)

	apiBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9hcGkuc29uZy5saW5rL3YxLWFscGhhLjEvbGlua3M/dXJsPQ==")
	apiURL := fmt.Sprintf("%s%s", string(apiBase), url.QueryEscape(spotifyURL))

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Resolving all platform URLs for %s...\n", spotifyTrackID)

	maxRetries := 3
	var resp *http.Response
	for i := 0; i < maxRetries; i++ {
		resp, err = s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to contact song.link: %w", err)
		}

		s.lastAPICallTime = time.Now()
		s.apiCallCount++

		if resp.StatusCode == 429 {
			resp.Body.Close()
			if i < maxRetries-1 {
				waitTime := 15 * time.Second
				fmt.Fprintf(os.Stderr, "Rate limited by song.link, waiting %v before retry...\n", waitTime)
				time.Sleep(waitTime)
				continue
			}
			return nil, fmt.Errorf("song.link rate limit exceeded after %d retries", maxRetries)
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("song.link returned HTTP %d", resp.StatusCode)
		}
		break
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read song.link response: %w", err)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("song.link returned empty response")
	}

	var songLinkResp struct {
		LinksByPlatform map[string]struct {
			URL string `json:"url"`
		} `json:"linksByPlatform"`
	}
	if err := json.Unmarshal(body, &songLinkResp); err != nil {
		preview := string(body)
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		return nil, fmt.Errorf("failed to decode song.link response: %w (body: %s)", err, preview)
	}

	result := &AllPlatformURLs{}

	if link, ok := songLinkResp.LinksByPlatform["tidal"]; ok && link.URL != "" {
		result.TidalURL = link.URL
		fmt.Fprintln(os.Stderr, "  ✓ Tidal")
	}
	if link, ok := songLinkResp.LinksByPlatform["amazonMusic"]; ok && link.URL != "" {
		result.AmazonURL = link.URL
		fmt.Fprintln(os.Stderr, "  ✓ Amazon")
	}
	// Fetch Deezer URL → extract ISRC → enables Qobuz without an extra song.link call.
	if link, ok := songLinkResp.LinksByPlatform["deezer"]; ok && link.URL != "" {
		result.DeezerURL = link.URL
		if isrc, err := GetDeezerISRC(link.URL); err == nil && isrc != "" {
			result.DeezerISRC = isrc
			fmt.Fprintf(os.Stderr, "  ✓ Qobuz (ISRC %s via Deezer)\n", isrc)
		} else {
			fmt.Fprintf(os.Stderr, "  - Deezer URL found but ISRC unavailable: %v\n", err)
		}
	}

	if result.TidalURL == "" && result.AmazonURL == "" && result.DeezerISRC == "" {
		return nil, fmt.Errorf("no usable streaming URLs found for %s", spotifyTrackID)
	}
	return result, nil
}
