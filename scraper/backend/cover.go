package backend

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	spotifySize300 = "ab67616d00001e02"
	spotifySize640 = "ab67616d0000b273"
	spotifySizeMax = "ab67616d000082c1"
)

type CoverClient struct {
	httpClient *http.Client
}

func NewCoverClient() *CoverClient {
	return &CoverClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func convertSmallToMedium(imageURL string) string {
	if strings.Contains(imageURL, spotifySize300) {
		return strings.Replace(imageURL, spotifySize300, spotifySize640, 1)
	}
	return imageURL
}

func (c *CoverClient) getMaxResolutionURL(imageURL string) string {

	mediumURL := convertSmallToMedium(imageURL)
	if strings.Contains(mediumURL, spotifySize640) {
		return strings.Replace(mediumURL, spotifySize640, spotifySizeMax, 1)
	}
	return mediumURL
}

func (c *CoverClient) DownloadCoverToPath(coverURL, outputPath string, embedMaxQualityCover bool) error {
	if coverURL == "" {
		return fmt.Errorf("cover URL is required")
	}

	// Reject animated/GIF covers before downloading.
	lowerURL := strings.ToLower(coverURL)
	if strings.Contains(lowerURL, ".gif") || strings.HasSuffix(strings.Split(lowerURL, "?")[0], ".gif") {
		return fmt.Errorf("refusing animated GIF cover: %s", coverURL)
	}

	downloadURL := convertSmallToMedium(coverURL)
	if embedMaxQualityCover {
		downloadURL = c.getMaxResolutionURL(downloadURL)
	}

	resp, err := c.httpClient.Get(downloadURL)
	if err != nil {
		return fmt.Errorf("failed to download cover: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download cover: HTTP %d", resp.StatusCode)
	}

	// Reject if the server signals GIF via Content-Type.
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(ct, "gif") {
		return fmt.Errorf("refusing animated GIF cover (Content-Type: %s)", ct)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write cover file: %v", err)
	}

	return nil
}

// FetchITunesCoverURL searches the iTunes Search API for a matching track and returns
// the highest available artwork URL. No API key is required. Returns an error when no
// match is found or the request fails.
func FetchITunesCoverURL(artist, title string) (string, error) {
	if artist == "" || title == "" {
		return "", fmt.Errorf("artist and title are required")
	}

	query := url.QueryEscape(artist + " " + title)
	apiURL := "https://itunes.apple.com/search?term=" + query + "&entity=song&limit=5&media=music"

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", fmt.Errorf("iTunes search request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("iTunes search returned HTTP %d", resp.StatusCode)
	}

	var result struct {
		ResultCount int `json:"resultCount"`
		Results     []struct {
			ArtworkUrl100 string `json:"artworkUrl100"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode iTunes response: %w", err)
	}

	if result.ResultCount == 0 || len(result.Results) == 0 {
		return "", fmt.Errorf("no iTunes results for %q by %q", title, artist)
	}

	artURL := result.Results[0].ArtworkUrl100
	if artURL == "" {
		return "", fmt.Errorf("iTunes result has no artwork URL")
	}

	// Upgrade to the largest available size by replacing the size token.
	// Apple serves up to 3000x3000 for most releases.
	artURL = strings.Replace(artURL, "100x100bb.jpg", "3000x3000bb.jpg", 1)
	artURL = strings.Replace(artURL, "100x100bb.png", "3000x3000bb.png", 1)

	return artURL, nil
}
