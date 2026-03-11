package backend

import (
	"fmt"
	"io"
	"net/http"
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
