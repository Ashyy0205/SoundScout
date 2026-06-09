package backend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func mapQobuzQualityToCommunity(quality string) string {
	switch strings.TrimSpace(quality) {
	case "27", "7":
		return "24"
	default:
		return "16"
	}
}

func (q *QobuzDownloader) getQobuzCommunityDownloadURL(trackID int64, quality string) (string, error) {
	payload, err := json.Marshal(map[string]string{
		"id":      fmt.Sprintf("%d", trackID),
		"quality": mapQobuzQualityToCommunity(quality),
	})
	if err != nil {
		return "", err
	}

	resp, err := doCommunityRequest(q.client, "Qobuz", func() (*http.Request, error) {
		req, err := NewRequestWithDefaultHeaders(http.MethodPost, GetQobuzCommunityDownloadURL(), bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		if err := setCommunityRequestHeaders(req); err != nil {
			return nil, err
		}
		return req, nil
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("qobuz community API returned status %d", resp.StatusCode)
	}

	downloadURL, err := extractQobuzDownloadURL(body)
	if err != nil {
		return "", fmt.Errorf("no streamable URL in qobuz community response: %w", err)
	}
	return downloadURL, nil
}
