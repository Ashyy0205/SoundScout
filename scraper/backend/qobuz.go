package backend

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type QobuzDownloader struct {
	client *http.Client
	appID  string
}

type QobuzSearchResponse struct {
	Query  string `json:"query"`
	Tracks struct {
		Limit  int          `json:"limit"`
		Offset int          `json:"offset"`
		Total  int          `json:"total"`
		Items  []QobuzTrack `json:"items"`
	} `json:"tracks"`
}

type QobuzTrack struct {
	ID                  int64   `json:"id"`
	Title               string  `json:"title"`
	Version             string  `json:"version"`
	Duration            int     `json:"duration"`
	TrackNumber         int     `json:"track_number"`
	MediaNumber         int     `json:"media_number"`
	ISRC                string  `json:"isrc"`
	Copyright           string  `json:"copyright"`
	MaximumBitDepth     int     `json:"maximum_bit_depth"`
	MaximumSamplingRate float64 `json:"maximum_sampling_rate"`
	Hires               bool    `json:"hires"`
	HiresStreamable     bool    `json:"hires_streamable"`
	ReleaseDateOriginal string  `json:"release_date_original"`
	Performer           struct {
		Name string `json:"name"`
		ID   int64  `json:"id"`
	} `json:"performer"`
	Album struct {
		Title string `json:"title"`
		ID    string `json:"id"`
		Image struct {
			Small     string `json:"small"`
			Thumbnail string `json:"thumbnail"`
			Large     string `json:"large"`
		} `json:"image"`
		Artist struct {
			Name string `json:"name"`
			ID   int64  `json:"id"`
		} `json:"artist"`
		Label struct {
			Name string `json:"name"`
		} `json:"label"`
	} `json:"album"`
}

type QobuzStreamResponse struct {
	URL string `json:"url"`
}

func extractQobuzDownloadURL(body []byte) (string, error) {
	bodyStr := strings.TrimSpace(string(body))
	if bodyStr == "" {
		return "", fmt.Errorf("empty response")
	}

	// Some upstream mirrors fail with an HTML page (Cloudflare/rate-limit/error page).
	if strings.HasPrefix(bodyStr, "<") {
		preview := bodyStr
		if len(preview) > 140 {
			preview = preview[:140] + "..."
		}
		return "", fmt.Errorf("received HTML instead of JSON: %s", preview)
	}

	var streamResp QobuzStreamResponse
	if err := json.Unmarshal(body, &streamResp); err == nil && streamResp.URL != "" {
		return streamResp.URL, nil
	}

	// Be tolerant to provider schema drift.
	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		preview := bodyStr
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		return "", fmt.Errorf("failed to decode JSON: %w (response: %s)", err, preview)
	}

	if v, ok := generic["url"].(string); ok && v != "" {
		return v, nil
	}
	if data, ok := generic["data"].(map[string]any); ok {
		if v, ok := data["url"].(string); ok && v != "" {
			return v, nil
		}
		if stream, ok := data["stream"].(map[string]any); ok {
			if v, ok := stream["url"].(string); ok && v != "" {
				return v, nil
			}
		}
	}

	return "", fmt.Errorf("no download URL found in response")
}

func NewQobuzDownloader() *QobuzDownloader {
	return &QobuzDownloader{
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		appID: "798273057",
	}
}

func (q *QobuzDownloader) SearchByISRC(isrc string) (*QobuzTrack, error) {

	apiBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly93d3cucW9idXouY29tL2FwaS5qc29uLzAuMi90cmFjay9zZWFyY2g/cXVlcnk9")
	url := fmt.Sprintf("%s%s&limit=1&app_id=%s", string(apiBase), isrc, q.appID)

	resp, err := q.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to search track: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var searchResp QobuzSearchResponse

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if len(body) == 0 {
		return nil, fmt.Errorf("API returned empty response")
	}

	if err := json.Unmarshal(body, &searchResp); err != nil {

		bodyStr := string(body)
		if len(bodyStr) > 200 {
			bodyStr = bodyStr[:200] + "..."
		}
		return nil, fmt.Errorf("failed to decode response: %w (response: %s)", err, bodyStr)
	}

	if len(searchResp.Tracks.Items) == 0 {
		return nil, fmt.Errorf("track not found for ISRC: %s", isrc)
	}

	return &searchResp.Tracks.Items[0], nil
}

func (q *QobuzDownloader) GetDownloadURL(trackID int64, quality string) (string, error) {

	qualityCode := quality
	if qualityCode == "" {
		qualityCode = "6"
	}

	fmt.Fprintf(os.Stderr, "Getting download URL for track ID: %d with requested quality: %s\n", trackID, qualityCode)
	fmt.Fprintf(os.Stderr, "Quality codes: 6=FLAC 16-bit, 7=FLAC 24-bit\n")

	primaryBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9kYWIueWVldC5zdS9hcGkvc3RyZWFtP3RyYWNrSWQ9")

	primaryURL := fmt.Sprintf("%s%d&quality=%s", string(primaryBase), trackID, qualityCode)
	fmt.Fprintf(os.Stderr, "Trying Primary API: %s\n", primaryURL)

	var parseErrors []string

	resp, err := q.client.Get(primaryURL)
	if err == nil && resp.StatusCode == 200 {
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		streamURL, parseErr := extractQobuzDownloadURL(body)
		if parseErr == nil {
			fmt.Fprintf(os.Stderr, "✓ Got download URL from Primary API\n")
			return streamURL, nil
		}
		parseErrors = append(parseErrors, fmt.Sprintf("primary parse error: %v", parseErr))
	} else if err != nil {
		parseErrors = append(parseErrors, fmt.Sprintf("primary request error: %v", err))
	} else {
		parseErrors = append(parseErrors, fmt.Sprintf("primary status: %d", resp.StatusCode))
	}
	if resp != nil {
		resp.Body.Close()
	}

	fmt.Fprintln(os.Stderr, "Primary API failed, trying Fallback API #1...")
	fallbackBase, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9kYWJtdXNpYy54eXovYXBpL3N0cmVhbT90cmFja0lkPQ==")
	fallbackURL := fmt.Sprintf("%s%d&quality=%s", string(fallbackBase), trackID, qualityCode)

	resp, err = q.client.Get(fallbackURL)
	if err == nil && resp.StatusCode == 200 {
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			streamURL, parseErr := extractQobuzDownloadURL(body)
			if parseErr == nil {
				fmt.Fprintf(os.Stderr, "✓ Got download URL from Fallback API #1\n")
				return streamURL, nil
			}
			parseErrors = append(parseErrors, fmt.Sprintf("fallback#1 parse error: %v", parseErr))
		} else if err != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("fallback#1 read error: %v", err))
		}
	} else if err != nil {
		parseErrors = append(parseErrors, fmt.Sprintf("fallback#1 request error: %v", err))
	} else {
		parseErrors = append(parseErrors, fmt.Sprintf("fallback#1 status: %d", resp.StatusCode))
	}
	if resp != nil {
		resp.Body.Close()
	}

	fmt.Fprintln(os.Stderr, "Fallback API #1 failed, trying Fallback API #2...")
	fallback2Base, _ := base64.StdEncoding.DecodeString("aHR0cHM6Ly9xb2J1ei5zcXVpZC53dGYvYXBpL2Rvd25sb2FkLW11c2ljP3RyYWNrX2lkPQ==")
	fallback2URL := fmt.Sprintf("%s%d&quality=%s", string(fallback2Base), trackID, qualityCode)

	resp, err = q.client.Get(fallback2URL)
	if err != nil {
		parseErrors = append(parseErrors, fmt.Sprintf("fallback#2 request error: %v", err))
		return "", fmt.Errorf("all APIs failed to get download URL: %s", strings.Join(parseErrors, " | "))
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		bodyStr := strings.TrimSpace(string(body))
		if len(bodyStr) > 120 {
			bodyStr = bodyStr[:120] + "..."
		}
		parseErrors = append(parseErrors, fmt.Sprintf("fallback#2 status %d: %s", resp.StatusCode, bodyStr))
		return "", fmt.Errorf("all APIs returned non-200 status: %s", strings.Join(parseErrors, " | "))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	if len(body) == 0 {
		return "", fmt.Errorf("API returned empty response")
	}

	fmt.Fprintf(os.Stderr, "Fallback API #2 response: %s\n", string(body))

	streamURL, parseErr := extractQobuzDownloadURL(body)
	if parseErr != nil {
		parseErrors = append(parseErrors, fmt.Sprintf("fallback#2 parse error: %v", parseErr))
		return "", fmt.Errorf("no usable download URL from any API: %s", strings.Join(parseErrors, " | "))
	}

	fmt.Fprintf(os.Stderr, "✓ Got download URL from Fallback API #2\n")
	return streamURL, nil
}

func (q *QobuzDownloader) DownloadFile(url, filepath string) error {
	fmt.Fprintln(os.Stderr, "Starting file download...")

	downloadClient := &http.Client{
		Timeout: 5 * time.Minute,
	}

	resp, err := downloadClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	fmt.Fprintf(os.Stderr, "Creating file: %s\n", filepath)
	out, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close()

	fmt.Fprintln(os.Stderr, "Downloading...")

	pw := NewProgressWriter(out)
	_, err = io.Copy(pw, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Fprintf(os.Stderr, "\rDownloaded: %.2f MB (Complete)\n", float64(pw.GetTotal())/(1024*1024))
	return nil
}

func (q *QobuzDownloader) DownloadCoverArt(coverURL, filepath string) error {
	if coverURL == "" {
		return fmt.Errorf("no cover URL provided")
	}

	resp, err := q.client.Get(coverURL)
	if err != nil {
		return fmt.Errorf("failed to download cover: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("cover download failed with status %d", resp.StatusCode)
	}

	out, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create cover file: %w", err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func buildQobuzFilename(title, artist, album, albumArtist, releaseDate string, trackNumber, discNumber int, format string, includeTrackNumber bool, position int, useAlbumTrackNumber bool) string {
	var filename string

	numberToUse := position
	if useAlbumTrackNumber && trackNumber > 0 {
		numberToUse = trackNumber
	}

	year := ""
	if len(releaseDate) >= 4 {
		year = releaseDate[:4]
	}

	if strings.Contains(format, "{") {
		filename = format
		filename = strings.ReplaceAll(filename, "{title}", title)
		filename = strings.ReplaceAll(filename, "{artist}", artist)
		filename = strings.ReplaceAll(filename, "{album}", album)
		filename = strings.ReplaceAll(filename, "{album_artist}", albumArtist)
		filename = strings.ReplaceAll(filename, "{year}", year)

		if discNumber > 0 {
			filename = strings.ReplaceAll(filename, "{disc}", fmt.Sprintf("%d", discNumber))
		} else {
			filename = strings.ReplaceAll(filename, "{disc}", "")
		}

		if numberToUse > 0 {
			filename = strings.ReplaceAll(filename, "{track}", fmt.Sprintf("%02d", numberToUse))
		} else {

			filename = regexp.MustCompile(`\{track\}\.\s*`).ReplaceAllString(filename, "")
			filename = regexp.MustCompile(`\{track\}\s*-\s*`).ReplaceAllString(filename, "")
			filename = regexp.MustCompile(`\{track\}\s*`).ReplaceAllString(filename, "")
		}
	} else {

		switch format {
		case "artist-title":
			filename = fmt.Sprintf("%s - %s", artist, title)
		case "title":
			filename = title
		default:
			filename = fmt.Sprintf("%s - %s", title, artist)
		}

		if includeTrackNumber && position > 0 {
			filename = fmt.Sprintf("%02d. %s", numberToUse, filename)
		}
	}

	return filename + ".flac"
}

func (q *QobuzDownloader) DownloadByISRC(deezerISRC, outputDir, quality, filenameFormat string, includeTrackNumber bool, position int, spotifyTrackName, spotifyArtistName, spotifyAlbumName, spotifyAlbumArtist, spotifyReleaseDate string, useAlbumTrackNumber bool, spotifyCoverURL string, embedMaxQualityCover bool, spotifyTrackNumber, spotifyDiscNumber, spotifyTotalTracks int, spotifyTotalDiscs int, spotifyCopyright, spotifyPublisher, spotifyURL string) (string, error) {
	fmt.Fprintf(os.Stderr, "Fetching track info for ISRC: %s\n", deezerISRC)

	if outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	track, err := q.SearchByISRC(deezerISRC)
	if err != nil {
		return "", err
	}

	artists := spotifyArtistName
	trackTitle := spotifyTrackName
	albumTitle := spotifyAlbumName

	fmt.Fprintf(os.Stderr, "Found track: %s - %s\n", artists, trackTitle)
	fmt.Fprintf(os.Stderr, "Album: %s\n", albumTitle)

	qualityInfo := "Standard"
	if track.Hires {
		qualityInfo = fmt.Sprintf("Hi-Res (%d-bit / %.1f kHz)", track.MaximumBitDepth, track.MaximumSamplingRate)
	}
	fmt.Fprintf(os.Stderr, "Quality: %s\n", qualityInfo)

	fmt.Fprintln(os.Stderr, "Getting download URL...")
	downloadURL, err := q.GetDownloadURL(track.ID, quality)
	if err != nil {
		return "", fmt.Errorf("failed to get download URL: %w", err)
	}

	if downloadURL == "" {
		return "", fmt.Errorf("received empty download URL")
	}

	urlPreview := downloadURL
	if len(downloadURL) > 60 {
		urlPreview = downloadURL[:60] + "..."
	}
	fmt.Fprintf(os.Stderr, "Download URL obtained: %s\n", urlPreview)

	safeArtist := sanitizeFilename(artists)
	safeTitle := sanitizeFilename(trackTitle)
	safeAlbum := sanitizeFilename(albumTitle)
	safeAlbumArtist := sanitizeFilename(spotifyAlbumArtist)

	filename := buildQobuzFilename(safeTitle, safeArtist, safeAlbum, safeAlbumArtist, spotifyReleaseDate, spotifyTrackNumber, spotifyDiscNumber, filenameFormat, includeTrackNumber, position, useAlbumTrackNumber)
	filepath := filepath.Join(outputDir, filename)

	if fileInfo, err := os.Stat(filepath); err == nil && fileInfo.Size() > 0 {
		fmt.Fprintf(os.Stderr, "File already exists: %s (%.2f MB)\n", filepath, float64(fileInfo.Size())/(1024*1024))
		return "EXISTS:" + filepath, nil
	}

	fmt.Fprintf(os.Stderr, "Downloading FLAC file to: %s\n", filepath)
	if err := q.DownloadFile(downloadURL, filepath); err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Downloaded: %s\n", filepath)

	coverPath := ""

	if spotifyCoverURL != "" {
		coverPath = filepath + ".cover.jpg"
		coverClient := NewCoverClient()
		if err := coverClient.DownloadCoverToPath(spotifyCoverURL, coverPath, embedMaxQualityCover); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to download Spotify cover: %v\n", err)
			coverPath = ""
		} else {
			defer os.Remove(coverPath)
			fmt.Fprintln(os.Stderr, "Spotify cover downloaded")
		}
	}

	fmt.Fprintln(os.Stderr, "Embedding metadata and cover art...")

	trackNumberToEmbed := spotifyTrackNumber
	if trackNumberToEmbed == 0 {
		trackNumberToEmbed = 1
	}

	metadata := Metadata{
		Title:       trackTitle,
		Artist:      artists,
		Album:       albumTitle,
		AlbumArtist: spotifyAlbumArtist,
		Date:        spotifyReleaseDate,
		TrackNumber: trackNumberToEmbed,
		TotalTracks: spotifyTotalTracks,
		DiscNumber:  spotifyDiscNumber,
		TotalDiscs:  spotifyTotalDiscs,
		URL:         spotifyURL,
		Copyright:   spotifyCopyright,
		Publisher:   spotifyPublisher,
		Description: "",
	}

	if err := EmbedMetadata(filepath, metadata, coverPath); err != nil {
		return "", fmt.Errorf("failed to embed metadata: %w", err)
	}

	fmt.Fprintln(os.Stderr, "Metadata embedded successfully!")
	return filepath, nil
}
