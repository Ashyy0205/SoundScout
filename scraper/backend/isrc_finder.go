package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
)

const (
	spotifyBase62Alphabet    = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	spotifyGIDMetadataURLFmt = "https://spclient.wg.spotify.com/metadata/4/track/%s?market=from_token"
)

// spotifyTrackRawData is the minimal shape of the Spotify GID metadata response
// needed to extract the ISRC from the external_id list.
type spotifyTrackRawData struct {
	ExternalID []struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	} `json:"external_id"`
}

// extractSpotifyTrackID normalises a Spotify track reference (URI, URL, or bare ID)
// to the bare track ID string.
func extractSpotifyTrackID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("track input is required")
	}

	if strings.HasPrefix(value, "spotify:track:") {
		return value[strings.LastIndex(value, ":")+1:], nil
	}

	parsed, err := url.Parse(value)
	if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		if len(parts) >= 2 && parts[0] == "track" {
			return parts[1], nil
		}
		return "", errors.New("expected URL like https://open.spotify.com/track/<id>")
	}

	if len(value) == 22 {
		return value, nil
	}

	return "", errors.New("track must be a Spotify track ID, URL, or URI")
}

// spotifyTrackIDToGID converts a base62 Spotify track ID to the 32-char hex GID
// used by the spclient metadata API.
func spotifyTrackIDToGID(trackID string) (string, error) {
	if trackID == "" {
		return "", errors.New("track ID is empty")
	}

	value := big.NewInt(0)
	base := big.NewInt(62)

	for _, char := range trackID {
		index := strings.IndexRune(spotifyBase62Alphabet, char)
		if index < 0 {
			return "", fmt.Errorf("invalid base62 character: %q", string(char))
		}
		value.Mul(value, base)
		value.Add(value, big.NewInt(int64(index)))
	}

	hexValue := value.Text(16)
	if len(hexValue) < 32 {
		hexValue = strings.Repeat("0", 32-len(hexValue)) + hexValue
	}

	return hexValue, nil
}

// fetchSpotifyTrackRawData calls the Spotify spclient GID metadata endpoint using
// the shared authenticated SpotifyClient to retrieve raw track JSON.
func fetchSpotifyTrackRawData(trackID string) ([]byte, error) {
	sc, err := GetSharedSpotifyClient()
	if err != nil {
		return nil, fmt.Errorf("spotify client unavailable: %w", err)
	}

	gid, err := spotifyTrackIDToGID(trackID)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(spotifyGIDMetadataURLFmt, gid), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+sc.accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := sc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			invalidateSharedSpotifyClient()
		}
		preview := strings.TrimSpace(string(body))
		if len(preview) > 200 {
			preview = preview[:200]
		}
		return nil, fmt.Errorf("Spotify GID metadata returned HTTP %d: %s", resp.StatusCode, preview)
	}

	return body, nil
}

// extractSpotifyTrackISRC parses the ISRC from a raw Spotify GID metadata JSON payload.
func extractSpotifyTrackISRC(payload []byte) (string, error) {
	var track spotifyTrackRawData
	if err := json.Unmarshal(payload, &track); err != nil {
		return "", fmt.Errorf("failed to decode Spotify track metadata: %w", err)
	}

	for _, extID := range track.ExternalID {
		if strings.EqualFold(strings.TrimSpace(extID.Type), "isrc") {
			if isrc := firstISRCMatch(extID.ID); isrc != "" {
				return isrc, nil
			}
		}
	}

	// Fallback: scan the raw bytes for any ISRC pattern
	if fallback := firstISRCMatch(string(payload)); fallback != "" {
		return fallback, nil
	}

	return "", fmt.Errorf("ISRC not found in Spotify track metadata")
}

// cacheResolvedSpotifyTrackISRC writes the ISRC to the BoltDB cache for both the original
// and (if different) the resolved track ID.
func cacheResolvedSpotifyTrackISRC(trackID string, resolvedTrackID string, isrc string) {
	if err := PutCachedISRC(trackID, isrc); err != nil {
		fmt.Printf("Warning: failed to write ISRC cache: %v\n", err)
	}
	if resolvedTrackID != "" && resolvedTrackID != trackID {
		if err := PutCachedISRC(resolvedTrackID, isrc); err != nil {
			fmt.Printf("Warning: failed to write ISRC cache for resolved track ID: %v\n", err)
		}
	}
}

// lookupSpotifyISRC resolves the ISRC for a Spotify track.  The lookup order is:
//  1. BoltDB ISRC cache (instant, persists across runs)
//  2. Spotify spclient GID metadata API (fast, uses the existing anonymous TOTP token)
//  3. Soundplate web scrape (fallback)
func (s *SongLinkClient) lookupSpotifyISRC(spotifyTrackID string) (string, error) {
	normalizedTrackID, err := extractSpotifyTrackID(spotifyTrackID)
	if err != nil {
		return "", err
	}

	// 1. Check BoltDB cache
	cachedISRC, cacheErr := GetCachedISRC(normalizedTrackID)
	if cacheErr != nil {
		fmt.Printf("Warning: failed to read ISRC cache: %v\n", cacheErr)
	} else if cachedISRC != "" {
		fmt.Printf("Found ISRC in cache: %s\n", cachedISRC)
		return cachedISRC, nil
	}

	// 2. Spotify spclient GID metadata API
	payload, metadataErr := fetchSpotifyTrackRawData(normalizedTrackID)
	if metadataErr == nil {
		isrc, extractErr := extractSpotifyTrackISRC(payload)
		if extractErr == nil {
			fmt.Printf("Found ISRC via Spotify metadata: %s\n", isrc)
			cacheResolvedSpotifyTrackISRC(normalizedTrackID, "", isrc)
			return isrc, nil
		}
		metadataErr = extractErr
	}

	if metadataErr != nil {
		fmt.Printf("Warning: Spotify metadata ISRC lookup failed, falling back to Soundplate: %v\n", metadataErr)
	}

	// 3. Soundplate fallback
	isrc, resolvedTrackID, soundplateErr := s.lookupSpotifyISRCViaSoundplate(normalizedTrackID)
	if soundplateErr == nil && isrc != "" {
		fmt.Printf("Found ISRC via Soundplate: %s\n", isrc)
		cacheResolvedSpotifyTrackISRC(normalizedTrackID, resolvedTrackID, isrc)
		return isrc, nil
	}

	if metadataErr != nil && soundplateErr != nil {
		return "", fmt.Errorf("spotify metadata lookup failed: %v | soundplate lookup failed: %w", metadataErr, soundplateErr)
	}
	if soundplateErr != nil {
		return "", soundplateErr
	}
	return "", metadataErr
}
