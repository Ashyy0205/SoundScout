package backend

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	mbAPIBase   = "https://musicbrainz.org/ws/2"
	mbUserAgent = "SoundScout/1.0 (https://github.com/soundscout/soundscout)"
)

// mbRateLimiter enforces MusicBrainz's 1 request/second policy (unauthenticated).
var mbRateLimiter = &mbRL{}

type mbRL struct {
	mu      sync.Mutex
	lastReq time.Time
}

func (r *mbRL) acquire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	since := time.Since(r.lastReq)
	if since < time.Second {
		time.Sleep(time.Second - since)
	}
	r.lastReq = time.Now()
}

var mbHTTPClient = &http.Client{Timeout: 15 * time.Second}

// MBRecording holds the MusicBrainz identifiers needed for Plex metadata matching.
type MBRecording struct {
	// TrackID is the MusicBrainz recording MBID. Stored as MUSICBRAINZ_TRACKID.
	TrackID string
	// AlbumID is the MusicBrainz release MBID. Stored as MUSICBRAINZ_ALBUMID.
	AlbumID string
	// ArtistID is the first artist's MusicBrainz MBID. Stored as MUSICBRAINZ_ARTISTID.
	ArtistID string
	// ISRC is the International Standard Recording Code confirmed by the lookup.
	ISRC string
}

func mbGetJSON(rawURL string) (map[string]interface{}, error) {
	mbRateLimiter.acquire()

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", mbUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := mbHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 404:
		return nil, nil
	case 503:
		return nil, fmt.Errorf("musicbrainz rate limited (503)")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("musicbrainz HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// LookupMBByISRC searches MusicBrainz for a recording that has the given ISRC.
// The ISRC is normalised (uppercased, hyphens removed) before the query.
// Returns nil, nil when no recording is found.
func LookupMBByISRC(isrc string) (*MBRecording, error) {
	isrc = strings.ToUpper(strings.ReplaceAll(isrc, "-", ""))
	if len(isrc) != 12 {
		return nil, nil
	}
	endpoint := fmt.Sprintf("%s/recording?query=isrc:%s&fmt=json&limit=1",
		mbAPIBase, url.PathEscape(isrc))

	data, err := mbGetJSON(endpoint)
	if err != nil || data == nil {
		return nil, err
	}
	return parseMBSearchResult(data, isrc)
}

// hasNonASCII reports whether s contains any non-ASCII rune.
func hasNonASCII(s string) bool {
	for _, r := range s {
		if r > 127 {
			return true
		}
	}
	return false
}

// mbScoreOf returns the float score field of a recording map entry.
func mbScoreOf(recMap map[string]interface{}) float64 {
	switch v := recMap["score"].(type) {
	case float64:
		return v
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f
	}
	return 0
}

// mbSearchOnce fires a single MusicBrainz recording query and returns the
// best result if its score meets minScore.
func mbSearchOnce(q string, minScore float64) (*MBRecording, error) {
	endpoint := fmt.Sprintf("%s/recording?query=%s&fmt=json&limit=3",
		mbAPIBase, url.QueryEscape(q))
	data, err := mbGetJSON(endpoint)
	if err != nil || data == nil {
		return nil, err
	}
	recs, _ := data["recordings"].([]interface{})
	for _, r := range recs {
		recMap, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		if mbScoreOf(recMap) >= minScore {
			return parseMBSearchResult(data, "")
		}
		break // results are score-sorted; first is best
	}
	return nil, nil
}

// SearchMBRecording searches MusicBrainz by track title and artist name using
// several progressive strategies to maximise coverage for niche, indie, and
// non-English track names:
//
//  1. Exact quoted Lucene search (score ≥ 85) — most precise.
//  2. Fuzzy tilde search (score ≥ 75) — handles spelling variants and diacritics.
//  3. Title-only search (score ≥ 90) — last resort when artist name confuses MB.
//
// Returns nil, nil when no confident match is found.
func SearchMBRecording(artist, title string) (*MBRecording, error) {
	if title == "" {
		return nil, nil
	}

	escape := func(s string) string { return strings.ReplaceAll(s, `"`, `\"`) }

	// Strategy 1: exact quoted search — works best for ASCII names.
	if artist != "" {
		q := fmt.Sprintf(`recording:"%s" AND artistname:"%s"`, escape(title), escape(artist))
		if rec, err := mbSearchOnce(q, 85); rec != nil || err != nil {
			return rec, err
		}
	}

	// Strategy 2: fuzzy tilde search — effective for non-English / transliterated names.
	// Apply ~ to unquoted tokens so Lucene does edit-distance matching.
	fuzzyToken := func(s string) string {
		if hasNonASCII(s) || strings.ContainsAny(s, " ") {
			// For multi-word or non-ASCII: wrap in quotes, skip tilde (MB doesn't
			// support tilde on phrases). Use unquoted title token for single words.
			return `"` + escape(s) + `"`
		}
		return escape(s) + "~"
	}
	if artist != "" {
		q := fmt.Sprintf("recording:%s AND artistname:%s", fuzzyToken(title), fuzzyToken(artist))
		if rec, err := mbSearchOnce(q, 75); rec != nil || err != nil {
			return rec, err
		}
	}

	// Strategy 3: title-only — some non-English artists have variant name spellings
	// in MusicBrainz that don't match the Spotify name.
	{
		q := fmt.Sprintf(`recording:"%s"`, escape(title))
		if rec, err := mbSearchOnce(q, 90); rec != nil || err != nil {
			return rec, err
		}
	}

	return nil, nil
}

// EnrichFromMusicBrainz first tries an ISRC lookup; on miss it falls back to a
// title+artist search. Returns nil, nil when no match is found by either method.
func EnrichFromMusicBrainz(artist, title, isrc string) (*MBRecording, error) {
	if isrc != "" {
		rec, err := LookupMBByISRC(isrc)
		if err == nil && rec != nil {
			return rec, nil
		}
	}
	return SearchMBRecording(artist, title)
}

// parseMBSearchResult extracts MBRecording fields from a MusicBrainz recording
// search response. confirmedISRC is set directly when we searched by ISRC.
func parseMBSearchResult(data map[string]interface{}, confirmedISRC string) (*MBRecording, error) {
	recs, ok := data["recordings"].([]interface{})
	if !ok || len(recs) == 0 {
		return nil, nil
	}
	recMap, ok := recs[0].(map[string]interface{})
	if !ok {
		return nil, nil
	}

	rec := &MBRecording{
		TrackID: getString(recMap, "id"),
		ISRC:    confirmedISRC,
	}
	if rec.TrackID == "" {
		return nil, nil
	}

	// Extract first artist MBID from artist-credit list.
	if credits, ok := recMap["artist-credit"].([]interface{}); ok && len(credits) > 0 {
		if cMap, ok := credits[0].(map[string]interface{}); ok {
			if artist, ok := cMap["artist"].(map[string]interface{}); ok {
				rec.ArtistID = getString(artist, "id")
			}
		}
	}

	// Extract first release MBID, preferring "Official" status.
	if releases, ok := recMap["releases"].([]interface{}); ok {
		for _, r := range releases {
			rMap, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			if rec.AlbumID == "" {
				rec.AlbumID = getString(rMap, "id")
			}
			if getString(rMap, "status") == "Official" {
				rec.AlbumID = getString(rMap, "id")
				break
			}
		}
	}

	return rec, nil
}

// LookupISRCByArtistTitle searches MusicBrainz by artist+title and returns the
// first ISRC attached to the matched recording. Returns "", nil when no
// confident match or no ISRCs are found — never retries on transient errors.
func LookupISRCByArtistTitle(artist, title string) (string, error) {
	rec, err := SearchMBRecording(artist, title)
	if err != nil || rec == nil || rec.TrackID == "" {
		return "", err
	}
	// Fetch the full recording with ISRCs included.
	endpoint := fmt.Sprintf("%s/recording/%s?fmt=json&inc=isrcs",
		mbAPIBase, url.PathEscape(rec.TrackID))
	data, err := mbGetJSON(endpoint)
	if err != nil || data == nil {
		return "", err
	}
	isrcs, _ := data["isrcs"].([]interface{})
	for _, v := range isrcs {
		if s, ok := v.(string); ok && s != "" {
			s = strings.ToUpper(strings.ReplaceAll(s, "-", ""))
			if len(s) == 12 {
				return s, nil
			}
		}
	}
	return "", nil
}
