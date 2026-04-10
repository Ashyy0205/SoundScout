package backend

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	linkResolverProviderSongstats      = "songstats"
	linkResolverProviderDeezerSongLink = "deezer-songlink"

	songLinkUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)

var (
	isrcPattern          = regexp.MustCompile(`\b([A-Z]{2}[A-Z0-9]{3}\d{7})\b`)
	amazonAlbumTrackPath = regexp.MustCompile(`/albums/[A-Z0-9]{10}/(B[0-9A-Z]{9})`)
	amazonTrackPath      = regexp.MustCompile(`/tracks/(B[0-9A-Z]{9})`)
)

// resolvedTrackLinks holds the streaming URLs and ISRC found during link resolution.
type resolvedTrackLinks struct {
	TidalURL  string
	AmazonURL string
	DeezerURL string
	ISRC      string
}

func hasAnySongLinkData(links *resolvedTrackLinks) bool {
	if links == nil {
		return false
	}
	return links.TidalURL != "" || links.AmazonURL != "" || links.DeezerURL != ""
}

// firstISRCMatch extracts the first ISRC pattern found in body (case-insensitive input).
func firstISRCMatch(body string) string {
	match := isrcPattern.FindStringSubmatch(strings.ToUpper(body))
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}

func normalizeAmazonMusicURL(rawURL string) string {
	amazonURL := strings.TrimSpace(rawURL)
	if amazonURL == "" {
		return ""
	}

	if strings.Contains(amazonURL, "trackAsin=") {
		parts := strings.Split(amazonURL, "trackAsin=")
		if len(parts) > 1 {
			trackAsin := strings.Split(parts[1], "&")[0]
			if trackAsin != "" {
				return fmt.Sprintf("https://music.amazon.com/tracks/%s?musicTerritory=US", trackAsin)
			}
		}
	}

	if match := amazonAlbumTrackPath.FindStringSubmatch(amazonURL); len(match) > 1 {
		return fmt.Sprintf("https://music.amazon.com/tracks/%s?musicTerritory=US", match[1])
	}

	if match := amazonTrackPath.FindStringSubmatch(amazonURL); len(match) > 1 {
		return fmt.Sprintf("https://music.amazon.com/tracks/%s?musicTerritory=US", match[1])
	}

	return ""
}

func normalizeDeezerTrackURL(rawURL string) string {
	cleanURL := strings.TrimSpace(rawURL)
	if cleanURL == "" {
		return ""
	}
	parts := strings.Split(cleanURL, "/track/")
	if len(parts) < 2 {
		return cleanURL
	}
	trackID := strings.Split(parts[1], "?")[0]
	trackID = strings.Trim(trackID, "/ ")
	if trackID == "" {
		return cleanURL
	}
	return fmt.Sprintf("https://www.deezer.com/track/%s", trackID)
}

// resolveLinksViaSongstats queries Songstats for streaming URLs using the ISRC in links.
// Returns (true, nil) if new data was added to links.
func (s *SongLinkClient) resolveLinksViaSongstats(links *resolvedTrackLinks) (bool, error) {
	if links == nil || links.ISRC == "" {
		return false, fmt.Errorf("ISRC is required for Songstats resolver")
	}

	before := *links

	fmt.Printf("Fetching Songstats links for ISRC %s\n", links.ISRC)
	if err := s.populateLinksFromSongstats(links, links.ISRC); err != nil {
		return false, err
	}

	changed := *links != before
	return changed, nil
}
