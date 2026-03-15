package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"scraper/backend"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var isrcRegex = regexp.MustCompile(`^[A-Z]{2}[A-Z0-9]{3}\d{2}\d{5}$`)

func isValidISRC(isrc string) bool {
	return isrcRegex.MatchString(isrc)
}

// songLinkCachePath returns the path for the persistent song.link resolved-URL cache.
// Configurable via SONGLINK_CACHE_PATH env var; defaults to /config/songlink_cache.json
// inside Docker or songlink_cache.json locally.
func songLinkCachePath() string {
	if p := strings.TrimSpace(os.Getenv("SONGLINK_CACHE_PATH")); p != "" {
		return p
	}
	if os.Getenv("IS_DOCKER") != "" {
		return "/config/songlink_cache.json"
	}
	return "songlink_cache.json"
}

type App struct {
	ctx context.Context
}

func parseSpotifyTrackID(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// spotify:track:<id>
	if strings.HasPrefix(strings.ToLower(s), "spotify:track:") {
		parts := strings.Split(s, ":")
		if len(parts) >= 3 {
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}

	// https://open.spotify.com/track/<id> or https://spotify.com/track/<id>
	lower := strings.ToLower(s)
	idx := strings.Index(lower, "/track/")
	if idx >= 0 {
		rest := s[idx+len("/track/"):]
		// Trim query params and slashes
		rest = strings.Split(rest, "?")[0]
		rest = strings.Split(rest, "#")[0]
		rest = strings.Trim(rest, "/")
		return strings.TrimSpace(rest)
	}

	// If it looks like a raw ID, just return it.
	if len(s) >= 20 && len(s) <= 40 && !strings.ContainsAny(s, " /?") {
		return s
	}
	return ""
}

func indexOfHeader(headers []string, want string) int {
	want = strings.ToLower(strings.TrimSpace(want))
	for i, h := range headers {
		if strings.ToLower(strings.TrimSpace(h)) == want {
			return i
		}
	}
	return -1
}

func pickColumnValue(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

// resolvedTrack holds a track entry together with its pre-fetched platform URLs and
// full Spotify track metadata. All fields are populated during Phase 1 so that
// DownloadTrack never needs to make an additional Spotify API call in Phase 2.
type resolvedTrack struct {
	index     int
	artist    string
	title     string
	spotifyID string
	platforms *backend.AllPlatformURLs
	// metaDone is closed by the metadata goroutine once all fields below are populated.
	// Download goroutines select on it (with timeout) before reading the metadata fields.
	metaDone chan struct{}
	// Pre-fetched Spotify metadata (populated concurrently during Phase 1).
	albumName   string
	albumArtist string
	releaseDate string
	coverURL    string
	copyright   string
	publisher   string
	trackNumber int
	discNumber  int
	totalTracks int
	totalDiscs  int
	duration    int
	// ISRC and MusicBrainz identifiers — fetched alongside Spotify metadata in Phase 1.
	// Used in Phase 2 to enrich the downloaded file's tags so Plex can reliably identify
	// each track, album, and artist in its online database.
	isrc       string
	mbTrackID  string
	mbAlbumID  string
	mbArtistID string
}

// buildAvailableServices returns only the services that have a usable pre-resolved URL.
func buildAvailableServices(platforms *backend.AllPlatformURLs) []string {
	if platforms == nil {
		return nil
	}
	svcs := make([]string, 0, 3)
	if platforms.TidalURL != "" {
		svcs = append(svcs, "tidal")
	}
	if platforms.DeezerISRC != "" {
		svcs = append(svcs, "qobuz")
	}
	if platforms.AmazonURL != "" {
		svcs = append(svcs, "amazon")
	}
	return svcs
}

// spotifyTrackMeta holds the supplementary Spotify track fields fetched during Phase 1
// to avoid a redundant per-track API call inside DownloadTrack during Phase 2.
type spotifyTrackMeta struct {
	albumName   string
	albumArtist string
	releaseDate string
	coverURL    string
	copyright   string
	publisher   string
	trackNumber int
	discNumber  int
	totalTracks int
	totalDiscs  int
	duration    int // seconds
	isrc        string
}

// fetchSpotifyTrackMeta retrieves full track metadata from Spotify for a given track ID.
// It runs concurrently for multiple tracks during Phase 1, overlapping with the mandatory
// 7-second song.link rate-limiting gaps so the fetch costs no additional wall-clock time.
func fetchSpotifyTrackMeta(spotifyID string) (*spotifyTrackMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	trackURL := fmt.Sprintf("https://open.spotify.com/track/%s", spotifyID)
	raw, err := backend.GetFilteredSpotifyData(ctx, trackURL, false, 0)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Track struct {
			AlbumName   string `json:"album_name"`
			AlbumArtist string `json:"album_artist"`
			Copyright   string `json:"copyright"`
			Publisher   string `json:"publisher"`
			TotalDiscs  int    `json:"total_discs"`
			TotalTracks int    `json:"total_tracks"`
			TrackNumber int    `json:"track_number"`
			DiscNumber  int    `json:"disc_number"`
			ReleaseDate string `json:"release_date"`
			DurationMS  int    `json:"duration_ms"`
			Images      string `json:"images"`
			ISRC        string `json:"isrc"`
		} `json:"track"`
	}
	jsonData, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		return nil, err
	}

	t := parsed.Track
	meta := &spotifyTrackMeta{
		albumName:   t.AlbumName,
		albumArtist: t.AlbumArtist,
		releaseDate: t.ReleaseDate,
		copyright:   t.Copyright,
		publisher:   t.Publisher,
		trackNumber: t.TrackNumber,
		discNumber:  t.DiscNumber,
		totalTracks: t.TotalTracks,
		totalDiscs:  t.TotalDiscs,
		duration:    t.DurationMS / 1000,
		isrc:        t.ISRC,
	}

	// Extract the first cover-art URL from the Images field (stored as a JSON array).
	if t.Images != "" {
		var images []struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal([]byte(t.Images), &images); err == nil && len(images) > 0 {
			meta.coverURL = images[0].URL
		} else {
			// Fallback: treat as plain comma-separated list.
			parts := strings.SplitN(t.Images, ",", 2)
			meta.coverURL = strings.TrimSpace(parts[0])
		}
	}
	return meta, nil
}

// DownloadSongsFromCSV downloads every track in the CSV file in three stages:
//
//	1a. Build track list — parse CSV and extract Spotify IDs from dedicated columns;
//	    tracks without an ID run Spotify searches in a parallel pool (up to 10).
//
//	1b. Platform resolution + metadata pre-fetch — for each track a Spotify metadata
//	    goroutine fires immediately (up to 8 concurrent) while the serial song.link
//	    call runs. The metadata fetch completes inside the mandatory 7-second gap at
//	    zero extra wall-clock cost, eliminating a redundant Spotify call per worker.
//
//	2.  Download phase (concurrent) — SCRAPER_WORKERS goroutines (default 5) download
//	    tracks in parallel using pre-resolved URLs and pre-fetched metadata.
func (a *App) DownloadSongsFromCSV(csvPath string, outputDir string) ([]DownloadResponse, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := backend.NewCSVReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	// Header-aware column mapping (case-insensitive).
	headers := []string{}
	if len(records) > 0 {
		headers = records[0]
	}
	artistIdx := 0
	titleIdx := 1
	spotifyIDIdx := -1
	spotifyURLIdx := -1
	if len(headers) > 0 {
		if ai := indexOfHeader(headers, "artist"); ai >= 0 {
			artistIdx = ai
		}
		if ti := indexOfHeader(headers, "title"); ti >= 0 {
			titleIdx = ti
		}
		spotifyIDIdx = indexOfHeader(headers, "spotify_id")
		if spotifyIDIdx < 0 {
			spotifyIDIdx = indexOfHeader(headers, "spotifyid")
		}
		spotifyURLIdx = indexOfHeader(headers, "spotify_url")
		if spotifyURLIdx < 0 {
			spotifyURLIdx = indexOfHeader(headers, "spotifyurl")
		}
	}

	// Fixed service priority: Tidal first, then Qobuz, then Amazon.
	// A track only fails after all three have been attempted.
	defaultOrder := []string{"tidal", "qobuz", "amazon"}

	// Number of concurrent download workers (default 5).
	// Spotify metadata is pre-fetched in Phase 1 so workers are purely network-I/O-bound
	// and can safely run at higher concurrency.
	// Override with SCRAPER_WORKERS=N.
	workers := 5
	if wStr := strings.TrimSpace(os.Getenv("SCRAPER_WORKERS")); wStr != "" {
		if w, convErr := strconv.Atoi(wStr); convErr == nil && w > 0 {
			workers = w
		}
	}

	// ── Stage 1a: Build track list from CSV ─────────────────────────────────────────────────
	resolved := make([]*resolvedTrack, 0, len(records)-1)

	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}
		if len(record) < 2 {
			continue
		}

		artist := pickColumnValue(record, artistIdx)
		title := pickColumnValue(record, titleIdx)
		if artist == "" || title == "" {
			artist = strings.TrimSpace(record[0])
			title = strings.TrimSpace(record[1])
		}

		rt := &resolvedTrack{index: i - 1, artist: artist, title: title, metaDone: make(chan struct{})}

		// Prefer spotify_id / spotify_url columns when present.
		if spotifyIDIdx >= 0 {
			rt.spotifyID = parseSpotifyTrackID(pickColumnValue(record, spotifyIDIdx))
		}
		if rt.spotifyID == "" && spotifyURLIdx >= 0 {
			rt.spotifyID = parseSpotifyTrackID(pickColumnValue(record, spotifyURLIdx))
		}

		resolved = append(resolved, rt)
	}

	// Parallel Spotify ID lookups for tracks that had no ID in the CSV.
	// Up to 10 searches run simultaneously instead of serially.
	{
		var idWg sync.WaitGroup
		idSem := make(chan struct{}, 10)
		for _, rt := range resolved {
			if rt.spotifyID != "" {
				continue
			}
			rt := rt // capture loop variable
			idWg.Add(1)
			go func() {
				idSem <- struct{}{}
				defer func() { <-idSem }()
				defer idWg.Done()
				searchReq := SpotifySearchByTypeRequest{
					Query:      fmt.Sprintf("%s %s", rt.artist, rt.title),
					SearchType: "track",
					Limit:      1,
					Offset:     0,
				}
				if results, err := a.SearchSpotifyByType(searchReq); err == nil && len(results) > 0 {
					rt.spotifyID = results[0].ID
				}
			}()
		}
		idWg.Wait()
	}

	// ── Stage 1b: Platform resolution + concurrent Spotify metadata pre-fetch ──────────────
	// Metadata goroutines fire immediately for all tracks and write their results directly into
	// the resolvedTrack, closing rt.metaDone when done so download goroutines can wait per-track.
	//
	// Song.link defaults to ONE worker: multiple parallel clients share the same external IP and
	// collectively exceed the ~10 req/min IP-level rate limit, causing 429 cascades and 15-second
	// retry waits that add minutes to startup. One client at 6-second intervals = ~10 calls/min.
	//
	// Downloads start immediately as each track is pushed to downloadCh — cache hits and
	// no-ID tracks begin downloading right away; SL-resolved tracks start within seconds.
	//
	// Tuneable via env vars:
	//   SCRAPER_SONGLINK_WORKERS — parallel song.link clients (default 1; increase carefully)
	//   SONGLINK_CACHE_PATH      — persistent cache file path
	slWorkers := 1
	if wStr := strings.TrimSpace(os.Getenv("SCRAPER_SONGLINK_WORKERS")); wStr != "" {
		if w, convErr := strconv.Atoi(wStr); convErr == nil && w >= 1 {
			slWorkers = w
		}
	}

	// downloadCh receives each *resolvedTrack as soon as it is ready for download.
	// Buffered to len(resolved) so producers never block.
	downloadCh := make(chan *resolvedTrack, len(resolved))

	metaSem := make(chan struct{}, 8) // up to 8 concurrent Spotify metadata fetches

	// Launch metadata goroutines for all tracks immediately.
	// Each goroutine applies fields directly to rt and closes rt.metaDone when done.
	for i, rt := range resolved {
		if rt.spotifyID == "" {
			fmt.Fprintf(os.Stderr, "[%d/%d] No Spotify ID for '%s - %s' — Qobuz/Amazon may be unavailable\n",
				i+1, len(resolved), rt.artist, rt.title)
			close(rt.metaDone) // nothing to fetch; signal immediately
			continue
		}
		rt := rt // capture loop variable
		go func() {
			metaSem <- struct{}{}
			defer func() { <-metaSem }()
			defer close(rt.metaDone) // signal done regardless of success/failure
			meta, err := fetchSpotifyTrackMeta(rt.spotifyID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  [meta] prefetch failed for %s: %v\n", rt.spotifyID, err)
				// Spotify failed — still try MusicBrainz by artist+title as fallback.
				if mb, mbErr := backend.EnrichFromMusicBrainz(rt.artist, rt.title, ""); mbErr == nil && mb != nil {
					rt.mbTrackID = mb.TrackID
					rt.mbAlbumID = mb.AlbumID
					rt.mbArtistID = mb.ArtistID
				}
				return
			}
			if rt.albumName == "" && meta.albumName != "" {
				rt.albumName = meta.albumName
			}
			if rt.albumArtist == "" && meta.albumArtist != "" {
				rt.albumArtist = meta.albumArtist
			}
			if rt.releaseDate == "" && meta.releaseDate != "" {
				rt.releaseDate = meta.releaseDate
			}
			if rt.coverURL == "" && meta.coverURL != "" {
				rt.coverURL = meta.coverURL
			}
			if rt.copyright == "" && meta.copyright != "" {
				rt.copyright = meta.copyright
			}
			if rt.publisher == "" && meta.publisher != "" {
				rt.publisher = meta.publisher
			}
			if rt.trackNumber == 0 && meta.trackNumber != 0 {
				rt.trackNumber = meta.trackNumber
			}
			if rt.discNumber == 0 && meta.discNumber != 0 {
				rt.discNumber = meta.discNumber
			}
			if rt.totalTracks == 0 && meta.totalTracks != 0 {
				rt.totalTracks = meta.totalTracks
			}
			if rt.totalDiscs == 0 && meta.totalDiscs != 0 {
				rt.totalDiscs = meta.totalDiscs
			}
			if rt.duration == 0 && meta.duration != 0 {
				rt.duration = meta.duration
			}
			if rt.isrc == "" && meta.isrc != "" {
				rt.isrc = meta.isrc
			}
			// MusicBrainz enrichment: ISRC lookup first, artist+title search as fallback.
			// Runs inside the same goroutine so it overlaps with the song.link wait.
			if mb, mbErr := backend.EnrichFromMusicBrainz(rt.artist, rt.title, rt.isrc); mbErr == nil && mb != nil {
				rt.mbTrackID = mb.TrackID
				rt.mbAlbumID = mb.AlbumID
				rt.mbArtistID = mb.ArtistID
				if rt.isrc == "" && mb.ISRC != "" {
					rt.isrc = mb.ISRC
				}
			}
		}()
	}

	// Load disk cache — cache hits skip song.link entirely and push to downloadCh immediately.
	slCache := backend.NewSongLinkCache(songLinkCachePath())
	if slCache.Len() > 0 {
		fmt.Fprintf(os.Stderr, "[songlink-cache] Loaded %d entries\n", slCache.Len())
	}

	type slWork struct {
		idx int
		rt  *resolvedTrack
	}
	slWorkCh := make(chan slWork, len(resolved))
	cacheHits := 0
	for i, rt := range resolved {
		if rt.spotifyID == "" {
			downloadCh <- rt // no SL needed; ready now
			continue
		}
		if cached, ok := slCache.Get(rt.spotifyID); ok {
			rt.platforms = cached
			cacheHits++
			downloadCh <- rt // cache hit; ready now
			continue
		}
		slWorkCh <- slWork{idx: i, rt: rt}
	}
	close(slWorkCh)

	if cacheHits > 0 {
		fmt.Fprintf(os.Stderr, "[songlink-cache] %d/%d tracks resolved from cache (no song.link call needed)\n", cacheHits, len(resolved))
	}

	// Song.link workers: resolve each track then push to downloadCh.
	var slResWg sync.WaitGroup
	for w := 0; w < slWorkers; w++ {
		slResWg.Add(1)
		client := backend.NewSongLinkClient()
		go func(c *backend.SongLinkClient) {
			defer slResWg.Done()
			for work := range slWorkCh {
				rt := work.rt
				fmt.Printf("[%d/%d] Resolving platforms: %s - %s\n", work.idx+1, len(resolved), rt.artist, rt.title)
				if platforms, resolveErr := c.GetAllPlatformURLs(rt.spotifyID); resolveErr != nil {
					fmt.Fprintf(os.Stderr, "  song.link failed (%v) — services will fall back to internal resolution\n", resolveErr)
				} else {
					rt.platforms = platforms
					slCache.Put(rt.spotifyID, platforms)
				}
				downloadCh <- rt // push immediately — download begins right away
			}
		}(client)
	}
	// Close downloadCh when all SL workers finish; drain loop in Phase 2 will exit.
	go func() { slResWg.Wait(); close(downloadCh) }()

	// ── Phase 2: Download — starts as soon as tracks arrive in downloadCh ────────────────
	// Cache hits and no-ID tracks start downloading immediately.
	// SL-resolved tracks start ~6 seconds apart (one per song.link call) for a single worker.
	responses := make([]DownloadResponse, len(resolved))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var downloadCounter int64

	fmt.Fprintf(os.Stderr, "\nDownloading %d track(s) with %d concurrent worker(s)...\n\n", len(resolved), workers)

	for rt := range downloadCh {
		wg.Add(1)
		sem <- struct{}{}
		go func(rt *resolvedTrack) {
			defer wg.Done()
			defer func() { <-sem }()
			// Wait for metadata goroutine to finish writing rt fields before reading them.
			// 20-second timeout prevents blocking if a metadata fetch hangs.
			select {
			case <-rt.metaDone:
			case <-time.After(20 * time.Second):
			}

			// Emit a structured download-start line so the Python orchestrator
			// can show "Downloading N/M" progress during the download phase.
			dlIdx := atomic.AddInt64(&downloadCounter, 1)
			fmt.Printf("[%d/%d] Downloading: %s - %s\n", dlIdx, len(resolved), rt.artist, rt.title)

			var lastResp DownloadResponse
			succeeded := false

			// Build the list of services this track can actually use.
			candidate := buildAvailableServices(rt.platforms)
			if len(candidate) == 0 {
				// No pre-resolved URLs — use the default order; each service's
				// downloader will call song.link internally for the ones it needs.
				for _, s := range defaultOrder {
					// Skip services that hard-require a SpotifyID when we don't have one.
					if rt.spotifyID == "" && (s == "qobuz" || s == "amazon") {
						continue
					}
					candidate = append(candidate, s)
				}
			}

			if len(candidate) == 0 {
				responses[rt.index] = DownloadResponse{
					Success:        false,
					Error:          "no candidate services available (missing Spotify ID?)",
					Message:        "Download failed",
					OriginalArtist: rt.artist,
					OriginalTitle:  rt.title,
				}
				fmt.Printf("[TRACK_FAIL] %s || %s || no candidate services available\n", rt.artist, rt.title)
				return
			}

			for _, svc := range candidate {
				req := DownloadRequest{
					ArtistName: rt.artist,
					TrackName:  rt.title,
					Service:    svc,
					OutputDir:  outputDir,
					SpotifyID:  rt.spotifyID,
				}
				// Inject pre-resolved URLs — bypasses each service's internal song.link call.
				if rt.platforms != nil {
					switch svc {
					case "tidal":
						req.ServiceURL = rt.platforms.TidalURL
					case "amazon":
						req.ServiceURL = rt.platforms.AmazonURL
					case "qobuz":
						req.ISRC = rt.platforms.DeezerISRC
					}
				}
				// Inject pre-fetched Spotify metadata — prevents a redundant Spotify API
				// call inside DownloadTrack for every single track downloaded.
				if rt.albumName != "" {
					req.AlbumName = rt.albumName
				}
				if rt.albumArtist != "" {
					req.AlbumArtist = rt.albumArtist
				}
				if rt.releaseDate != "" {
					req.ReleaseDate = rt.releaseDate
				}
				if rt.coverURL != "" {
					req.CoverURL = rt.coverURL
				}
				if rt.copyright != "" {
					req.Copyright = rt.copyright
				}
				if rt.publisher != "" {
					req.Publisher = rt.publisher
				}
				if rt.trackNumber != 0 {
					req.SpotifyTrackNumber = rt.trackNumber
				}
				if rt.discNumber != 0 {
					req.SpotifyDiscNumber = rt.discNumber
				}
				if rt.totalTracks != 0 {
					req.SpotifyTotalTracks = rt.totalTracks
				}
				if rt.totalDiscs != 0 {
					req.SpotifyTotalDiscs = rt.totalDiscs
				}
				if rt.duration != 0 {
					req.Duration = rt.duration
				}
				// Inject ISRC and MusicBrainz IDs so DownloadTrack can enrich the file tags.
				if rt.isrc != "" {
					req.ISRC = rt.isrc
				}
				if rt.mbTrackID != "" {
					req.MusicBrainzTrackID = rt.mbTrackID
				}
				if rt.mbAlbumID != "" {
					req.MusicBrainzAlbumID = rt.mbAlbumID
				}
				if rt.mbArtistID != "" {
					req.MusicBrainzArtistID = rt.mbArtistID
				}

				resp, dlErr := a.DownloadTrack(req)
				resp.OriginalArtist = rt.artist
				resp.OriginalTitle = rt.title
				lastResp = resp

				if dlErr == nil && (resp.Success || resp.AlreadyExists) {
					succeeded = true
					break
				}
				fmt.Fprintf(os.Stderr, "  [%s] failed for '%s - %s': %v\n", svc, rt.artist, rt.title, dlErr)
			}

			if !succeeded {
				lastResp.Success = false
				lastResp.OriginalArtist = rt.artist
				lastResp.OriginalTitle = rt.title
				if lastResp.Error == "" {
					lastResp.Error = "all services failed"
				}
				if lastResp.Message == "" {
					lastResp.Message = "Download failed"
				}
			}
			responses[rt.index] = lastResp

			// Emit structured progress line for real-time tracking by the Python orchestrator.
			if succeeded {
				fmt.Printf("[TRACK_OK] %s || %s\n", rt.artist, rt.title)
			} else {
				// Truncate error to first line, max 120 chars, to avoid HTML blobs in structured output.
				errMsg := lastResp.Error
				if nl := strings.IndexByte(errMsg, '\n'); nl >= 0 {
					errMsg = errMsg[:nl]
				}
				errMsg = strings.TrimSpace(errMsg)
				if len(errMsg) > 120 {
					errMsg = errMsg[:120] + "..."
				}
				fmt.Printf("[TRACK_FAIL] %s || %s || %s\n", rt.artist, rt.title, errMsg)
			}
		}(rt)
	}

	wg.Wait()
	return responses, nil
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

type DownloadRequest struct {
	ISRC                 string `json:"isrc"`
	Service              string `json:"service"`
	Query                string `json:"query,omitempty"`
	TrackName            string `json:"track_name,omitempty"`
	ArtistName           string `json:"artist_name,omitempty"`
	AlbumName            string `json:"album_name,omitempty"`
	AlbumArtist          string `json:"album_artist,omitempty"`
	ReleaseDate          string `json:"release_date,omitempty"`
	CoverURL             string `json:"cover_url,omitempty"`
	ApiURL               string `json:"api_url,omitempty"`
	OutputDir            string `json:"output_dir,omitempty"`
	AudioFormat          string `json:"audio_format,omitempty"`
	FilenameFormat       string `json:"filename_format,omitempty"`
	TrackNumber          bool   `json:"track_number,omitempty"`
	Position             int    `json:"position,omitempty"`
	UseAlbumTrackNumber  bool   `json:"use_album_track_number,omitempty"`
	SpotifyID            string `json:"spotify_id,omitempty"`
	EmbedMaxQualityCover bool   `json:"embed_max_quality_cover,omitempty"`
	ServiceURL           string `json:"service_url,omitempty"`
	Duration             int    `json:"duration,omitempty"`
	ItemID               string `json:"item_id,omitempty"`
	SpotifyTrackNumber   int    `json:"spotify_track_number,omitempty"`
	SpotifyDiscNumber    int    `json:"spotify_disc_number,omitempty"`
	SpotifyTotalTracks   int    `json:"spotify_total_tracks,omitempty"`
	SpotifyTotalDiscs    int    `json:"spotify_total_discs,omitempty"`
	Copyright            string `json:"copyright,omitempty"`
	Publisher            string `json:"publisher,omitempty"`
	// MusicBrainz identifiers embedded as extra tags after download for Plex matching.
	MusicBrainzTrackID  string `json:"musicbrainz_track_id,omitempty"`
	MusicBrainzAlbumID  string `json:"musicbrainz_album_id,omitempty"`
	MusicBrainzArtistID string `json:"musicbrainz_artist_id,omitempty"`
}

type DownloadResponse struct {
	Success        bool   `json:"success"`
	Message        string `json:"message"`
	File           string `json:"file,omitempty"`
	Error          string `json:"error,omitempty"`
	AlreadyExists  bool   `json:"already_exists,omitempty"`
	ItemID         string `json:"item_id,omitempty"`
	OriginalArtist string `json:"original_artist,omitempty"`
	OriginalTitle  string `json:"original_title,omitempty"`
}

type SpotifySearchByTypeRequest struct {
	Query      string `json:"query"`
	SearchType string `json:"search_type"`
	Limit      int    `json:"limit"`
	Offset     int    `json:"offset"`
}

func (a *App) SearchSpotifyByType(req SpotifySearchByTypeRequest) ([]backend.SearchResult, error) {
	if req.Query == "" {
		return nil, fmt.Errorf("search query is required")
	}

	if req.SearchType == "" {
		return nil, fmt.Errorf("search type is required")
	}

	if req.Limit <= 0 {
		req.Limit = 50
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return backend.SearchSpotifyByType(ctx, req.Query, req.SearchType, req.Limit, req.Offset)
}

func (a *App) DownloadTrack(req DownloadRequest) (DownloadResponse, error) {

	if req.Service == "qobuz" && req.ISRC == "" && req.SpotifyID == "" {
		return DownloadResponse{
			Success: false,
			Error:   "Spotify ID is required for Qobuz",
		}, fmt.Errorf("spotify ID is required for Qobuz")
	}

	if req.Service == "" {
		req.Service = "tidal"
	}

	if req.OutputDir == "" {
		req.OutputDir = "."
	} else {

		req.OutputDir = backend.NormalizePath(req.OutputDir)
	}

	if req.AudioFormat == "" {
		req.AudioFormat = "LOSSLESS"
	}

	var err error
	var filename string

	if req.FilenameFormat == "" {
		req.FilenameFormat = "title-artist"
	}

	itemID := req.ItemID
	if itemID == "" {

		if req.SpotifyID != "" {
			itemID = fmt.Sprintf("%s-%d", req.SpotifyID, time.Now().UnixNano())
		} else {
			itemID = fmt.Sprintf("%s-%s-%d", req.TrackName, req.ArtistName, time.Now().UnixNano())
		}

		backend.AddToQueue(itemID, req.TrackName, req.ArtistName, req.AlbumName, req.SpotifyID)
	}

	backend.SetDownloading(true)
	backend.StartDownloadItem(itemID)
	defer backend.SetDownloading(false)

	spotifyURL := ""
	if req.SpotifyID != "" {
		spotifyURL = fmt.Sprintf("https://open.spotify.com/track/%s", req.SpotifyID)
	}

	if req.SpotifyID != "" && (req.Copyright == "" || req.Publisher == "" || req.SpotifyTotalDiscs == 0 || req.ReleaseDate == "" || req.SpotifyTotalTracks == 0 || req.SpotifyTrackNumber == 0) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		trackURL := fmt.Sprintf("https://open.spotify.com/track/%s", req.SpotifyID)
		trackData, err := backend.GetFilteredSpotifyData(ctx, trackURL, false, 0)
		if err == nil {

			var trackResp struct {
				Track struct {
					Name        string `json:"name"`
					AlbumName   string `json:"album_name"`
					Copyright   string `json:"copyright"`
					Publisher   string `json:"publisher"`
					TotalDiscs  int    `json:"total_discs"`
					TotalTracks int    `json:"total_tracks"`
					TrackNumber int    `json:"track_number"`
					ReleaseDate string `json:"release_date"`
				} `json:"track"`
			}
			if jsonData, jsonErr := json.Marshal(trackData); jsonErr == nil {
				if json.Unmarshal(jsonData, &trackResp) == nil {
					// Populate basic metadata if missing
					if req.TrackName == "" && trackResp.Track.Name != "" {
						req.TrackName = trackResp.Track.Name
					}
					if req.AlbumName == "" && trackResp.Track.AlbumName != "" {
						req.AlbumName = trackResp.Track.AlbumName
					}

					if req.Copyright == "" && trackResp.Track.Copyright != "" {
						req.Copyright = trackResp.Track.Copyright
					}
					if req.Publisher == "" && trackResp.Track.Publisher != "" {
						req.Publisher = trackResp.Track.Publisher
					}
					if req.SpotifyTotalDiscs == 0 && trackResp.Track.TotalDiscs > 0 {
						req.SpotifyTotalDiscs = trackResp.Track.TotalDiscs
					}
					if req.SpotifyTotalTracks == 0 && trackResp.Track.TotalTracks > 0 {
						req.SpotifyTotalTracks = trackResp.Track.TotalTracks
					}
					if req.SpotifyTrackNumber == 0 && trackResp.Track.TrackNumber > 0 {
						req.SpotifyTrackNumber = trackResp.Track.TrackNumber
					}
					if req.ReleaseDate == "" && trackResp.Track.ReleaseDate != "" {
						req.ReleaseDate = trackResp.Track.ReleaseDate
					}
				}
			}
		}
	}

	if req.TrackName != "" && req.ArtistName != "" {
		/* year := ""
		if len(req.ReleaseDate) >= 4 {
			year = req.ReleaseDate[:4]
		} */

		// Check for missing album/artist data, which causes mis-categorized folders like "(2021)/Title.flac"
		// If album metadata is missing, the folder will just be "(Year)" or "Unknown (Year)"

		cleanAlbum := backend.SanitizeFolderPath(req.AlbumName)
		if cleanAlbum == "" {
			cleanAlbum = "Unknown Album"
		}

		albumFolder := cleanAlbum
		// Removed year suffix: folder will just be "Album Name"
		// if year != "" {
		// 	albumFolder = fmt.Sprintf("%s (%s)", albumFolder, year)
		// }

		cleanArtist := backend.SanitizeFolderPath(req.AlbumArtist)
		if cleanArtist == "" {
			cleanArtist = backend.SanitizeFolderPath(req.ArtistName)
		}
		if cleanArtist == "" {
			cleanArtist = "Unknown Artist"
		}

		subfolder := filepath.Join(cleanArtist, albumFolder)
		fullOutputDir := filepath.Join(req.OutputDir, subfolder)
		// Always create output subfolders before saving the file
		if err := os.MkdirAll(fullOutputDir, 0755); err != nil {
			return DownloadResponse{Success: false, Error: err.Error(), Message: "directory error"}, err
		}

		// Force filename format to be simple "Title.flac"
		req.FilenameFormat = "title"

		// DO NOT overwrite OutputDir with the file path anymore.
		// The downloader functions (Tidal, Amazon, etc.) expect a DIRECTORY.
		// We set req.OutputDir to the full subdirectory we just created.
		req.OutputDir = fullOutputDir

		// Calculate expected path just for the "Already Exists" check
		expectedFilename := backend.BuildExpectedFilename(req.TrackName, req.ArtistName, req.AlbumName, req.AlbumArtist, req.ReleaseDate, req.FilenameFormat, req.TrackNumber, req.Position, req.SpotifyDiscNumber, req.UseAlbumTrackNumber)
		expectedPath := filepath.Join(fullOutputDir, expectedFilename)

		if fileInfo, err := os.Stat(expectedPath); err == nil && fileInfo.Size() > 100*1024 {

			backend.SkipDownloadItem(itemID, expectedPath)
			return DownloadResponse{
				Success:       true,
				Message:       "File already exists",
				File:          expectedPath,
				AlreadyExists: true,
				ItemID:        itemID,
			}, nil
		}

		// Clean up valid output dir if download fails later
		defer func() {
			if err != nil {
				// If error occurred, check if directory is empty and remove it
				f, _ := os.Open(fullOutputDir)
				_, errRead := f.Readdirnames(1)
				f.Close()
				if errRead == io.EOF {
					// Directory is empty
					os.Remove(fullOutputDir)
					// Also try to remove artist folder if that's empty now
					artistDir := filepath.Dir(fullOutputDir)
					fArtist, _ := os.Open(artistDir)
					_, errReadArtist := fArtist.Readdirnames(1)
					fArtist.Close()
					if errReadArtist == io.EOF {
						os.Remove(artistDir)
					}
				}
			}
		}()
	}

	switch req.Service {
	case "amazon":
		downloader := backend.NewAmazonDownloader()
		if req.ServiceURL != "" {

			filename, err = downloader.DownloadByURL(req.ServiceURL, req.OutputDir, req.AudioFormat, req.FilenameFormat, req.TrackNumber, req.Position, req.TrackName, req.ArtistName, req.AlbumName, req.AlbumArtist, req.ReleaseDate, req.CoverURL, req.SpotifyTrackNumber, req.SpotifyDiscNumber, req.SpotifyTotalTracks, req.EmbedMaxQualityCover, req.SpotifyTotalDiscs, req.Copyright, req.Publisher, spotifyURL)
		} else {
			if req.SpotifyID == "" {
				return DownloadResponse{
					Success: false,
					Error:   "Spotify ID is required for Amazon Music",
				}, fmt.Errorf("spotify ID is required for Amazon Music")
			}
			filename, err = downloader.DownloadBySpotifyID(req.SpotifyID, req.OutputDir, req.AudioFormat, req.FilenameFormat, req.TrackNumber, req.Position, req.TrackName, req.ArtistName, req.AlbumName, req.AlbumArtist, req.ReleaseDate, req.CoverURL, req.SpotifyTrackNumber, req.SpotifyDiscNumber, req.SpotifyTotalTracks, req.EmbedMaxQualityCover, req.SpotifyTotalDiscs, req.Copyright, req.Publisher, spotifyURL)
		}

	case "tidal":
		downloader := backend.NewTidalDownloader(req.ApiURL)
		if req.ServiceURL != "" {
			filename, err = downloader.DownloadByURL(req.ServiceURL, req.OutputDir, req.AudioFormat, req.FilenameFormat, req.TrackNumber, req.Position, req.TrackName, req.ArtistName, req.AlbumName, req.AlbumArtist, req.ReleaseDate, req.UseAlbumTrackNumber, req.CoverURL, req.EmbedMaxQualityCover, req.SpotifyTrackNumber, req.SpotifyDiscNumber, req.SpotifyTotalTracks, req.SpotifyTotalDiscs, req.Copyright, req.Publisher, spotifyURL)
		} else {
			if req.SpotifyID == "" {
				return DownloadResponse{
					Success: false,
					Error:   "Spotify ID is required for Tidal",
				}, fmt.Errorf("spotify ID is required for Tidal")
			}
			filename, err = downloader.Download(req.SpotifyID, req.OutputDir, req.AudioFormat, req.FilenameFormat, req.TrackNumber, req.Position, req.TrackName, req.ArtistName, req.AlbumName, req.AlbumArtist, req.ReleaseDate, req.UseAlbumTrackNumber, req.CoverURL, req.EmbedMaxQualityCover, req.SpotifyTrackNumber, req.SpotifyDiscNumber, req.SpotifyTotalTracks, req.SpotifyTotalDiscs, req.Copyright, req.Publisher, spotifyURL)
		}

	case "qobuz":
		downloader := backend.NewQobuzDownloader()
		if req.SpotifyID != "" && req.ServiceURL == "" {
			// Try direct download via Spotify ID (if supported) or just skip to ISRC
			// The original code tried 'Download', but qobuz downloader typically needs ISRC or explicit URL.
			// Let's rely on standard ISRC flow below unless there's a specific ID method.
		}

		quality := req.AudioFormat
		if quality == "" {
			quality = "6"
		}

		deezerISRC := req.ISRC

		if len(deezerISRC) != 12 || !isValidISRC(deezerISRC) {
			deezerISRC = ""
		}

		if deezerISRC == "" && req.SpotifyID != "" {

			songlinkClient := backend.NewSongLinkClient()
			deezerURL, err := songlinkClient.GetDeezerURLFromSpotify(req.SpotifyID)
			if err != nil {
				return DownloadResponse{
					Success: false,
					Error:   fmt.Sprintf("Failed to get Deezer URL: %v", err),
				}, err
			}
			deezerISRC, err = backend.GetDeezerISRC(deezerURL)
			if err != nil {
				return DownloadResponse{
					Success: false,
					Error:   fmt.Sprintf("Failed to get ISRC from Deezer: %v", err),
				}, err
			}
		}
		if deezerISRC == "" {
			return DownloadResponse{
				Success: false,
				Error:   "ISRC is required for Qobuz (could not fetch from Deezer)",
			}, fmt.Errorf("ISRC is required for Qobuz")
		}
		filename, err = downloader.DownloadByISRC(deezerISRC, req.OutputDir, quality, req.FilenameFormat, req.TrackNumber, req.Position, req.TrackName, req.ArtistName, req.AlbumName, req.AlbumArtist, req.ReleaseDate, req.UseAlbumTrackNumber, req.CoverURL, req.EmbedMaxQualityCover, req.SpotifyTrackNumber, req.SpotifyDiscNumber, req.SpotifyTotalTracks, req.SpotifyTotalDiscs, req.Copyright, req.Publisher, spotifyURL)

	default:
		return DownloadResponse{
			Success: false,
			Error:   fmt.Sprintf("Unknown service: %s", req.Service),
		}, fmt.Errorf("unknown service: %s", req.Service)
	}

	if err != nil {
		backend.FailDownloadItem(itemID, fmt.Sprintf("Download failed: %v", err))

		if filename != "" && !strings.HasPrefix(filename, "EXISTS:") {

			if _, statErr := os.Stat(filename); statErr == nil {
				fmt.Fprintf(os.Stderr, "Removing corrupted/partial file after failed download: %s\n", filename)
				if removeErr := os.Remove(filename); removeErr != nil {
					fmt.Fprintf(os.Stderr, "Warning: Failed to remove corrupted file %s: %v\n", filename, removeErr)
				}
			}
		}

		return DownloadResponse{
			Success: false,
			Error:   fmt.Sprintf("Download failed: %v", err),
			ItemID:  itemID,
		}, err
	}

	alreadyExists := false
	if strings.HasPrefix(filename, "EXISTS:") {
		alreadyExists = true
		filename = strings.TrimPrefix(filename, "EXISTS:")
	}

	// Enrich the downloaded file with ISRC and MusicBrainz identifiers so Plex can
	// definitively match the track, album, and artist in its online database.
	// This is a lightweight read-modify-write that preserves all existing tags.
	if !alreadyExists {
		extraTags := map[string]string{
			"ISRC":                 req.ISRC,
			"MUSICBRAINZ_TRACKID":  req.MusicBrainzTrackID,
			"MUSICBRAINZ_ALBUMID":  req.MusicBrainzAlbumID,
			"MUSICBRAINZ_ARTISTID": req.MusicBrainzArtistID,
		}
		if enrichErr := backend.EnrichFileTags(filename, extraTags); enrichErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: ISRC/MusicBrainz tag enrichment failed for %s: %v\n",
				filename, enrichErr)
		}
	}

	message := "Download completed successfully"
	if alreadyExists {
		message = "File already exists"
		backend.SkipDownloadItem(itemID, filename)
	} else {

		if fileInfo, statErr := os.Stat(filename); statErr == nil {
			finalSize := float64(fileInfo.Size()) / (1024 * 1024)
			backend.CompleteDownloadItem(itemID, filename, finalSize)
		} else {

			backend.CompleteDownloadItem(itemID, filename, 0)
		}
	}

	return DownloadResponse{
		Success:       true,
		Message:       message,
		File:          filename,
		AlreadyExists: alreadyExists,
		ItemID:        itemID,
	}, nil
}
