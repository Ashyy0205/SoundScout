package backend

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	qobuzAPIBaseURL          = "https://www.qobuz.com/api.json/0.2"
	qobuzDefaultAPIAppID     = "712109809"
	qobuzDefaultAPIAppSecret = "589be88e4538daea11f509d29e4a23b1"
	qobuzCredentialsCacheFile = "qobuz-api-credentials.json"
	qobuzCredentialsCacheTTL  = 24 * time.Hour
	qobuzOpenTrackProbeURL    = "https://open.qobuz.com/track/1"
)

var (
	qobuzCredentialsMu           sync.Mutex
	qobuzCachedCredentials       *qobuzAPICredentials
	qobuzOpenBundleScriptPattern = regexp.MustCompile(`<script[^>]+src="([^"]+/js/main\.js|/resources/[^"]+/js/main\.js)"`)
	qobuzOpenAPIConfigPattern    = regexp.MustCompile(`app_id:"(?P<app_id>\d{9})",app_secret:"(?P<app_secret>[a-f0-9]{32})"`)
)

type qobuzAPICredentials struct {
	AppID         string `json:"app_id"`
	AppSecret     string `json:"app_secret"`
	Source        string `json:"source,omitempty"`
	FetchedAtUnix int64  `json:"fetched_at_unix"`
}

func defaultQobuzAPICredentials() *qobuzAPICredentials {
	return &qobuzAPICredentials{
		AppID:         qobuzDefaultAPIAppID,
		AppSecret:     qobuzDefaultAPIAppSecret,
		Source:        "embedded-default",
		FetchedAtUnix: time.Now().Unix(),
	}
}

func qobuzCredentialsCachePath() (string, error) {
	appDir, err := EnsureAppDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(appDir, qobuzCredentialsCacheFile), nil
}

func loadQobuzCachedCredentials() (*qobuzAPICredentials, error) {
	cachePath, err := qobuzCredentialsCachePath()
	if err != nil {
		return nil, err
	}

	body, err := os.ReadFile(cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read qobuz credentials cache: %w", err)
	}

	var creds qobuzAPICredentials
	if err := json.Unmarshal(body, &creds); err != nil {
		return nil, fmt.Errorf("failed to parse qobuz credentials cache: %w", err)
	}

	if strings.TrimSpace(creds.AppID) == "" || strings.TrimSpace(creds.AppSecret) == "" {
		return nil, fmt.Errorf("qobuz credentials cache is incomplete")
	}

	return &creds, nil
}

func saveQobuzCachedCredentials(creds *qobuzAPICredentials) error {
	if creds == nil {
		return fmt.Errorf("qobuz credentials are required")
	}

	cachePath, err := qobuzCredentialsCachePath()
	if err != nil {
		return err
	}

	body, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(cachePath, body, 0o644); err != nil {
		return fmt.Errorf("failed to write qobuz credentials cache: %w", err)
	}

	return nil
}

func qobuzCredentialsCacheIsFresh(creds *qobuzAPICredentials) bool {
	if creds == nil || creds.FetchedAtUnix == 0 ||
		strings.TrimSpace(creds.AppID) == "" || strings.TrimSpace(creds.AppSecret) == "" {
		return false
	}
	return time.Since(time.Unix(creds.FetchedAtUnix, 0)) < qobuzCredentialsCacheTTL
}

func scrapeQobuzOpenCredentials(client *http.Client) (*qobuzAPICredentials, error) {
	req, err := http.NewRequest(http.MethodGet, qobuzOpenTrackProbeURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", DefaultDownloaderUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch open.qobuz.com shell: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("open.qobuz.com returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(preview)))
	}

	htmlBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read open.qobuz.com shell: %w", err)
	}

	scriptMatch := qobuzOpenBundleScriptPattern.FindStringSubmatch(string(htmlBody))
	if len(scriptMatch) < 2 {
		return nil, fmt.Errorf("qobuz open bundle URL not found in page")
	}

	bundleURL := strings.TrimSpace(scriptMatch[1])
	if strings.HasPrefix(bundleURL, "/") {
		bundleURL = "https://open.qobuz.com" + bundleURL
	}
	if bundleURL == "" {
		return nil, fmt.Errorf("qobuz open bundle URL is empty")
	}

	bundleReq, err := http.NewRequest(http.MethodGet, bundleURL, nil)
	if err != nil {
		return nil, err
	}
	bundleReq.Header.Set("User-Agent", DefaultDownloaderUserAgent)

	bundleResp, err := client.Do(bundleReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch qobuz open bundle: %w", err)
	}
	defer bundleResp.Body.Close()

	if bundleResp.StatusCode != http.StatusOK {
		preview, _ := io.ReadAll(io.LimitReader(bundleResp.Body, 512))
		return nil, fmt.Errorf("qobuz open bundle returned status %d: %s", bundleResp.StatusCode, strings.TrimSpace(string(preview)))
	}

	bundleBody, err := io.ReadAll(bundleResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read qobuz open bundle: %w", err)
	}

	configMatch := qobuzOpenAPIConfigPattern.FindStringSubmatch(string(bundleBody))
	if len(configMatch) < 3 {
		return nil, fmt.Errorf("qobuz api app_id/app_secret not found in open.qobuz.com bundle")
	}

	return &qobuzAPICredentials{
		AppID:         strings.TrimSpace(configMatch[1]),
		AppSecret:     strings.TrimSpace(configMatch[2]),
		Source:        bundleURL,
		FetchedAtUnix: time.Now().Unix(),
	}, nil
}

func qobuzNormalizedPath(path string) string {
	return strings.Trim(strings.TrimSpace(path), "/")
}

func qobuzSignaturePayload(path string, params url.Values, timestamp, secret string) string {
	normalizedPath := strings.ReplaceAll(qobuzNormalizedPath(path), "/", "")
	keys := make([]string, 0, len(params))
	for key := range params {
		switch key {
		case "app_id", "request_ts", "request_sig":
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	builder.WriteString(normalizedPath)
	for _, key := range keys {
		values := params[key]
		if len(values) == 0 {
			builder.WriteString(key)
			continue
		}
		for _, value := range values {
			builder.WriteString(key)
			builder.WriteString(value)
		}
	}
	builder.WriteString(timestamp)
	builder.WriteString(secret)
	return builder.String()
}

func qobuzRequestSignature(path string, params url.Values, timestamp, secret string) string {
	sum := md5.Sum([]byte(qobuzSignaturePayload(path, params, timestamp, secret)))
	return hex.EncodeToString(sum[:])
}

func newQobuzSignedRequestWithCredentials(method, path string, params url.Values, creds *qobuzAPICredentials) (*http.Request, error) {
	normalizedPath := qobuzNormalizedPath(path)
	if normalizedPath == "" {
		return nil, fmt.Errorf("qobuz request path is empty")
	}
	if creds == nil || strings.TrimSpace(creds.AppID) == "" || strings.TrimSpace(creds.AppSecret) == "" {
		return nil, fmt.Errorf("qobuz credentials are incomplete")
	}

	clonedParams := url.Values{}
	for key, values := range params {
		for _, value := range values {
			clonedParams.Add(key, value)
		}
	}

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	clonedParams.Set("app_id", creds.AppID)
	clonedParams.Set("request_ts", timestamp)
	clonedParams.Set("request_sig", qobuzRequestSignature(normalizedPath, params, timestamp, creds.AppSecret))

	reqURL := fmt.Sprintf("%s/%s?%s", qobuzAPIBaseURL, normalizedPath, clonedParams.Encode())
	req, err := http.NewRequest(method, reqURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", DefaultDownloaderUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-App-Id", creds.AppID)

	return req, nil
}

func qobuzCredentialsSupportSignedMetadata(client *http.Client, creds *qobuzAPICredentials) bool {
	if creds == nil {
		return false
	}

	req, err := newQobuzSignedRequestWithCredentials(http.MethodGet, "track/search", url.Values{
		"query": {"USUM71703861"},
		"limit": {"1"},
	}, creds)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func getQobuzAPICredentials(forceRefresh bool) (*qobuzAPICredentials, error) {
	qobuzCredentialsMu.Lock()
	defer qobuzCredentialsMu.Unlock()

	if !forceRefresh && qobuzCredentialsCacheIsFresh(qobuzCachedCredentials) {
		return qobuzCachedCredentials, nil
	}

	cachedFromDisk, diskErr := loadQobuzCachedCredentials()
	if diskErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to read Qobuz credentials cache: %v\n", diskErr)
	}
	if !forceRefresh && qobuzCredentialsCacheIsFresh(cachedFromDisk) {
		qobuzCachedCredentials = cachedFromDisk
		return qobuzCachedCredentials, nil
	}

	client := &http.Client{Timeout: 30 * time.Second}
	scrapedCreds, scrapeErr := scrapeQobuzOpenCredentials(client)
	if scrapeErr == nil {
		if qobuzCredentialsSupportSignedMetadata(client, scrapedCreds) {
			qobuzCachedCredentials = scrapedCreds
			if err := saveQobuzCachedCredentials(scrapedCreds); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to write Qobuz credentials cache: %v\n", err)
			}
			fmt.Fprintf(os.Stderr, "Loaded fresh Qobuz credentials (app_id=%s)\n", scrapedCreds.AppID)
			return qobuzCachedCredentials, nil
		}
		scrapeErr = fmt.Errorf("scraped credentials did not pass validation")
	}

	if cachedFromDisk != nil {
		qobuzCachedCredentials = cachedFromDisk
		if scrapeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: using cached Qobuz credentials: %v\n", scrapeErr)
		}
		return qobuzCachedCredentials, nil
	}

	if qobuzCachedCredentials != nil {
		if scrapeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: using in-memory Qobuz credentials: %v\n", scrapeErr)
		}
		return qobuzCachedCredentials, nil
	}

	fallback := defaultQobuzAPICredentials()
	qobuzCachedCredentials = fallback
	if scrapeErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: using embedded Qobuz credentials fallback: %v\n", scrapeErr)
	}
	return qobuzCachedCredentials, nil
}

func doQobuzSignedRequest(method, path string, params url.Values, client *http.Client) (*http.Response, error) {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}

	call := func(forceRefresh bool) (*http.Response, error) {
		creds, err := getQobuzAPICredentials(forceRefresh)
		if err != nil {
			return nil, err
		}
		req, err := newQobuzSignedRequestWithCredentials(method, path, params, creds)
		if err != nil {
			return nil, err
		}
		return client.Do(req)
	}

	resp, err := call(false)
	if err != nil {
		return nil, err
	}

	// On 400/401 refresh credentials and retry once.
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		return call(true)
	}

	return resp, nil
}
