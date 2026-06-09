package backend

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const communityDownloadPath = "/api/dl"

var communityURLSeedParts = [][]byte{
	[]byte("spotif"),
	[]byte("lac:co"),
	[]byte("mmunity:url:v1"),
}

var communityURLAAD = []byte("spotiflac|community|url|v1")

var (
	tidalCommunityURLNonce = []byte{
		0x6a, 0x2a, 0x9e, 0xf3, 0x25, 0x5f, 0x48, 0x3c, 0xc3, 0xdf, 0x1d, 0xa9,
	}
	tidalCommunityURLCiphertext = []byte{
		0x8f, 0x90, 0xa4, 0x28, 0x24, 0x06, 0x35, 0x13, 0x2d, 0x33, 0x96, 0x9a,
		0xd7, 0x2c, 0x31, 0x42, 0x6a, 0xf3, 0xee, 0x86, 0x34, 0x99, 0x15, 0x1e,
		0xa9, 0x07, 0x06, 0xe6, 0xee, 0x0d, 0x75,
	}
	tidalCommunityURLTag = []byte{
		0x4d, 0x1c, 0x4e, 0x98, 0x96, 0x07, 0x16, 0xad, 0x6a, 0x7c, 0xa0, 0xdf,
		0xe9, 0xc5, 0xf6, 0x87,
	}

	qobuzCommunityURLNonce = []byte{
		0x5f, 0xd8, 0xfd, 0xfd, 0x89, 0x83, 0xe7, 0x6c, 0xde, 0x48, 0x47, 0x8d,
	}
	qobuzCommunityURLCiphertext = []byte{
		0xfa, 0x35, 0x21, 0xba, 0x02, 0xc6, 0x15, 0x1f, 0x0e, 0xa3, 0xa6, 0x16,
		0x64, 0x2b, 0xd8, 0xfb, 0xf5, 0x35, 0xfe, 0xe9, 0x0e, 0x59, 0xd9, 0x25,
		0x72, 0x57, 0x88, 0x94, 0xa9, 0xb7, 0x70,
	}
	qobuzCommunityURLTag = []byte{
		0xd7, 0x72, 0xb5, 0x2b, 0x1c, 0xb1, 0xfd, 0xba, 0x22, 0x09, 0x25, 0x41,
		0x87, 0x85, 0x30, 0x1b,
	}

	amazonCommunityURLNonce = []byte{
		0x55, 0x18, 0x01, 0x42, 0x42, 0x0c, 0xf6, 0x78, 0x8a, 0x73, 0xd7, 0x63,
	}
	amazonCommunityURLCiphertext = []byte{
		0xd2, 0xf3, 0xdc, 0xe8, 0x62, 0xf0, 0xad, 0xc2, 0x4a, 0x43, 0xb1, 0xa2,
		0x1c, 0x0d, 0x41, 0x3e, 0x2e, 0x30, 0x29, 0x5e, 0x46, 0xe2, 0xc2, 0xd6,
		0xc1, 0xf3, 0xe3, 0x1a, 0x8f, 0x67, 0xfe,
	}
	amazonCommunityURLTag = []byte{
		0xf9, 0x0a, 0xfd, 0xed, 0x9e, 0xe8, 0xb4, 0xc0, 0x75, 0xf3, 0xd5, 0x74,
		0x3c, 0xb6, 0xa1, 0xb9,
	}
)

var (
	communityURLGCMOnce sync.Once
	communityURLGCM     cipher.AEAD
	communityURLGCMErr  error
)

func communityURLCipher() (cipher.AEAD, error) {
	communityURLGCMOnce.Do(func() {
		hasher := sha256.New()
		for _, part := range communityURLSeedParts {
			hasher.Write(part)
		}
		block, err := aes.NewCipher(hasher.Sum(nil))
		if err != nil {
			communityURLGCMErr = err
			return
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			communityURLGCMErr = err
			return
		}
		communityURLGCM = gcm
	})
	return communityURLGCM, communityURLGCMErr
}

func decryptCommunityURL(nonce, ciphertext, tag []byte) (string, error) {
	gcm, err := communityURLCipher()
	if err != nil {
		return "", err
	}
	sealed := make([]byte, 0, len(ciphertext)+len(tag))
	sealed = append(sealed, ciphertext...)
	sealed = append(sealed, tag...)
	plaintext, err := gcm.Open(nil, nonce, sealed, communityURLAAD)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

const communityRateLimitMaxRetries = 6

const communityRateLimitFallbackWait = 30 * time.Second

func GetTidalCommunityDownloadURL() string {
	base, _ := decryptCommunityURL(tidalCommunityURLNonce, tidalCommunityURLCiphertext, tidalCommunityURLTag)
	return base + communityDownloadPath
}

func GetQobuzCommunityDownloadURL() string {
	base, _ := decryptCommunityURL(qobuzCommunityURLNonce, qobuzCommunityURLCiphertext, qobuzCommunityURLTag)
	return base + communityDownloadPath
}

func GetAmazonCommunityDownloadURL() string {
	base, _ := decryptCommunityURL(amazonCommunityURLNonce, amazonCommunityURLCiphertext, amazonCommunityURLTag)
	return base + communityDownloadPath
}

func communityRetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return communityRateLimitFallbackWait
	}
	if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil && secs >= 0 {
			return time.Duration(secs)*time.Second + 250*time.Millisecond
		}
	}
	if reset := strings.TrimSpace(resp.Header.Get("X-RateLimit-Reset")); reset != "" {
		if epoch, err := strconv.ParseInt(reset, 10, 64); err == nil {
			if wait := time.Until(time.Unix(epoch, 0)); wait > 0 {
				return wait + 250*time.Millisecond
			}
		}
	}
	return communityRateLimitFallbackWait
}

func doCommunityRequest(client *http.Client, service string, reqFn func() (*http.Request, error)) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt <= communityRateLimitMaxRetries; attempt++ {
		req, err := reqFn()
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			ClearRateLimitCooldown()
			return resp, nil
		}

		wait := communityRetryAfter(resp)
		resp.Body.Close()
		lastErr = fmt.Errorf("%s community API rate limited (429)", service)

		if attempt == communityRateLimitMaxRetries {
			break
		}
		fmt.Printf("%s rate limited, waiting %.0fs before retry (%d/%d)...\n", service, wait.Seconds(), attempt+1, communityRateLimitMaxRetries)
		SetRateLimitCooldown(wait.Seconds())
		if sleepErr := SleepWithDownloadContext(wait); sleepErr != nil {
			ClearRateLimitCooldown()
			return nil, sleepErr
		}
		ClearRateLimitCooldown()
	}
	return nil, lastErr
}
