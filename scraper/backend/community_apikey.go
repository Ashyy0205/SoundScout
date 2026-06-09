package backend

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// AppVersion is set at build time; used in community User-Agent strings.
var AppVersion = ""

var (
	communityAPIKeyOnce sync.Once
	communityAPIKey     string
	communityAPIKeyErr  error
)

var communityAPIKeySeedParts = [][]byte{
	[]byte("spotif"),
	[]byte("lac:co"),
	[]byte("mmunity:apikey:v1"),
}

var communityAPIKeyAAD = []byte("spotiflac|community|apikey|v1")

var communityAPIKeyNonce = []byte{
	0x20, 0x5c, 0x92, 0x4b, 0x61, 0xc2, 0x79, 0xd3, 0xea, 0x5d, 0xdd, 0xd4,
}

var communityAPIKeyCiphertext = []byte{
	0x51, 0x0b, 0x26, 0xaf, 0xac, 0x6f, 0xf6, 0x41, 0x79, 0xde, 0x8d, 0x36,
	0x83, 0x46, 0xb5, 0xd5, 0x96, 0xef, 0xad, 0xed, 0xe0, 0xd0, 0xc7, 0xc2,
	0x90, 0x01, 0x50, 0x5f, 0x55, 0x59, 0x9f, 0xac, 0x1f, 0xd0, 0x70, 0x18,
	0x91, 0x4f, 0x7a, 0x32,
}

var communityAPIKeyTag = []byte{
	0x56, 0xb0, 0x28, 0x68, 0x9f, 0x39, 0x0d, 0xbc, 0xc0, 0x8e, 0xfb, 0x52,
	0x3a, 0xd6, 0x18, 0xae,
}

func getCommunityAPIKey() (string, error) {
	communityAPIKeyOnce.Do(func() {
		hasher := sha256.New()
		for _, part := range communityAPIKeySeedParts {
			hasher.Write(part)
		}

		block, err := aes.NewCipher(hasher.Sum(nil))
		if err != nil {
			communityAPIKeyErr = err
			return
		}

		gcm, err := cipher.NewGCM(block)
		if err != nil {
			communityAPIKeyErr = err
			return
		}

		sealed := make([]byte, 0, len(communityAPIKeyCiphertext)+len(communityAPIKeyTag))
		sealed = append(sealed, communityAPIKeyCiphertext...)
		sealed = append(sealed, communityAPIKeyTag...)

		plaintext, err := gcm.Open(nil, communityAPIKeyNonce, sealed, communityAPIKeyAAD)
		if err != nil {
			communityAPIKeyErr = err
			return
		}

		communityAPIKey = string(plaintext)
	})

	if communityAPIKeyErr != nil {
		return "", communityAPIKeyErr
	}
	return communityAPIKey, nil
}

func communityUserAgent() string {
	version := strings.TrimSpace(AppVersion)
	if version == "" || version == "Unknown" {
		return "SoundScout-Scraper"
	}
	return "SoundScout-Scraper/" + version
}

func setCommunityRequestHeaders(req *http.Request) error {
	apiKey, err := getCommunityAPIKey()
	if err != nil {
		return fmt.Errorf("failed to prepare community API key: %w", err)
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("User-Agent", communityUserAgent())
	return nil
}
