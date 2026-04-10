package backend

import (
	"fmt"
	"math"
)

const (
	previewMaxSeconds         = 35
	previewExpectedMinSeconds = 60
	largeMismatchMinExpected  = 90
	minAllowedDurationDiff    = 15
	durationDiffRatio         = 0.25
)

// ValidateDownloadedTrackDuration checks that a downloaded file is the expected length.
// It catches "preview/sample" downloads (≤35 s when ≥60 s expected) and large duration
// mismatches that suggest a wrong track was served.  Returns (false, nil) if validation
// was skipped (missing inputs).  Returns (true, err) if a problem was found.
func ValidateDownloadedTrackDuration(filePath string, expectedSeconds int) (bool, error) {
	if filePath == "" || expectedSeconds <= 0 {
		return false, nil
	}

	actualDuration, err := GetAudioDuration(filePath)
	if err != nil || actualDuration <= 0 {
		return false, nil
	}

	actualSeconds := int(math.Round(actualDuration))
	if actualSeconds <= 0 {
		return false, nil
	}

	// Detect preview/sample download: very short file when a full track was expected.
	if expectedSeconds >= previewExpectedMinSeconds && actualSeconds <= previewMaxSeconds {
		return true, fmt.Errorf(
			"detected preview/sample download: file is %ds, expected about %ds. file was removed",
			actualSeconds, expectedSeconds,
		)
	}

	// Detect gross duration mismatch for longer tracks (different track served).
	if expectedSeconds >= largeMismatchMinExpected {
		allowedDiff := int(math.Max(minAllowedDurationDiff, math.Round(float64(expectedSeconds)*durationDiffRatio)))
		diff := int(math.Abs(float64(actualSeconds - expectedSeconds)))
		if diff > allowedDiff {
			return true, fmt.Errorf(
				"downloaded file duration mismatch: file is %ds, expected about %ds. file was removed",
				actualSeconds, expectedSeconds,
			)
		}
	}

	return true, nil
}
