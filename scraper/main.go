package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	csvPath := flag.String("csv", "", "Path to CSV file containing songs")
	outputDir := flag.String("output", ".", "Output directory for downloads")
	workers := flag.Int("workers", 0,
		"Number of concurrent download workers (default 3). "+
			"Also settable via SCRAPER_WORKERS env var.")
	flag.Parse()

	// Apply CLI flags to env vars so DownloadSongsFromCSV picks them up.
	if *workers > 0 {
		os.Setenv("SCRAPER_WORKERS", fmt.Sprintf("%d", *workers))
	}

	if *csvPath != "" {
		app := NewApp()
		fmt.Fprintf(os.Stderr, "Batch downloading songs from CSV: %s\n", *csvPath)
		responses, err := app.DownloadSongsFromCSV(*csvPath, *outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		// Write evaluated CSV next to the input report so pipeline.py can find it.
		evaluatedPath := filepath.Join(filepath.Dir(*csvPath), "discover-weekly-report-evaluated.csv")
		f, err := os.Create(evaluatedPath)
		if err == nil {
			defer f.Close()
			f.WriteString("artist,title\n")
		}

		succeeded, failed := 0, 0
		for _, resp := range responses {
			if resp.Success || resp.AlreadyExists {
				succeeded++
				if f != nil {
					cleanArtist := escapeCSV(resp.OriginalArtist)
					cleanTitle := escapeCSV(resp.OriginalTitle)
					f.WriteString(fmt.Sprintf("%s,%s\n", cleanArtist, cleanTitle))
				}
			} else {
				failed++
			}
		}
		fmt.Fprintf(os.Stderr, "\nBatch complete: %d downloaded, %d failed.\n", succeeded, failed)
		return
	}

}

func escapeCSV(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(s, "\"", "\"\""))
	}
	return s
}
