# SoundScout

Automated music discovery and download tool for Plex. SoundScout uses your Last.fm listening history to generate personalised recommendations, acquires missing tracks via Tidal or Qobuz, and keeps your Plex library and playlists up to date — all from a self-hosted web interface.

## Features

- **Discover Weekly** — generates a Plex playlist of recommended tracks seeded from your Last.fm history
- **Web UI** — search Spotify for tracks, albums, and artists; queue downloads directly to your library
- **Per-user scheduling** — each Plex user can link their own Last.fm account and set their own Auto Discovery schedule in Settings
- **Plex login gate** — only verified Plex users with access to your server can use the UI
- **Spotify import** — paste any Spotify playlist, album, or track URL to preview and download it to Plex; no developer app or Spotify account needed
- **Download queue** — live progress display with speed and ETA; concurrent downloads supported
- **Import step** — optional post-download tagging/import pipeline (e.g. beets)

## Quick Start (Docker Compose)

```yaml
services:
  soundscout:
    image: ghcr.io/ashyy0205/soundscout:main
    container_name: soundscout
    restart: unless-stopped
    ports:
      - "5000:5000"
    environment:
      TZ: America/New_York
      PLEX_BASEURL: http://192.168.1.10:32400
      PLEX_MUSIC_LIBRARY: Music
      WEBUI_SECRET_KEY: a_long_random_string
      WEBUI_PUBLIC_URL: http://192.168.1.10:5000
    volumes:
      - /path/to/music:/music
      - /path/to/appdata/soundscout/config:/config
      - /path/to/appdata/soundscout/data:/app/data
      - /path/to/appdata/soundscout/scraper:/root/.scraper
```

Then open `http://<your-ip>:5000` and sign in with Plex.

A full reference file with all options is in [`docker-compose.yml`](docker-compose.yml). For local development, copy [`.env.example`](.env.example) to `.env` and fill in your values.

## Configuration

### Required

| Variable | Description |
|---|---|
| `PLEX_BASEURL` | Your Plex server URL, e.g. `http://192.168.1.10:32400` |
| `PLEX_MUSIC_LIBRARY` | Name of your Plex music library section, e.g. `Music` |

### Web UI

| Variable | Default | Description |
|---|---|---|
| `ENABLE_WEBUI` | `true` | Enable the web interface |
| `WEBUI_PORT` | `5000` | Port the web interface listens on |
| `WEBUI_SECRET_KEY` | — | Flask session secret — set a long random string to keep sessions valid across restarts |
| `WEBUI_PUBLIC_URL` | — | Public URL of this container, e.g. `http://192.168.1.10:5000` — required for Plex OAuth sign-in to work |
| `WEBUI_REQUIRE_PLEX_LOGIN` | `1` | Set `0` to disable Plex sign-in requirement (not recommended) |
| `PLEX_VERIFY_SSL` | `1` | Set `0` to skip SSL verification for Plex connections using self-signed certificates |
| `PLEX_OAUTH_CLIENT_ID` | — | Optional override for the Plex OAuth client identifier |

### Last.fm

The Last.fm API key is bundled — no setup is required for basic use. If you hit rate limits or want to track usage under your own account, you can create your own key at [last.fm/api](https://www.last.fm/api/account/create).

| Variable | Default | Description |
|---|---|---|
| `LASTFM_API_KEY` | *(bundled)* | Optional override for the Last.fm API key |
| `LASTFM_USERNAME` | — | Single-user fallback username; preferred: each user links their account in Settings |
| `LASTFM_MODE` | `recommendations` | `recommendations` (default) or `weekly_plays` |
| `LASTFM_SEED_COUNT` | `25` | Number of last-week tracks used to seed recommendations |
| `LASTFM_SIMILAR_PER_SEED` | `5` | Similar tracks fetched per seed track |

## Spotify import (optional)

Paste any Spotify track, album, or playlist URL into the Import view. SoundScout resolves the track list using an anonymous token — no developer app or Spotify account required. Downloaded tracks are added to your Plex library and optionally created as a Plex playlist.

### Playlist & Discovery

| Variable | Default | Description |
|---|---|---|
| `PLAYLIST_NAME` | `Discover Weekly` | Name of the Plex playlist to create/update |
| `MAX_TRACKS` | `50` | Maximum tracks in the playlist |
| `DRY_RUN` | `0` | Set `1` to generate reports without downloading or modifying Plex |
| `REPORT_PATH` | — | When set, writes CSV discovery reports to this path |

### Scheduling

When `ENABLE_WEBUI=true` (the default), the container runs as a long-lived web server. Auto Discovery schedules are configured per Plex user in the **Settings** tab.

| Variable | Default | Description |
|---|---|---|
| `TZ` | `UTC` | Timezone for schedule evaluation, e.g. `America/New_York` |
| `CRON_SCHEDULE` | — | Default schedule pre-filled in the Web UI when a user first enables Auto Discovery |
| `RUN_ON_STARTUP` | `false` | Run discovery immediately when the container starts |

### Acquisition (optional)

SoundScout includes a bundled scraper for acquiring tracks from Tidal or Qobuz. Credentials for the service are read from `/root/.scraper/config.json` (mount a volume from your host as shown in the Quick Start).

| Variable | Default | Description |
|---|---|---|
| `DISCOVERY_ACQUIRE` | `1` | Set `0` to skip the acquisition step |
| `SCRAPER_SERVICE` | `tidal` | `tidal` or `qobuz` |
| `DOWNLOAD_CONCURRENCY` | `6` | Parallel scraper processes |
| `SCRAPER_WORKERS` | `8` | Goroutine workers per scraper batch |

### Import step (optional)

Run a tagging/import command against an inbox folder after downloads complete.

| Variable | Default | Description |
|---|---|---|
| `ENABLE_IMPORT` | `0` | Set `1` to enable |
| `IMPORT_INBOX_DIR` | `/inbox` | Folder to watch for incoming files |
| `IMPORT_CMD` | — | Command to run, e.g. `beet import -q /inbox` |

## Volumes

| Container path | Purpose |
|---|---|
| `/music` | Your music library — downloaded tracks are saved here |
| `/config` | Persistent config — stores Plex tokens, user settings, session data |
| `/app/data` | Persistent data — download backlog, discovery reports |
| `/root/.scraper` | Scraper credentials — `config.json` with Tidal/Qobuz tokens |

## Unraid

An Unraid community application template is included at [`SoundScout-template.xml`](SoundScout-template.xml). Add it via **Apps → Install via XML** or place it in your Unraid templates folder.

## One-shot run (no Web UI)

To run a single discovery cycle without the web interface:

```bash
docker run --rm \
  -e PLEX_BASEURL=http://192.168.1.10:32400 \
  -e PLEX_MUSIC_LIBRARY=Music \
  -e LASTFM_USERNAME=your_lastfm_username \
  -e TZ=America/New_York \
  -e ENABLE_WEBUI=false \
  -v /path/to/music:/music \
  -v /path/to/appdata/soundscout/config:/config \
  -v /path/to/appdata/soundscout/scraper:/root/.scraper \
  ghcr.io/ashyy0205/soundscout:main
```

## Report mode (CSV)

To generate a list of recommended tracks you don't yet own without touching your Plex library or downloading anything:

```bash
REPORT_PATH=/app/data/soundscout-report.csv
DRY_RUN=1
```

Three files are written:
- `soundscout-report.csv` — final recommendations list
- `soundscout-report-evaluated.csv` — full evaluation with owned/missing flags
- `soundscout-report-missing.csv` — all missing tracks found

## Notes

- Plex metadata matching is best-effort. Track names with featured artists, punctuation differences, or remasters may not match exactly. Check the CSV report to see what was and wasn't matched.
- Plex tokens are obtained via the Web UI sign-in flow and stored in `/config/webui_users.json`.
- Each user's recommendations are generated from their own linked Last.fm account — the API credentials identify the SoundScout application, not individual users.
- The Docker image includes `ffmpeg`. For local development on Windows: `winget install Gyan.FFmpeg`.
- `WEBUI_PUBLIC_URL` must be reachable from the browser, not just within Docker. Use your host machine's IP, not `localhost` or `127.0.0.1`.

---

## Legal

SoundScout is an independent, open-source project. It is not affiliated with, endorsed by, or connected to Spotify, Tidal, Qobuz, Amazon Music, Last.fm, Plex, or any other platform or service referenced in this project. All product names and trademarks are the property of their respective owners.

This software is intended for **personal and private use only**. The author does not condone piracy or any use of this software that violates applicable law or the Terms of Service of any third-party platform.

By using this software, you accept full and sole responsibility for:

- Verifying that your use is lawful in your country or region
- Reading and complying with the Terms of Service of any platform this software interacts with
- Any consequences, including but not limited to account termination, legal action, or financial liability, that result from your use of this tool

This software is provided "as is" and without warranty of any kind, express or implied. The author makes no guarantees regarding fitness for a particular purpose, availability, or continued functionality. The author accepts no liability whatsoever for damages, losses, or legal issues of any kind arising from the use or misuse of this software.
