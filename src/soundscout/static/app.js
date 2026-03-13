// Global state
let downloadQueue = [];
let viewStack = [];
let currentView = { kind: 'search', state: null };

// Prevent async race conditions when switching views quickly.
// Each view-opening action increments this token; only the latest token may render.
let activeViewToken = 0;
let activeViewKind = 'search';
let activeBodyViewClass = '';

function normalizeViewKind(kind) {
    const cleaned = String(kind || 'search').toLowerCase().replace(/[^a-z0-9_-]+/g, '-');
    return cleaned || 'search';
}

function updateBodyViewVariant(kind) {
    const body = document.body;
    if (!body) return;

    if (activeBodyViewClass) {
        body.classList.remove(activeBodyViewClass);
    }

    const safeKind = normalizeViewKind(kind);
    activeBodyViewClass = `view-${safeKind}`;
    body.classList.add(activeBodyViewClass);
}

function updateViewportVariantClass() {
    const body = document.body;
    if (!body) return;
    const mobile = isMobileWebUi();
    body.classList.toggle('is-mobile-webui', mobile);
    body.classList.toggle('is-desktop-webui', !mobile);
}

function isMobileWebUi() {
    const ua = navigator.userAgent || '';
    const mobileUa = /Android|iPhone|iPad|iPod|IEMobile|Opera Mini|Mobile/i.test(ua);
    return mobileUa && window.matchMedia('(max-width: 900px)').matches;
}

function setPlayerVisible(visible) {
    if (!playerBarEl) return;
    if (document.body && document.body.classList.contains('is-desktop-webui')) {
        playerBarEl.classList.add('is-visible');
        document.body.classList.add('playerbar-visible');
        return;
    }
    const show = !!visible;
    playerBarEl.classList.toggle('is-visible', show);
    document.body.classList.toggle('playerbar-visible', show);
}

function updateTopbarVisibility() {
    const topbar = document.querySelector('.topbar');
    if (!topbar) return;
    if (document.body && document.body.classList.contains('is-desktop-webui')) {
        topbar.classList.add('is-visible');
        return;
    }
    const kind = String(activeViewKind || '').toLowerCase();
    const show = kind === 'search' || kind === 'home';
    topbar.classList.toggle('is-visible', show);
}

function beginView(kind) {
    activeViewToken += 1;
    activeViewKind = String(kind || 'search');
    updateBodyViewVariant(activeViewKind);
    updateTopbarVisibility();
    return activeViewToken;
}

function isActiveView(kind, token) {
    return String(kind || '') === activeViewKind && Number(token) === Number(activeViewToken);
}

let downloadsPollTimer = null;
let downloadsLast = null;
let downloadsRecentlyVisibleUntil = 0;

// Note: downloads state is sourced from the server (/api/downloads).
// Avoid client-only "recent finished" heuristics that can flicker on refresh.

let navSearchBtn = null;
let navLibraryBtn = null;
let navDownloadsBtn = null;
let navSettingsBtn = null;
let navImportBtn = null;
let navShazamBtn = null;

let previewAudio = null;
let currentPreviewKey = null;
let currentPreviewButton = null;
let currentPreviewUrl = null;
let currentPreviewMeta = null;

let playerBarEl = null;
let playerArtEl = null;
let playerTitleEl = null;
let playerArtistEl = null;
let playerPlayBtn = null;
let playerProgressFill = null;
let playerProgressTimer = null;

// Escape a string for safe insertion into innerHTML.
function escHtml(s) {
    return String(s || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

let authRequired = false;
let authAuthed = false;
let authUser = null;
let authPollTimer = null;
let authPollStartedAt = 0;
let authPopupWindow = null;

// Auth should default to ON; if the backend can't be reached,
// we should prompt instead of silently allowing access.
let authFailClosed = true;

let homeLoadedOnce = false;

// ── In-memory view-data cache ────────────────────────────────────────────────
// Stores the last successful API response for heavy views so navigating away
// and back is instant.  The server also caches these but the JS cache avoids
// the round-trip entirely.
const _viewDataCache = {
    recommendations: { data: null, ts: 0, ttlMs: 5 * 60 * 1000 },
    library:         { data: null, ts: 0, ttlMs: 5 * 60 * 1000 },
};

function _cacheGet(key) {
    const e = _viewDataCache[key];
    if (!e || !e.data) return null;
    if (Date.now() - e.ts > e.ttlMs) return null;
    return e.data;
}

function _cacheSet(key, data) {
    const e = _viewDataCache[key];
    if (!e) return;
    e.data = data;
    e.ts = Date.now();
}

function _cacheInvalidate(key) {
    const e = _viewDataCache[key];
    if (!e) return;
    e.data = null;
    e.ts = 0;
}
// ─────────────────────────────────────────────────────────────────────────────

let lastfmLinked = false;
let lastfmUsername = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    updateViewportVariantClass();
    updateBodyViewVariant(currentView.kind);
    updateTopbarVisibility();
    window.addEventListener('resize', updateViewportVariantClass);
    initPlexLastfmModal();

    // Render Lucide icons in the bottom nav
    if (typeof lucide !== 'undefined') lucide.createIcons();

    const searchInput = document.getElementById('searchInput');
    
    // Allow Enter key to trigger search
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            performSearch();
        }
    });

    previewAudio = new Audio();
    previewAudio.preload = 'none';

    playerBarEl = document.getElementById('playerBar');
    playerArtEl = playerBarEl ? playerBarEl.querySelector('.playerbar-art') : null;
    playerTitleEl = document.getElementById('playerTitle');
    playerArtistEl = document.getElementById('playerArtist');
    playerPlayBtn = document.getElementById('playerPlayBtn');
    playerProgressFill = document.getElementById('playerProgressFill');

    if (playerPlayBtn && !playerPlayBtn.dataset.bound) {
        playerPlayBtn.dataset.bound = '1';
        playerPlayBtn.addEventListener('click', async () => {
            try {
                if (!previewAudio || !currentPreviewUrl) return;
                if (previewAudio.paused) {
                    if (!previewAudio.src) previewAudio.src = currentPreviewUrl;
                    await previewAudio.play();
                    setPlayerPlayState('pause');
                } else {
                    previewAudio.pause();
                    setPlayerPlayState('play');
                }
            } catch (e) {
                // ignore
            }
        });
    }

    previewAudio.addEventListener('timeupdate', () => {
        updatePlayerProgress();
    });

    previewAudio.addEventListener('pause', () => {
        if (currentPreviewUrl) setPlayerPlayState('play');
    });

    previewAudio.addEventListener('play', () => {
        if (currentPreviewUrl) setPlayerPlayState('pause');
    });

    previewAudio.addEventListener('ended', () => {
        if (currentPreviewButton) setPreviewButtonState(currentPreviewButton, 'play');
        currentPreviewKey = null;
        currentPreviewButton = null;
        currentPreviewUrl = null;
        currentPreviewMeta = null;
        setPlayerMeta(null);
    });

    setPlayerVisible(false);

    window.addEventListener('message', (event) => {
        try {
            if (event && event.data && event.data.type === 'plex-auth-complete') {
                setAuthLoadingState(true, 'Loading your Plex account...');
                initAuth();
            }
        } catch (e) {
            // ignore
        }
    });

    initAuth();

    // Always start polling; endpoint is auth-gated when configured.
    ensureDownloadsPolling();

    // Sidebar navigation
    navSearchBtn = document.getElementById('navSearchBtn');
    navLibraryBtn = document.getElementById('navLibraryBtn');
    navDownloadsBtn = document.getElementById('navDownloadsBtn');
    navSettingsBtn = document.getElementById('navSettingsBtn');
    navImportBtn = document.getElementById('navImportBtn');

    if (navSearchBtn && !navSearchBtn.dataset.bound) {
        navSearchBtn.dataset.bound = '1';
        navSearchBtn.addEventListener('click', () => {
            // Search is the Home tab: show recommendations.
            // Force a refresh only when already on home (re-clicking the tab);
            // when coming from another view just serve from the JS cache.
            const alreadyHere = (activeViewKind === 'home' && currentView.kind === 'home');
            viewStack = [];
            setActiveNav('search');
            const si = document.getElementById('searchInput');
            if (si) si.value = '';
            openHomeRecommendations(alreadyHere ? true : false);
        });
    }

    if (navLibraryBtn && !navLibraryBtn.dataset.bound) {
        navLibraryBtn.dataset.bound = '1';
        navLibraryBtn.addEventListener('click', () => {
            const alreadyHere = (activeViewKind === 'library' && currentView.kind === 'library');
            viewStack = [];
            setActiveNav('library');
            openLibrary(alreadyHere ? true : false);
        });
    }

    if (navDownloadsBtn && !navDownloadsBtn.dataset.bound) {
        navDownloadsBtn.dataset.bound = '1';
        navDownloadsBtn.addEventListener('click', () => {
            viewStack = [];
            setActiveNav('downloads');
            openDownloads();
        });
    }

    if (navSettingsBtn && !navSettingsBtn.dataset.bound) {
        navSettingsBtn.dataset.bound = '1';
        navSettingsBtn.addEventListener('click', () => {
            viewStack = [];
            setActiveNav('settings');
            openSettings();
        });
    }

    if (navImportBtn && !navImportBtn.dataset.bound) {
        navImportBtn.dataset.bound = '1';
        navImportBtn.addEventListener('click', () => {
            viewStack = [];
            setActiveNav('import');
            openImport();
        });
    }

    navShazamBtn = document.getElementById('navShazamBtn');
    if (navShazamBtn && !navShazamBtn.dataset.bound) {
        navShazamBtn.dataset.bound = '1';
        navShazamBtn.addEventListener('click', () => {
            viewStack = [];
            setActiveNav('scout');
            openScout();
        });
    }
});

async function initLastfm() {
    if (authRequired && !authAuthed) return;
    try {
        const data = await apiFetchJson('/api/lastfm/status');
        lastfmLinked = !!data.linked;
        lastfmUsername = data.username || null;
    } catch (e) {
        lastfmLinked = false;
        lastfmUsername = null;
    }
}

async function linkLastfmByUsername() {
    const input = document.getElementById('lastfmUsernameInput');
    const username = input ? String(input.value || '').trim() : '';
    if (!username) {
        showError('Enter your Last.fm username.');
        return;
    }

    try {
        await apiFetchJson('/api/lastfm/link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username })
        });
        await initLastfm();
        openSettings();
        if (lastfmLinked) {
            setPlexLastfmModalVisible(true);
        }
        // Refresh recommendations if user is on home.
        if (currentView && currentView.kind === 'home') {
            openHomeRecommendations(true);
        }
    } catch (e) {
        showError(e && e.message ? e.message : 'Failed to link Last.fm');
    }
}

async function unlinkLastfm() {
    try {
        await apiFetchJson('/api/lastfm/unlink', { method: 'POST' });
    } catch (e) {
        // ignore
    }
    await initLastfm();
    if (currentView && currentView.kind === 'settings') {
        openSettings();
    }
}

function setPlexLastfmModalVisible(visible) {
    const modal = document.getElementById('plexLastfmModal');
    if (!modal) return;
    modal.style.display = visible ? 'flex' : 'none';
}

function initPlexLastfmModal() {
    const modal = document.getElementById('plexLastfmModal');
    const closeBtn = document.getElementById('plexLastfmCloseBtn');
    const notNowBtn = document.getElementById('plexLastfmNotNowBtn');
    if (!modal) return;

    if (closeBtn && !closeBtn.dataset.bound) {
        closeBtn.dataset.bound = '1';
        closeBtn.addEventListener('click', () => setPlexLastfmModalVisible(false));
    }

    if (notNowBtn && !notNowBtn.dataset.bound) {
        notNowBtn.dataset.bound = '1';
        notNowBtn.addEventListener('click', () => setPlexLastfmModalVisible(false));
    }

    if (!modal.dataset.bound) {
        modal.dataset.bound = '1';
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                setPlexLastfmModalVisible(false);
            }
        });
    }
}

const weekdayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function weekdayName(n) {
    const i = Number(n);
    if (!isFinite(i)) return 'Mon';
    return weekdayNames[((i % 7) + 7) % 7] || 'Mon';
}

async function fetchAutodiscoverySettings() {
    try {
        return await apiFetchJson('/api/autodiscovery/settings');
    } catch (e) {
        return null;
    }
}

async function saveAutodiscoverySettings() {
    const enabledEl = document.getElementById('autodiscEnabled');
    const weekdayEl = document.getElementById('autodiscWeekday');
    const timeEl = document.getElementById('autodiscTime');

    const enabled = !!(enabledEl && enabledEl.checked);
    const weekday = weekdayEl ? Number(weekdayEl.value) : 0;
    const time = timeEl ? String(timeEl.value || '').trim() : '';

    try {
        await apiFetchJson('/api/autodiscovery/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled, weekday, time })
        });
        openSettings();
    } catch (e) {
        showError(e && e.message ? e.message : 'Failed to save auto discovery settings');
    }
}

async function openSettings() {
    if (authRequired && !authAuthed) return;

    const token = beginView('settings');

    // Clear the old view immediately so previous page content doesn't bleed through
    // while the two async fetches below are in-flight.
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) resultsContainer.innerHTML = '';
    setLoading(true, 'Loading settings\u2026');
    setViewHeader(`
        <div class="view-title">Settings</div>
    `);

    await initLastfm();
    const autodisc = await fetchAutodiscoverySettings();
    if (!isActiveView('settings', token)) return;

    setLoading(false);
    setResultsMode('list');
    setViewHeader(`
        <div class="view-title">Settings</div>
    `);

    if (!resultsContainer) return;

    const linkedText = lastfmLinked
        ? `Linked as <strong>${escapeHtml(lastfmUsername || '')}</strong>`
        : 'Not linked';

    const usernameLinkControls = !lastfmLinked ? `
        <div class="settings-link-row">
            <input
                id="lastfmUsernameInput"
                class="hud-input"
                type="text"
                placeholder="Last.fm username"
                autocomplete="off"
            >
            <button class="nav-btn nav-btn-primary" type="button" onclick="linkLastfmByUsername()">Save</button>
        </div>
    ` : '';

    const unlinkButton = lastfmLinked
        ? `<button class="nav-btn" type="button" onclick="unlinkLastfm()">Unlink Last.fm</button>`
        : '';

    const adLinked = autodisc ? !!autodisc.linked_lastfm : !!lastfmLinked;
    const adEnabled = autodisc ? !!autodisc.enabled : false;
    const adWeekday = autodisc ? Number(autodisc.weekday) : 0;
    const adTime = autodisc ? String(autodisc.time || '').trim() : '';
    const adTz = autodisc ? String(autodisc.tz || '').trim() : 'UTC';

    const adSub = adEnabled
        ? `Enabled — <strong>${escapeHtml(weekdayName(adWeekday))} ${escapeHtml(adTime || '')}</strong> (${escapeHtml(adTz || 'UTC')})`
        : 'Disabled';

    const adHint = adLinked
        ? `Uses server timezone: ${escapeHtml(adTz || 'UTC')}.`
        : 'Link Last.fm to enable auto discovery.';

    const weekdayOptions = weekdayNames
        .map((label, idx) => `<option value="${idx}" ${idx === ((adWeekday % 7) + 7) % 7 ? 'selected' : ''}>${label}</option>`)
        .join('');

    const adDisabledAttr = adLinked ? '' : 'disabled';

    resultsContainer.innerHTML = `
      <div class="downloads-now">
        <div class="downloads-now-art" aria-hidden="true"></div>
        <div>
          <div class="downloads-now-title">Last.fm</div>
          <div class="downloads-now-sub">${linkedText}</div>
          ${usernameLinkControls}
          <div class="settings-actions-row">${unlinkButton}</div>
        </div>
      </div>

            <div class="downloads-now">
                <div class="downloads-now-art" aria-hidden="true"></div>
                <div>
                    <div class="downloads-now-title">Auto Discovery</div>
                    <div class="downloads-now-sub">${adSub}</div>
                    <div class="downloads-now-meta">${adHint}</div>

                    <div class="settings-link-row">
                        <label style="display:flex;align-items:center;gap:8px;">
                            <input id="autodiscEnabled" type="checkbox" ${adEnabled ? 'checked' : ''} ${adDisabledAttr}>
                            <span>Enabled</span>
                        </label>

                        <select id="autodiscWeekday" class="hud-input" ${adDisabledAttr}>
                            ${weekdayOptions}
                        </select>

                        <input id="autodiscTime" class="hud-input" type="time" step="60" value="${escapeHtml(adTime || '')}" ${adDisabledAttr}>

                        <button class="nav-btn nav-btn-primary" type="button" onclick="saveAutodiscoverySettings()" ${adDisabledAttr}>Save</button>
                    </div>
                </div>
            </div>
    `;

    currentView = { kind: 'settings', state: null };
}

function ensureDownloadsPolling() {
    if (downloadsPollTimer) return;
    downloadsPollTimer = setInterval(pollDownloadsOnce, 1000);
    pollDownloadsOnce();
}

function formatEta(seconds) {
    if (seconds == null || !isFinite(seconds) || seconds <= 0) return '';
    const s = Math.floor(seconds);
    const m = Math.floor(s / 60);
    const r = s % 60;
    if (m <= 0) return `${r}s`;
    if (m < 60) return `${m}m ${r}s`;
    const h = Math.floor(m / 60);
    const rm = m % 60;
    return `${h}h ${rm}m`;
}

function formatMbps(mbps) {
    if (!mbps || !isFinite(mbps) || mbps <= 0) return '';
    return `${mbps.toFixed(1)} Mbps`;
}

function formatElapsed(seconds) {
    if (seconds == null || !isFinite(seconds) || seconds <= 0) return '';
    const s = Math.floor(seconds);
    const m = Math.floor(s / 60);
    const r = s % 60;
    if (m <= 0) return `${r}s`;
    if (m < 60) return `${m}m ${r}s`;
    const h = Math.floor(m / 60);
    const rm = m % 60;
    return `${h}h ${rm}m`;
}

function timeAgo(ts) {
    if (!ts) return '';
    const diff = Math.floor(Date.now() / 1000 - Number(ts));
    if (diff < 60) return 'just now';
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
}

function normStatus(s) {
    return String(s || '').trim().toLowerCase();
}

function isActiveStatus(s) {
    const t = normStatus(s);
    return t === 'queued' || t === 'running';
}

function isRunningStatus(s) {
    return normStatus(s) === 'running';
}

function isTerminalStatus(s) {
    const t = normStatus(s);
    return t === 'completed' || t === 'failed' || t === 'partial';
}

function getRunningJob(jobs) {
    if (!Array.isArray(jobs)) return null;
    return jobs.find(j => isRunningStatus(j && j.status)) || null;
}

function displayJobProgressText(job) {
    if (!job) return '';
    const type = String(job.type || '').toLowerCase();
    const totalTracks = Number(job.total_tracks || 0);
    // Only show X/Y style progress for albums (multi-track jobs).
    if (type !== 'album') return '';
    if (!(totalTracks > 1)) return '';
    const t = String(job.progress_text || '').trim();
    return t || '';
}

function jobBadgeClass(status) {
    const s = String(status || '').toLowerCase();
    if (s === 'running') return 'is-running';
    if (s === 'completed') return 'is-completed';
    if (s === 'failed' || s === 'partial') return 'is-failed';
    return '';
}

function jobBadgeText(status) {
    const s = String(status || '').toLowerCase();
    if (s === 'queued') return 'QUEUED';
    if (s === 'running') return 'RUNNING';
    if (s === 'completed') return 'DONE';
    if (s === 'partial') return 'PARTIAL';
    if (s === 'failed') return 'FAILED';
    return s ? s.toUpperCase() : '';
}

function updateButtonsFromJobs(jobs) {
    if (!Array.isArray(jobs)) return;
    for (const job of jobs) {
        const id = job && job.id ? String(job.id) : '';
        if (!id) continue;
        const status = String(job.status || '').toLowerCase();
        const esc = (window.CSS && typeof window.CSS.escape === 'function') ? window.CSS.escape(id) : id.replace(/"/g, '\\"');

        // Cover status badges (cards grid)
        const badges = document.querySelectorAll(`.cover-status-badge[data-download-job-id="${esc}"]`);
        badges.forEach((badge) => {
            if (!badge) return;
            badge.classList.remove('badge-downloading', 'badge-error');
            if (status === 'completed') {
                badge.classList.add('badge-in-library');
                badge.disabled = true;
                badge.title = 'In Library';
                badge.setAttribute('aria-label', 'In Library');
            } else if (status === 'failed' || status === 'partial') {
                badge.classList.add('badge-error');
                badge.disabled = false;
            } else if (status === 'running' || status === 'queued') {
                badge.classList.add('badge-downloading');
                badge.disabled = true;
            }
        });

        // Classic text download buttons (track list rows in album/artist views)
        const buttons = document.querySelectorAll(`.download-btn[data-download-job-id="${esc}"]`);
        if (!buttons || buttons.length === 0) continue;

        buttons.forEach((btn) => {
            if (!btn) return;
            if (status === 'completed') {
                btn.textContent = '✓ Downloaded';
                btn.classList.remove('downloading', 'error');
                btn.classList.add('success');
                btn.disabled = true;
            } else if (status === 'failed' || status === 'partial') {
                btn.textContent = '✗ Failed';
                btn.classList.remove('downloading', 'success');
                btn.classList.add('error');
                btn.disabled = false;
            } else if (status === 'running') {
                btn.textContent = 'Downloading…';
                btn.classList.add('downloading');
                btn.disabled = true;
            } else if (status === 'queued') {
                btn.textContent = 'In Queue…';
                btn.classList.add('downloading');
                btn.disabled = true;
            }
        });
    }
}

function renderSidebarDownloads(data) {
    const container = document.getElementById('sidebarDownloads');
    const listEl = document.getElementById('sidebarDownloadsList');
    const summaryEl = document.getElementById('sidebarDownloadsSummary');
    if (!container || !listEl) return;

    const jobs = (data && Array.isArray(data.jobs)) ? data.jobs : [];
    const summary = (data && data.summary) ? data.summary : {};
    const active = Number(summary.active || 0);
    const queued = Number(summary.queued || 0);
    const running = Number(summary.running || 0);

    const queueProgressText = String(summary.queue_progress_text || '').trim();
    const queueEtaSeconds = summary.queue_eta_seconds;
    const queueSpeedMbps = Number(summary.speed_mbps || 0);

    // Sidebar requirement: brief summary only.
    // Show only when there is queued/running work.
    const shouldShow = active > 0;
    if (!shouldShow) {
        container.style.display = 'none';
        listEl.innerHTML = '';
        if (summaryEl) summaryEl.textContent = '';
        // Also hide desktop sidebar downloads
        const desktopContainer = document.getElementById('sidebarDownloadsDesktop');
        const desktopListEl = document.getElementById('sidebarDownloadsListDesktop');
        const desktopSummaryEl = document.getElementById('sidebarDownloadsSummaryDesktop');
        if (desktopContainer) desktopContainer.style.display = 'none';
        if (desktopListEl) desktopListEl.innerHTML = '';
        if (desktopSummaryEl) desktopSummaryEl.textContent = '';
        return;
    }

    container.style.display = 'flex';

    const runningJob = getRunningJob(jobs);
    const showProgress = !!runningJob;
    // Overall queue progress (tracks) and queue-wide ETA are provided by the server.
    // Between downloads (queued but not running), show _/_.
    const progressText = (showProgress && queueProgressText) ? queueProgressText : '_/_';

    // Speed/ETA only while actively running.
    let speedInline = '';
    let etaInline = '';
    if (showProgress) {
        const speedText = formatMbps(queueSpeedMbps);
        if (speedText) speedInline = speedText;
        const etaText = formatEta(queueEtaSeconds);
        if (etaText) etaInline = `ETA ${etaText}`;
    }

    const rightParts = [etaInline, speedInline].filter(Boolean).join(' • ');

    if (summaryEl) {
        const parts = [];
        if (running) parts.push(`${running} running`);
        summaryEl.textContent = parts.join(' • ');
    }

    listEl.innerHTML = '';
    const row = document.createElement('div');
    row.className = 'download-summary-row';
    row.innerHTML = `
        <div class="download-summary-main">${escapeHtml(progressText || '_/_')}</div>
        <div class="download-summary-meta">${escapeHtml(rightParts || '')}</div>
    `;
    listEl.appendChild(row);

    updateButtonsFromJobs(jobs);

    // Mirror to desktop sidebar downloads (separate element inside .bottom-nav)
    const desktopContainer = document.getElementById('sidebarDownloadsDesktop');
    const desktopListEl = document.getElementById('sidebarDownloadsListDesktop');
    const desktopSummaryEl = document.getElementById('sidebarDownloadsSummaryDesktop');
    if (desktopContainer && desktopListEl) {
        desktopContainer.style.display = 'flex';
        if (desktopSummaryEl) desktopSummaryEl.textContent = summaryEl ? summaryEl.textContent : '';
        desktopListEl.innerHTML = listEl.innerHTML;
    }
}

async function pollDownloadsOnce() {
    if (authRequired && !authAuthed) return;
    try {
        const data = await apiFetchJson('/api/downloads');
        downloadsLast = data;
        // Scan-only jobs are background library checks — filter them from
        // the downloads view and sidebar so they don't clutter the UI.
        const visJobs    = (data.jobs    || []).filter(j => !j.scan_only);
        const visHistory = (data.history || []).filter(j => !j.scan_only);
        const visRunning = visJobs.filter(j => normStatus(j.status) === 'running').length;
        const visQueued  = visJobs.filter(j => normStatus(j.status) === 'queued').length;
        const visSummary = data.summary
            ? { ...data.summary, active: visRunning + visQueued, running: visRunning, queued: visQueued }
            : {};
        const visData = { ...data, jobs: visJobs, history: visHistory, summary: visSummary };
        renderSidebarDownloads(visData);
        if (activeViewKind === 'downloads') {
            renderDownloadsView({
                jobs:    visJobs,
                summary: visSummary,
                history: visHistory,
                is_admin: !!(data && data.is_admin),
            });
        }
    } catch (e) {
        // Likely auth required; ignore.
    }
}

async function openDownloads() {
    if (authRequired && !authAuthed) return;

    const token = beginView('downloads');

    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) resultsContainer.innerHTML = '';
    setResultsMode('list');
    setLoading(true, 'Loading downloads…');
    setViewHeader(`
        <div class="view-title">Downloads</div>
    `);

    try {
        const data = await apiFetchJson('/api/downloads');
        if (!isActiveView('downloads', token)) return;
        setLoading(false);
        const visJobs    = (data.jobs    || []).filter(j => !j.scan_only);
        const visHistory = (data.history || []).filter(j => !j.scan_only);
        const visRunning = visJobs.filter(j => normStatus(j.status) === 'running').length;
        const visQueued  = visJobs.filter(j => normStatus(j.status) === 'queued').length;
        const visSummary = data.summary
            ? { ...data.summary, active: visRunning + visQueued, running: visRunning, queued: visQueued }
            : {};
        renderDownloadsView({
            jobs:    visJobs,
            summary: visSummary,
            history: visHistory,
            is_admin: !!(data && data.is_admin),
        });
    } catch (e) {
        if (!isActiveView('downloads', token)) return;
        setLoading(false);
        showError('Failed to load downloads.');
    }
}

function renderDownloadsView(state) {
    currentView = { kind: 'downloads', state };
    setResultsMode('list');

    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer) return;
    resultsContainer.innerHTML = '';

    const jobs = (state && Array.isArray(state.jobs)) ? state.jobs : [];
    const runningJob = getRunningJob(jobs);
    const activeJobs = jobs.filter(j => isActiveStatus(j && j.status));
    const queuedCount = activeJobs.filter(j => normStatus(j && j.status) === 'queued').length;

    // Header: currently downloading
    const header = document.createElement('div');
    header.className = 'downloads-now';

    let coverCandidate = '';
    let titleLine = 'Waiting for next download…';
    let subLine = '';
    let metaParts = [];
    let progressRatioPct = 0;

    if (runningJob) {
        const ct = (runningJob.current_track && typeof runningJob.current_track === 'object') ? runningJob.current_track : null;
        const cta = ct ? (ct.artist || '') : '';
        const ctt = ct ? (ct.title || '') : '';

        // Prefer current-track cover if provided.
        coverCandidate = runningJob.current_track_cover_url || '';

        const jobArtist = runningJob.artist || '';
        const jobTitle = runningJob.title || '';
        titleLine = ctt ? `${cta || jobArtist} — ${ctt}` : `${jobArtist} — ${jobTitle}`;

        const type = runningJob.type || '';
        const progressText = displayJobProgressText(runningJob);
        subLine = [type, progressText].filter(Boolean).join(' • ');

        const totalTracks = Number(runningJob.total_tracks || 0);
        const isAlbumJob = String(runningJob.type || '').toLowerCase() === 'album' && totalTracks > 1;
        const etaText = formatEta(runningJob.eta_seconds);
        const elapsedText = formatElapsed(Number(runningJob.elapsed_seconds || 0));
        const speedText = isAlbumJob ? formatMbps(Number(runningJob.speed_mbps || 0)) : '';

        if (elapsedText) metaParts.push(`Elapsed ${elapsedText}`);
        if (etaText && isAlbumJob) metaParts.push(`ETA ${etaText}`);
        if (speedText && isAlbumJob) metaParts.push(speedText);

        progressRatioPct = Math.max(0, Math.min(100, Number(runningJob.progress_ratio || 0) * 100));
    } else {
        // In-between downloads: show _/_ until next item starts.
        titleLine = queuedCount > 0 ? 'Waiting for next download…' : 'No downloads in queue';
        subLine = queuedCount > 0 ? `_/_ • ${queuedCount} queued` : '';
        metaParts = [];
        progressRatioPct = 0;
    }

    const fallbackCover = '/static/cover-placeholder.svg';
    const directUrl = coverCandidate || '';
    const proxyUrl = (directUrl && (directUrl.startsWith('http://') || directUrl.startsWith('https://')))
        ? `/api/image?u=${encodeURIComponent(directUrl)}`
        : '';
    const coverUrl = directUrl || fallbackCover;

    header.innerHTML = `
        <div class="downloads-now-art">
            <img
                src="${coverUrl}"
                alt="Now downloading"
                data-proxy-src="${proxyUrl}"
                data-fallback-src="${fallbackCover}"
                data-tried-proxy="0"
                onerror="handleImgError(this)"
            >
        </div>
        <div>
            <div class="downloads-now-title">${escapeHtml(titleLine)}</div>
            ${subLine ? `<div class="downloads-now-sub">${escapeHtml(subLine)}</div>` : ''}
            ${metaParts.length ? `<div class="downloads-now-meta">${metaParts.map(escapeHtml).join(' • ')}</div>` : ''}
            <div class="downloads-now-progress">
                <div class="download-job-bar"><div class="download-job-bar-fill" style="width:${progressRatioPct.toFixed(1)}%"></div></div>
            </div>
        </div>
    `;

    resultsContainer.appendChild(header);

    // Full list view (like track list)
    const list = document.createElement('div');
    list.className = 'tracks-list';

    // Only show active jobs; finished jobs should drop off this view.
    const sorted = [...activeJobs];
    // Server already sorts, but keep a stable client-side guard.
    sorted.sort((a, b) => {
        const sa = normStatus(a && a.status);
        const sb = normStatus(b && b.status);
        const bucket = (s) => (s === 'running' ? 0 : (s === 'queued' ? 1 : 2));
        const ba = bucket(sa);
        const bb = bucket(sb);
        if (ba !== bb) return ba - bb;
        const ta = Number(a && (a.started_at || a.created_at) || 0);
        const tb = Number(b && (b.started_at || b.created_at) || 0);
        return tb - ta;
    });

    sorted.forEach((job, idx) => {
        const status = normStatus(job && job.status);
        const artist = (job && job.artist) ? String(job.artist) : '';
        const title = (job && job.title) ? String(job.title) : '';
        const type = (job && job.type) ? String(job.type) : '';
        const progressText = displayJobProgressText(job);
        const pct = Math.max(0, Math.min(100, Number(job && job.progress_ratio || 0) * 100));
        const submittedByBadge = (state && state.is_admin && job && job.submitted_by)
            ? `<span class="history-user-badge">${escapeHtml(String(job.submitted_by))}</span>`
            : '';
        // Show last_error inline for jobs that failed at the scraper level (no per-track errors).
        const jobErr = (status === 'failed' && job && job.last_error)
            ? String(job.last_error).replace(/\s+/g, ' ').trim()
            : '';
        const subParts = [type, progressText, jobErr ? `Error: ${jobErr.length > 120 ? jobErr.slice(0, 120) + '…' : jobErr}` : ''].filter(Boolean);

        const row = document.createElement('div');
        row.className = 'track-row download-track-row';

        row.innerHTML = `
            <div class="track-num">${escapeHtml(String(idx + 1))}</div>
            <div class="track-main">
                <div class="track-row-title">${escapeHtml(artist)} — ${escapeHtml(title)} ${submittedByBadge}</div>
                <div class="track-row-sub${jobErr ? ' is-error' : ''}">${escapeHtml(subParts.join(' • '))}</div>
                <div class="downloads-row-bar">
                    <div class="download-job-bar"><div class="download-job-bar-fill" style="width:${pct.toFixed(1)}%"></div></div>
                </div>
            </div>
            <div class="track-status">
                <div class="download-job-badge ${jobBadgeClass(status)}">${escapeHtml(jobBadgeText(status))}</div>
            </div>
            <div class="track-actions"></div>
        `;

        list.appendChild(row);
    });

    resultsContainer.appendChild(list);

    // ── Failed tracks section (from recently-finished in-memory jobs) ────
    // Includes jobs with per-track failures AND jobs that failed at the scraper level (no track list).
    const failedJobs = jobs.filter(j => {
        const s = normStatus(j && j.status);
        if (s !== 'partial' && s !== 'failed') return false;
        return (Array.isArray(j.failed_tracks_list) && j.failed_tracks_list.length > 0) ||
               (s === 'failed' && j.last_error);
    });

    if (failedJobs.length > 0) {
        const failSection = document.createElement('div');
        failSection.className = 'failed-tracks-section';

        const heading = document.createElement('div');
        heading.className = 'failed-tracks-heading';
        heading.textContent = 'Could not download';
        failSection.appendChild(heading);

        failedJobs.forEach(job => {
            const ftl = job.failed_tracks_list || [];
            const jobLabel = [job.artist, job.title].filter(Boolean).join(' — ') || 'Unknown job';

            // Scraper-level failure (no per-track list) — show the error message directly.
            if (ftl.length === 0 && job.last_error) {
                const crashBlock = document.createElement('div');
                crashBlock.className = 'failed-tracks-job-header';
                const errText = String(job.last_error).replace(/\s+/g, ' ').trim();
                crashBlock.innerHTML = `<span>Scraper failed for <em>${escapeHtml(jobLabel)}</em>: <span class="failed-track-error" title="${escapeHtml(errText)}">${escapeHtml(errText.length > 200 ? errText.slice(0, 200) + '\u2026' : errText)}</span></span>`;
                failSection.appendChild(crashBlock);
                return;
            }

            // Copy button — builds a plain-text list for the user.
            const copyBtn = document.createElement('button');
            copyBtn.className = 'nav-btn';
            copyBtn.style.cssText = 'font-size:0.72rem;padding:2px 8px;margin-bottom:6px;';
            copyBtn.textContent = 'Copy list';
            copyBtn.onclick = () => {
                const lines = ftl.map(f => `${f.artist} — ${f.title}`).join('\n');
                navigator.clipboard.writeText(lines).then(() => {
                    copyBtn.textContent = '✓ Copied';
                    setTimeout(() => { copyBtn.textContent = 'Copy list'; }, 1800);
                }).catch(() => {
                    copyBtn.textContent = 'Copy failed';
                });
            };

            const jobHeader = document.createElement('div');
            jobHeader.className = 'failed-tracks-job-header';
            jobHeader.innerHTML = `<span>${escapeHtml(ftl.length)} track${ftl.length === 1 ? '' : 's'} from <em>${escapeHtml(jobLabel)}</em></span>`;
            jobHeader.appendChild(copyBtn);
            failSection.appendChild(jobHeader);

            const trackList = document.createElement('div');
            trackList.className = 'failed-tracks-list';
            ftl.forEach(f => {
                const row = document.createElement('div');
                row.className = 'failed-track-row';
                const brief = (f.error || '').replace(/\s+/g, ' ').trim();
                row.innerHTML = `
                    <span class="failed-track-name">${escapeHtml(f.artist)} — ${escapeHtml(f.title)}</span>
                    ${brief ? `<span class="failed-track-error" title="${escapeHtml(brief)}">${escapeHtml(brief.length > 80 ? brief.slice(0, 80) + '…' : brief)}</span>` : ''}
                `;
                trackList.appendChild(row);
            });
            failSection.appendChild(trackList);
        });

        resultsContainer.appendChild(failSection);
    }

    // ── History section ─────────────────────────────────────────────────────
    const history = Array.isArray(state && state.history) ? state.history : [];
    const isAdmin = !!(state && state.is_admin);

    if (history.length > 0) {
        const histSection = document.createElement('div');
        histSection.className = 'history-section';

        const histHeading = document.createElement('div');
        histHeading.className = 'history-heading';
        histHeading.textContent = `History (${history.length})`;
        histSection.appendChild(histHeading);

        const histList = document.createElement('div');
        histList.className = 'tracks-list';

        history.forEach((h, idx) => {
            const hStatus = normStatus(h && h.status);
            const hArtist = escapeHtml(String((h && h.artist) || ''));
            const hTitle  = escapeHtml(String((h && h.title)  || ''));
            const hType   = escapeHtml(String((h && h.type)   || ''));
            const total     = Number((h && h.total_tracks)     || 0);
            const completed = Number((h && h.completed_tracks) || 0);
            const failed    = Number((h && h.failed_tracks)    || 0);
            const trackLine = total > 1 ? `${completed}/${total} tracks` : (total === 1 ? '1 track' : '');
            const ago = timeAgo((h && h.finished_at) || (h && h.created_at));
            const metaParts = [hType, trackLine, ago].filter(Boolean);

            const userBadge = (isAdmin && h && h.submitted_by)
                ? `<span class="history-user-badge">${escapeHtml(String(h.submitted_by))}</span>`
                : '';

            const ftl = Array.isArray(h && h.failed_tracks_list) ? h.failed_tracks_list : [];
            const failedHtml = (ftl.length > 0)
                ? `<div class="history-failed-list">${ftl.map(f =>
                    `<span class="history-failed-item" title="${escapeHtml((f && f.error) || '')}">${escapeHtml((f && f.artist) || '')} — ${escapeHtml((f && f.title) || '')}</span>`
                  ).join('')}</div>`
                : '';

            const hRow = document.createElement('div');
            hRow.className = 'track-row download-history-row';
            hRow.innerHTML = `
                <div class="track-num">${idx + 1}</div>
                <div class="track-main">
                    <div class="track-row-title">${hArtist} — ${hTitle} ${userBadge}</div>
                    <div class="track-row-sub">${escapeHtml(metaParts.join(' • '))}</div>
                    ${failedHtml}
                </div>
                <div class="track-status">
                    <div class="download-job-badge ${jobBadgeClass(hStatus)}">${escapeHtml(jobBadgeText(hStatus))}</div>
                </div>
                <div class="track-actions"></div>
            `;
            histList.appendChild(hRow);
        });

        histSection.appendChild(histList);
        resultsContainer.appendChild(histSection);
    }
}

async function openHomeRecommendations(force) {
    if (authRequired && !authAuthed) return;
    if (!force && homeLoadedOnce && currentView.kind === 'home') return;

    // Serve from the JS-level cache when navigating back to this view.
    if (!force) {
        const cached = _cacheGet('home');
        if (cached) {
            const token = beginView('home');
            const resultsContainer = document.getElementById('resultsContainer');
            if (resultsContainer) resultsContainer.innerHTML = '';
            setLoading(false);
            setViewHeader(`
                <div class="view-title">Home</div>
            `);
            renderHomeView(cached);
            homeLoadedOnce = true;
            return;
        }
    }

    const token = beginView('home');

    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) resultsContainer.innerHTML = '';
    setLoading(true, 'Loading…');
    setViewHeader(`
        <div class="view-title">Home</div>
    `);

    try {
        const url = force ? '/api/home?bust=1' : '/api/home';
        const data = await apiFetchJson(url);
        if (!isActiveView('home', token)) return;
        setLoading(false);
        const shelves = (data && data.shelves) ? data.shelves : [];
        const state = { shelves };
        _cacheSet('home', state);
        renderHomeView(state);
        homeLoadedOnce = true;
    } catch (e) {
        if (!isActiveView('home', token)) return;
        setLoading(false);
        showError('Failed to load home. Check your Last.fm settings.');
        console.error('Home error:', e);
    }
}

function renderShelf(shelf) {
    const section = document.createElement('div');
    section.className = 'home-shelf';

    const titleEl = document.createElement('div');
    titleEl.className = 'shelf-title';
    titleEl.textContent = shelf.title || '';
    section.appendChild(titleEl);

    const track = document.createElement('div');
    track.className = 'shelf-track';

    (shelf.items || []).forEach(item => {
        track.appendChild(createResultCard(item, 'track'));
    });

    section.appendChild(track);
    return section;
}

function renderHomeView(state) {
    currentView = { kind: 'home', state };
    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer) return;
    resultsContainer.innerHTML = '';
    resultsContainer.classList.remove('tracks-list-container');
    resultsContainer.classList.add('home-shelves-container');

    const shelves = (state && state.shelves) ? state.shelves : [];
    if (shelves.length === 0) {
        resultsContainer.innerHTML = '<div class="empty-state">No content available. Connect Last.fm in Settings.</div>';
        return;
    }

    shelves.forEach(shelf => {
        resultsContainer.appendChild(renderShelf(shelf));
    });

    initLazyCoverObserver();
}

function setActiveNav(which) {
    const w = String(which || '').toLowerCase();
    if (navSearchBtn) navSearchBtn.classList.toggle('is-active', w === 'search');
    if (navLibraryBtn) navLibraryBtn.classList.toggle('is-active', w === 'library');
    if (navDownloadsBtn) navDownloadsBtn.classList.toggle('is-active', w === 'downloads');
    if (navSettingsBtn) navSettingsBtn.classList.toggle('is-active', w === 'settings');
    if (navImportBtn) navImportBtn.classList.toggle('is-active', w === 'import');
    if (navShazamBtn) navShazamBtn.classList.toggle('is-active', w === 'scout');
    updateTopbarVisibility();
}

function setPlayerMeta(meta) {
    if (!playerTitleEl || !playerArtistEl || !playerPlayBtn) return;

    if (!meta) {
        playerTitleEl.textContent = 'No preview playing';
        playerArtistEl.textContent = 'Search and press play on a track preview';
        playerPlayBtn.disabled = true;
        playerPlayBtn.classList.remove('is-pause');
        if (playerProgressFill) playerProgressFill.style.width = '0%';
        if (playerArtEl) playerArtEl.style.backgroundImage = '';
        setPlayerVisible(false);
        return;
    }

    playerTitleEl.textContent = meta.title || 'Preview';
    playerArtistEl.textContent = meta.artist || '';
    if (playerArtEl) {
        playerArtEl.style.backgroundImage = meta.coverUrl ? `url('${meta.coverUrl}')` : '';
    }
    playerPlayBtn.disabled = false;
}

function setPlayerPlayState(state) {
    if (!playerPlayBtn) return;
    playerPlayBtn.classList.toggle('is-pause', state === 'pause');
    playerPlayBtn.setAttribute('aria-label', state === 'pause' ? 'Pause preview' : 'Play preview');
    setPlayerVisible(state === 'pause' && !!currentPreviewUrl);
}

function updatePlayerProgress() {
    if (!playerProgressFill || !previewAudio) return;
    const dur = Number(previewAudio.duration || 0);
    const cur = Number(previewAudio.currentTime || 0);
    if (!dur || !isFinite(dur) || dur <= 0) {
        playerProgressFill.style.width = '0%';
        return;
    }
    const pct = Math.max(0, Math.min(100, (cur / dur) * 100));
    playerProgressFill.style.width = `${pct.toFixed(1)}%`;
}

function setAuthOverlayVisible(visible) {
    const overlay = document.getElementById('authOverlay');
    if (!overlay) return;
    overlay.style.display = visible ? 'flex' : 'none';
}

function setAuthError(message) {
    const el = document.getElementById('authError');
    if (!el) return;
    if (!message) {
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    el.style.display = 'block';
    el.innerHTML = message;
}

function setAuthLoadingState(loading, message) {
    const titleEl = document.getElementById('authTitle');
    const subtitleEl = document.getElementById('authSubtitle');
    const actionsEl = document.getElementById('authActions');
    const loadingEl = document.getElementById('authLoading');
    const loadingTextEl = document.getElementById('authLoadingText');
    const loginBtn = document.getElementById('plexLoginBtn');

    if (loading) {
        if (titleEl) titleEl.textContent = 'Signing In';
        if (subtitleEl) {
            subtitleEl.textContent = 'Please wait while your Plex account loads';
        }
        if (loadingTextEl) {
            loadingTextEl.textContent = String(message || 'Loading your Plex account...');
        }
        if (actionsEl) actionsEl.style.display = 'none';
        if (loadingEl) loadingEl.style.display = 'block';
        if (loginBtn) loginBtn.disabled = true;
        setAuthOverlayVisible(true);
        return;
    }

    if (titleEl) {
        titleEl.textContent = titleEl.dataset.defaultText || 'Authentication Required';
    }
    if (subtitleEl) {
        subtitleEl.textContent = subtitleEl.dataset.defaultText || 'VERIFIED PLEX USERS ONLY';
    }
    if (actionsEl) actionsEl.style.display = '';
    if (loadingEl) loadingEl.style.display = 'none';
    if (loginBtn) loginBtn.disabled = false;
}

async function initAuth() {
    try {
        const resp = await fetch('/api/auth/status');
        const data = await resp.json();
        authRequired = !!data.require_login;
        authAuthed = !!data.authed;
        authUser = data.user || null;
        const canRunDiscovery = !!data.can_run_discovery;

        const userLabel = document.getElementById('authUserLabel');

        const pill = document.getElementById('authUserPill');
        const logoutBtn = document.getElementById('logoutBtn');
        const runBtn = document.getElementById('runDiscoveryBtn');

        if (pill) {
            if (authAuthed && authUser && (authUser.username || authUser.title)) {
                pill.textContent = `Signed in as ${authUser.username || authUser.title}`;
                pill.style.display = 'inline-flex';
            } else {
                pill.textContent = '';
                pill.style.display = 'none';
            }
        }

        if (userLabel) {
            if (authAuthed && authUser && (authUser.username || authUser.title)) {
                userLabel.textContent = `Logged in as ${authUser.username || authUser.title}`;
            } else {
                userLabel.textContent = '';
            }
        }
        if (logoutBtn) {
            logoutBtn.style.display = authAuthed ? 'inline-flex' : 'none';
            if (!logoutBtn.dataset.bound) {
                logoutBtn.dataset.bound = '1';
                logoutBtn.addEventListener('click', async () => {
                    try {
                        await fetch('/api/auth/logout', { method: 'POST' });
                    } catch (e) {
                        // ignore
                    }
                    authAuthed = false;
                    authUser = null;
                    if (pill) {
                        pill.textContent = '';
                        pill.style.display = 'none';
                    }
                    logoutBtn.style.display = 'none';
                    if (authRequired) {
                        setAuthError('');
                        setAuthLoadingState(false);
                        setAuthOverlayVisible(true);
                    }
                });
            }
        }

        if (runBtn) {
            runBtn.style.display = canRunDiscovery ? 'inline-flex' : 'none';
            if (!runBtn.dataset.bound) {
                runBtn.dataset.bound = '1';
                runBtn.addEventListener('click', async () => {
                    const originalText = runBtn.textContent;
                    runBtn.disabled = true;
                    runBtn.textContent = 'Running…';
                    try {
                        await apiFetchJson('/api/discovery/run', { method: 'POST' });
                        runBtn.textContent = 'Started';
                        setTimeout(() => {
                            runBtn.textContent = originalText;
                            runBtn.disabled = false;
                        }, 2500);
                    } catch (e) {
                        runBtn.textContent = originalText;
                        runBtn.disabled = false;
                        try {
                            let msg = (e && e.message) ? String(e.message) : 'Failed to start discovery';
                            const detail = e && e.data && e.data.detail ? String(e.data.detail) : '';
                            if (detail) msg = `${msg}: ${detail}`;
                            showError(msg);
                        } catch (e2) {
                            showError(e && e.message ? e.message : 'Failed to start discovery');
                        }
                    }
                });
            }
        }

        // Desktop sidebar footer: user label
        const sidebarUserLabel = document.getElementById('sidebarUserLabel');
        if (sidebarUserLabel) {
            if (authAuthed && authUser && (authUser.username || authUser.title)) {
                sidebarUserLabel.textContent = `Logged in as ${authUser.username || authUser.title}`;
            } else {
                sidebarUserLabel.textContent = '';
            }
        }

        // Desktop sidebar footer: logout button
        const sidebarLogoutBtn = document.getElementById('sidebarLogoutBtn');
        if (sidebarLogoutBtn) {
            sidebarLogoutBtn.style.display = authAuthed ? 'inline-flex' : 'none';
            if (!sidebarLogoutBtn.dataset.bound) {
                sidebarLogoutBtn.dataset.bound = '1';
                sidebarLogoutBtn.addEventListener('click', async () => {
                    try {
                        await fetch('/api/auth/logout', { method: 'POST' });
                    } catch (e) {
                        // ignore
                    }
                    authAuthed = false;
                    authUser = null;
                    if (pill) { pill.textContent = ''; pill.style.display = 'none'; }
                    if (logoutBtn) logoutBtn.style.display = 'none';
                    if (sidebarUserLabel) sidebarUserLabel.textContent = '';
                    sidebarLogoutBtn.style.display = 'none';
                    const sidebarRunBtnEl = document.getElementById('sidebarRunDiscoveryBtn');
                    if (sidebarRunBtnEl) sidebarRunBtnEl.style.display = 'none';
                    if (authRequired) {
                        setAuthError('');
                        setAuthLoadingState(false);
                        setAuthOverlayVisible(true);
                    }
                });
            }
        }

        // Desktop sidebar footer: discover button
        const sidebarRunBtn = document.getElementById('sidebarRunDiscoveryBtn');
        if (sidebarRunBtn) {
            sidebarRunBtn.style.display = canRunDiscovery ? 'inline-flex' : 'none';
            if (!sidebarRunBtn.dataset.bound) {
                sidebarRunBtn.dataset.bound = '1';
                sidebarRunBtn.addEventListener('click', async () => {
                    const originalText = sidebarRunBtn.textContent;
                    sidebarRunBtn.disabled = true;
                    sidebarRunBtn.textContent = 'Running…';
                    try {
                        await apiFetchJson('/api/discovery/run', { method: 'POST' });
                        sidebarRunBtn.textContent = 'Started';
                        setTimeout(() => {
                            sidebarRunBtn.textContent = originalText;
                            sidebarRunBtn.disabled = false;
                        }, 2500);
                    } catch (e) {
                        sidebarRunBtn.textContent = originalText;
                        sidebarRunBtn.disabled = false;
                        try {
                            let msg = (e && e.message) ? String(e.message) : 'Failed to start discovery';
                            const detail = e && e.data && e.data.detail ? String(e.data.detail) : '';
                            if (detail) msg = `${msg}: ${detail}`;
                            showError(msg);
                        } catch (e2) {
                            showError(e && e.message ? e.message : 'Failed to start discovery');
                        }
                    }
                });
            }
        }

        const loginBtn = document.getElementById('plexLoginBtn');
        if (loginBtn && !loginBtn.dataset.bound) {
            loginBtn.dataset.bound = '1';
            loginBtn.addEventListener('click', startPlexLogin);
        }

        if (authRequired && !authAuthed) {
            setAuthError('');
            setAuthLoadingState(false);
            setAuthOverlayVisible(true);
        } else {
            setAuthLoadingState(false);
            setAuthOverlayVisible(false);
            // Refresh per-user Last.fm state after auth.
            initLastfm();
            // Load home feed after auth becomes available.
            setActiveNav('search');
            openHomeRecommendations(false);
        }

    } catch (e) {
        // If auth endpoints aren't available, fail closed and show the auth overlay.
        authRequired = authFailClosed;
        authAuthed = false;
        authUser = null;
        const userLabel = document.getElementById('authUserLabel');
        if (userLabel) userLabel.textContent = '';
        if (authRequired) {
            setAuthLoadingState(false);
            setAuthError('Unable to contact the server auth endpoint. Please refresh and try again.');
            setAuthOverlayVisible(true);
        } else {
            setAuthLoadingState(false);
            setAuthOverlayVisible(false);
            setActiveNav('search');
            openHomeRecommendations(false);
        }
    }
}

async function startPlexLogin() {
    setAuthError('');
    setAuthLoadingState(true, 'Connecting to Plex...');

    // Open the popup window SYNCHRONOUSLY — before any await — so it is treated as a
    // direct response to the user's click gesture. Safari and Chrome on Mac (and iOS)
    // block window.open() calls that happen after an await because the user-gesture
    // context is lost across the async boundary.
    let loginWindow = null;
    try {
        loginWindow = window.open('', 'plexLogin', 'width=860,height=920');
        if (loginWindow) {
            loginWindow.document.write(
                '<!doctype html><html><head>' +
                '<meta charset="utf-8">' +
                '<meta name="viewport" content="width=device-width,initial-scale=1">' +
                '<style>body{margin:0;min-height:100vh;background:#00060E;color:#EAF2FF;' +
                'font-family:system-ui,sans-serif;display:flex;align-items:center;' +
                'justify-content:center;}</style></head>' +
                '<body><p style="opacity:.6;letter-spacing:.06em">Loading Plex login\u2026</p></body></html>'
            );
        }
    } catch (e) {
        loginWindow = null;
    }
    authPopupWindow = loginWindow;

    try {
        const resp = await fetch('/api/auth/start', { method: 'POST' });
        const data = await resp.json();

        if (!resp.ok) {
            if (loginWindow && !loginWindow.closed) loginWindow.close();
            authPopupWindow = null;
            setAuthLoadingState(false);
            const rawErr = (data && data.error) ? String(data.error) : '';
            const friendly = rawErr.replace(/_/g, ' ') || 'Failed to start Plex login';
            setAuthError(friendly.charAt(0).toUpperCase() + friendly.slice(1));
            return;
        }

        const authUrl = data.auth_url;
        const pinId = data.pin_id;
        if (!authUrl || !pinId) {
            if (loginWindow && !loginWindow.closed) loginWindow.close();
            authPopupWindow = null;
            setAuthLoadingState(false);
            setAuthError('Failed to start Plex login: no auth URL returned by server');
            return;
        }

        // Navigate the already-open popup to the real Plex auth URL.
        if (loginWindow && !loginWindow.closed) {
            loginWindow.location.href = authUrl;
        } else {
            // Popup was blocked — try once more with the real URL now that we have it.
            try { loginWindow = window.open(authUrl, 'plexLogin', 'width=860,height=920'); } catch (e) { loginWindow = null; }
            authPopupWindow = loginWindow;
            if (!loginWindow || loginWindow.closed) {
                setAuthLoadingState(false);
                // Final fallback: show a clickable link so the user can still complete login.
                setAuthError(
                    'Popup blocked by your browser. Please allow popups for this site and try again, ' +
                    'or <a href="' + authUrl + '" target="_blank" rel="noopener" ' +
                    'style="color:#FFD300">open the Plex login page manually</a>.'
                );
            }
        }

        setAuthLoadingState(true, 'Waiting for Plex approval...');

        if (authPollTimer) clearInterval(authPollTimer);
        authPollStartedAt = Date.now();
        authPollTimer = setInterval(() => pollPlexLogin(pinId), 2000);
        pollPlexLogin(pinId);
    } catch (e) {
        if (loginWindow && !loginWindow.closed) loginWindow.close();
        authPopupWindow = null;
        setAuthLoadingState(false);
        setAuthError('Failed to start Plex login');
    }
}

async function pollPlexLogin(pinId) {
    try {
        const resp = await fetch(`/api/auth/poll/${encodeURIComponent(pinId)}`);
        let data = null;
        try {
            data = await resp.json();
        } catch (e) {
            data = null;
        }

        if (data && data.status === 'pending') {
            if (authPollStartedAt && (Date.now() - authPollStartedAt) > 180000) {
                if (authPollTimer) {
                    clearInterval(authPollTimer);
                    authPollTimer = null;
                }
                setAuthLoadingState(false);
                setAuthError('Plex login timed out. Please click “Sign in with Plex” again.');
                setAuthOverlayVisible(true);
            }
            return;
        }

        if (authPollTimer) {
            clearInterval(authPollTimer);
            authPollTimer = null;
        }

        if (data.status === 'authed') {
            authAuthed = true;
            authUser = data.user || null;
            setAuthLoadingState(true, 'Loading your Plex account...');
            if (authPopupWindow && !authPopupWindow.closed) {
                try { authPopupWindow.close(); } catch (e) {}
            }
            authPopupWindow = null;
            // Refresh pill/buttons (and can_run_discovery) immediately.
            await initAuth();
            return;
        }

        if (data.status === 'denied') {
            setAuthLoadingState(false);
            setAuthError(escHtml(data.reason || 'Access denied'));
            setAuthOverlayVisible(true);
            return;
        }

        if (data && data.status === 'error') {
            const stage = data.stage ? escHtml(String(data.stage)) : '';
            const detail = data.detail ? escHtml(String(data.detail)) : '';
            let msg = escHtml(data.reason || 'Login failed');
            if (stage) msg = `${msg} (${stage})`;
            if (detail) msg = `${msg}: ${detail}`;
            setAuthLoadingState(false);
            setAuthError(msg);
            setAuthOverlayVisible(true);
            return;
        }

        // Fallback
        setAuthLoadingState(false);
        setAuthError('Login failed');
        setAuthOverlayVisible(true);
    } catch (e) {
        // Keep polling; network hiccup.
        // But if we consistently get non-JSON responses, show something actionable.
        try {
            const msg = (e && e.message) ? String(e.message) : '';
            if (msg && msg.toLowerCase().includes('json')) {
                setAuthLoadingState(false);
                setAuthError('Login failed: unexpected server response.');
                setAuthOverlayVisible(true);
            }
        } catch (e2) {}
    }
}

async function apiFetchJson(url, options) {
    const resp = await fetch(url, options);
    let data = null;
    try {
        data = await resp.json();
    } catch (e) {
        data = null;
    }

    if (resp.status === 401 && data && data.error === 'auth_required') {
        authAuthed = false;
        if (authRequired) {
            setAuthError('Please sign in with Plex to continue.');
            setAuthOverlayVisible(true);
        }
        throw new Error('auth_required');
    }

    if (!resp.ok) {
        const msg = (data && (data.error || data.reason)) ? String(data.error || data.reason) : `Request failed (${resp.status})`;
        const err = new Error(msg);
        err.status = resp.status;
        err.data = data;
        throw err;
    }

    return data;
}

function setPreviewButtonState(button, state) {
    if (!button) return;
    button.dataset.state = state;
    button.classList.toggle('is-playing', state === 'pause');
    if (state === 'none') {
        button.disabled = true;
        button.title = 'No preview available';
        button.setAttribute('aria-label', 'No preview available');
        return;
    }
    if (state === 'loading') {
        button.disabled = true;
        button.title = 'Loading preview';
        button.setAttribute('aria-label', 'Loading preview');
        return;
    }
    button.disabled = false;
    if (state === 'pause') {
        button.title = 'Pause preview';
        button.setAttribute('aria-label', 'Pause preview');
    } else {
        button.title = 'Play preview';
        button.setAttribute('aria-label', 'Play preview');
    }
}

function previewKey(artist, title) {
    return `${artist || ''}|||${title || ''}`;
}

async function togglePreview(artist, title, button, coverUrl) {
    if (!previewAudio) return;
    const key = previewKey(artist, title);

    // Toggle pause/play if same track
    if (currentPreviewKey === key && !previewAudio.paused) {
        previewAudio.pause();
        if (button) setPreviewButtonState(button, 'play');
        return;
    }

    // Stop previous track
    try {
        previewAudio.pause();
        previewAudio.currentTime = 0;
    } catch (e) {
        // ignore
    }

    if (currentPreviewButton && currentPreviewButton !== button) {
        setPreviewButtonState(currentPreviewButton, 'play');
    }

    currentPreviewKey = key;
    currentPreviewButton = button;
    currentPreviewMeta = { artist, title, coverUrl: coverUrl || '' };
    setPlayerMeta(currentPreviewMeta);
    setPlayerPlayState('play');

    if (button) setPreviewButtonState(button, 'loading');

    try {
        const data = await apiFetchJson(`/api/preview?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(title)}`);
        const url = data.preview_url || '';

        if (!url) {
            if (button) setPreviewButtonState(button, 'none');
            currentPreviewUrl = null;
            currentPreviewMeta = null;
            setPlayerMeta(null);
            return;
        }

        currentPreviewUrl = url;
        previewAudio.src = url;
        await previewAudio.play();

        if (button) setPreviewButtonState(button, 'pause');
        setPlayerPlayState('pause');
    } catch (e) {
        if (button) setPreviewButtonState(button, 'none');
        currentPreviewUrl = null;
        currentPreviewMeta = null;
        setPlayerMeta(null);
        console.error('Preview error:', e);
    }
}

function setLoading(isLoading, message) {
    const loadingIndicator = document.getElementById('loadingIndicator');
    if (!loadingIndicator) return;

    const msgEl = loadingIndicator.querySelector('p');
    if (msgEl && message) msgEl.textContent = message;

    loadingIndicator.style.display = isLoading ? 'block' : 'none';
}

function setViewHeader(html) {
    const header = document.getElementById('viewHeader');
    if (!header) return;
    if (!html) {
        header.style.display = 'none';
        header.innerHTML = '';
        return;
    }
    header.innerHTML = html;
    header.style.display = 'flex';
}

function pushView() {
    viewStack.push(currentView);
}

function goBack() {
    const prev = viewStack.pop();
    if (!prev) {
        setViewHeader('');
        return;
    }

    // Invalidate any in-flight async work from the view we're leaving.
    beginView(prev.kind);
    if (prev.kind === 'library') {
        setActiveNav('library');
    } else if (prev.kind === 'downloads') {
        setActiveNav('downloads');
    } else {
        // home/search/artist/album live under the Search nav.
        setActiveNav('search');
    }

    if (prev.kind === 'search') {
        renderSearchView(prev.state);
    } else if (prev.kind === 'artist') {
        renderArtistView(prev.state);
    } else if (prev.kind === 'album') {
        renderAlbumView(prev.state);
    } else if (prev.kind === 'library') {
        renderLibraryView(prev.state);
    } else if (prev.kind === 'home') {
        renderHomeView(prev.state);
    }
}

async function openLibrary(force) {
    const token = beginView('library');
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) resultsContainer.innerHTML = '';

    // Serve from JS cache on re-visits unless the user hit Refresh.
    if (!force) {
        const cached = _cacheGet('library');
        if (cached) {
            setLoading(false);
            setViewHeader(`
                <div class="view-title">Library — Albums</div>
            `);
            renderLibraryView(cached);
            return;
        }
    }

    setLoading(true, 'Loading your library…');
    setViewHeader(`
        <div class="view-title">Library — Albums</div>
    `);

    try {
        const data = await apiFetchJson('/api/library/albums?limit=5000');
        if (!isActiveView('library', token)) return;
        setLoading(false);
        const items = (data && data.items) ? data.items : [];
        const state = { items, root: (data && data.root) ? data.root : '' };
        _cacheSet('library', state);
        renderLibraryView(state);
    } catch (e) {
        if (!isActiveView('library', token)) return;
        setLoading(false);
        showError('Failed to load library albums. Check OUTPUT_PATH and try again.');
        console.error('Library albums error:', e);
    }
}

function renderLibraryView(state) {
    currentView = { kind: 'library', state };
    setResultsMode('grid');

    const count = Array.isArray(state && state.items) ? state.items.length : 0;
    const root = (state && state.root) ? String(state.root) : '';

    setViewHeader(`
        <div class="view-title">Library — Albums <span class="muted">(${count})</span></div>
    `);

    displayResults((state && state.items) ? state.items : [], 'album');

    // Hydrate completeness (requires LASTFM_API_KEY). If unavailable, badges stay as “–”.
    hydrateLibraryAlbumCompleteness((state && state.items) ? state.items : []);
}

async function hydrateLibraryAlbumCompleteness(items) {
    if (!currentView || currentView.kind !== 'library') return;
    const albumItems = (items || []).filter(i => i && (i.type || 'album').toLowerCase() === 'album' && i.library_owned);
    if (albumItems.length === 0) return;

    const concurrency = 6;
    let idx = 0;

    async function worker() {
        while (idx < albumItems.length) {
            if (!currentView || currentView.kind !== 'library') return;
            const myIdx = idx;
            idx += 1;

            const it = albumItems[myIdx];
            if (it.complete === true) continue;

            const artist = it.artist || '';
            const album = it.name || it.title || '';
            if (!artist || !album) continue;

            try {
                const data = await apiFetchJson(`/api/album/status?artist=${encodeURIComponent(artist)}&album=${encodeURIComponent(album)}`);
                const isComplete = !!(data && data.in_library);
                it.complete = isComplete;
                it.missing = (data && typeof data.missing === 'number') ? data.missing : null;

                if (!currentView || currentView.kind !== 'library') return;

                const cards = document.querySelectorAll('.result-card[data-item-type="album"]');
                cards.forEach((card) => {
                    const owned = card.dataset.libraryOwned === '1';
                    if (!owned) return;
                    const ca = card.dataset.itemArtist || '';
                    const ct = card.dataset.itemTitle || '';
                    if (ca !== artist || ct !== album) return;

                    const badge = card.querySelector('.cover-status-badge');
                    if (!badge) return;
                    if (isComplete) {
                        badge.classList.add('badge-in-library');
                        badge.classList.remove('badge-partial', 'badge-downloading', 'badge-error');
                        badge.disabled = true;
                        badge.title = 'In Library';
                        badge.setAttribute('aria-label', 'In Library');
                    } else {
                        badge.classList.add('badge-partial');
                        badge.classList.remove('badge-in-library', 'badge-downloading', 'badge-error');
                        badge.disabled = false;
                        badge.title = 'Download Missing';
                        badge.setAttribute('aria-label', 'Download Missing');
                    }
                });
            } catch (e) {
                // If album/status can't be computed (e.g. LASTFM_API_KEY missing), leave dash.
            }
        }
    }

    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(worker());
    await Promise.all(workers);
}

async function performSearch() {
    setActiveNav('search');
    const query = document.getElementById('searchInput').value.trim();
    const searchType = document.getElementById('searchType').value;
    
    if (!query) {
        openHomeRecommendations(true);
        return;
    }

    const token = beginView('search');

    const resultsContainer = document.getElementById('resultsContainer');
    setViewHeader('');
    setLoading(true, 'Searching...');
    resultsContainer.innerHTML = '';

    try {
        const data = await apiFetchJson(`/api/search?q=${encodeURIComponent(query)}&type=${searchType}&limit=24`);

        if (!isActiveView('search', token)) return;

        setLoading(false);

        if (!data.items || data.items.length === 0) {
            resultsContainer.innerHTML = '<div class="no-results">No results found. Try a different search.</div>';
            return;
        }

        const state = { query, type: searchType, items: data.items };
        renderSearchView(state);

    } catch (error) {
        if (!isActiveView('search', token)) return;
        setLoading(false);
        showError('Failed to search. Please try again.');
        console.error('Search error:', error);
    }
}

function renderSearchView(state) {
    currentView = { kind: 'search', state };
    setResultsMode('grid');
    displayResults(state.items, state.type);
    setViewHeader('');
}

async function openArtist(artistName) {
    if (!artistName) return;
    pushView();

    const token = beginView('artist');

    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = '';
    const downloadLabel = isMobileWebUi() ? 'Download' : 'Download Top Tracks';
    setViewHeader(`
        <button class="nav-btn" onclick="goBack()">← Back</button>
        <div class="view-title">${escapeHtml(artistName)}</div>
        <div class="view-actions">
            <button class="nav-btn" onclick="downloadItem('${escapeJsString(artistName)}', '${escapeJsString(artistName)}', 'artist', this)">${downloadLabel}</button>
        </div>
    `);
    setLoading(true, 'Loading artist…');

    try {
        const [albumsData, topData, newData] = await Promise.all([
            apiFetchJson(`/api/artist/albums?artist=${encodeURIComponent(artistName)}&limit=60`),
            apiFetchJson(`/api/artist/top_tracks?artist=${encodeURIComponent(artistName)}&limit=10`),
            apiFetchJson(`/api/artist/new_release?artist=${encodeURIComponent(artistName)}&album_limit=10`),
        ]);
        if (!isActiveView('artist', token)) return;
        setLoading(false);

        const albums = (albumsData && albumsData.items) ? albumsData.items : [];
        const popular = (topData && topData.items) ? topData.items : [];
        const newRelease = (newData && newData.items) ? {
            album: newData.album || '',
            cover_url: newData.cover_url || '',
            in_library: !!newData.in_library,
            items: newData.items || []
        } : null;

        renderArtistView({ artist: artistName, popular_items: popular, new_release: newRelease, items: albums });
    } catch (e) {
        if (!isActiveView('artist', token)) return;
        setLoading(false);
        showError('Failed to load artist. Please try again.');
        console.error('Artist view error:', e);
    }
}

function renderArtistView(state) {
    currentView = { kind: 'artist', state };
    setResultsMode('list');
    const downloadLabel = isMobileWebUi() ? 'Download' : 'Download Top Tracks';
    setViewHeader(`
        <button class="nav-btn" onclick="goBack()">← Back</button>
        <div class="view-title">${escapeHtml(state.artist)}</div>
        <div class="view-actions">
            <button class="nav-btn" onclick="downloadItem('${escapeJsString(state.artist)}', '${escapeJsString(state.artist)}', 'artist', this)">${downloadLabel}</button>
        </div>
    `);

    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer) return;
    resultsContainer.innerHTML = '';

    // Popular songs list
    if ((state.popular_items || []).length > 0) {
        const section = document.createElement('div');
        section.className = 'results-section';
        section.innerHTML = `<div class="results-section-title">Popular</div>`;

        const listEl = createTrackListElement(state.popular_items || [], {
            artist: state.artist,
            album: '',
            cover_url: ''
        }, { allowLazyTrackCovers: true });

        section.appendChild(listEl);
        resultsContainer.appendChild(section);
    }

    // New release track list (best-effort)
    if (state.new_release && (state.new_release.items || []).length > 0) {
        const section = document.createElement('div');
        section.className = 'results-section';
        const albumTitle = state.new_release.album ? ` — ${escapeHtml(state.new_release.album)}` : '';
        section.innerHTML = `<div class="results-section-title">New Release${albumTitle}</div>`;

        const listEl = createTrackListElement(state.new_release.items || [], {
            artist: state.artist,
            album: state.new_release.album || '',
            cover_url: state.new_release.cover_url || '',
            in_library: !!state.new_release.in_library
        }, { allowLazyTrackCovers: false });

        section.appendChild(listEl);
        resultsContainer.appendChild(section);
    }

    // Albums grid
    const albumsSection = document.createElement('div');
    albumsSection.className = 'results-section';
    albumsSection.innerHTML = `<div class="results-section-title">Albums</div>`;
    const grid = document.createElement('div');
    grid.className = 'results-container';
    (state.items || []).forEach((album) => {
        const card = createResultCard(album, 'album');
        grid.appendChild(card);
    });
    albumsSection.appendChild(grid);
    resultsContainer.appendChild(albumsSection);

    // Some albums may not exist as folders because we avoid duplicates across albums.
    // Hydrate album cards by checking whether *all tracks* are already in the library.
    hydrateAlbumStatuses(state.items || []);

    initLazyCoverObserver();
}

async function openAlbum(artistName, albumName) {
    if (!artistName || !albumName) return;
    pushView();

    const token = beginView('album');

    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = '';
    const downloadLabel = isMobileWebUi() ? 'Download' : 'Download Album';
    setViewHeader(`
        <button class="nav-btn" onclick="goBack()">← Back</button>
        <div class="view-title">${escapeHtml(artistName)} — ${escapeHtml(albumName)}</div>
        <div class="view-actions">
            <button class="nav-btn" onclick="downloadItem('${escapeJsString(artistName)}', '${escapeJsString(albumName)}', 'album', this)">${downloadLabel}</button>
        </div>
    `);
    setLoading(true, 'Loading tracks...');

    try {
        const data = await apiFetchJson(`/api/album/tracks?artist=${encodeURIComponent(artistName)}&album=${encodeURIComponent(albumName)}`);
        if (!isActiveView('album', token)) return;
        setLoading(false);

        renderAlbumView({ artist: artistName, album: albumName, items: data.items || [], cover_url: data.cover_url || '', in_library: !!data.in_library });
    } catch (e) {
        if (!isActiveView('album', token)) return;
        setLoading(false);
        showError('Failed to load tracks. Please try again.');
        console.error('Album tracks error:', e);
    }
}

function renderAlbumView(state) {
    currentView = { kind: 'album', state };
    setResultsMode('list');
    const downloadLabel = isMobileWebUi() ? 'Download' : 'Download Album';
    setViewHeader(`
        <button class="nav-btn" onclick="goBack()">← Back</button>
        <div class="view-title">${escapeHtml(state.artist)} — ${escapeHtml(state.album)}</div>
        <div class="view-actions">
            <button class="nav-btn" onclick="downloadItem('${escapeJsString(state.artist)}', '${escapeJsString(state.album)}', 'album', this)">${downloadLabel}</button>
        </div>
    `);

    displayTrackList(state.items || [], state);

    // If backend says album is already in library, disable header download.
    if (state.in_library) {
        const header = document.getElementById('viewHeader');
        const buttons = header ? header.querySelectorAll('button.nav-btn') : [];
        buttons.forEach((b) => {
            if ((b.textContent || '').toLowerCase().includes('download album')) {
                b.disabled = true;
                b.textContent = '✓ In Library';
            }
        });
    }
}

function setResultsMode(mode) {
    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer) return;
    resultsContainer.classList.remove('tracks-list-container', 'home-shelves-container');
    if (mode === 'list') {
        resultsContainer.classList.add('tracks-list-container');
    }
}

function displayTrackList(items, albumState) {
    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = '';

    const list = createTrackListElement(items || [], albumState || {}, { allowLazyTrackCovers: false });
    resultsContainer.appendChild(list);
}

function createTrackListElement(items, albumState, opts) {
    const options = opts || {};
    const allowLazyTrackCovers = options.allowLazyTrackCovers === true;

    const list = document.createElement('div');
    list.className = 'tracks-list';

    const sorted = [...(items || [])].sort((a, b) => {
        const ra = Number(a.rank || 0);
        const rb = Number(b.rank || 0);
        if (ra && rb) return ra - rb;
        return 0;
    });

    const fallbackCover = '/static/cover-placeholder.svg';

    sorted.forEach((item, idx) => {
        const row = document.createElement('div');
        row.className = 'track-row with-cover';

        const title = item.title || item.name || '';
        const artist = item.artist || albumState.artist || 'Unknown Artist';
        const inLib = !!item.in_library;

        const coverCandidate = item.cover_url || item.album?.cover_url || albumState.cover_url || '';
        const directUrl = (!coverCandidate || isLastFmPlaceholder(coverCandidate)) ? '' : coverCandidate;
        const proxyUrl = (directUrl && (directUrl.startsWith('http://') || directUrl.startsWith('https://')))
            ? `/api/image?u=${encodeURIComponent(directUrl)}`
            : '';
        const coverUrl = directUrl || fallbackCover;

        const lazyCoverEndpoint = (!directUrl && allowLazyTrackCovers && artist && title)
            ? `/api/lastfm/track_cover?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(title)}`
            : '';
        const lazyCoverAttr = lazyCoverEndpoint
            ? `data-lazy-cover-endpoint="${lazyCoverEndpoint}" data-lazy-cover-loaded="0" data-lazy-cover-artist="${escHtml(artist)}"`
            : '';

        row.innerHTML = `
            <div class="track-num">${escapeHtml(String(item.rank || (idx + 1)))}</div>
            <div class="track-cover">
                <img
                    class="track-cover-img${lazyCoverEndpoint ? ' lazy-cover' : ''}"
                    src="${coverUrl}"
                    alt="${escapeHtml(title)}"
                    data-proxy-src="${proxyUrl}"
                    data-fallback-src="${fallbackCover}"
                    data-tried-proxy="0"
                    ${lazyCoverAttr}
                    onerror="handleImgError(this)"
                >
            </div>
            <div class="track-main">
                <div class="track-row-title">${escapeHtml(title)}</div>
                <div class="track-row-sub">${escapeHtml(artist)}</div>
            </div>
            <div class="track-status">${inLib ? '<span class="library-tick">✓</span>' : ''}</div>
            <div class="track-actions">
                <div class="track-action-row">
                    <button class="preview-btn track-preview-btn" data-state="play" aria-label="Play preview" title="Play preview"></button>
                    <button class="download-btn track-download-btn">${inLib ? '✓ In Library' : 'Download'}</button>
                </div>
            </div>
        `;

        const pbtn = row.querySelector('.track-preview-btn');
        if (pbtn) {
            setPreviewButtonState(pbtn, 'play');
            pbtn.addEventListener('click', () => togglePreview(artist, title, pbtn, coverUrl));
        }

        const btn = row.querySelector('.track-download-btn');
        if (btn) {
            if (inLib) {
                btn.disabled = true;
                btn.classList.add('success');
            } else {
                btn.addEventListener('click', () => downloadItem(artist, title, 'track', btn));
            }
        }

        list.appendChild(row);
    });

    return list;
}

function displayResults(items, type) {
    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = '';

    items.forEach(item => {
        const card = createResultCard(item, type);
        resultsContainer.appendChild(card);
    });

    initLazyCoverObserver();
}

let lazyCoverObserver = null;
const lazyCoverCache = new Map();       // endpoint URL -> proxy URL
const lazyCoverInFlight = new Map();   // endpoint URL -> Promise<string>
const lazyCoverByAlbumCache = new Map(); // "artist||album" -> proxy URL

function initLazyCoverObserver() {
    try {
        if (lazyCoverObserver) {
            lazyCoverObserver.disconnect();
            lazyCoverObserver = null;
        }
        if (!('IntersectionObserver' in window)) return;

        const imgs = Array.from(document.querySelectorAll('img[data-lazy-cover-endpoint]'));
        if (imgs.length === 0) return;

        lazyCoverObserver = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (!entry.isIntersecting) return;
                const img = entry.target;
                if (!img) return;
                lazyCoverObserver.unobserve(img);
                requestLazyCover(img);
            });
        }, {
            root: null,
            // Start fetching before scrolling into view — include horizontal margin for shelf rows.
            rootMargin: '220px 400px',
            threshold: 0.01
        });

        imgs.forEach((img) => lazyCoverObserver.observe(img));
    } catch (e) {
        // ignore
    }
}

async function requestLazyCover(img) {
    try {
        if (!img || img.dataset.lazyCoverLoaded === '1') return;
        const endpoint = img.dataset.lazyCoverEndpoint || '';
        if (!endpoint) return;

        const artist = (img.dataset.lazyCoverArtist || '').toLowerCase().trim();

        // 1. Endpoint-level cache — exact match for this artist+track.
        if (lazyCoverCache.has(endpoint)) {
            const cached = lazyCoverCache.get(endpoint) || '';
            if (cached) applyLazyCover(img, cached);
            return;
        }

        if (lazyCoverInFlight.has(endpoint)) {
            const p = lazyCoverInFlight.get(endpoint);
            const url = await p;
            if (url) applyLazyCover(img, url);
            return;
        }

        img.classList.add('is-cover-loading');

        const p = (async () => {
            try {
                const data = await apiFetchJson(endpoint);
                const url = (data && data.proxy_url) ? String(data.proxy_url) : '';
                lazyCoverCache.set(endpoint, url);
                // Populate album cache keyed by "artist||album" so other tracks from
                // the same album can reuse this cover without a second API call.
                const album = (data && data.album) ? String(data.album).toLowerCase().trim() : '';
                if (url && artist && album) {
                    lazyCoverByAlbumCache.set(`${artist}||${album}`, url);
                }
                return url;
            } catch (e) {
                lazyCoverCache.set(endpoint, '');
                return '';
            } finally {
                lazyCoverInFlight.delete(endpoint);
            }
        })();

        lazyCoverInFlight.set(endpoint, p);
        const url = await p;
        img.classList.remove('is-cover-loading');
        if (url) applyLazyCover(img, url);
    } catch (e) {
        try { img.classList.remove('is-cover-loading'); } catch (_) {}
    }
}

function applyLazyCover(img, proxyUrl) {
    try {
        if (!img || !proxyUrl) return;
        if (img.dataset.lazyCoverLoaded === '1') return;
        img.dataset.lazyCoverLoaded = '1';

        img.dataset.proxySrc = proxyUrl;
        img.dataset.triedProxy = '1';

        const pre = new Image();
        pre.onload = () => {
            img.src = proxyUrl;
            img.classList.add('is-cover-loaded');
        };
        pre.onerror = () => {
            // Fall back to setting it anyway; handleImgError will catch failures.
            img.src = proxyUrl;
        };
        pre.src = proxyUrl;
    } catch (e) {
        // ignore
    }
}

function createResultCard(item, type) {
    const card = document.createElement('div');
    const normalizedType = (item.type || type || 'track').toLowerCase();
    const isArtist = normalizedType === 'artist';
    const isNavigable = normalizedType === 'artist' || normalizedType === 'album';
    card.className = `result-card${isArtist ? ' result-card-artist' : ''}${isNavigable ? ' is-clickable' : ''}`;

    const fallbackCover = isArtist
        ? '/static/artist-placeholder.svg'
        : '/static/cover-placeholder.svg';

    const coverCandidate = item.cover_url || item.album?.cover_url || '';
    const directUrl = (!coverCandidate || isLastFmPlaceholder(coverCandidate)) ? '' : coverCandidate;
    const proxyUrl = (directUrl && (directUrl.startsWith('http://') || directUrl.startsWith('https://')))
        ? `/api/image?u=${encodeURIComponent(directUrl)}`
        : '';
    const coverUrl = directUrl || fallbackCover;
    const title = item.title || item.name || '';
    const artist = item.artist || item.artists?.join(', ') || 'Unknown Artist';
    const album = item.album?.name || '';
    const itemType = item.type ? item.type.toUpperCase() : (type ? type.toUpperCase() : 'TRACK');
    const inLibrary = !!item.in_library;
    const libraryOwned = !!item.library_owned;
    const partial = !!item.partial;
    const complete = item.complete === true;

    card.dataset.itemType = normalizedType;
    card.dataset.itemArtist = artist;
    card.dataset.itemTitle = title;
    card.dataset.inLibrary = inLibrary ? '1' : '0';
    card.dataset.libraryOwned = libraryOwned ? '1' : '0';

    const lazyCoverEndpoint = (!directUrl && normalizedType === 'track' && artist && title)
        ? `/api/lastfm/track_cover?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(title)}`
        : '';

    const lazyCoverAttr = lazyCoverEndpoint
        ? `data-lazy-cover-endpoint="${lazyCoverEndpoint}" data-lazy-cover-loaded="0" data-lazy-cover-artist="${escHtml(artist)}"`
        : '';

    // Determine status badge state
    let badgeClass = 'cover-status-badge';
    let badgeDisabled = '';
    let badgeTitle = 'Download';
    if (!isArtist) {
        if (inLibrary || complete) {
            badgeClass += ' badge-in-library';
            badgeDisabled = ' disabled';
            badgeTitle = 'In Library';
        } else if ((partial && normalizedType === 'album') || (libraryOwned && normalizedType === 'album')) {
            // Album folder exists but might be incomplete
            badgeClass += ' badge-partial';
            badgeTitle = 'Download Missing';
        }
    }

    card.innerHTML = `
        <div class="card-image-container">
            <img
                class="cover-art${isArtist ? ' artist-avatar' : ''}${lazyCoverEndpoint ? ' lazy-cover' : ''}"
                src="${coverUrl}"
                alt="${escHtml(title)}"
                data-proxy-src="${proxyUrl}"
                data-fallback-src="${fallbackCover}"
                data-tried-proxy="0"
                ${lazyCoverAttr}
                onerror="handleImgError(this)"
            >
            <span class="type-badge">${itemType}</span>
            ${!isArtist ? `<button class="${badgeClass}"${badgeDisabled} title="${badgeTitle}" aria-label="${badgeTitle}"></button>` : ''}
            ${normalizedType === 'track' ? '<button class="preview-btn preview-overlay-btn" data-state="play" aria-label="Play preview" title="Play preview"></button>' : ''}
        </div>
        <div class="card-content">
            <div class="track-title">${escapeHtml(title)}</div>
            <div class="track-artist">${escapeHtml(artist)}</div>
            ${album ? `<div class="track-album">${escapeHtml(album)}</div>` : ''}
        </div>
    `;

    const previewBtn = card.querySelector('.preview-overlay-btn');
    if (previewBtn && normalizedType === 'track') {
        setPreviewButtonState(previewBtn, 'play');
        previewBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            togglePreview(artist, title, previewBtn, coverUrl);
        });
    }

    const btn = card.querySelector('.cover-status-badge');
    if (btn && !isArtist) {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            if (btn.disabled) return;
            const t = normalizedType;
            if (t === 'album') {
                downloadItem(artist, title, 'album', btn);
            } else {
                downloadItem(artist, title, 'track', btn);
            }
        });
    }

    if (isNavigable) {
        card.addEventListener('click', () => {
            if (normalizedType === 'artist') {
                openArtist(title || artist);
            } else if (normalizedType === 'album') {
                openAlbum(artist, title);
            }
        });
    }

    return card;
}

async function hydrateAlbumStatuses(items) {
    if (!currentView || currentView.kind !== 'artist') return;
    // Only run for albums that are not already marked.
    const albumItems = (items || []).filter(i => ((i.type || 'album').toLowerCase() === 'album') && !i.in_library);
    if (albumItems.length === 0) return;

    const concurrency = 6;
    let idx = 0;

    async function worker() {
        while (idx < albumItems.length) {
            if (!currentView || currentView.kind !== 'artist') return;
            const myIdx = idx;
            idx += 1;

            const it = albumItems[myIdx];
            const artist = it.artist || '';
            const album = it.name || it.title || '';
            if (!artist || !album) continue;

            try {
                const data = await apiFetchJson(`/api/album/status?artist=${encodeURIComponent(artist)}&album=${encodeURIComponent(album)}`);

                if (!currentView || currentView.kind !== 'artist') return;

                if (data.in_library) {
                    // Update in-memory state so it sticks.
                    it.in_library = true;

                    // Update the currently rendered card.
                    const cards = document.querySelectorAll('.result-card[data-item-type="album"]');
                    cards.forEach((card) => {
                        const ca = card.dataset.itemArtist || '';
                        const ct = card.dataset.itemTitle || '';
                        if (ca === artist && ct === album) {
                            card.dataset.inLibrary = '1';

                            const badge = card.querySelector('.cover-status-badge');
                            if (badge) {
                                badge.classList.add('badge-in-library');
                                badge.classList.remove('badge-partial', 'badge-downloading', 'badge-error');
                                badge.disabled = true;
                                badge.title = 'In Library';
                                badge.setAttribute('aria-label', 'In Library');
                            }
                        }
                    });
                }
            } catch (e) {
                // ignore
            }
        }
    }

    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(worker());
    await Promise.all(workers);
}

function handleImgError(img) {
    try {
        const triedProxy = img.dataset.triedProxy === '1';
        const proxySrc = img.dataset.proxySrc || '';
        const fallbackSrc = img.dataset.fallbackSrc || '/static/cover-placeholder.svg';

        if (!triedProxy && proxySrc) {
            img.dataset.triedProxy = '1';
            img.src = proxySrc;
            return;
        }

        img.onerror = null;
        img.src = fallbackSrc;
    } catch (e) {
        img.onerror = null;
        img.src = '/static/cover-placeholder.svg';
    }
}

function isLastFmPlaceholder(url) {
    return typeof url === 'string' && url.includes('2a96cbd8b46e442fc41c2b86b821562f');
}

async function downloadItem(artist, title, type, buttonElement) {
    const isBadge = buttonElement && buttonElement.classList.contains('cover-status-badge');

    if (buttonElement && buttonElement.disabled) {
        // Allow retrying errors on badges; block if truly in-library
        if (isBadge && buttonElement.classList.contains('badge-in-library')) return;
        if (!isBadge && (buttonElement.textContent || '').includes('In Library')) return;
    }

    // Disable button
    buttonElement.disabled = true;
    if (isBadge) {
        buttonElement.classList.remove('badge-partial', 'badge-error');
        buttonElement.classList.add('badge-downloading');
    } else {
        buttonElement.textContent = 'Queued…';
        buttonElement.classList.add('downloading');
    }

    const downloadData = {
        artist: artist,
        title: title,
        type: type
    };

    try {
        const result = await apiFetchJson('/api/download', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(downloadData)
        });

        if (result.success) {
            if (result.already_in_library) {
                if (isBadge) {
                    buttonElement.classList.remove('badge-downloading');
                    buttonElement.classList.add('badge-in-library');
                    buttonElement.title = 'In Library';
                    buttonElement.setAttribute('aria-label', 'In Library');
                } else {
                    buttonElement.textContent = '✓ In Library';
                    buttonElement.classList.remove('downloading');
                    buttonElement.classList.add('success');
                }
            } else {
                const jobId = result.download_id;
                if (jobId) {
                    buttonElement.dataset.downloadJobId = String(jobId);
                }
                if (!isBadge) {
                    buttonElement.textContent = 'In Queue…';
                }
                // Keep disabled; the poller will flip it when done.
            }
            pollDownloadsOnce();
        } else {
            if (isBadge) {
                buttonElement.classList.remove('badge-downloading');
                buttonElement.classList.add('badge-error');
                buttonElement.disabled = false;
                setTimeout(() => {
                    buttonElement.classList.remove('badge-error');
                }, 3000);
            } else {
                buttonElement.textContent = '✗ Failed';
                buttonElement.classList.remove('downloading');
                buttonElement.classList.add('error');
                buttonElement.disabled = false;
                setTimeout(() => {
                    buttonElement.textContent = 'Retry Download';
                    buttonElement.classList.remove('error');
                }, 3000);
            }
        }

    } catch (error) {
        console.error('Download error:', error);
        if (isBadge) {
            buttonElement.classList.remove('badge-downloading');
            buttonElement.classList.add('badge-error');
            buttonElement.disabled = false;
            setTimeout(() => {
                buttonElement.classList.remove('badge-error');
            }, 3000);
        } else {
            buttonElement.textContent = '✗ Error';
            buttonElement.classList.remove('downloading');
            buttonElement.classList.add('error');
            buttonElement.disabled = false;
            setTimeout(() => {
                buttonElement.textContent = 'Retry Download';
                buttonElement.classList.remove('error');
            }, 3000);
        }
    }
}

function addToQueue(artist, title) {
    const queueContainer = document.getElementById('downloadQueue');
    const queueList = document.getElementById('queueList');

    // Legacy UI: if these elements aren't present, no-op safely.
    if (!queueContainer || !queueList) return;
    
    const queueId = `${artist}_${title}`;
    
    if (downloadQueue.find(item => item.id === queueId)) {
        return; // Already in queue
    }

    const queueItem = {
        id: queueId,
        artist: artist,
        title: title,
        status: 'downloading'
    };

    downloadQueue.push(queueItem);

    const itemElement = document.createElement('div');
    itemElement.className = 'queue-item';
    itemElement.id = `queue-${queueId}`;
    itemElement.innerHTML = `
        <div class="queue-item-title">${escapeHtml(artist)} - ${escapeHtml(title)}</div>
        <div class="queue-item-status">Downloading...</div>
    `;

    queueList.appendChild(itemElement);
    queueContainer.style.display = 'block';
}

function updateQueueStatus(artist, title, status) {
    const queueId = `${artist}_${title}`;
    const itemElement = document.getElementById(`queue-${queueId}`);
    
    if (itemElement) {
        const statusElement = itemElement.querySelector('.queue-item-status');
        
        switch(status) {
            case 'completed':
                statusElement.textContent = '✓ Completed';
                statusElement.style.color = 'var(--success)';
                break;
            case 'failed':
            case 'error':
                statusElement.textContent = '✗ Failed';
                statusElement.style.color = 'var(--error)';
                break;
        }

        // Remove from queue after 5 seconds
        setTimeout(() => {
            itemElement.remove();
            downloadQueue = downloadQueue.filter(item => item.id !== queueId);
            
            if (downloadQueue.length === 0) {
                const container = document.getElementById('downloadQueue');
                if (container) container.style.display = 'none';
            }
        }, 5000);
    }
}

function showError(message) {
    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = `<div class="error-message">${escapeHtml(message)}</div>`;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function escapeJsString(text) {
    return String(text)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'")
        .replace(/\n/g, '\\n')
        .replace(/\r/g, '\\r');
}

// ============================================================
//  IMPORT VIEW
// ============================================================

// Cache for the last resolved Spotify import (survives nav away/back).
let _importState = null; // { url, result } — result is the /api/spotify/resolve response

function openImport() {
    if (authRequired && !authAuthed) return;

    beginView('import');
    currentView = { kind: 'import', state: null };

    setResultsMode('list');

    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) resultsContainer.innerHTML = '';

    setViewHeader(`
        <div class="view-title">Import from Spotify</div>
    `);

    setLoading(false);
    _renderImportShell();
}

/** Render the URL input shell (and populate track list if _importState is set). */
function _renderImportShell() {
    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer) return;

    const cachedUrl = _importState ? escHtml(_importState.url) : '';

    resultsContainer.innerHTML = `
        <div class="import-header">
            <p>Paste a Spotify link (track, album, or playlist) to preview and download it to Plex.</p>
        </div>
        <div class="import-url-bar">
            <input id="importUrlInput" class="import-url-input" type="text"
                placeholder="https://open.spotify.com/playlist/…"
                value="${cachedUrl}" autocomplete="off" spellcheck="false">
            <button class="nav-btn" id="importResolveBtn">Load</button>
        </div>
        <div id="importTrackArea"></div>
    `;

    const input = document.getElementById('importUrlInput');
    const btn = document.getElementById('importResolveBtn');

    if (btn) {
        btn.addEventListener('click', () => {
            const url = (input ? input.value : '').trim();
            if (!url) return;
            loadSpotifyImport(url);
        });
    }
    if (input) {
        input.addEventListener('keypress', e => {
            if (e.key === 'Enter') {
                const url = input.value.trim();
                if (url) loadSpotifyImport(url);
            }
        });
    }

    // If we have a cached result, render it immediately.
    if (_importState && _importState.result) {
        _renderImportTracks(_importState.result);
    }
}

async function loadSpotifyImport(url) {
    if (!url) return;

    const trackArea = document.getElementById('importTrackArea');
    if (trackArea) {
        trackArea.innerHTML = `<div class="loading-placeholder"><div class="spinner"></div><p>Resolving Spotify link…</p></div>`;
    }
    const resolveBtn = document.getElementById('importResolveBtn');
    if (resolveBtn) { resolveBtn.disabled = true; resolveBtn.textContent = 'Loading…'; }

    try {
        const data = await apiFetchJson('/api/spotify/resolve', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url }),
        });

        if (data && data.error) {
            if (trackArea) trackArea.innerHTML = `<div class="error-message">${escHtml(data.error)}</div>`;
            return;
        }

        _importState = { url, result: data };
        _renderImportTracks(data);

    } catch (e) {
        if (trackArea) trackArea.innerHTML = `<div class="error-message">Failed to resolve URL: ${escHtml(String(e))}</div>`;
    } finally {
        if (resolveBtn) { resolveBtn.disabled = false; resolveBtn.textContent = 'Load'; }
    }
}

function _renderImportTracks(data) {
    const trackArea = document.getElementById('importTrackArea');
    if (!trackArea) return;

    const tracks = data.tracks || [];
    const total = tracks.length;
    const inLibCount = tracks.filter(t => t.in_library).length;
    const toDownload = tracks.filter(t => !t.in_library);
    const isPlaylist = data.type === 'playlist';
    const defaultName = data.name || '';

    // Cover art hero (playlist / album header image)
    const rawHeroCover = data.cover_url || '';
    const heroCoverSrc = (rawHeroCover && rawHeroCover.startsWith('http'))
        ? `/api/image?u=${encodeURIComponent(rawHeroCover)}`
        : rawHeroCover;
    const heroHtml = heroCoverSrc ? `
        <div class="import-result-hero">
            <img class="import-result-cover"
                src="${heroCoverSrc}"
                alt="${escHtml(defaultName)}"
                onerror="this.style.display='none'">
            <div class="import-result-info">
                <div class="import-result-type">${isPlaylist ? 'Playlist' : data.type === 'album' ? 'Album' : 'Track'}</div>
                <div class="import-result-name">${escHtml(defaultName)}</div>
                ${data.artist ? `<div class="import-result-artist">${escHtml(data.artist)}</div>` : ''}
                <div class="import-result-meta">
                    ${total} track${total !== 1 ? 's' : ''}
                    &nbsp;·&nbsp; <span class="in-lib-count">${inLibCount} already in library</span>
                    &nbsp;·&nbsp; ${toDownload.length} to download
                </div>
            </div>
        </div>` : `
        <div class="import-summary">
            <strong>${escHtml(defaultName || 'Import')}</strong>
            &nbsp;·&nbsp; ${total} track${total !== 1 ? 's' : ''}
            &nbsp;·&nbsp; <span class="in-lib-count">${inLibCount} already in library</span>
            &nbsp;·&nbsp; ${toDownload.length} to download
        </div>`;

    const playlistToggleHtml = isPlaylist ? `
        <label class="import-playlist-toggle" for="importCreatePlaylist">
            <input type="checkbox" id="importCreatePlaylist" checked>
            Create Plex playlist after download
        </label>
    ` : '';

    const playlistNameHtml = isPlaylist ? `
        <div class="import-playlist-name-row" id="importPlaylistNameRow">
            <label style="font-size:0.875rem;color:var(--muted);">Playlist name:</label>
            <input type="text" id="importPlaylistNameInput" class="import-playlist-name-input"
                value="${escHtml(defaultName)}" placeholder="My Playlist">
        </div>
    ` : '';

    const downloadBtnLabel = toDownload.length > 0
        ? `Download ${toDownload.length} track${toDownload.length !== 1 ? 's' : ''}`
        : 'Nothing to download';

    trackArea.innerHTML = `
        ${heroHtml}
        <div class="import-actions">
            <button class="nav-btn accent-btn" id="importDownloadBtn"
                ${toDownload.length === 0 ? 'disabled' : ''}>
                ${escHtml(downloadBtnLabel)}
            </button>
            ${playlistToggleHtml}
        </div>
        ${playlistNameHtml}
        <div class="tracks-list" id="importTracksList"></div>
    `;

    // Toggle playlist name row visibility.
    const chk = document.getElementById('importCreatePlaylist');
    const nameRow = document.getElementById('importPlaylistNameRow');
    if (chk && nameRow) {
        nameRow.style.display = chk.checked ? '' : 'none';
        chk.addEventListener('change', () => {
            nameRow.style.display = chk.checked ? '' : 'none';
        });
    }

    // Render track rows.
    const list = document.getElementById('importTracksList');
    if (list) {
        const fallbackCover = '/static/cover-placeholder.svg';
        tracks.forEach((t, idx) => {
            const row = document.createElement('div');
            row.className = 'track-row with-cover';

            const title = t.name || t.title || '';
            const artist = t.artist || '';
            const inLib = !!t.in_library;

            const rawCover = t.cover_url || '';
            const proxyUrl = (rawCover && rawCover.startsWith('http'))
                ? `/api/image?u=${encodeURIComponent(rawCover)}`
                : '';
            const coverSrc = rawCover || fallbackCover;

            const badgeClass = inLib ? 'in-lib' : 'not-lib';
            const badgeLabel = inLib ? 'In Library' : 'Missing';

            row.innerHTML = `
                <div class="track-num">${idx + 1}</div>
                <div class="track-cover">
                    <img class="track-cover-img"
                        src="${coverSrc}"
                        alt="${escHtml(title)}"
                        data-proxy-src="${proxyUrl}"
                        data-fallback-src="${fallbackCover}"
                        data-tried-proxy="0"
                        onerror="handleImgError(this)">
                </div>
                <div class="track-main import-track-meta">
                    <div class="track-row-title import-track-name">${escHtml(title)}</div>
                    <div class="track-row-sub import-track-artist">${escHtml(artist)}</div>
                </div>
                <div class="track-status">
                    <span class="import-track-badge ${badgeClass}">${badgeLabel}</span>
                </div>
                <div class="track-actions"></div>
            `;
            list.appendChild(row);
        });
    }

    // Wire up download button.
    const dlBtn = document.getElementById('importDownloadBtn');
    if (dlBtn && toDownload.length > 0) {
        dlBtn.addEventListener('click', () => {
            const createPl = isPlaylist && chk && chk.checked;
            const plName = (createPl && document.getElementById('importPlaylistNameInput'))
                ? (document.getElementById('importPlaylistNameInput').value.trim() || defaultName)
                : defaultName;
            downloadImportItems(tracks, plName, createPl, data.cover_url || '');
        });
    }
}

async function downloadImportItems(tracks, playlistName, createPlexPlaylist, coverUrl = '') {
    const dlBtn = document.getElementById('importDownloadBtn');
    if (dlBtn) { dlBtn.disabled = true; dlBtn.textContent = 'Queuing…'; }

    try {
        const resp = await apiFetchJson('/api/import/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tracks,
                playlist_name: playlistName || '',
                create_plex_playlist: !!createPlexPlaylist,
                cover_url: coverUrl || '',
            }),
        });

        if (resp && resp.error) {
            showError(resp.error);
            if (dlBtn) { dlBtn.disabled = false; dlBtn.textContent = 'Download'; }
            return;
        }

        // Invalidate library cache so the next Library visit reflects new tracks.
        _cacheInvalidate('library');

        // Switch to Downloads tab to show progress.
        viewStack = [];
        setActiveNav('downloads');
        openDownloads();

    } catch (e) {
        showError('Failed to queue import: ' + String(e));
        if (dlBtn) { dlBtn.disabled = false; dlBtn.textContent = 'Download'; }
    }
}

// ── Scout (Shazam-like track identification) ─────────────────────────────────

let _scoutMediaRecorder = null;
let _scoutChunks = [];
let _scoutStopping = false;
let _scoutToken = null;

// Global entry-point called by the button's onclick attribute.
function scoutButtonTap() {
    startScoutListen(_scoutToken);
}

function openScout() {
    if (authRequired && !authAuthed) return;
    const token = beginView('scout');
    _scoutToken = token;

    setLoading(false);
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) resultsContainer.innerHTML = '';

    const viewHeader = document.getElementById('viewHeader');
    if (viewHeader) {
        viewHeader.style.display = 'none';
        viewHeader.innerHTML = '';
    }

    _renderScoutIdle(token);
    currentView = { kind: 'scout', state: null };
}

function _renderScoutIdle(token) {
    _scoutToken = token;
    const rc = document.getElementById('resultsContainer');
    if (!rc) return;

    rc.innerHTML = `
        <div class="scout-stage" id="scoutStage">
          <div class="scout-ring" id="scoutRing">
            <button class="scout-btn" id="scoutBtn" type="button"
                    aria-label="Tap to identify song"
                    onclick="scoutButtonTap()">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <path d="M19.07 4.93A10 10 0 0 0 6.99 3.34"/>
                <path d="M4 6h.01"/>
                <path d="M2.29 9.62A10 10 0 1 0 21.31 8.35"/>
                <path d="M16.24 7.76A6 6 0 1 0 8.23 16.67"/>
                <path d="M12 18h.01"/>
                <path d="M17.99 11.66A6 6 0 0 1 15.77 16.67"/>
                <circle cx="12" cy="12" r="2"/>
                <path d="m13.41 10.59 5.66-5.66"/>
              </svg>
            </button>
          </div>
          <div class="scout-label" id="scoutLabel">TAP TO IDENTIFY</div>
        </div>
    `;
}

async function startScoutListen(token) {
    const ringEl = document.getElementById('scoutRing');
    const btnEl = document.getElementById('scoutBtn');
    const labelEl = document.getElementById('scoutLabel');

    // Check MediaRecorder support
    if (!window.MediaRecorder || !navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        _renderScoutError(token, 'Microphone access is not supported in this browser.');
        return;
    }

    // Disable button and show guidance — calling getUserMedia IS the OS permission
    // prompt, so we tell the user to watch for it before we fire the request.
    if (btnEl) btnEl.disabled = true;
    if (labelEl) { labelEl.textContent = 'TAP ALLOW ON THE POPUP…'; }

    let stream;
    try {
        stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
    } catch (e) {
        if (btnEl) btnEl.disabled = false;
        if (e.name === 'NotAllowedError' || e.name === 'PermissionDeniedError') {
            _renderScoutMicBlocked(token);
        } else if (e.name === 'NotFoundError') {
            _renderScoutError(token, 'No microphone found on this device.');
        } else if (e.name === 'SecurityError') {
            _renderScoutError(token, 'A secure (HTTPS) connection is required to access the microphone.');
        } else {
            _renderScoutError(token, 'Could not access microphone: ' + e.name);
        }
        return;
    }

    if (!isActiveView('scout', token)) {
        stream.getTracks().forEach(t => t.stop());
        return;
    }

    // Transition to listening state
    if (ringEl) ringEl.classList.add('is-listening');
    if (btnEl) {
        btnEl.classList.add('is-listening');
        btnEl.disabled = false;
        btnEl.setAttribute('aria-label', 'Stop listening');
    }
    if (labelEl) { labelEl.textContent = 'LISTENING…'; labelEl.className = 'scout-label is-listening'; }

    // Show progress dots
    const stage = document.getElementById('scoutStage');
    if (stage) {
        const dots = document.createElement('div');
        dots.className = 'scout-dots';
        dots.innerHTML = '<span class="scout-dot"></span><span class="scout-dot"></span><span class="scout-dot"></span>';
        stage.appendChild(dots);
    }

    _scoutChunks = [];
    _scoutStopping = false;

    // Choose a supported MIME type
    const mimeType = (['audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus', 'audio/ogg', ''])
        .find(m => !m || MediaRecorder.isTypeSupported(m)) || '';
    const recOpts = mimeType ? { mimeType } : {};

    try {
        _scoutMediaRecorder = new MediaRecorder(stream, recOpts);
    } catch (e) {
        stream.getTracks().forEach(t => t.stop());
        _renderScoutError(token, 'Could not start recording: ' + String(e));
        return;
    }

    _scoutMediaRecorder.addEventListener('dataavailable', (e) => {
        if (e.data && e.data.size > 0) _scoutChunks.push(e.data);
    });

    _scoutMediaRecorder.addEventListener('stop', async () => {
        stream.getTracks().forEach(t => t.stop());
        if (!isActiveView('scout', token)) return;
        await _submitScoutAudio(token, _scoutChunks, mimeType || 'audio/webm');
    });

    _scoutMediaRecorder.start(250); // collect 250 ms chunks

    // Allow user to stop early by tapping the button again
    if (btnEl) {
        btnEl.onclick = () => {
            if (_scoutMediaRecorder && _scoutMediaRecorder.state === 'recording') {
                _scoutStopping = true;
                _scoutMediaRecorder.stop();
            }
        };
    }

    // Auto-stop after 12 seconds — more audio improves recognition accuracy
    setTimeout(() => {
        if (_scoutMediaRecorder && _scoutMediaRecorder.state === 'recording' && !_scoutStopping) {
            _scoutStopping = true;
            _scoutMediaRecorder.stop();
        }
    }, 12000);
}

async function _submitScoutAudio(token, chunks, mimeType) {
    if (!isActiveView('scout', token)) return;

    const ringEl = document.getElementById('scoutRing');
    const btnEl = document.getElementById('scoutBtn');
    const labelEl = document.getElementById('scoutLabel');

    if (ringEl) ringEl.classList.remove('is-listening');
    if (btnEl) { btnEl.classList.remove('is-listening'); btnEl.disabled = true; }
    if (labelEl) { labelEl.textContent = 'IDENTIFYING…'; labelEl.className = 'scout-label'; }

    if (!chunks || chunks.length === 0) {
        _renderScoutError(token, 'No audio captured. Try again.');
        return;
    }

    const blob = new Blob(chunks, { type: mimeType || 'audio/webm' });
    const formData = new FormData();
    formData.append('audio', blob, 'recording.webm');

    try {
        const resp = await fetch('/api/shazam', { method: 'POST', body: formData });
        if (!isActiveView('scout', token)) return;

        const data = await resp.json();
        if (!isActiveView('scout', token)) return;

        if (!resp.ok || data.error) {
            _renderScoutError(token, data.error || 'Recognition failed. Try again.');
            return;
        }

        if (!data.found) {
            _renderScoutNotFound(token);
            return;
        }

        _renderScoutResult(token, data);

    } catch (e) {
        if (!isActiveView('scout', token)) return;
        _renderScoutError(token, 'Connection error. Check your network and try again.');
    }
}

// Module-level result data so onclick= attributes on the result card can
// access it without fragile closures.
let _scoutResultData = null;

function scoutPreviewTap() {
    if (!_scoutResultData) return;
    const btn = document.getElementById('scoutPreviewBtn');
    togglePreview(
        _scoutResultData.artist || '',
        _scoutResultData.title || '',
        btn,
        _scoutResultData.cover_url || ''
    );
    // Icon is updated reactively by _watchScoutPlayBtn's MutationObserver
}

function scoutDownloadTap() {
    if (!_scoutResultData) return;
    const btn = document.getElementById('scoutDownloadBtn');
    downloadItem(_scoutResultData.artist || '', _scoutResultData.title || '', 'track', btn);
}

function _renderScoutResult(token, data) {
    if (!isActiveView('scout', token)) return;
    const rc = document.getElementById('resultsContainer');
    if (!rc) return;

    _scoutResultData = data;

    const title  = escapeHtml(data.title  || 'Unknown Title');
    const artist = escapeHtml(data.artist || 'Unknown Artist');
    const genre  = escapeHtml(data.genre  || '');
    const coverRaw = data.cover_url || '';
    const coverEsc = escapeHtml(coverRaw);

    const bgStyle    = coverRaw ? ` style="background-image:url('${coverEsc}')"` : '';
    const genrePill  = genre ? `<div class="scout-match-genre">${genre}</div>` : '';
    const coverTag   = coverRaw
        ? `<img class="scout-match-cover" src="${coverEsc}" alt="Album art"
                onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
        : '';

    rc.innerHTML = `
        <div class="scout-match" id="scoutResult">
          <div class="scout-match-bg"${bgStyle}></div>
          <div class="scout-match-inner">
            <div class="scout-match-cover-wrap">
              ${coverTag}
              <div class="scout-result-cover-placeholder"${coverRaw ? ' style="display:none"' : ''}>
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                  <path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>
                </svg>
              </div>
              <div class="scout-match-scan"></div>
            </div>
            <div class="scout-match-badge">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                   stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" width="14" height="14">
                <polyline points="20 6 9 17 4 12"/>
              </svg>
              MATCH FOUND
            </div>
            <div class="scout-match-title">${title}</div>
            <div class="scout-match-artist">${artist}</div>
            ${genrePill}
            <div class="scout-match-actions">
              <!-- Left: Scan Again -->
              <button class="scout-circle-btn scout-circle-btn--sm" type="button"
                      aria-label="Scan again" onclick="openScout()" title="Scan Again">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
                     stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74"/>
                  <polyline points="3 3 3 9 9 9"/>
                </svg>
              </button>
              <!-- Centre: Play/Preview -->
              <button class="scout-circle-btn scout-circle-btn--lg" id="scoutPreviewBtn" type="button"
                      aria-label="Preview" onclick="scoutPreviewTap()" title="Preview">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor"
                     stroke="none" aria-hidden="true">
                  <polygon points="6 3 20 12 6 21"/>
                </svg>
              </button>
              <!-- Right: Download -->
              <button class="scout-circle-btn scout-circle-btn--sm" id="scoutDownloadBtn" type="button"
                      aria-label="Download" onclick="scoutDownloadTap()" title="Download">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
                     stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                  <polyline points="7 10 12 15 17 10"/>
                  <line x1="12" y1="15" x2="12" y2="3"/>
                </svg>
              </button>
            </div>
          </div>
        </div>
    `;

    currentView = { kind: 'scout', state: { result: data } };

    // Reactive icon swap: observe data-state attribute set by setPreviewButtonState
    _watchScoutPlayBtn();
    // Async library check — green tick + disabled download if already owned
    _checkScoutLibrary(token);
}

function _watchScoutPlayBtn() {
    const btn = document.getElementById('scoutPreviewBtn');
    if (!btn) return;
    const playIcon  = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" stroke="none" aria-hidden="true"><polygon points="6 3 20 12 6 21"/></svg>`;
    const pauseIcon = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" stroke="none" aria-hidden="true"><rect x="5" y="4" width="4" height="16"/><rect x="15" y="4" width="4" height="16"/></svg>`;
    const loadIcon  = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="10" opacity=".25"/><path d="M12 2a10 10 0 0 1 10 10" style="animation:scout-spin .8s linear infinite;transform-origin:center"/></svg>`;
    const obs = new MutationObserver(() => {
        const s = btn.dataset.state;
        if (s === 'pause')   btn.innerHTML = pauseIcon;
        else if (s === 'loading') btn.innerHTML = loadIcon;
        else                  btn.innerHTML = playIcon;
    });
    obs.observe(btn, { attributes: true, attributeFilter: ['data-state'] });
}

async function _checkScoutLibrary(token) {
    if (!_scoutResultData) return;
    const { artist, title } = _scoutResultData;
    if (!artist || !title) return;
    try {
        const resp = await fetch(`/api/library/check?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(title)}`);
        if (!resp.ok || !isActiveView('scout', token)) return;
        const json = await resp.json();
        if (!isActiveView('scout', token) || !json.in_library) return;
        // Already owned — add green tick badge on the cover
        const wrap = document.querySelector('.scout-match-cover-wrap');
        if (wrap && !wrap.querySelector('.scout-inlib-tick')) {
            const tick = document.createElement('div');
            tick.className = 'scout-inlib-tick';
            tick.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`;
            wrap.appendChild(tick);
        }
        // Grey out and disable the download button
        const dlBtn = document.getElementById('scoutDownloadBtn');
        if (dlBtn) {
            dlBtn.disabled = true;
            dlBtn.title = 'Already in your library';
            dlBtn.setAttribute('aria-label', 'Already in your library');
        }
    } catch (_) { /* network error — silently skip */ }
}

function _renderScoutNotFound(token) {
    if (!isActiveView('scout', token)) return;
    const rc = document.getElementById('resultsContainer');
    if (!rc) return;

    rc.innerHTML = `
        <div class="scout-stage">
          <div class="scout-ring">
            <button class="scout-btn" type="button" aria-label="Try again" style="border-color:rgba(255,42,42,0.55);color:var(--state-error);background:radial-gradient(circle at 35% 30%,rgba(255,42,42,0.14),rgba(255,42,42,0.04) 60%),rgba(7,11,18,0.85);">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true">
                <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
              </svg>
            </button>
          </div>
          <div class="scout-label is-error">NOT RECOGNISED</div>
          <div style="font-size:0.82rem;color:var(--muted);text-align:center;">Make sure music is playing nearby and try again.</div>
          <button class="nav-btn nav-btn-primary" type="button" onclick="openScout()" style="margin-top:8px;">Try Again</button>
        </div>
    `;
    currentView = { kind: 'scout', state: null };
}

function _renderScoutError(token, message) {
    if (!isActiveView('scout', token)) return;
    const rc = document.getElementById('resultsContainer');
    if (!rc) return;

    rc.innerHTML = `
        <div class="scout-stage">
          <div class="scout-label is-error">${escapeHtml(String(message || 'Something went wrong'))}</div>
          <button class="nav-btn nav-btn-primary" type="button" onclick="openScout()" style="margin-top:8px;">Try Again</button>
        </div>
    `;
    currentView = { kind: 'scout', state: null };
}

function _renderScoutMicBlocked(token) {
    if (!isActiveView('scout', token)) return;
    const rc = document.getElementById('resultsContainer');
    if (!rc) return;

    // Detect platform so we can show the right settings path.
    const ua = navigator.userAgent || '';
    const isIOS = /iPad|iPhone|iPod/i.test(ua) && !window.MSStream;
    const isAndroid = /Android/i.test(ua);
    const isPWA = window.matchMedia('(display-mode: standalone)').matches ||
                  (window.navigator.standalone === true);

    let steps;
    if (isIOS && isPWA) {
        steps = `
            <li>Open your device <strong>Settings</strong></li>
            <li>Scroll down and tap <strong>SoundScout</strong></li>
            <li>Enable <strong>Microphone</strong></li>
            <li>Return here and tap <strong>Try Again</strong></li>`;
    } else if (isIOS) {
        steps = `
            <li>Open your device <strong>Settings</strong></li>
            <li>Tap <strong>Privacy &amp; Security &rarr; Microphone</strong></li>
            <li>Find your browser and switch it <strong>On</strong></li>
            <li>Return here and tap <strong>Try Again</strong></li>`;
    } else if (isAndroid && isPWA) {
        steps = `
            <li>Open your device <strong>Settings &rarr; Apps</strong></li>
            <li>Find and tap <strong>SoundScout</strong></li>
            <li>Tap <strong>Permissions &rarr; Microphone &rarr; Allow</strong></li>
            <li>Return here and tap <strong>Try Again</strong></li>`;
    } else if (isAndroid) {
        steps = `
            <li>Open your device <strong>Settings &rarr; Apps</strong></li>
            <li>Tap <strong>Chrome</strong> (or your browser)</li>
            <li>Tap <strong>Permissions &rarr; Microphone &rarr; Allow</strong></li>
            <li>Return here and tap <strong>Try Again</strong></li>`;
    } else {
        steps = `
            <li>Open your browser&apos;s <strong>Settings</strong></li>
            <li>Go to <strong>Privacy / Site Settings &rarr; Microphone</strong></li>
            <li>Allow this site to use the microphone</li>
            <li>Refresh the page and tap <strong>Try Again</strong></li>`;
    }

    rc.innerHTML = `
        <div class="scout-stage scout-stage--message">
          <div class="scout-mic-blocked">
            <div class="scout-mic-blocked__icon">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
                   stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                <line x1="2" y1="2" x2="22" y2="22"/>
                <path d="M18.89 13.23A7.12 7.12 0 0 0 19 12v-2"/>
                <path d="M5 10v2a7 7 0 0 0 12 5"/>
                <path d="M15 9.34V5a3 3 0 0 0-5.68-1.33"/>
                <path d="M9 9v3a3 3 0 0 0 5.12 2.12"/>
                <line x1="12" y1="19" x2="12" y2="22"/>
                <line x1="8" y1="22" x2="16" y2="22"/>
              </svg>
            </div>
            <p class="scout-mic-blocked__title">Microphone Blocked</p>
            <p class="scout-mic-blocked__body">
              Scout needs microphone access to identify songs.
              Enable it in your device settings:
            </p>
            <ol class="scout-mic-blocked__steps">${steps}</ol>
          </div>
          <button class="nav-btn nav-btn-primary" type="button" onclick="openScout()">Try Again</button>
        </div>
    `;
    currentView = { kind: 'scout', state: null };
}


