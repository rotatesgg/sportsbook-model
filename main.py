"""
Osirion Leaderboards — dense data interface for competitive Fortnite analysis.

Home page (/) is a betting-pipeline-grade leaderboard explorer powered by the
free Osirion public API at https://fnapi.osirion.gg. Per-team derived stats
(placement mean/std/CV, elims mean/std/CV, hit rates, VR rate, trend), event
metadata (scoring rules, payout, match cap), and an event-level distribution
panel are all exposed for downstream analysis.

The legacy paid-API explorer (which talks to api.osirion.gg/fortnite/v1) is
available at /explorer for manual debugging.
"""
from __future__ import annotations

import io
import hashlib
import json
import logging
import math
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import Config
from api.fortnite_client import FortniteAPIClient, API_ENDPOINTS
from api.public_client import (
    PublicFortniteAPIClient,
    PublicClientConfig,
    MARKET_CATALOG,
    REGIONS,
    STAT_LABELS,
    aggregate_markets_across_events,
    evaluate_markets,
    event_tier,
    extract_event_info,
    find_player_in_cached_entries,
    normalize_leaderboard_entries,
    scoring_rules_fingerprint,
    summarize_leaderboard_distribution,
    summarize_tournaments_response,
)
from api.pricing import (
    detect_pool_anomalies,
    enrich_rows_with_pricing,
    to_offer,
)
from api.predictions import (
    build_pedigree,
    build_team_forms,
    _pool_caps_for,
    qualifier_team_keys,
    qualifier_pool_breakdown,
    resolve_prediction_pool,
    team_key as _prediction_team_key,
)
# Lazy-loaded to avoid pulling pandas/openpyxl at startup (~150MB).
# from api.data_processor import DataProcessor
# from api.excel_exporter import ExcelExporter
from signals import (
    Signal as SignalRow,
    get_store as _get_signal_store,
    overlay_rev as _overlay_rev,
    write_overlay_entry as _write_overlay_entry,
    read_overlay as _read_overlay,
    extract_signal_fields as _extract_signal_fields,
)
from signals import runner as _signal_runner
from storage import (
    ping as _mongo_ping,
    builder_probs_upsert as _m_probs_upsert,
    builder_probs_get as _m_probs_get,
    builder_probs_list as _m_probs_list,
    event_calendar_cache_get as _m_event_cache_get,
    event_calendar_cache_put as _m_event_cache_put,
    qualification_workspace_get as _m_qualification_workspace_get,
    qualification_workspace_put as _m_qualification_workspace_put,
    dropmap_get as _m_dropmap_get,
    dropmap_put as _m_dropmap_put,
    approved_offers_get as _m_offers_get,
    approved_offers_put as _m_offers_put,
    overlay_bump_rev as _m_bump_rev,
    catalogue_profiles_upsert_many as _m_catalogue_upsert_many,
    catalogue_profile_get as _m_catalogue_profile_get,
    catalogue_profile_update as _m_catalogue_profile_update,
    catalogue_snapshot_get as _m_catalogue_snapshot_get,
    catalogue_snapshot_put as _m_catalogue_snapshot_put,
    wiki_event_snapshot_get as _wiki_event_snap_get,
    wiki_event_snapshot_upsert as _wiki_event_snap_upsert,
    wiki_player_snapshot_get as _wiki_player_snap_get,
    wiki_player_snapshot_upsert as _wiki_player_snap_upsert,
    wiki_event_directory_bulk_upsert as _wiki_dir_bulk_upsert,
    wiki_event_directory_find as _wiki_dir_find,
    wiki_event_directory_count as _wiki_dir_count,
    wiki_event_directory_clear as _wiki_dir_clear,
    catalogue_builder_weights_get as _m_catalogue_builder_weights_get,
    catalogue_builder_weights_put as _m_catalogue_builder_weights_put,
    builder_snapshot_put as _m_builder_snapshot_put,
    builder_snapshots_list as _m_builder_snapshots_list,
    builder_snapshot_get as _m_builder_snapshot_get,
)
# Lazy-loaded to avoid pulling numpy into memory at startup on small dynos.
# Imported on first use inside _run_builder_points_simulation().
# from pricing.bookmaker import build_market_offer_pack
# from simulation.points_simulator import (
#     PointsSimulationConfig,
#     build_team_inputs_from_builder_rows,
#     run_points_event,
# )

ROOT = Path(__file__).parent
EXPORTS_DIR = ROOT / "exports"
PUBLIC_CACHE_DIR = EXPORTS_DIR / "public"
PUBLIC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATES_DIR = ROOT / "templates"


def _read_template(name: str) -> str:
    try:
        return (TEMPLATES_DIR / name).read_text(encoding="utf-8")
    except Exception as e:
        return f"<h1>Template load error: {e}</h1>"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

paid_client = FortniteAPIClient()
# Lazy-loaded on first use to avoid importing pandas/openpyxl at startup.
_processor = None
_exporter = None

def _get_processor():
    global _processor
    if _processor is None:
        from api.data_processor import DataProcessor
        _processor = DataProcessor()
    return _processor

def _get_exporter():
    global _exporter
    if _exporter is None:
        from api.excel_exporter import ExcelExporter
        _exporter = ExcelExporter()
    return _exporter

public_client = PublicFortniteAPIClient(
    PublicClientConfig(
        base_url=Config.PUBLIC_API_BASE_URL,
        rate_limit_rpm=Config.PUBLIC_API_RATE_LIMIT_RPM,
    )
)

_leaderboard_cache: Dict[str, Dict[str, Any]] = {}
_tournaments_cache: Dict[str, Dict[str, Any]] = {}
_tournaments_raw_cache: Dict[str, Dict[str, Any]] = {}  # region -> raw /v1/tournaments
paid_cache: Dict[str, Any] = {}
BUILDER_ONLY = True

_LEADERBOARD_CACHE_MAX = 10
_TOURNAMENTS_CACHE_MAX = 20
_PAID_CACHE_MAX = 5


@asynccontextmanager
async def lifespan(app: FastAPI):
    _entry = "/" if REACT_BUILDER_DIST.exists() else "/builder/"
    logger.info(
        f"Rating Builder starting on http://{Config.HOST}:{Config.PORT} "
        f"(open http://127.0.0.1:{Config.PORT}{_entry} for the app)"
    )
    logger.info(f"Public API base: {Config.PUBLIC_API_BASE_URL}")
    if BUILDER_ONLY:
        logger.info("Builder-only mode: skipping lab Mongo health check and signal pollers")
        yield
        await public_client.close()
        return
    # Confirm MongoDB is reachable so any file-system fallbacks can be
    # caught quickly on startup rather than surfacing mid-request.
    try:
        status = _mongo_ping()
        if status.get("ok"):
            logger.info(f"MongoDB connected at {status.get('uri')} (db={status.get('db')})")
        else:
            logger.warning(f"MongoDB ping failed: {status.get('error')}")
    except Exception as e:
        logger.warning(f"MongoDB health check failed: {e}")
    # Warm the signal extractor's subject index from cached snapshots, then
    # spawn any enabled source pollers. Failures in signal startup must not
    # take the server down.
    try:
        _signal_runner.refresh_subject_index()
        await _signal_runner.start()
    except Exception as e:
        logger.warning(f"signal runner failed to start: {e}")
    yield
    try:
        await _signal_runner.stop()
    except Exception as e:
        logger.warning(f"signal runner failed to stop cleanly: {e}")
    await public_client.close()


app = FastAPI(title="Forecast Rating Builder", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Phase 0 scoreboard endpoints. Imported lazily so scientific deps
# (numpy / scipy / sklearn) only load when the router itself does.
# In BUILDER_ONLY mode, skip entirely to save ~160MB of RAM on small dynos.
if not BUILDER_ONLY:
    try:
        from api.routes_backtest import router as _backtest_router
        app.include_router(_backtest_router)
    except Exception as _e:  # pragma: no cover
        logger.warning(f"backtest router failed to load: {_e}")

# ---------------------------------------------------------------------------
# Mount the React Rating Builder at /builder when a Vite build exists.
# Keep the legacy static HTML builder at /builder-legacy for quick rollback.
# ---------------------------------------------------------------------------
REACT_BUILDER_DIST = ROOT / "builder-react" / "dist"
# When the Vite build exists, we also mount it at / (end of file) so the client
# loads on http://127.0.0.1:PORT/ — same origin as /api without a separate dev server.
_BUILDER_CANDIDATES = [
    ROOT / "webapp",
    Path(r"C:/Users/sorry/sportsbook model/webapp"),
]
BUILDER_DIR: Optional[Path] = next((p for p in _BUILDER_CANDIDATES if p.exists()), None)
if BUILDER_DIR is not None:
    app.mount(
        "/builder-legacy",
        StaticFiles(directory=str(BUILDER_DIR), html=True),
        name="builder-legacy",
    )
    logger.info(f"Mounted legacy Rating Builder at /builder-legacy from {BUILDER_DIR}")
else:
    logger.warning(
        "Legacy Rating Builder directory not found; /builder-legacy will 404. "
        f"Tried: {[str(p) for p in _BUILDER_CANDIDATES]}"
    )

_LEGACY_BUILDER_ROUTES = {
    "tools.html": "sandbox",
    "index.html": "variables",
    "builder.html": "builder",
    "distributions.html": "distributions",
    "testing.html": "testing",
    "sandbox": "sandbox",
    "variables": "variables",
    "builder": "builder/builder",
    "distributions": "distributions",
    "testing": "testing",
    "catalogue": "catalogue",
    "wiki": "wiki",
}


@app.get("/builder/{legacy_page}")
async def react_builder_legacy_redirect(legacy_page: str, request: Request):
    """Redirect old Builder HTML URLs to the equivalent React hash route."""
    route = _LEGACY_BUILDER_ROUTES.get(legacy_page)
    if not route:
        raise HTTPException(status_code=404, detail="Not Found")
    query = f"?{request.url.query}" if request.url.query else ""
    if REACT_BUILDER_DIST.exists():
        dest = f"/{query}#/{route}" if query else f"/#/{route}"
    else:
        dest = f"/builder/{query}#/{route}" if query else f"/builder/#/{route}"
    return RedirectResponse(url=dest, status_code=307)

if REACT_BUILDER_DIST.exists():
    app.mount(
        "/builder",
        StaticFiles(directory=str(REACT_BUILDER_DIST), html=True),
        name="builder",
    )
    logger.info(f"Mounted React Rating Builder at /builder from {REACT_BUILDER_DIST}")
elif BUILDER_DIR is not None:
    app.mount(
        "/builder",
        StaticFiles(directory=str(BUILDER_DIR), html=True),
        name="builder-fallback",
    )
    logger.warning("React Builder build missing; mounted legacy Rating Builder at /builder as fallback")


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
def _now() -> float:
    return time.time()


def _cache_get(store: Dict[str, Dict[str, Any]], key: str, ttl: int) -> Optional[Any]:
    entry = store.get(key)
    if not entry:
        return None
    if _now() - entry["ts"] > ttl:
        return None
    return entry["value"]


def _cache_set(store: Dict[str, Dict[str, Any]], key: str, value: Any, max_size: int = 0) -> None:
    store[key] = {"ts": _now(), "value": value}
    if max_size and len(store) > max_size:
        oldest_key = min(store, key=lambda k: store[k]["ts"])
        store.pop(oldest_key, None)


def _disk_cache_path(event_id: str, window_id: str, max_pages: int) -> Path:
    safe = f"{event_id}__{window_id}__p{max_pages}".replace("/", "_")
    return PUBLIC_CACHE_DIR / f"{safe}.json"


def _disk_cache_read(path: Path, ttl: int) -> Optional[Any]:
    try:
        if not path.exists():
            return None
        age = _now() - path.stat().st_mtime
        if age > ttl:
            return None
        with open(path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception as e:
        logger.warning(f"disk cache read failed for {path.name}: {e}")
        return None


def _disk_cache_write(path: Path, value: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(value, fp)
    except Exception as e:
        logger.warning(f"disk cache write failed for {path.name}: {e}")


async def _get_tournaments_raw(region: str, include_historic: bool = False) -> Dict[str, Any]:
    """
    Fetch /v1/tournaments for a region with memory + Mongo caching.
    Calendar metadata changes slowly, so we persist it and only hit the public
    API when the stored copy is stale. Leaderboards/results are still fetched
    separately when qualification or settlement needs fresh data.
    """
    key = f"{region}|{int(include_historic)}"
    cached = _cache_get(_tournaments_raw_cache, key, ttl=600)
    if cached is not None:
        return cached

    try:
        cached_doc = _m_event_cache_get(key)
        if cached_doc:
            updated_raw = cached_doc.get("updatedAt")
            try:
                updated_at = datetime.fromisoformat(str(updated_raw).replace("Z", "+00:00"))
                ttl_seconds = 6 * 60 * 60 if not include_historic else 24 * 60 * 60
                if (datetime.now(timezone.utc) - updated_at).total_seconds() < ttl_seconds:
                    payload = cached_doc.get("payload") or {}
                    if payload:
                        _cache_set(_tournaments_raw_cache, key, payload, _TOURNAMENTS_CACHE_MAX)
                        return payload
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[event-cache] mongo read skipped for {key}: {e}")

    payload = await public_client.list_tournaments(
        region=region, include_historic=include_historic
    )
    _cache_set(_tournaments_raw_cache, key, payload, _TOURNAMENTS_CACHE_MAX)
    try:
        _m_event_cache_put(key, payload)
    except Exception as e:
        logger.debug(f"[event-cache] mongo write skipped for {key}: {e}")
    return payload


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
def _builder_spa_url() -> str:
    """SPA entry when React dist exists (served at /); otherwise legacy /builder/."""
    return "/" if REACT_BUILDER_DIST.exists() else "/builder/"


if not REACT_BUILDER_DIST.exists():
    @app.get("/")
    async def builder_home_redirect():
        """No root static mount yet — send users to /builder/ (React or legacy)."""
        return RedirectResponse(url="/builder/", status_code=307)


@app.get("/lab", response_class=HTMLResponse)
async def leaderboard_home():
    return RedirectResponse(url=_builder_spa_url(), status_code=307)


@app.get("/explorer", response_class=HTMLResponse)
async def explorer_home():
    return RedirectResponse(url=_builder_spa_url(), status_code=307)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "publicApi": Config.PUBLIC_API_BASE_URL, "port": Config.PORT}


# ---------------------------------------------------------------------------
# Public-API-backed endpoints
# ---------------------------------------------------------------------------
@app.get("/api/regions")
async def api_regions():
    return {"regions": REGIONS, "statLabels": STAT_LABELS}


@app.get("/api/events")
async def api_events(
    region: Optional[str] = Query(None),
    includeHistoric: bool = Query(False),
    force: bool = Query(False),
):
    """
    List selectable tournament windows from the public API, optionally
    filtered by region. Cached in memory for 10 minutes.
    """
    if region and region not in REGIONS:
        raise HTTPException(400, f"Unknown region '{region}'. Allowed: {REGIONS}")

    key = f"{region or 'ALL'}|{int(includeHistoric)}"
    if not force:
        cached = _cache_get(_tournaments_cache, key, ttl=600)
        if cached is not None:
            return cached

    try:
        regions_to_pull = [region] if region else REGIONS
        all_events: List[Dict[str, Any]] = []
        seen = set()
        for r in regions_to_pull:
            try:
                payload = await _get_tournaments_raw(r, include_historic=includeHistoric)
                rows = summarize_tournaments_response(payload)
                for row in rows:
                    k = (row["eventId"], row["windowId"])
                    if k in seen:
                        continue
                    seen.add(k)
                    all_events.append(row)
            except Exception as e:
                logger.warning(f"list_tournaments({r}) failed: {e}")

        value = {"region": region, "events": all_events, "total": len(all_events)}
        _cache_set(_tournaments_cache, key, value, _TOURNAMENTS_CACHE_MAX)
        return value
    except Exception as e:
        logger.exception("events fetch failed")
        raise HTTPException(502, f"Public API error: {e}")


@app.get("/api/event-info")
async def api_event_info(
    eventId: str = Query(..., description="leaderboardEventId"),
    windowId: str = Query(..., description="leaderboardEventWindowId"),
    region: Optional[str] = Query(None, description="Hint for the right tournaments payload"),
    force: bool = Query(False),
):
    """
    Resolve event metadata for (eventId, windowId): scoring rules, payout
    table, match cap, platforms, round label, timing, lock types, etc.
    Pulls from the cached /v1/tournaments payload per region.
    """
    regions_to_try: List[str] = [region] if region else REGIONS
    for r in regions_to_try:
        try:
            if force:
                _tournaments_raw_cache.pop(f"{r}|0", None)
                _tournaments_raw_cache.pop(f"{r}|1", None)
            payload = await _get_tournaments_raw(r, include_historic=False)
            info = extract_event_info(
                payload,
                leaderboard_event_id=eventId,
                leaderboard_window_id=windowId,
            )
            if info:
                return info
            # Also try historic as a fallback
            payload_h = await _get_tournaments_raw(r, include_historic=True)
            info_h = extract_event_info(
                payload_h,
                leaderboard_event_id=eventId,
                leaderboard_window_id=windowId,
            )
            if info_h:
                return info_h
        except Exception as e:
            logger.warning(f"event-info({r}) failed: {e}")
    raise HTTPException(404, f"No tournament found matching {eventId}/{windowId}")


@app.get("/api/leaderboard")
async def api_leaderboard(
    eventId: str = Query(..., description="leaderboardEventId"),
    windowId: str = Query(..., description="leaderboardEventWindowId"),
    pages: int = Query(5, ge=1, le=100, description="Number of leaderboard pages (100 entries each)"),
    minSessions: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="player name substring filter"),
    force: bool = Query(False),
    includeSeries: bool = Query(
        True,
        description="Include per-session series (allows row-expand UI). Disable for smaller responses.",
    ),
):
    """
    Fetch tournament leaderboard with per-team derived stats and event-level
    distribution. Disk + memory cache with a 5-minute TTL.
    """
    cache_key = f"{eventId}|{windowId}|{pages}"
    disk_path = _disk_cache_path(eventId, windowId, pages)

    payload = None
    if not force:
        payload = _cache_get(_leaderboard_cache, cache_key, Config.LEADERBOARD_TTL_SECONDS)
        if payload is None:
            payload = _disk_cache_read(disk_path, Config.LEADERBOARD_TTL_SECONDS)
            if payload is not None:
                _cache_set(_leaderboard_cache, cache_key, payload, _LEADERBOARD_CACHE_MAX)

    if payload is None:
        try:
            data = await public_client.get_full_leaderboard(eventId, windowId, max_pages=pages)
        except Exception as e:
            logger.exception("leaderboard fetch failed")
            raise HTTPException(502, f"Public API error: {e}")

        lb = data.get("leaderboard") or {}
        entries = normalize_leaderboard_entries(lb.get("entries") or [])
        distribution = summarize_leaderboard_distribution(entries)

        payload = {
            "eventId": eventId,
            "windowId": windowId,
            "updatedAt": lb.get("updatedAt"),
            "fetchedAt": _now(),
            "totalPages": lb.get("totalPages"),
            "pagesFetched": lb.get("pagesFetched", 1),
            "entries": entries,
            "totalEntries": len(entries),
            "distribution": distribution,
            "statLabels": STAT_LABELS,
        }
        _cache_set(_leaderboard_cache, cache_key, payload, _LEADERBOARD_CACHE_MAX)
        _disk_cache_write(disk_path, payload)

    entries = payload["entries"]
    if minSessions > 0:
        entries = [e for e in entries if (e.get("sessions") or 0) >= minSessions]
    if search:
        q = search.lower().strip()
        entries = [
            e
            for e in entries
            if any(q in (n or "").lower() for n in e.get("playerList") or [])
        ]

    if not includeSeries:
        entries = [
            {k: v for k, v in e.items() if k not in ("sessionHistory", "derived")}
            for e in entries
        ]

    return {
        **{k: v for k, v in payload.items() if k != "entries"},
        "entries": entries,
        "shown": len(entries),
    }


@app.get("/api/leaderboard/raw")
async def api_leaderboard_raw(
    eventId: str = Query(...),
    windowId: str = Query(...),
    page: int = Query(0, ge=0, description="Specific raw page to fetch (100 entries)"),
):
    """
    Returns the unmodified /v1/tournaments/leaderboard response for a single
    page — intended for JSON inspection in the UI.
    """
    try:
        return await public_client.get_leaderboard_page(eventId, windowId, page)
    except Exception as e:
        logger.exception("raw leaderboard fetch failed")
        raise HTTPException(502, f"Public API error: {e}")


@app.get("/api/player-lookup")
async def api_player_lookup(
    q: str = Query(..., min_length=2, description="Player name substring or accountId"),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Scan every on-disk cached leaderboard snapshot and return every team
    appearance where any player matches the query. Lets you see how a player
    has performed across every event you've already fetched.
    """
    q = q.strip()
    if not q:
        return {"query": q, "results": [], "cachedSnapshots": 0}

    snapshots: List[Tuple[Dict[str, Any], List[dict]]] = []
    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            meta = {
                "eventId": data.get("eventId"),
                "windowId": data.get("windowId"),
                "updatedAt": data.get("updatedAt"),
                "fetchedAt": data.get("fetchedAt"),
                "cacheFile": path.name,
            }
            snapshots.append((meta, data.get("entries") or []))
        except Exception as e:
            logger.warning(f"player-lookup: couldn't read {path.name}: {e}")

    results = find_player_in_cached_entries(snapshots, q)
    return {
        "query": q,
        "cachedSnapshots": len(snapshots),
        "totalAppearances": len(results),
        "results": results[:limit],
    }


@app.get("/api/cache/list")
async def api_cache_list():
    """Return a list of on-disk leaderboard snapshots available for lookup."""
    files = []
    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            files.append(
                {
                    "file": path.name,
                    "eventId": data.get("eventId"),
                    "windowId": data.get("windowId"),
                    "updatedAt": data.get("updatedAt"),
                    "fetchedAt": data.get("fetchedAt"),
                    "totalEntries": data.get("totalEntries"),
                    "sizeBytes": path.stat().st_size,
                }
            )
        except Exception:
            continue
    return {"count": len(files), "files": files}


def _catalogue_clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _catalogue_score(value: float) -> float:
    return round(_catalogue_clamp(value, -100.0, 100.0), 1)


def _catalogue_score_from_0_1(value: float) -> float:
    return _catalogue_score(_catalogue_clamp(value, 0.0, 1.0) * 200.0 - 100.0)


def _catalogue_score_from_0_100(value: float) -> float:
    return _catalogue_score(_catalogue_clamp(value, 0.0, 100.0) * 2.0 - 100.0)


def _catalogue_safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return fallback
        return float(value)
    except Exception:
        return fallback


def _catalogue_safe_int(value: Any, fallback: int = 0) -> int:
    try:
        if value in (None, ""):
            return fallback
        return int(float(value))
    except Exception:
        return fallback


def _catalogue_slug(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-") or "unknown"


def _catalogue_region_matches(snapshot: Dict[str, Any], region: str) -> bool:
    region_key = (region or "").upper()
    snap_region = str(snapshot.get("region") or "").upper()
    event_id = str(snapshot.get("eventId") or "")
    window_id = str(snapshot.get("windowId") or "")
    return snap_region == region_key or event_id.endswith(f"_{region_key}") or window_id.endswith(f"_{region_key}")


def _catalogue_parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _catalogue_recency_weight(value: Any, anchor: datetime) -> float:
    ts = _catalogue_parse_ts(value)
    if not ts:
        return 0.35
    age_days = max(0.0, (anchor - ts).total_seconds() / 86400.0)
    return 0.5 ** (age_days / 60.0)


def _catalogue_player_name(names: Any, index: int, account_id: str) -> str:
    if isinstance(names, list) and index < len(names) and str(names[index]).strip():
        return str(names[index]).strip()
    if isinstance(names, str) and names.strip():
        parts = [p.strip() for p in names.replace(",", " / ").split("/") if p.strip()]
        if index < len(parts):
            return parts[index]
    return account_id[:8] if account_id else "Unknown player"


def _catalogue_event_label(event_id: str) -> str:
    return event_id.replace("epicgames_", "").replace("_", " ") or "Unknown event"


def _catalogue_delta(variable: str, score_delta: float) -> Dict[str, Any]:
    score_delta = int(round(score_delta))
    return {
        "variable": variable,
        "scoreDelta": score_delta,
        "percent": int(round(score_delta / 2.0)),
    }


_CATALOGUE_VARIABLE_DEFS: List[Tuple[str, str, str]] = [
    ("v1",  "Points / match",            "form"),
    ("v2",  "Elim rate",                 "form"),
    ("v3",  "Victory Royale rate",       "form"),
    ("v4",  "Rank percentile",           "form"),
    ("v5",  "Consistency (1 - CV)",      "form"),
    ("v6",  "Placement trend",           "form"),
    ("v7",  "Elim trend",                "form"),
    ("v8",  "Best single-event rank",    "form"),
    ("v9",  "Qualifier cumulative pts",  "form"),
    ("v10", "Qualifier matches played",  "volume"),
    ("v11", "Events participated in",    "volume"),
    ("v12", "Total elims in qualifier",  "volume"),
    ("v13", "Total VRs in qualifier",    "volume"),
    ("v14", "Placement stdev",           "risk"),
    ("v15", "Pts/match stdev",           "risk"),
    ("v16", "Top-3 game rate",           "risk"),
    ("v17", "Top-10 game rate",          "risk"),
    ("v18", "Pts-floor (worst event)",   "risk"),
    ("v19", "FNCS pedigree score",       "pedigree"),
    ("v20", "Grand-final appearances",   "pedigree"),
    ("v21", "Major top-10 finishes",     "pedigree"),
    ("v22", "Div-1 top-10 finishes",     "pedigree"),
    ("v23", "Best career finish",        "pedigree"),
    ("v24", "Roster stability",          "roster"),
    ("v25", "Roster size (duo/trio)",    "roster"),
    ("v26", "Recency (days since play)", "roster"),
    ("v27", "Community power rank",      "curated"),
    ("v28", "Expert win probability %",  "curated"),
    ("v29", "Analyst tier",              "curated"),
    ("v30", "Clutch reputation",         "curated"),
    ("v31", "Landing spot quality",      "external"),
    ("v32", "Landing contest risk",      "external"),
    ("v33", "Injury / absence flag",     "external"),
    ("v34", "Scrim / ranked form (7d)",  "external"),
    ("v35", "LAN experience",            "external"),
    ("v36", "POI meta fit (patch)",      "external"),
]

_CATALOGUE_VARIABLE_NAMES = [name for _, name, _ in _CATALOGUE_VARIABLE_DEFS]


def _catalogue_fold(value: str) -> str:
    s = (value or "").lower()
    unfold = {
        "\u0430": "a", "\u0435": "e", "\u043e": "o", "\u0440": "p",
        "\u0441": "c", "\u0445": "x", "\u0443": "y", "\u0456": "i",
        "\u0451": "e", "\u043a": "k", "\u0455": "s",
    }
    for src, dst in unfold.items():
        s = s.replace(src, dst)
    return s


_CATALOGUE_CURATED_STRENGTH: Dict[str, float] = {
    "peterbot": 100, "pollo": 96, "polla": 96, "clix": 92, "higgs": 88,
    "th0mas": 88, "thomashd": 88, "bugha": 84, "khanada": 82, "mero": 82,
    "eomzo": 82, "epikwhale": 78, "reet": 78, "acorn": 78, "cold": 76,
    "rapid": 76, "muz": 76, "dubs": 74, "death": 74, "sphinx": 72,
    "cooper": 72, "scented": 70, "cented": 70, "avery": 70, "vivid": 70,
    "cam": 70, "ritual": 68, "dukez": 68, "sprite": 68, "ajerss": 68,
    "edgey": 68, "xen": 66, "xeno": 66, "rise": 66, "boltz": 66,
    "tayson": 92, "malibuca": 88, "pinq": 86, "queasy": 86, "veno": 86,
    "merstach": 84, "mrsavage": 82, "jannisz": 82, "chapix": 78,
    "andilex": 78, "anas": 78, "k1nzell": 76, "swizzY": 76, "swizzy": 76,
    "benjyfishy": 74, "mitr0": 74, "mongraal": 72, "setty": 72,
}


def _catalogue_curated_strength(name: str) -> float:
    folded = _catalogue_fold(name)
    best = 0.0
    for needle, strength in _CATALOGUE_CURATED_STRENGTH.items():
        if needle.lower() in folded:
            best = max(best, float(strength))
    return best


def _catalogue_event_weight(event_id: str) -> float:
    text = event_id.lower()
    if "fncs" in text and ("final" in text or "grand" in text):
        return 1.45
    if "fncs" in text and ("heat" in text or "lastchance" in text or "major" in text):
        return 1.25
    if "fncs" in text:
        return 1.1
    if "cash" in text:
        return 0.82
    if "solo" in text or "victory" in text:
        return 0.78
    return 0.9


def _catalogue_event_key(impact: Dict[str, Any]) -> str:
    return "|".join(
        str(impact.get(key) or "")
        for key in ("eventId", "windowId", "updatedAt")
    )


def _catalogue_distribution(default_scores: Dict[str, Any], placements: int = 200) -> List[float]:
    neutral = (_catalogue_safe_float(default_scores.get("Rank percentile"), 0.0) + 100.0) / 2.0
    upside = (_catalogue_safe_float(default_scores.get("Best single-event rank"), 0.0) + 100.0) / 2.0
    consistency = (_catalogue_safe_float(default_scores.get("Consistency (1 - CV)"), 0.0) + 100.0) / 2.0
    outlier = (_catalogue_safe_float(default_scores.get("Clutch reputation"), 0.0) + 100.0) / 2.0
    recent = (_catalogue_safe_float(default_scores.get("Placement trend"), default_scores.get("Rank percentile", 0.0)) + 100.0) / 2.0

    mode = _catalogue_clamp(placements - (neutral / 100.0) * (placements - 4), 1.0, float(placements))
    mode -= (recent - neutral) * 0.18
    mode = _catalogue_clamp(mode, 1.0, float(placements))
    spread = _catalogue_clamp(7.0 + (100.0 - consistency) * 0.18 + outlier * 0.08, 4.0, 44.0)
    pop_mode = _catalogue_clamp(8.0 + (100.0 - max(upside, neutral)) * 0.45, 1.0, float(placements))
    pop_weight = _catalogue_clamp((outlier / 240.0 + max(0.0, upside - neutral) / 320.0) * (neutral / 100.0), 0.0, 0.32)

    probs: List[float] = []
    for placement in range(1, placements + 1):
        primary = math.exp(-0.5 * ((placement - mode) / spread) ** 2)
        pop = math.exp(-0.5 * ((placement - pop_mode) / max(4.0, spread * 0.55)) ** 2)
        probs.append(max(0.0, (1.0 - pop_weight) * primary + pop_weight * pop))
    total = sum(probs) or 1.0
    return [p / total for p in probs]


def _catalogue_builder_weight(weights: Dict[str, float], category: str, variable: str) -> float:
    """Return Builder-style weight (-100..100) for a catalogue parameter/variable."""
    if not weights:
        return 0.0
    keyed_names = (f"{category}|{variable}", f"{category}:{variable}")
    for key in keyed_names:
        if key in weights:
            return _catalogue_clamp(_catalogue_safe_float(weights.get(key), 0.0), -100.0, 100.0)
    # Compatibility with the earlier one-weight-per-variable multiplier format.
    if variable in weights:
        legacy = _catalogue_safe_float(weights.get(variable), 1.0)
        return _catalogue_clamp((legacy - 1.0) * 50.0, -100.0, 100.0)
    return 0.0


def _catalogue_parameter_signal(default_scores: Dict[str, Any], weights: Dict[str, float], category: str) -> float:
    if not weights:
        return 0.0
    weighted = 0.0
    total = 0.0
    for variable, score in (default_scores or {}).items():
        weight = _catalogue_builder_weight(weights, category, str(variable))
        if abs(weight) < 1e-9:
            continue
        weighted += _catalogue_safe_float(score, 0.0) * (weight / 100.0)
        total += abs(weight) / 100.0
    if total <= 0.0:
        return 0.0
    return _catalogue_clamp(weighted / total, -100.0, 100.0)


def _catalogue_weighted_distribution(
    default_scores: Dict[str, Any],
    builder_weights: Dict[str, float],
    placements: int = 200,
) -> List[float]:
    """Distribution with catalogue Builder parameter weights applied.

    The UI mirrors Builder's parameter model, but catalogue curves are placement
    based. These signals nudge the same shape controls used by the neutral graph.
    """
    if not builder_weights:
        return _catalogue_distribution(default_scores, placements)

    neutral = (_catalogue_safe_float(default_scores.get("Rank percentile"), 0.0) + 100.0) / 2.0
    upside = (_catalogue_safe_float(default_scores.get("Best single-event rank"), 0.0) + 100.0) / 2.0
    consistency = (_catalogue_safe_float(default_scores.get("Consistency (1 - CV)"), 0.0) + 100.0) / 2.0
    outlier = (_catalogue_safe_float(default_scores.get("Clutch reputation"), 0.0) + 100.0) / 2.0
    recent = (_catalogue_safe_float(default_scores.get("Placement trend"), default_scores.get("Rank percentile", 0.0)) + 100.0) / 2.0

    mode_sig = _catalogue_parameter_signal(default_scores, builder_weights, "mode")
    spread_sig = _catalogue_parameter_signal(default_scores, builder_weights, "spread")
    upper_sig = _catalogue_parameter_signal(default_scores, builder_weights, "upperSkew")
    lower_sig = _catalogue_parameter_signal(default_scores, builder_weights, "lowerSkew")
    kurtosis_sig = _catalogue_parameter_signal(default_scores, builder_weights, "kurtosis")
    bimodal_sig = _catalogue_parameter_signal(default_scores, builder_weights, "bimodalStrength")
    mode2_sig = _catalogue_parameter_signal(default_scores, builder_weights, "mode2")

    mode = _catalogue_clamp(placements - (neutral / 100.0) * (placements - 4), 1.0, float(placements))
    mode -= (recent - neutral) * 0.18
    mode -= mode_sig * 0.32
    mode += lower_sig * 0.12
    mode = _catalogue_clamp(mode, 1.0, float(placements))

    spread = _catalogue_clamp(
        7.0
        + (100.0 - consistency) * 0.18
        + outlier * 0.08
        + spread_sig * 0.13
        + abs(kurtosis_sig) * 0.07,
        4.0,
        52.0,
    )
    pop_mode = _catalogue_clamp(
        8.0 + (100.0 - max(upside, neutral)) * 0.45 - mode2_sig * 0.18,
        1.0,
        float(placements),
    )
    pop_weight = _catalogue_clamp(
        (outlier / 240.0 + max(0.0, upside - neutral) / 320.0) * (neutral / 100.0)
        + upper_sig / 950.0
        + bimodal_sig / 1100.0,
        0.0,
        0.42,
    )
    bad_tail_mode = _catalogue_clamp(placements - 18.0 + lower_sig * 0.10, 1.0, float(placements))
    bad_tail_weight = _catalogue_clamp(max(0.0, lower_sig) / 1400.0 + max(0.0, kurtosis_sig) / 2400.0, 0.0, 0.12)

    probs: List[float] = []
    for placement in range(1, placements + 1):
        primary = math.exp(-0.5 * ((placement - mode) / spread) ** 2)
        pop = math.exp(-0.5 * ((placement - pop_mode) / max(4.0, spread * 0.55)) ** 2)
        bad_tail = math.exp(-0.5 * ((placement - bad_tail_mode) / max(8.0, spread * 0.7)) ** 2)
        probs.append(max(0.0, (1.0 - pop_weight - bad_tail_weight) * primary + pop_weight * pop + bad_tail_weight * bad_tail))
    total = sum(probs) or 1.0
    return [p / total for p in probs]


def _catalogue_outlier_distribution(chart_rank: Any, baseline_score: Any, placements: int = 200) -> List[float]:
    """Achiever overlays represent a likely surprise top-100/200 finish, not
    a player-specific win projection.
    """
    slot = _catalogue_clamp(_catalogue_safe_float(chart_rank, 150.0), 90.0, float(placements))
    score = _catalogue_clamp(_catalogue_safe_float(baseline_score, 50.0), 0.0, 100.0)
    main_spread = _catalogue_clamp(28.0 + (100.0 - score) * 0.16, 24.0, 48.0)
    breakthrough_mode = _catalogue_clamp(32.0 - score * 0.18, 10.0, 34.0)
    breakthrough_spread = _catalogue_clamp(13.0 + (100.0 - score) * 0.06, 10.0, 20.0)
    breakthrough_weight = _catalogue_clamp(0.035 + score / 900.0, 0.035, 0.14)
    deep_tail_weight = _catalogue_clamp(0.006 + score / 3500.0, 0.006, 0.035)
    probs: List[float] = []
    for placement in range(1, placements + 1):
        primary = math.exp(-0.5 * ((placement - slot) / main_spread) ** 2)
        breakthrough = math.exp(-0.5 * ((placement - breakthrough_mode) / breakthrough_spread) ** 2)
        deep_tail = math.exp(-0.5 * ((placement - 5.0) / 5.0) ** 2)
        probs.append(
            max(
                0.0,
                (1.0 - breakthrough_weight - deep_tail_weight) * primary
                + breakthrough_weight * breakthrough
                + deep_tail_weight * deep_tail,
            )
        )
    total = sum(probs) or 1.0
    return [p / total for p in probs]


def _catalogue_build(region: str, limit: int, outlier_limit: int) -> Dict[str, Any]:
    region_key = (region or "EU").upper()
    anchor = datetime.now(timezone.utc)
    players: Dict[str, Dict[str, Any]] = {}
    snapshots_scanned = 0

    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                snapshot = json.load(fp)
        except Exception:
            continue
        if not _catalogue_region_matches(snapshot, region_key):
            continue
        entries = snapshot.get("entries") or []
        if not isinstance(entries, list) or not entries:
            continue
        snapshots_scanned += 1
        total_entries = _catalogue_safe_int(snapshot.get("totalEntries"), len(entries)) or len(entries)
        event_id = str(snapshot.get("eventId") or "")
        window_id = str(snapshot.get("windowId") or "")
        updated_at = snapshot.get("updatedAt") or snapshot.get("fetchedAt")
        event_weight = _catalogue_event_weight(event_id)

        for entry in entries:
            names = entry.get("playerList") or entry.get("players") or []
            account_ids = entry.get("accountIds") or []
            if not isinstance(account_ids, list):
                account_ids = []
            if not account_ids and isinstance(names, list):
                account_ids = [f"name:{_catalogue_slug(str(name))}" for name in names if str(name).strip()]
            if not account_ids:
                continue
            rank = _catalogue_safe_int(entry.get("rank"), total_entries)
            sessions = _catalogue_safe_float(entry.get("sessions") or entry.get("matchesPlayed"), 0.0)
            points = _catalogue_safe_float(entry.get("pointsEarned") or entry.get("points"), 0.0)
            elims = _catalogue_safe_float(entry.get("elims") or entry.get("eliminations"), 0.0)
            rank_pct = _catalogue_clamp(1.0 - ((rank - 1) / max(1, total_entries - 1)), 0.0, 1.0)
            team_display = _entry_display(entry)
            for idx, raw_id in enumerate(account_ids):
                account_id = str(raw_id or "").lower().strip()
                if not account_id:
                    continue
                player = players.setdefault(
                    account_id,
                    {
                        "id": account_id,
                        "name": _catalogue_player_name(names, idx, account_id),
                        "region": region_key,
                        "appearances": [],
                    },
                )
                if player.get("name") == "Unknown player":
                    player["name"] = _catalogue_player_name(names, idx, account_id)
                player["appearances"].append(
                    {
                        "eventId": event_id,
                        "windowId": window_id,
                        "event": _catalogue_event_label(event_id),
                        "updatedAt": updated_at,
                        "rank": rank,
                        "total": total_entries,
                        "rankPct": rank_pct,
                        "eventWeight": event_weight,
                        "points": points,
                        "pointsPerMatch": points / sessions if sessions else 0.0,
                        "elims": elims,
                        "elimRate": elims / sessions if sessions else 0.0,
                        "sessions": sessions,
                        "team": team_display,
                    }
                )

    try:
        var_weights: Dict[str, float] = _m_catalogue_builder_weights_get(region_key) or {}
    except Exception:
        var_weights = {}

    profiles: List[Dict[str, Any]] = []
    for player in players.values():
        appearances = list(player.get("appearances") or [])
        if not appearances:
            continue
        appearances.sort(key=lambda app: str(app.get("updatedAt") or ""))
        weights = [
            _catalogue_recency_weight(app.get("updatedAt"), anchor) * _catalogue_safe_float(app.get("eventWeight"), 1.0)
            for app in appearances
        ]
        weight_total = sum(weights) or 1.0
        rank_values = [_catalogue_safe_float(app.get("rankPct"), 0.0) for app in appearances]
        weighted_rank = sum(w * r for w, r in zip(weights, rank_values)) / weight_total
        recent_apps = appearances[-5:]
        recent_weights = [_catalogue_safe_float(app.get("eventWeight"), 1.0) for app in recent_apps]
        recent_weight_total = sum(recent_weights) or 1.0
        recent_rank = sum(_catalogue_safe_float(app.get("rankPct"), 0.0) * w for app, w in zip(recent_apps, recent_weights)) / recent_weight_total
        mean_rank = sum(rank_values) / len(rank_values)
        variance = sum((r - mean_rank) ** 2 for r in rank_values) / max(1, len(rank_values))
        consistency = _catalogue_clamp(100.0 - (math.sqrt(variance) * 130.0))
        top_100 = sum(1 for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 100)
        top_200 = sum(1 for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 200)
        top_500 = sum(1 for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 500)
        top_100_weight = sum(_catalogue_safe_float(app.get("eventWeight"), 1.0) for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 100)
        top_200_weight = sum(_catalogue_safe_float(app.get("eventWeight"), 1.0) for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 200)
        top_500_weight = sum(_catalogue_safe_float(app.get("eventWeight"), 1.0) for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 500)
        evidence_weight_total = sum(_catalogue_safe_float(app.get("eventWeight"), 1.0) for app in appearances) or 1.0
        best_rank = min(_catalogue_safe_int(app.get("rank"), 999999) for app in appearances)
        best_rank_pct = max(rank_values)
        max_elim_rate = max((_catalogue_safe_float(app.get("elimRate"), 0.0) for app in appearances), default=0.0)
        avg_points_per_match = sum(w * _catalogue_safe_float(app.get("pointsPerMatch"), 0.0) for w, app in zip(weights, appearances)) / weight_total
        avg_elim_rate = sum(w * _catalogue_safe_float(app.get("elimRate"), 0.0) for w, app in zip(weights, appearances)) / weight_total
        total_sessions = sum(_catalogue_safe_float(app.get("sessions"), 0.0) for app in appearances)
        total_points = sum(_catalogue_safe_float(app.get("points"), 0.0) for app in appearances)
        total_elims = sum(_catalogue_safe_float(app.get("elims"), 0.0) for app in appearances)
        total_wins = sum(1 for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) == 1)
        vr_rate = total_wins / max(1.0, total_sessions)
        recent_points = sum(_catalogue_safe_float(app.get("pointsPerMatch"), 0.0) for app in recent_apps) / max(1, len(recent_apps))
        recent_elims = sum(_catalogue_safe_float(app.get("elimRate"), 0.0) for app in recent_apps) / max(1, len(recent_apps))
        placement_trend = _catalogue_clamp((recent_rank - mean_rank) * 2.5, -1.0, 1.0)
        elim_trend = _catalogue_clamp((recent_elims - avg_elim_rate) / 4.0, -1.0, 1.0)
        points_stdev = math.sqrt(sum((_catalogue_safe_float(app.get("pointsPerMatch"), 0.0) - avg_points_per_match) ** 2 for app in appearances) / max(1, len(appearances)))
        min_points_per_match = min((_catalogue_safe_float(app.get("pointsPerMatch"), 0.0) for app in appearances), default=0.0)
        days_since_last = 30.0
        last_ts = _catalogue_parse_ts(appearances[-1].get("updatedAt"))
        if last_ts:
            days_since_last = max(0.0, (anchor - last_ts).total_seconds() / 86400.0)
        final_apps = sum(1 for app in appearances if "final" in str(app.get("eventId") or "").lower())
        major_top10 = sum(1 for app in appearances if "fncs" in str(app.get("eventId") or "").lower() and _catalogue_safe_int(app.get("rank"), 999999) <= 10)
        div1_top10 = sum(1 for app in appearances if _catalogue_safe_int(app.get("rank"), 999999) <= 10)
        event_count_score = _catalogue_clamp(math.log1p(len(appearances)) * 24.0)
        reliability = _catalogue_clamp(math.sqrt(len(appearances) / 14.0), 0.25, 1.0)
        top100_rate = top_100_weight / evidence_weight_total
        top200_rate = top_200_weight / evidence_weight_total
        top500_rate = top_500_weight / evidence_weight_total
        curated_strength = _catalogue_curated_strength(str(player.get("name") or ""))
        pedigree = _catalogue_clamp(
            event_count_score * 0.45
            + top_100_weight * 4.2
            + top_200_weight * 2.1
            + best_rank_pct * 18.0
            + curated_strength * 0.45
        )
        upside = _catalogue_clamp(best_rank_pct * 72.0 + top_100 * 4.0 + top_200 * 2.0 + max_elim_rate * 3.0)
        evidence_score = (
            weighted_rank * 32.0
            + recent_rank * 13.0
            + best_rank_pct * 10.0
            + top100_rate * 18.0
            + top200_rate * 10.0
            + top500_rate * 5.0
            + consistency * 0.06
            + event_count_score * 0.12
            + pedigree * 0.28
        )
        raw_score = evidence_score * reliability + curated_strength * 0.42
        outlier_chance = _catalogue_clamp((best_rank_pct - weighted_rank) * 85.0 + top_200 * 4.0 + top_500 * 1.5)
        default_scores = {
            "Points / match": _catalogue_score_from_0_1(avg_points_per_match / 18.0),
            "Elim rate": _catalogue_score_from_0_1(avg_elim_rate / 7.0),
            "Victory Royale rate": _catalogue_score_from_0_1(vr_rate / 0.18),
            "Rank percentile": _catalogue_score_from_0_1(weighted_rank),
            "Consistency (1 - CV)": _catalogue_score_from_0_100(consistency),
            "Placement trend": _catalogue_score(placement_trend * 100.0),
            "Elim trend": _catalogue_score(elim_trend * 100.0),
            "Best single-event rank": _catalogue_score_from_0_1(best_rank_pct),
            "Qualifier cumulative pts": _catalogue_score_from_0_1(total_points / 2500.0),
            "Qualifier matches played": _catalogue_score_from_0_1(total_sessions / 180.0),
            "Events participated in": _catalogue_score_from_0_1(len(appearances) / 80.0),
            "Total elims in qualifier": _catalogue_score_from_0_1(total_elims / 700.0),
            "Total VRs in qualifier": _catalogue_score_from_0_1(total_wins / 35.0),
            "Placement stdev": _catalogue_score_from_0_1(math.sqrt(variance) * 2.0),
            "Pts/match stdev": _catalogue_score_from_0_1(points_stdev / 8.0),
            "Top-3 game rate": _catalogue_score_from_0_1(top100_rate),
            "Top-10 game rate": _catalogue_score_from_0_1(top200_rate),
            "Pts-floor (worst event)": _catalogue_score_from_0_1(min_points_per_match / 12.0),
            "FNCS pedigree score": _catalogue_score_from_0_100(pedigree),
            "Grand-final appearances": _catalogue_score_from_0_1(final_apps / 12.0),
            "Major top-10 finishes": _catalogue_score_from_0_1(major_top10 / 12.0),
            "Div-1 top-10 finishes": _catalogue_score_from_0_1(div1_top10 / 24.0),
            "Best career finish": _catalogue_score_from_0_1(best_rank_pct),
            "Roster stability": _catalogue_score_from_0_100(consistency),
            "Roster size (duo/trio)": 0.0,
            "Recency (days since play)": _catalogue_score(100.0 - _catalogue_clamp(days_since_last / 60.0, 0.0, 1.0) * 200.0),
            "Community power rank": _catalogue_score_from_0_100(curated_strength),
            "Expert win probability %": _catalogue_score_from_0_100(max(curated_strength * 0.85, weighted_rank * 100.0)),
            "Analyst tier": _catalogue_score_from_0_100(max(curated_strength * 0.9, pedigree)),
            "Clutch reputation": _catalogue_score_from_0_100(max(outlier_chance, top100_rate * 100.0, curated_strength * 0.75)),
            "Landing spot quality": 0.0,
            "Landing contest risk": 0.0,
            "Injury / absence flag": 0.0,
            "Scrim / ranked form (7d)": _catalogue_score_from_0_1(recent_rank),
            "LAN experience": _catalogue_score_from_0_1(final_apps / 10.0),
            "POI meta fit (patch)": 0.0,
        }
        if var_weights:
            weight_adj = 0.0
            for vname, sval in default_scores.items():
                w = _catalogue_builder_weight(var_weights, "mode", str(vname))
                if abs(w) < 1e-9:
                    continue
                weight_adj += _catalogue_safe_float(sval, 0.0) * (w / 100.0) * 0.07
            raw_score = raw_score + weight_adj

        profile = {
            "id": player["id"],
            "name": player["name"],
            "region": region_key,
            "rawScore": raw_score,
            "bestRank": best_rank,
            "eventsPlayed": len(appearances),
            "top100s": top_100,
            "top200s": top_200,
            "defaultScores": default_scores,
            "_rankMetrics": {
                "weightedRank": weighted_rank,
                "recentRank": recent_rank,
                "upside": upside,
                "consistency": consistency,
                "pedigree": pedigree,
                "outlierChance": outlier_chance,
            },
            "_appearances": appearances,
        }
        profiles.append(profile)

    if not profiles:
        return {
            "region": region_key,
            "generatedAt": anchor.isoformat(),
            "snapshotsScanned": snapshots_scanned,
            "totalPlayers": 0,
            "players": [],
            "message": "No cached leaderboard snapshots found for this region.",
        }

    raw_scores = [p["rawScore"] for p in profiles]
    raw_min = min(raw_scores)
    raw_max = max(raw_scores)
    span = raw_max - raw_min or 1.0
    for profile in profiles:
        baseline = _catalogue_clamp(((profile["rawScore"] - raw_min) / span) * 100.0)
        profile["baselineScore"] = round(baseline, 1)
        profile["defaultScores"]["Rank percentile"] = max(
            _catalogue_safe_float(profile["defaultScores"].get("Rank percentile"), 0.0),
            _catalogue_score_from_0_100(baseline),
        )

    profiles.sort(key=lambda p: p["baselineScore"], reverse=True)
    for index, profile in enumerate(profiles, start=1):
        profile["baselineRank"] = index

    core = profiles[:limit]
    core_ids = {p["id"] for p in core}
    outliers = [
        profile
        for profile in profiles[limit:]
        if _catalogue_safe_int(profile.get("bestRank"), 999999) <= 1000
        and _catalogue_safe_float(profile.get("defaultScores", {}).get("Clutch reputation"), -100.0) >= -70.0
    ]
    outliers.sort(
        key=lambda p: (
            _catalogue_safe_float(p.get("defaultScores", {}).get("Clutch reputation"), -100.0),
            -_catalogue_safe_int(p.get("bestRank"), 999999),
        ),
        reverse=True,
    )
    selected_outliers = [p for p in outliers if p["id"] not in core_ids][:outlier_limit]

    for profile in core:
        profile["catalogueType"] = "baseline"
        profile["chartRank"] = profile["baselineRank"]
    for index, profile in enumerate(selected_outliers):
        profile["catalogueType"] = "outlier"
        profile["chartRank"] = min(200, max(100, _catalogue_safe_int(profile.get("bestRank"), 150) or (100 + index)))

    displayed = core + selected_outliers
    for profile in displayed:
        baseline_score = _catalogue_safe_float(profile.get("baselineScore"), 0.0)
        baseline_pct = (_catalogue_safe_float((profile.get("defaultScores") or {}).get("Rank percentile"), 0.0) + 100.0) / 2.0
        metrics = profile.get("_rankMetrics") or {}
        recent_form_pct = (_catalogue_safe_float((profile.get("defaultScores") or {}).get("Placement trend"), 0.0) + 100.0) / 2.0
        upside = _catalogue_safe_float(metrics.get("upside"), 0.0)
        event_impacts: List[Dict[str, Any]] = []
        for app in list(profile.get("_appearances") or [])[-8:][::-1]:
            rank_pct_score = _catalogue_safe_float(app.get("rankPct"), 0.0) * 100.0
            rank_score = _catalogue_score_from_0_100(rank_pct_score)
            rank = _catalogue_safe_int(app.get("rank"), 999999)
            total = _catalogue_safe_int(app.get("total"), 0)
            deltas = [
                _catalogue_delta("Rank percentile", (rank_pct_score - baseline_pct) * 0.5),
                _catalogue_delta("Placement trend", (rank_pct_score - recent_form_pct) * 0.7),
                _catalogue_delta("Points / match", (_catalogue_safe_float(app.get("pointsPerMatch"), 0.0) - 8.0) * 2.2),
                _catalogue_delta("Elim rate", (_catalogue_safe_float(app.get("elimRate"), 0.0) - 2.5) * 4.0),
            ]
            upside_delta = min(35.0, (201 - rank) / 6.0 + 8.0) if rank <= 200 else max(-20.0, (rank_pct_score - upside) * 0.20)
            deltas.append(_catalogue_delta("Best single-event rank", upside_delta))
            consistency_delta = 8.0 if abs(rank_pct_score - baseline_score) <= 25.0 else -abs(rank_pct_score - baseline_score) * 0.20
            deltas.append(_catalogue_delta("Consistency (1 - CV)", consistency_delta))
            pedigree_delta = 12.0 if rank <= 100 else 8.0 if rank <= 200 else 5.0 if rank <= 500 else 0.0
            deltas.append(_catalogue_delta("FNCS pedigree score", pedigree_delta))
            if rank <= 200:
                deltas.append(_catalogue_delta("Clutch reputation", max(6.0, (rank_pct_score - baseline_score) * 0.35)))
            elif total and rank > total * 0.7:
                deltas.append(_catalogue_delta("Clutch reputation", -5.0))
            if "final" in str(app.get("eventId") or "").lower() and rank <= 200:
                deltas.append(_catalogue_delta("Grand-final appearances", 10.0))
                deltas.append(_catalogue_delta("LAN experience", 8.0))
            if rank <= 10:
                deltas.append(_catalogue_delta("Major top-10 finishes", 10.0))
                deltas.append(_catalogue_delta("Div-1 top-10 finishes", 8.0))
                deltas.append(_catalogue_delta("Best career finish", max(8.0, rank_score * 0.12)))
            event_impacts.append(
                {
                    "eventId": app.get("eventId"),
                    "windowId": app.get("windowId"),
                    "event": app.get("event"),
                    "updatedAt": app.get("updatedAt"),
                    "team": app.get("team"),
                    "rank": rank,
                    "total": total,
                    "deltas": [delta for delta in deltas if abs(delta["scoreDelta"]) >= 8],
                }
            )
        profile["eventImpacts"] = event_impacts
        saved_doc: Optional[Dict[str, Any]] = None
        try:
            saved_doc = _m_catalogue_profile_get(region_key, str(profile.get("id") or ""))
        except Exception:
            saved_doc = None
        if saved_doc:
            saved_scores = saved_doc.get("manualScores")
            if isinstance(saved_scores, dict):
                merged_scores = dict(profile.get("defaultScores") or {})
                for key, value in saved_scores.items():
                    merged_scores[str(key)] = _catalogue_score(_catalogue_safe_float(value, 0.0))
                profile["defaultScores"] = merged_scores
                profile["baselineScore"] = round((_catalogue_safe_float(merged_scores.get("Rank percentile"), 0.0) + 100.0) / 2.0, 1)
        decisions = saved_doc.get("eventDecisions") if saved_doc else None
        if isinstance(decisions, dict):
            profile["eventDecisions"] = decisions
        else:
            decisions = {}
        for impact in profile["eventImpacts"]:
            for delta in impact.get("deltas") or []:
                decision_key = f"{_catalogue_event_key(impact)}|{delta.get('variable')}"
                delta["decisionKey"] = decision_key
                decision = decisions.get(decision_key)
                if decision:
                    delta["decision"] = decision
        if profile.get("catalogueType") == "outlier":
            profile["placementDistribution"] = _catalogue_outlier_distribution(profile.get("chartRank"), profile.get("baselineScore"), 200)
        else:
            profile["placementDistribution"] = _catalogue_weighted_distribution(profile.get("defaultScores") or {}, var_weights, 200)
        profile.pop("_rankMetrics", None)
        profile.pop("_appearances", None)
        profile.pop("rawScore", None)

    for profile in displayed:
        if profile.get("catalogueType") == "baseline":
            profile["_poolRank"] = int(profile.get("baselineRank") or 0)

    baseline_display = sorted(
        [profile for profile in displayed if profile.get("catalogueType") == "baseline"],
        key=lambda p: _catalogue_safe_float(p.get("baselineScore"), 0.0),
        reverse=True,
    )
    outlier_display = [profile for profile in displayed if profile.get("catalogueType") == "outlier"]
    # Global score order: achievers can sort above weaker baseline players by composite score.
    displayed = sorted(
        baseline_display + outlier_display,
        key=lambda p: _catalogue_safe_float(p.get("baselineScore"), 0.0),
        reverse=True,
    )
    for index, profile in enumerate(displayed, start=1):
        profile["catalogueScoreRank"] = index
    for profile in displayed:
        if profile.get("catalogueType") == "baseline" and profile.get("_poolRank") is not None:
            profile["baselineRank"] = int(profile["_poolRank"])
        else:
            profile["baselineRank"] = int(profile.get("catalogueScoreRank") or 0)
        profile.pop("_poolRank", None)

    storage_warning = None
    try:
        _m_catalogue_upsert_many(region_key, displayed)
        snapshot_payload = {
            "schemaVersion": 7,
            "region": region_key,
            "generatedAt": anchor.isoformat(),
            "snapshotsScanned": snapshots_scanned,
            "totalPlayers": len(profiles),
            "baselineCount": len(core),
            "outlierCount": len(selected_outliers),
            "variables": _CATALOGUE_VARIABLE_NAMES,
            "players": displayed,
        }
        _m_catalogue_snapshot_put(region_key, snapshot_payload)
    except Exception as e:
        storage_warning = str(e)

    return {
        "schemaVersion": 7,
        "region": region_key,
        "generatedAt": anchor.isoformat(),
        "snapshotsScanned": snapshots_scanned,
        "totalPlayers": len(profiles),
        "baselineCount": len(core),
        "outlierCount": len(selected_outliers),
        "variables": _CATALOGUE_VARIABLE_NAMES,
        "players": displayed,
        "storageWarning": storage_warning,
    }


_CATALOGUE_SCHEMA_VERSION = 7


@app.get("/api/catalogue")
async def api_catalogue(
    region: str = Query("EU"),
    limit: int = Query(200, ge=25, le=500),
    outliers: int = Query(36, ge=0, le=100),
    force: bool = Query(False),
):
    """Neutral, event-agnostic player catalogue with outlier/achiever overlays."""
    region_key = (region or "EU").upper()
    if region_key not in REGIONS:
        raise HTTPException(400, f"Unknown region '{region}'. Allowed: {REGIONS}")
    if not force and limit == 200 and outliers == 36:
        try:
            cached = _m_catalogue_snapshot_get(region_key)
            if cached and cached.get("schemaVersion") == _CATALOGUE_SCHEMA_VERSION and isinstance(cached.get("players"), list):
                return {**cached, "fromCache": True}
        except Exception as e:
            logger.debug(f"catalogue snapshot read skipped for {region_key}: {e}")
    return _catalogue_build(region_key, limit, outliers)


@app.get("/api/catalogue/player")
async def api_catalogue_player(region: str = Query("EU"), playerId: str = Query(...)):
    """Return the persisted catalogue instance for a player if one exists."""
    try:
        doc = _m_catalogue_profile_get(region, playerId)
    except Exception as e:
        raise HTTPException(503, f"Catalogue storage unavailable: {e}")
    if not doc:
        raise HTTPException(404, "Catalogue player not found. Load /api/catalogue for this region first.")
    return doc


class CataloguePlayerUpdate(BaseModel):
    region: str
    playerId: str
    defaultScores: Optional[Dict[str, float]] = None
    eventDecisions: Optional[Dict[str, str]] = None


class CatalogueBuilderWeightsUpdate(BaseModel):
    region: str
    weights: Dict[str, float]


class BuilderSnapshotPayload(BaseModel):
    type: str
    name: str
    region: Optional[str] = None
    eventId: Optional[str] = None
    source: Optional[str] = "builder"
    notes: Optional[str] = ""
    payload: Dict[str, Any]


@app.get("/api/catalogue/builder-weights")
async def api_catalogue_builder_weights_get(region: str = Query("EU")):
    """Per-variable weights applied when composing catalogue baseline scores."""
    region_key = (region or "EU").upper()
    if region_key not in REGIONS:
        raise HTTPException(400, f"Unknown region '{region}'. Allowed: {REGIONS}")
    try:
        weights = _m_catalogue_builder_weights_get(region_key)
    except Exception as e:
        raise HTTPException(503, f"Catalogue builder weights unavailable: {e}")
    return {"region": region_key, "weights": weights or {}, "variables": _CATALOGUE_VARIABLE_NAMES}


@app.post("/api/catalogue/builder-weights")
async def api_catalogue_builder_weights_save(payload: CatalogueBuilderWeightsUpdate):
    """Persist catalogue variable weights (clamped server-side). Rebuild catalogue to apply."""
    region_key = (payload.region or "EU").upper()
    if region_key not in REGIONS:
        raise HTTPException(400, f"Unknown region '{payload.region}'. Allowed: {REGIONS}")
    try:
        doc = _m_catalogue_builder_weights_put(region_key, dict(payload.weights or {}))
    except Exception as e:
        raise HTTPException(503, f"Catalogue builder weights unavailable: {e}")
    return doc


@app.get("/api/builder/snapshots")
async def api_builder_snapshots_list(
    type: Optional[str] = Query(None, description="weights or ratings"),
    region: Optional[str] = Query(None),
    limit: int = Query(80, ge=1, le=200),
):
    """List DB-backed point-in-time Builder snapshots."""
    if type and type.lower() not in {"weights", "ratings"}:
        raise HTTPException(400, "type must be weights or ratings")
    try:
        return {"snapshots": _m_builder_snapshots_list(type, region, limit)}
    except Exception as e:
        raise HTTPException(503, f"Builder snapshots unavailable: {e}")


@app.get("/api/builder/snapshots/{snapshot_id}")
async def api_builder_snapshot_get(snapshot_id: str):
    try:
        doc = _m_builder_snapshot_get(snapshot_id)
    except Exception as e:
        raise HTTPException(503, f"Builder snapshot unavailable: {e}")
    if not doc:
        raise HTTPException(404, "Builder snapshot not found")
    return doc


@app.post("/api/builder/snapshots")
async def api_builder_snapshot_save(payload: BuilderSnapshotPayload):
    """Persist named Builder weight or player-rating snapshots in Mongo."""
    snap_type = (payload.type or "").strip().lower()
    if snap_type not in {"weights", "ratings"}:
        raise HTTPException(400, "type must be weights or ratings")
    if not payload.payload:
        raise HTTPException(400, "payload is required")
    try:
        return _m_builder_snapshot_put(
            {
                "type": snap_type,
                "name": payload.name,
                "region": payload.region,
                "eventId": payload.eventId,
                "source": payload.source or "builder",
                "notes": payload.notes or "",
                "payload": payload.payload,
            }
        )
    except Exception as e:
        raise HTTPException(503, f"Builder snapshot save failed: {e}")


@app.post("/api/catalogue/player")
async def api_catalogue_player_save(payload: CataloguePlayerUpdate):
    """Persist trader edits to catalogue variables and event-effect decisions."""
    region_key = (payload.region or "EU").upper()
    if region_key not in REGIONS:
        raise HTTPException(400, f"Unknown region '{payload.region}'. Allowed: {REGIONS}")
    patch: Dict[str, Any] = {}
    if payload.defaultScores is not None:
        sanitized_scores = {
            str(key): _catalogue_score(_catalogue_safe_float(value, 0.0))
            for key, value in payload.defaultScores.items()
        }
        patch["manualScores"] = sanitized_scores
        patch["defaultScores"] = sanitized_scores
        patch["baselineScore"] = round((_catalogue_safe_float(patch["defaultScores"].get("Rank percentile"), 0.0) + 100.0) / 2.0, 1)
        patch["placementDistribution"] = _catalogue_distribution(patch["defaultScores"], 200)
    if payload.eventDecisions is not None:
        patch["eventDecisions"] = {
            str(key): str(value)
            for key, value in payload.eventDecisions.items()
            if str(value) in {"accepted", "rejected"}
        }
    try:
        return _m_catalogue_profile_update(region_key, payload.playerId, patch)
    except Exception as e:
        raise HTTPException(503, f"Catalogue storage unavailable: {e}")


@app.get("/api/markets/catalog")
async def api_markets_catalog():
    """Return the full market catalog so the UI can label / filter rows."""
    return {"markets": MARKET_CATALOG}


@app.get("/api/markets")
async def api_markets(
    region: Optional[str] = Query(None, description="If set, restrict to events in this region"),
    minEvents: int = Query(2, ge=2, le=50, description="Minimum events per pool to aggregate"),
    groupBy: str = Query(
        "family",
        description=(
            "Pool signature. Options: "
            "'family' = family+region+stage (default, week-over-week same cup); "
            "'strict' = family+region+stage+scoring (splits when Epic changes scoring); "
            "'tier'   = tier+region (loose: all Cash Cups together, all FNCS together); "
            "'tier_scoring' = tier+region+scoring (cross-family but respects patch)"
        ),
    ),
    audience: str = Query(
        "all",
        description="'all' | 'retail' | 'analyst' — filter markets by audience tag.",
    ),
    halfLifeDays: float = Query(
        30.0,
        ge=1.0, le=365.0,
        description="Half-life (days) for recency-weighted μ/σ. Smaller = more weight on recent events.",
    ),
    lastN: int = Query(
        0,
        ge=0, le=200,
        description="If >0, only consider the most recent N snapshots per pool (by updatedAt).",
    ),
):
    """
    Scan disk-cached leaderboards, compute candidate markets per event, then
    aggregate across events grouped by a pool signature.  Rows sorted by CV
    ascending — the lowest-CV markets are the most stable lines to offer.
    """
    from api.public_client import _parse_lb_event_id, _parse_window_stage

    # Dedupe cached files that differ only in `pages=` (keep the one with the
    # deepest leaderboard; prefer the most recently fetched on tie).
    best_by_key: Dict[Tuple[str, str], Tuple[Path, Dict[str, Any]]] = {}
    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8-sig") as fp:
                data = json.load(fp)
        except Exception:
            continue
        key = (data.get("eventId") or "", data.get("windowId") or "")
        if not key[0]:
            continue
        prev = best_by_key.get(key)
        if prev is None:
            best_by_key[key] = (path, data)
            continue
        prev_data = prev[1]
        prev_entries = prev_data.get("totalEntries") or 0
        cur_entries = data.get("totalEntries") or 0
        if cur_entries > prev_entries:
            best_by_key[key] = (path, data)
        elif cur_entries == prev_entries and (data.get("fetchedAt") or 0) > (prev_data.get("fetchedAt") or 0):
            best_by_key[key] = (path, data)

    snapshots: List[Dict[str, Any]] = []
    regions_touched: set = set()
    for (event_id, window_id), (_path, data) in best_by_key.items():
        parsed = _parse_lb_event_id(event_id)
        ev_region = parsed.get("region")
        if region and ev_region != region:
            continue
        if ev_region:
            regions_touched.add(ev_region)
        event_meta = None
        if ev_region:
            try:
                raw = await _get_tournaments_raw(ev_region)
                event_meta = extract_event_info(
                    raw, leaderboard_event_id=event_id, leaderboard_window_id=window_id
                )
            except Exception:
                event_meta = None
        entries = data.get("entries") or []
        markets = evaluate_markets(entries, event_meta=event_meta)
        stage = _parse_window_stage(window_id)
        family = parsed.get("family") or "unknown"
        tier = event_tier(family)
        season = parsed.get("season")
        scoring_fp = scoring_rules_fingerprint((event_meta or {}).get("scoringRules"))
        family_base = f"{family}·{stage}" if stage else family

        # Build the pool key according to requested grouping
        if groupBy == "strict":
            pool = f"{family_base} [{scoring_fp}]"
        elif groupBy == "tier":
            pool = tier
        elif groupBy == "tier_scoring":
            pool = f"{tier} [{scoring_fp}]"
        else:  # 'family' default
            pool = family_base

        snapshots.append(
            {
                "eventId": event_id,
                "windowId": window_id,
                "eventGroup": pool,
                "region": ev_region,
                "tier": tier,
                "family": family_base,
                "season": season,
                "scoringHash": scoring_fp,
                "updatedAt": data.get("updatedAt"),
                "totalEntries": data.get("totalEntries"),
                "matchCap": (event_meta or {}).get("matchCap"),
                "markets": markets,
            }
        )

    # Optional lastN trim per pool (most recent by updatedAt)
    if lastN > 0:
        by_pool: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for s in snapshots:
            by_pool.setdefault((s["eventGroup"], s["region"] or "?"), []).append(s)
        snapshots = []
        for key, lst in by_pool.items():
            lst.sort(key=lambda s: (s.get("updatedAt") or ""), reverse=True)
            snapshots.extend(lst[:lastN])

    rows = aggregate_markets_across_events(snapshots, min_samples=minEvents)

    # Audience filter
    if audience in ("retail", "analyst"):
        rows = [r for r in rows if MARKET_CATALOG.get(r["market"], {}).get("audience") == audience]

    # Enrich each row with audience tag for the UI
    for r in rows:
        meta = MARKET_CATALOG.get(r["market"], {})
        r["audience"] = meta.get("audience", "analyst")

    # Empirical pricing + grade + recency-weighted stats
    enrich_rows_with_pricing(rows, half_life_days=halfLifeDays)

    groups = sorted({(r["group"], r["region"]) for r in rows})

    # Pool descriptors so the UI can explain what's in each pool + anomaly flags
    pool_descriptors: Dict[Tuple[str, str], Dict[str, Any]] = {}
    pool_snaps_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for s in snapshots:
        key = (s["eventGroup"], s["region"])
        pool_snaps_by_key.setdefault(key, []).append(s)
        pool_descriptors.setdefault(key, {
            "tier": s.get("tier"),
            "scoringHashes": set(),
            "seasons": set(),
            "eventCount": 0,
            "sampleEventIds": [],
        })
        d = pool_descriptors[key]
        d["scoringHashes"].add(s.get("scoringHash"))
        if s.get("season"):
            d["seasons"].add(s["season"])
        d["eventCount"] += 1
        if len(d["sampleEventIds"]) < 3:
            d["sampleEventIds"].append(s.get("eventId"))

    pool_descriptors_out = []
    for k, v in pool_descriptors.items():
        anomalies = detect_pool_anomalies(
            pool_key=k,
            snapshots=pool_snaps_by_key.get(k, []),
        )
        pool_descriptors_out.append({
            "pool": k[0],
            "region": k[1],
            "tier": v["tier"],
            "events": v["eventCount"],
            "scoringHashes": sorted(h for h in v["scoringHashes"] if h),
            "seasons": sorted(v["seasons"]),
            "sampleEventIds": v["sampleEventIds"],
            "anomalies": anomalies,
            "anomalyCount": len(anomalies),
        })
    pool_descriptors_out.sort(key=lambda d: (-d["events"], d["pool"], d["region"]))

    # Grade histogram
    grade_hist = {"A": 0, "B": 0, "C": 0, "D": 0}
    for r in rows:
        g = (r.get("grade") or {}).get("grade")
        if g in grade_hist:
            grade_hist[g] += 1

    return {
        "snapshots": len(snapshots),
        "regionsTouched": sorted(regions_touched),
        "groupBy": groupBy,
        "audience": audience,
        "halfLifeDays": halfLifeDays,
        "lastN": lastN,
        "pools": pool_descriptors_out,
        "groups": [{"family": g, "region": r} for g, r in groups],
        "markets": len(rows),
        "gradeCounts": grade_hist,
        "rows": rows,
        "catalog": MARKET_CATALOG,
    }


# ---------------------------------------------------------------------------
# Forecast.gg-facing: offerable candidates only
# ---------------------------------------------------------------------------

@app.get("/api/offer/candidates")
async def api_offer_candidates(
    region: Optional[str] = Query(None),
    minEvents: int = Query(4, ge=2, le=50),
    groupBy: str = Query("family"),
    audience: str = Query("retail"),
    halfLifeDays: float = Query(30.0, ge=1.0, le=365.0),
    lastN: int = Query(0, ge=0, le=200),
    minGrade: str = Query(
        "B",
        description="Only return offers at this letter grade or better (A|B|C|D).",
    ),
    maxJuicePct: float = Query(
        0.10, ge=0.0, le=0.50,
        description="Drop anything our engine wants to juice above this cap.",
    ),
):
    """
    Returns only the rows that have graded through to 'offerable' as a flat
    list of forecast.gg-ready offers with {line, juice, max stake weight,
    evidence}. This is the surface Forecast.gg integrates against.
    """
    data = await api_markets(  # type: ignore[arg-type]
        region=region,
        minEvents=minEvents,
        groupBy=groupBy,
        audience=audience,
        halfLifeDays=halfLifeDays,
        lastN=lastN,
    )
    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    min_rank = grade_order.get(minGrade.upper(), 1)

    offers: List[Dict[str, Any]] = []
    for r in data["rows"]:
        grade = (r.get("grade") or {}).get("grade")
        if not grade or grade_order[grade] > min_rank:
            continue
        offer = to_offer(r)
        if not offer:
            continue
        if offer.get("juicePct") is not None and offer["juicePct"] > maxJuicePct:
            continue
        offers.append(offer)

    offers.sort(key=lambda o: (grade_order[o["grade"]], -(o.get("score") or 0)))

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(offers),
        "criteria": {
            "region": region,
            "groupBy": groupBy,
            "audience": audience,
            "minEvents": minEvents,
            "minGrade": minGrade.upper(),
            "maxJuicePct": maxJuicePct,
            "halfLifeDays": halfLifeDays,
            "lastN": lastN,
        },
        "gradeCounts": data.get("gradeCounts"),
        "offers": offers,
    }


def _market_decision(market: str, aggregate: Dict[str, Any]) -> Dict[str, Any]:
    points = aggregate.get("points_sim") or {}
    baseline = aggregate.get("baseline") or {}
    crps_edge = float(baseline.get("crps", 0) or 0) - float(points.get("crps", 0) or 0)
    top10_edge = float(baseline.get("brierTop10", 0) or 0) - float(points.get("brierTop10", 0) or 0)
    top25_edge = float(baseline.get("brierTop25", 0) or 0) - float(points.get("brierTop25", 0) or 0)
    winner_edge = float(baseline.get("logLossWinner", 0) or 0) - float(points.get("logLossWinner", 0) or 0)
    events = int(points.get("eventsScored", 0) or 0)

    raw = {
        "eventsScored": events,
        "modelCrps": round(float(points.get("crps", 0) or 0), 4),
        "baselineCrps": round(float(baseline.get("crps", 0) or 0), 4),
        "modelBrierTop10": round(float(points.get("brierTop10", 0) or 0), 6),
        "baselineBrierTop10": round(float(baseline.get("brierTop10", 0) or 0), 6),
        "modelBrierTop25": round(float(points.get("brierTop25", 0) or 0), 6),
        "baselineBrierTop25": round(float(baseline.get("brierTop25", 0) or 0), 6),
        "modelLogLossWinner": round(float(points.get("logLossWinner", 0) or 0), 4),
        "baselineLogLossWinner": round(float(baseline.get("logLossWinner", 0) or 0), 4),
        "crpsEdge": round(crps_edge, 6),
        "top10BrierEdge": round(top10_edge, 6),
        "top25BrierEdge": round(top25_edge, 6),
        "winnerLogLossEdge": round(winner_edge, 6),
    }

    if market == "Winner":
        blocked = winner_edge < 0
        return {
            "market": market,
            "decision": "blocked" if blocked else "low_limit",
            "reason": "winner log loss is worse than baseline" if blocked else "winner log loss beats baseline but still needs low limits",
            "marginBand": "very wide" if blocked else "wide",
            "maxStakeBand": "internal only" if blocked else "low",
            "lmsrEnabled": False,
            "evidence": raw,
        }

    if market == "Top 25":
        viable = top25_edge > 0.015 and events >= 20
        reason = "top25 Brier improves vs baseline with enough scored events" if viable else "top25 signal exists but needs more calibration"
    elif market == "Top 10":
        viable = top10_edge > 0.01 and events >= 20
        reason = "top10 Brier improves vs baseline with enough scored events" if viable else "top10 needs more calibration"
    elif market == "Top 5":
        viable = False
        reason = "top5 is inferred from top10 quality but not directly backtested yet"
    elif market == "Top 3":
        viable = False
        reason = "podium markets behave more like winner risk and need direct Brier calibration"
    elif market == "H2H":
        viable = crps_edge > 0 and events >= 20
        reason = "H2H can use sample paths, but only clear edges should be bookable"
    else:
        viable = crps_edge > 0 and events >= 20
        reason = "points lines need dedicated settlement/backtest before normal limits"

    decision = "viable" if viable and market in ("Top 10", "Top 25") else ("low_limit" if crps_edge > 0 else "internal_only")
    return {
        "market": market,
        "decision": decision,
        "reason": reason,
        "marginBand": "standard-wide" if decision == "viable" else "wide",
        "maxStakeBand": "medium" if decision == "viable" else "low",
        "lmsrEnabled": decision in ("viable", "low_limit") and market in ("Top 10", "Top 25", "H2H"),
        "evidence": raw,
    }


@app.get("/api/market-viability")
async def api_market_viability():
    """Commercial readiness summary from latest backtest + offerability stats."""
    try:
        from storage import collections_backtest as _cb
        run = _cb.latest_run()
    except Exception:
        run = None
    if not run:
        try:
            from backtest import runner as _runner
            run = getattr(_runner, "LAST_RUN", None)
        except Exception:
            run = None
    if not run:
        return {"hasRun": False, "message": "No backtest run found. Run Ops backtest first.", "markets": []}

    aggregate = run.get("aggregate") or {}
    markets = [_market_decision(m, aggregate) for m in ("Winner", "Top 3", "Top 5", "Top 10", "Top 25", "H2H", "Points Lines")]
    try:
        market_rows = await api_markets(minEvents=2, groupBy="tier", audience="retail", lastN=50)
        grade_counts = market_rows.get("gradeCounts", {})
        market_count = market_rows.get("markets", 0)
    except Exception as e:
        grade_counts = {}
        market_count = 0
        logger.warning(f"[market-viability] market offerability summary failed: {e}")

    return {
        "hasRun": True,
        "runId": run.get("runId"),
        "modelVersion": run.get("modelVersion"),
        "createdAt": run.get("createdAt"),
        "aggregate": aggregate,
        "markets": markets,
        "offerability": {"marketCount": market_count, "gradeCounts": grade_counts},
    }


# ---------------------------------------------------------------------------
# Predictions (who will win upcoming events)
# ---------------------------------------------------------------------------

def _load_all_cached_snapshots() -> List[Dict[str, Any]]:
    """
    Scan disk cache and return every leaderboard snapshot with parsed
    region/family metadata attached. Dedupe by (eventId, windowId), keeping
    the deepest / freshest per key. Used by the prediction engine.
    """
    from api.public_client import _parse_lb_event_id  # local import to avoid cycles

    best_by_key: Dict[Tuple[str, str], Tuple[Path, Dict[str, Any]]] = {}
    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8-sig") as fp:
                data = json.load(fp)
        except Exception:
            continue
        key = (data.get("eventId") or "", data.get("windowId") or "")
        if not key[0]:
            continue
        prev = best_by_key.get(key)
        if prev is None:
            best_by_key[key] = (path, data)
            continue
        prev_data = prev[1]
        if (data.get("totalEntries") or 0) > (prev_data.get("totalEntries") or 0):
            best_by_key[key] = (path, data)
        elif (data.get("totalEntries") or 0) == (prev_data.get("totalEntries") or 0) and \
             (data.get("fetchedAt") or 0) > (prev_data.get("fetchedAt") or 0):
            best_by_key[key] = (path, data)

    out: List[Dict[str, Any]] = []
    for (event_id, window_id), (_p, data) in best_by_key.items():
        parsed = _parse_lb_event_id(event_id)
        out.append(
            {
                "eventId": event_id,
                "windowId": window_id,
                "updatedAt": data.get("updatedAt"),
                "fetchedAt": data.get("fetchedAt"),
                "region": parsed.get("region"),
                "family": parsed.get("family"),
                "season": parsed.get("season"),
                "totalEntries": data.get("totalEntries"),
                "entries": data.get("entries") or [],
            }
        )
    return out


def _load_cached_snapshots_for_event(
    event_id: str,
    *,
    window_id: Optional[str] = None,
    target_region: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Load cached snapshots for a single event/window without scanning every JSON body."""
    from api.public_client import _parse_lb_event_id  # local import to avoid cycles

    best_by_key: Dict[Tuple[str, str], Tuple[Path, Dict[str, Any]]] = {}
    for path in sorted(PUBLIC_CACHE_DIR.glob(f"{event_id}__*.json")):
        if window_id and f"__{window_id}__" not in path.name:
            continue
        try:
            with open(path, "r", encoding="utf-8-sig") as fp:
                data = json.load(fp)
        except Exception:
            continue
        key = (data.get("eventId") or "", data.get("windowId") or "")
        if not key[0]:
            continue
        parsed = _parse_lb_event_id(key[0])
        if target_region and (parsed.get("region") or "") != target_region:
            continue
        prev = best_by_key.get(key)
        if prev is None:
            best_by_key[key] = (path, data)
            continue
        prev_data = prev[1]
        if (data.get("totalEntries") or 0) > (prev_data.get("totalEntries") or 0):
            best_by_key[key] = (path, data)
        elif (data.get("totalEntries") or 0) == (prev_data.get("totalEntries") or 0) and \
             (data.get("fetchedAt") or 0) > (prev_data.get("fetchedAt") or 0):
            best_by_key[key] = (path, data)

    out: List[Dict[str, Any]] = []
    for (snapshot_event_id, snapshot_window_id), (_p, data) in best_by_key.items():
        parsed = _parse_lb_event_id(snapshot_event_id)
        out.append(
            {
                "eventId": snapshot_event_id,
                "windowId": snapshot_window_id,
                "updatedAt": data.get("updatedAt"),
                "fetchedAt": data.get("fetchedAt"),
                "region": parsed.get("region"),
                "family": parsed.get("family"),
                "season": parsed.get("season"),
                "totalEntries": data.get("totalEntries"),
                "entries": data.get("entries") or [],
            }
        )
    return out


async def _fetch_leaderboard_snapshot_for_preview(
    event_id: str,
    window_id: str,
    *,
    target_region: Optional[str],
    top_teams: int,
) -> List[Dict[str, Any]]:
    """Fetch and cache a selected leaderboard window when no disk snapshot exists."""
    from api.public_client import _parse_lb_event_id  # local import to avoid cycles

    pages = max(1, min(100, math.ceil((max(1, top_teams) or 50) / 100)))
    cache_key = f"{event_id}|{window_id}|{pages}"
    disk_path = _disk_cache_path(event_id, window_id, pages)
    payload = _cache_get(_leaderboard_cache, cache_key, Config.LEADERBOARD_TTL_SECONDS)
    if payload is None:
        payload = _disk_cache_read(disk_path, Config.LEADERBOARD_TTL_SECONDS)
        if payload is not None:
            _cache_set(_leaderboard_cache, cache_key, payload, _LEADERBOARD_CACHE_MAX)

    if payload is None:
        data = await public_client.get_full_leaderboard(event_id, window_id, max_pages=pages)
        lb = data.get("leaderboard") or {}
        entries = normalize_leaderboard_entries(lb.get("entries") or [])
        payload = {
            "eventId": event_id,
            "windowId": window_id,
            "updatedAt": lb.get("updatedAt"),
            "fetchedAt": _now(),
            "totalPages": lb.get("totalPages"),
            "pagesFetched": lb.get("pagesFetched", pages),
            "entries": entries,
            "totalEntries": len(entries),
            "distribution": summarize_leaderboard_distribution(entries),
            "statLabels": STAT_LABELS,
        }
        _cache_set(_leaderboard_cache, cache_key, payload, _LEADERBOARD_CACHE_MAX)
        _disk_cache_write(disk_path, payload)

    parsed = _parse_lb_event_id(event_id)
    if target_region and (parsed.get("region") or "") != target_region:
        return []
    return [{
        "eventId": event_id,
        "windowId": window_id,
        "updatedAt": payload.get("updatedAt"),
        "fetchedAt": payload.get("fetchedAt"),
        "region": parsed.get("region"),
        "family": parsed.get("family"),
        "season": parsed.get("season"),
        "totalEntries": payload.get("totalEntries"),
        "entries": payload.get("entries") or [],
    }]


@app.get("/api/predictions/upcoming")
async def api_predictions_upcoming(
    horizonDays: int = Query(45, ge=1, le=120, description="How far out to look."),
):
    """
    List the upcoming events that we can currently predict — i.e. those with
    enough cached qualifier data to generate a ranked field.
    """
    now = datetime.now(timezone.utc)
    horizon = now.replace(microsecond=0)
    out: List[Dict[str, Any]] = []

    # One pass over every region we know
    all_snaps = _load_all_cached_snapshots()

    for region in REGIONS:
        try:
            raw = await _get_tournaments_raw(region, include_historic=False)
        except Exception:
            continue

        for t in raw.get("tournaments", []) or []:
            event_id = t.get("eventId") or ""
            for w in (t.get("eventWindows") or []):
                begin_raw = w.get("beginTime")
                try:
                    bt = datetime.fromisoformat((begin_raw or "").replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    continue
                if bt < now:
                    continue
                if (bt - now).days > horizonDays:
                    continue

                pool = resolve_prediction_pool(event_id, all_snaps, target_region=region)
                pool = [p for p in pool if p.get("entries")]
                if not pool:
                    # No predictive coverage — still list it but flag it.
                    coverage = "empty"
                else:
                    coverage = "ok" if len(pool) >= 2 else "thin"

                # Pull a human label from the event if possible
                label = t.get("name") or event_id.split("_")[-2] if "_" in event_id else event_id

                out.append({
                    "eventId": event_id,
                    "windowId": w.get("windowId") or w.get("id") or "",
                    "region": region,
                    "begin": begin_raw,
                    "end": w.get("endTime"),
                    "matchCap": w.get("matchCap"),
                    "label": label,
                    "poolEvents": len(pool),
                    "coverage": coverage,
                })

    # Dedupe by (event_id, begin) — tournaments feed may repeat
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for e in out:
        k = (e["eventId"], e["begin"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(e)

    # Sort by time ascending
    deduped.sort(key=lambda e: e.get("begin") or "")
    # Push predictable events first within the same day bucket
    deduped.sort(key=lambda e: (0 if e.get("coverage") == "ok" else 1, e.get("begin") or ""))

    # Always bubble FNCS Major 1 Finals to the top if present
    priority = [e for e in deduped if "FNCSMajor1_Final" in e["eventId"]]
    rest = [e for e in deduped if "FNCSMajor1_Final" not in e["eventId"]]
    ordered = priority + rest

    return {
        "count": len(ordered),
        "horizonDays": horizonDays,
        "now": now.isoformat(),
        "events": ordered,
    }


# ---------------------------------------------------------------------------
# NOTE: `/api/predictions/event` used to return a softmax-ranked list of
# predicted winners with fair odds. That lived here until the Rating Builder
# took over all forecasting work. The endpoint + its `rank_teams` helper are
# gone; anything that still needs win probabilities should go through the
# Builder's Monte Carlo (see /builder and the publish-probs endpoint).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Rating Builder bridge — export the prediction pool as an importable
# Rating-Builder state (see C:\Users\sorry\sportsbook model\webapp). This lets
# the analyst tune the formula visually at /builder (same origin) with the
# pool, metrics, and per-player pedigree the prediction API sees.
# ---------------------------------------------------------------------------
@app.get("/api/predictions/rating-builder-export")
async def api_predictions_rating_builder_export(
    eventId: str = Query(...),
    region: Optional[str] = Query(None),
    windowId: Optional[str] = Query(None),
    halfLifeDays: float = Query(7.0, ge=1.0, le=60.0),
    minEvents: int = Query(1, ge=1, le=10),
    restrictToQualifiers: bool = Query(True),
    sourceMode: str = Query("qualification", description="'qualification' uses qualification graph; 'eventTop' uses top teams from the selected event snapshot."),
    topTeams: int = Query(50, ge=1, le=500),
    scale: str = Query(
        "percentile",
        description="'percentile' = pool percentile × 2 − 100 (median=0). "
                    "'zscore' = z·30 clamped ±100.",
    ),
    seedCurated: bool = Query(True, description="Pre-fill community-power-rank / expert-win-% for known top names."),
):
    """
    Build a Rating-Builder-importable JSON (30 variables) for the given
    upcoming event. Schema matches `exportFullJson()` in webapp/app.js.

    - One 'player' per accountId in the qualifier pool (per-player pedigree,
      expert priors, etc.)
    - One 'team' per duo/trio in the pool with correct memberIds.
    - 30 canonical variables across Form / Risk / Pedigree / Roster /
      Curated-prior buckets, every score mapped to the Builder's −100..+100.
    """
    from statistics import mean, pstdev

    target_region = region
    if target_region is None:
        for r in REGIONS:
            if eventId.endswith(f"_{r}"):
                target_region = r
                break

    all_snaps = _load_all_cached_snapshots()
    workspace = _load_qualification_workspace(eventId, target_region)
    source_mode = (sourceMode or "qualification").strip()
    restrict_keys = None
    if source_mode == "eventTop":
        pool = [
            snap for snap in all_snaps
            if (snap.get("eventId") or "") == eventId
            and (not target_region or (snap.get("region") or "") == target_region)
            and (not windowId or (snap.get("windowId") or "") == windowId)
            and snap.get("entries")
        ]
        if not pool:
            raise HTTPException(400, f"No cached leaderboard snapshot found for {eventId}{' / ' + windowId if windowId else ''}.")
        ranked_entries: List[Tuple[int, str]] = []
        for snap in pool:
            for entry in snap.get("entries") or []:
                key = _prediction_team_key(entry.get("accountIds") or [])
                if not key:
                    continue
                ranked_entries.append((_safe_int(entry.get("rank"), 10**9) or 10**9, key))
        restrict_keys = set()
        for _, key in sorted(ranked_entries, key=lambda item: item[0]):
            restrict_keys.add(key)
            if len(restrict_keys) >= topTeams:
                break
    else:
        pool = _prediction_pool_for_workspace(eventId, all_snaps, target_region=target_region, workspace=workspace)
        pool = [p for p in pool if p.get("entries")]
    if not pool:
        raise HTTPException(400, f"No qualifying-pool data cached for {eventId}. Backfill heats/LCQ first.")

    if source_mode != "eventTop" and restrictToQualifiers:
        audit = _qualification_audit(
            pool,
            event_id=eventId,
            region=target_region,
            workspace=workspace,
        )
        restrict_keys = {
            str(team.get("teamKey") or "").strip().lower()
            for team in audit.get("qualifiedTeams") or []
            if team.get("teamKey")
        }
    forms = build_team_forms(pool, half_life_days=halfLifeDays, restrict_to_keys=restrict_keys)
    forms = {k: f for k, f in forms.items() if (f.get("nEvents") or 0) >= minEvents}
    expected_roster_size = _expected_roster_size(eventId, forms)
    forms, roster_size_counts = _filter_forms_by_roster_size(forms, expected_roster_size)
    player_pedigree = build_pedigree(all_snaps, region=target_region)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _norm(values: List[float]) -> List[float]:
        """Map values to Builder's −100..+100 range."""
        n = len(values)
        if n == 0:
            return []
        if scale == "zscore":
            mu = mean(values)
            sd = pstdev(values) or 1e-9
            return [max(-100.0, min(100.0, ((v - mu) / sd) * 30.0)) for v in values]
        sorted_pairs = sorted(enumerate(values), key=lambda t: t[1])
        rank_of: Dict[int, float] = {}
        for r, (orig_idx, _) in enumerate(sorted_pairs):
            rank_of[orig_idx] = (r + 0.5) / n
        return [round(rank_of[i] * 200.0 - 100.0, 1) for i in range(n)]

    def _norm_inv(values: List[float]) -> List[float]:
        return [-v for v in _norm(values)]

    def _stdev(xs: List[float]) -> float:
        return pstdev(xs) if len(xs) > 1 else 0.0

    # ------------------------------------------------------------------
    # Per-team derived metrics (everything we can pull from `forms`)
    # ------------------------------------------------------------------
    team_keys = list(forms.keys())

    raw: Dict[str, List[float]] = {}
    def _put(name: str, values: List[float]) -> None:
        raw[name] = values

    _put("pts",        [float(forms[k].get("ptsPerMatch") or 0.0) for k in team_keys])
    _put("elim",       [float(forms[k].get("elimRate")    or 0.0) for k in team_keys])
    _put("vr",         [float(forms[k].get("vrRate")      or 0.0) for k in team_keys])
    _put("rank",       [float(forms[k].get("rankPct")     or 0.0) for k in team_keys])
    _put("cons",       [float(forms[k].get("consistency") or 0.0) for k in team_keys])
    _put("ptrend",     [float(forms[k].get("placementTrend") or 0.0) for k in team_keys])
    _put("etrend",     [float(forms[k].get("elimsTrend")     or 0.0) for k in team_keys])

    # Totals & richer stats
    _put("total_pts",   [float(forms[k].get("totalPts")   or 0.0) for k in team_keys])
    _put("total_elims", [float(forms[k].get("totalElims") or 0.0) for k in team_keys])
    _put("total_wins",  [float(forms[k].get("totalWins")  or 0.0) for k in team_keys])
    _put("n_events",    [float(forms[k].get("nEvents")    or 0.0) for k in team_keys])
    _put("sessions",    [float(forms[k].get("totalSessions") or 0.0) for k in team_keys])

    # Per-event derived: best single-event rank, placement stdev, avg top3/top10 rate
    best_rank: List[float] = []
    placement_stdev: List[float] = []
    pts_per_event_stdev: List[float] = []
    top3_rate: List[float] = []
    top10_rate: List[float] = []
    max_elim_game: List[float] = []
    min_pts_game: List[float] = []
    days_since_last: List[float] = []
    for k in team_keys:
        apps = forms[k].get("appearances") or []
        ranks = [a.get("rank") for a in apps if a.get("rank") is not None]
        best_rank.append(float(min(ranks)) if ranks else 9999.0)
        placement_stdev.append(_stdev([float(r) for r in ranks]) if ranks else 0.0)
        ppms = [float(a.get("ptsPerMatch") or 0.0) for a in apps]
        pts_per_event_stdev.append(_stdev(ppms))
        top3_rate.append(mean([float(a.get("top3Rate") or 0.0) for a in apps]) if apps else 0.0)
        top10_rate.append(mean([float(a.get("top10Rate") or 0.0) for a in apps]) if apps else 0.0)
        elim_rates = [float(a.get("elimRate") or 0.0) for a in apps]
        max_elim_game.append(max(elim_rates) if elim_rates else 0.0)
        min_pts_game.append(min(ppms) if ppms else 0.0)
        # days since most recent appearance
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc)
        ts_list: List[float] = []
        for a in apps:
            ts = a.get("updatedAt")
            try:
                if ts:
                    dt = _dt.fromisoformat(str(ts).replace("Z", "+00:00"))
                    ts_list.append((now - dt).total_seconds() / 86400.0)
            except Exception:
                pass
        days_since_last.append(min(ts_list) if ts_list else 30.0)
    _put("best_rank",         best_rank)  # LOWER is better → invert later
    _put("placement_stdev",   placement_stdev)  # LOWER is better
    _put("pts_event_stdev",   pts_per_event_stdev)  # LOWER = more consistent
    _put("top3_rate",         top3_rate)
    _put("top10_rate",        top10_rate)
    _put("max_elim_game",     max_elim_game)
    _put("min_pts_game",      min_pts_game)
    _put("days_since_last",   days_since_last)  # LOWER is better

    # Roster metadata
    roster_size: List[float] = []
    roster_stable: List[float] = []  # 100 if same accountIds across all appearances, else lower
    for k in team_keys:
        aids = forms[k].get("accountIds") or []
        roster_size.append(float(len(aids)))
        apps = forms[k].get("appearances") or []
        same = all((tuple(a.get("playerList") or []) == tuple(apps[0].get("playerList") or [])) for a in apps) if apps else True
        roster_stable.append(100.0 if same else 50.0)
    _put("roster_size",   roster_size)
    _put("roster_stable", roster_stable)

    # ------------------------------------------------------------------
    # Per-player pedigree derivatives
    # ------------------------------------------------------------------
    account_display: Dict[str, str] = {}
    account_team_keys: Dict[str, List[str]] = {}
    for k in team_keys:
        f = forms[k]
        ids = f.get("accountIds") or []
        names = []
        try:
            last_apps = (f.get("appearances") or [])
            if last_apps:
                names = last_apps[-1].get("playerList") or []
        except Exception:
            names = []
        if len(names) != len(ids):
            names = [""] * len(ids)
        for aid, nm in zip(ids, names):
            if aid not in account_display:
                account_display[aid] = nm or (aid[:8] + "…")
            account_team_keys.setdefault(aid, []).append(k)

    pool_account_ids = list(account_display.keys())
    ped_score:    List[float] = []
    ped_finals:   List[float] = []
    ped_majortop: List[float] = []
    ped_div1:     List[float] = []
    ped_best:     List[float] = []  # lower is better
    for aid in pool_account_ids:
        p = player_pedigree.get(aid) or {}
        ped_score.append(float(p.get("score") or 0.0))
        ped_finals.append(float(p.get("finalAppearances") or 0))
        ped_majortop.append(float(p.get("majorTopFinishes") or 0))
        ped_div1.append(float(p.get("div1TopFinishes") or 0))
        ped_best.append(float(p.get("bestFinishRank") or 9999))

    norm_ped_score   = dict(zip(pool_account_ids, _norm(ped_score)))
    norm_ped_finals  = dict(zip(pool_account_ids, _norm(ped_finals)))
    norm_ped_majtop  = dict(zip(pool_account_ids, _norm(ped_majortop)))
    norm_ped_div1    = dict(zip(pool_account_ids, _norm(ped_div1)))
    norm_ped_best    = dict(zip(pool_account_ids, _norm_inv(ped_best)))

    # ------------------------------------------------------------------
    # Curated priors (known NAC / EU names → expert win %, power rank, tier).
    # ------------------------------------------------------------------
    # NOTE: seeds are CONSERVATIVE starting points — user edits in Builder.
    # Match on case-insensitive substring of the player's display name.
    # Format: { "substring": { "win%": 0-100, "power": -100..100, "tier": -100..100, "clutch": -100..100 } }
    # All values are DIRECT Builder scores in -100..+100 range.
    # 0 = neutral (same as uncurated). Positive = better.
    # Peterbot is explicitly seeded for >50% win probability;
    # Clix for ~20% — matching the user's baseline intuition.
    KNOWN: Dict[str, Dict[str, float]] = {
        # Tier S — NAC (duo partners get the SAME seed so the team
        # average isn't halved by one member being unlisted)
        "peterbot":   {"win": 90, "power": 98, "tier": 90, "clutch": 85},
        "polla":      {"win": 90, "power": 95, "tier": 88, "clutch": 80},  # peterbot's duo
        "pollo":      {"win": 90, "power": 95, "tier": 88, "clutch": 80},
        "clix":       {"win": 65, "power": 88, "tier": 80, "clutch": 80},
        "higgs":      {"win": 65, "power": 82, "tier": 75, "clutch": 70},  # clix's duo
        # Tier A — NAC
        "th0mas":     {"win": 40, "power": 75, "tier": 65, "clutch": 65},
        "thomashd":   {"win": 40, "power": 75, "tier": 65, "clutch": 65},
        "bugha":      {"win": 35, "power": 72, "tier": 62, "clutch": 80},
        "khanada":    {"win": 30, "power": 68, "tier": 58, "clutch": 55},
        "mero":       {"win": 30, "power": 68, "tier": 58, "clutch": 50},
        "eomzo":      {"win": 28, "power": 70, "tier": 58, "clutch": 55},
        "reverse2k":  {"win": 22, "power": 55, "tier": 45, "clutch": 45},
        "epikwhale":  {"win": 22, "power": 55, "tier": 45, "clutch": 50},
        # Tier B
        "riversan":   {"win": 15, "power": 45, "tier": 35, "clutch": 35},
        "reet":       {"win": 15, "power": 43, "tier": 35, "clutch": 35},
        "stretch":    {"win": 15, "power": 43, "tier": 35, "clutch": 35},
        "cented":     {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "avery":      {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "pinq":       {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "malibuca":   {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "jannisz":    {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "muz":        {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "acorn":      {"win": 12, "power": 40, "tier": 30, "clutch": 30},
        "pauqz":      {"win":  8, "power": 30, "tier": 22, "clutch": 22},
        "rehx":       {"win":  8, "power": 30, "tier": 22, "clutch": 22},
        "chapix":     {"win":  8, "power": 30, "tier": 22, "clutch": 22},
        "andilex":    {"win":  8, "power": 30, "tier": 22, "clutch": 22},
        "dubs":       {"win":  6, "power": 25, "tier": 18, "clutch": 18},
        # EU names (lighter seeds in case the user exports EU finals)
        "tayson":     {"win": 65, "power": 88, "tier": 80, "clutch": 80},
        "mrsavage":   {"win": 35, "power": 75, "tier": 65, "clutch": 65},
        "mitr0":      {"win": 30, "power": 70, "tier": 60, "clutch": 60},
        "benjyfishy": {"win": 25, "power": 65, "tier": 55, "clutch": 55},
        "queasy":     {"win": 22, "power": 60, "tier": 50, "clutch": 50},
        "anas":       {"win": 22, "power": 55, "tier": 50, "clutch": 50},
        "k1nzell":    {"win": 22, "power": 55, "tier": 45, "clutch": 45},
        "th0masfn":   {"win": 22, "power": 55, "tier": 45, "clutch": 45},
    }

    # Fold Cyrillic / mojibaked look-alikes back to ASCII so "pollo" and
    # "ajerss" style names match even when Osirion returns Cyrillic о etc.
    _UNFOLD_SINGLE = {
        "\u0430": "a", "\u0435": "e", "\u043e": "o", "\u0440": "p",
        "\u0441": "c", "\u0445": "x", "\u0443": "y", "\u0456": "i",
        "\u0451": "e", "\u043a": "k", "\u0455": "s",
    }
    _UNFOLD_DOUBLE = {  # latin-1 rendering of utf-8 cyrillic bytes
        "\u00d0\u00b0": "a", "\u00d0\u00b5": "e", "\u00d0\u00be": "o",
        "\u00d1\u0080": "p", "\u00d1\u0081": "c", "\u00d1\u0083": "y",
        "\u00d1\u0085": "x",
    }
    def _fold(s: str) -> str:
        s = s.lower()
        for k, v in _UNFOLD_DOUBLE.items():
            s = s.replace(k, v)
        for k, v in _UNFOLD_SINGLE.items():
            s = s.replace(k, v)
        return s

    def _match_known(name: str) -> Optional[Dict[str, float]]:
        if not seedCurated or not name:
            return None
        low = _fold(name)
        for sub, vals in KNOWN.items():
            if sub in low:
                return vals
        return None

    # Raw curated values (0-100 in KNOWN, 0 for uncurated)
    raw_curated_win:    Dict[str, float] = {aid: 0.0 for aid in account_display}
    raw_curated_power:  Dict[str, float] = {aid: 0.0 for aid in account_display}
    raw_curated_tier:   Dict[str, float] = {aid: 0.0 for aid in account_display}
    raw_curated_clutch: Dict[str, float] = {aid: 0.0 for aid in account_display}
    curated_hits = 0
    for aid, disp in account_display.items():
        hit = _match_known(disp)
        if hit:
            curated_hits += 1
            raw_curated_win[aid]    = float(hit.get("win", 0))
            raw_curated_power[aid]  = float(hit.get("power", 0))
            raw_curated_tier[aid]   = float(hit.get("tier", 0))
            raw_curated_clutch[aid] = float(hit.get("clutch", 0))

    # ------------------------------------------------------------------
    # External / soft-data priors (landing spots, injuries, scrims, ...)
    #
    # Loaded from data/landing_spots.json (ranked POI quality scores) and
    # data/team_soft_data.json (per-team overlay). Resolver tries:
    #   1) accountIds hash   : "aid:" + "+".join(sorted(ids))
    #   2) folded display-names: _fold(p1) + "+" + _fold(p2)
    #   3) permuted fold     : both orderings
    #   4) default (all zeros)
    # Missing files / missing rows just leave v31..v36 at 0.
    # ------------------------------------------------------------------
    DATA_DIR = ROOT / "data"
    _landing_quality: Dict[str, float] = {}
    try:
        lp = DATA_DIR / "landing_spots.json"
        if lp.exists():
            lj = json.loads(lp.read_text(encoding="utf-8"))
            for spot in (lj.get("spots") or []):
                nm = str(spot.get("name") or "").strip().lower()
                if nm:
                    _landing_quality[nm] = float(spot.get("quality") or 0.0)
    except Exception as e:
        logger.warning(f"[rb-export] failed to load landing_spots.json: {e}")

    _team_soft_raw: Dict[str, Dict[str, Any]] = {}
    try:
        tp = DATA_DIR / "team_soft_data.json"
        if tp.exists():
            tj = json.loads(tp.read_text(encoding="utf-8"))
            if isinstance(tj, dict):
                for k, v in tj.items():
                    if k.startswith("__") or not isinstance(v, dict):
                        continue
                    _team_soft_raw[k.lower().strip()] = v
    except Exception as e:
        logger.warning(f"[rb-export] failed to load team_soft_data.json: {e}")

    # --- Signal-applied overlay merge (Phase 5) ---------------------------
    # data/soft_overlay.json carries TTL-bounded override rows written by
    # /api/signals/{id}/apply. These stack ON TOP of team_soft_data.json,
    # with v31..v36_override values winning the final score. Expired rows
    # are pruned by _read_overlay() on read.
    _active_overlays: Dict[str, Dict[str, Any]] = {}
    try:
        _raw_overlay = _read_overlay()
        for k, v in _raw_overlay.items():
            if not isinstance(v, dict):
                continue
            k_low = k.lower().strip()
            merged = dict(_team_soft_raw.get(k_low) or {})
            for field in ("landing_spot", "contested_by", "injury_flag",
                          "scrim_form", "lan_experience", "poi_meta_fit",
                          "note", "roster_change_flag"):
                if field in v and v[field] not in (None, ""):
                    merged[field] = v[field]
            if v.get("note_overlay"):
                existing_note = merged.get("note") or ""
                merged["note"] = (existing_note + " | " if existing_note else "") + str(v["note_overlay"])
            merged["__overlay_applied"] = True
            merged["__overlay_source"] = v.get("appliedBy")
            merged["__overlay_expires_at"] = v.get("expiresAt")
            for ovkey in ("v31_override", "v32_override", "v33_override",
                          "v34_override", "v35_override", "v36_override"):
                if ovkey in v and v[ovkey] is not None:
                    merged[ovkey] = v[ovkey]
            _team_soft_raw[k_low] = merged
            _active_overlays[k_low] = v
    except Exception as e:
        logger.warning(f"[rb-export] overlay merge failed: {e}")

    def _soft_team_keys(aids: List[str], names: List[str]) -> List[str]:
        """Candidate keys to look up in team_soft_data (in priority order)."""
        out: List[str] = []
        ids = [str(a).lower().strip() for a in (aids or []) if a]
        if ids:
            out.append("aid:" + "+".join(sorted(ids)))
        folded = [_fold(n).strip() for n in (names or []) if n]
        folded = [f for f in folded if f]
        if folded:
            out.append("+".join(folded))
            out.append("+".join(sorted(folded)))
            if len(folded) == 2:
                out.append(f"{folded[1]}+{folded[0]}")
        # Also try any single-name match (e.g. "peterbot+pollo" keyed when
        # only one partner's name is in the file for whatever reason).
        for fname in folded:
            out.append(fname)
        # Dedup preserving order
        seen, uniq = set(), []
        for k in out:
            if k and k not in seen:
                uniq.append(k); seen.add(k)
        return uniq

    def _soft_for_team(aids: List[str], names: List[str]) -> Optional[Dict[str, Any]]:
        for k in _soft_team_keys(aids, names):
            if k in _team_soft_raw:
                return _team_soft_raw[k]
            # Also match composite substring keys (so "peterbot+pollo" hits
            # when team is "falcon peterbotǃ+falcоn pоllоǃ" via fold).
            for stored in _team_soft_raw:
                if "+" in stored and all(part in k for part in stored.split("+")):
                    return _team_soft_raw[stored]
        return None

    def _clamp(x: float) -> float:
        return max(-100.0, min(100.0, float(x)))

    team_soft_scores: Dict[str, Dict[str, float]] = {}
    team_soft_resolved: Dict[str, Dict[str, Any]] = {}
    soft_hits = 0
    for k in team_keys:
        f = forms[k]
        aids = f.get("accountIds") or []
        last_apps = f.get("appearances") or []
        names = (last_apps[-1].get("playerList") if last_apps else []) or []
        row = _soft_for_team(aids, names)
        if row:
            soft_hits += 1
            landing_name = str(row.get("landing_spot") or "").strip().lower()
            landing_q = _landing_quality.get(landing_name, 0.0) if landing_name else 0.0
            contested = row.get("contested_by")
            if contested is None:
                contest_count = 0
            elif isinstance(contested, list):
                contest_count = len(contested)
            else:
                contest_count = 1 if contested else 0
            scores_row = {
                "v31": _clamp(landing_q),
                "v32": _clamp(-contest_count * 30.0),
                "v33": -100.0 if row.get("injury_flag") else 0.0,
                "v34": _clamp(row.get("scrim_form", 0.0)),
                "v35": _clamp(row.get("lan_experience", 0.0)),
                "v36": _clamp(row.get("poi_meta_fit", 0.0)),
            }
            # Signal-overlay v31..v36_override values win outright over the
            # baseline computed above. This is how applied signals flow
            # into the Builder: the rating-builder-export JSON changes,
            # which the Builder picks up on its next refresh.
            for vid in ("v31", "v32", "v33", "v34", "v35", "v36"):
                ovk = vid + "_override"
                if ovk in row and row[ovk] is not None:
                    try:
                        scores_row[vid] = _clamp(float(row[ovk]))
                    except Exception:
                        pass
            team_soft_resolved[k] = {
                "landing_spot": row.get("landing_spot"),
                "landing_quality": landing_q,
                "contested_by": contested,
                "injury_flag": bool(row.get("injury_flag") or False),
                "scrim_form": row.get("scrim_form"),
                "lan_experience": row.get("lan_experience"),
                "poi_meta_fit": row.get("poi_meta_fit"),
                "note": row.get("note"),
                "display": f.get("display") or " · ".join(names),
                "overlayApplied": bool(row.get("__overlay_applied")),
                "overlaySource": row.get("__overlay_source"),
                "overlayExpiresAt": row.get("__overlay_expires_at"),
            }
        else:
            scores_row = {"v31": 0.0, "v32": 0.0, "v33": 0.0, "v34": 0.0, "v35": 0.0, "v36": 0.0}
        team_soft_scores[k] = scores_row

    logger.info(
        "[rb-export] soft-data resolved: %d of %d teams (landing_spots=%d names, team_soft rows=%d)",
        soft_hits, len(team_keys), len(_landing_quality), len(_team_soft_raw),
    )

    # KNOWN values are ALREADY in -100..+100 Builder-score range.
    # Uncurated players remain at 0 (neutral, mid-distribution). Curated
    # players carry their signed seed — Peterbot v28=+90 gives the mode
    # calc enough lift to make him the true Monte-Carlo favorite.
    curated_win    = raw_curated_win
    curated_power  = raw_curated_power
    curated_tier   = raw_curated_tier
    curated_clutch = raw_curated_clutch

    # ------------------------------------------------------------------
    # Normalize every per-team metric across the pool
    # ------------------------------------------------------------------
    norms: Dict[str, List[float]] = {}
    norms["n_pts"]         = _norm(raw["pts"])
    norms["n_elim"]        = _norm(raw["elim"])
    norms["n_vr"]          = _norm(raw["vr"])
    norms["n_rank"]        = _norm(raw["rank"])
    norms["n_cons"]        = _norm(raw["cons"])
    norms["n_ptrend"]      = _norm(raw["ptrend"])
    norms["n_etrend"]      = _norm(raw["etrend"])
    norms["n_total_pts"]   = _norm(raw["total_pts"])
    norms["n_total_elims"] = _norm(raw["total_elims"])
    norms["n_total_wins"]  = _norm(raw["total_wins"])
    norms["n_n_events"]    = _norm(raw["n_events"])
    norms["n_sessions"]    = _norm(raw["sessions"])
    norms["n_best_rank"]   = _norm_inv(raw["best_rank"])            # lower rank = better
    norms["n_plc_stdev"]   = _norm_inv(raw["placement_stdev"])      # lower stdev = better
    norms["n_pts_stdev"]   = _norm_inv(raw["pts_event_stdev"])      # lower stdev = better
    norms["n_top3"]        = _norm(raw["top3_rate"])
    norms["n_top10"]       = _norm(raw["top10_rate"])
    norms["n_max_elim"]    = _norm(raw["max_elim_game"])
    norms["n_min_pts"]     = _norm(raw["min_pts_game"])             # higher floor = better
    norms["n_recency"]     = _norm_inv(raw["days_since_last"])      # recent = better
    norms["n_roster_stb"]  = _norm(raw["roster_stable"])
    # roster_size: 2 vs 3 is binary in practice; we just push it through _norm
    norms["n_roster_sz"]   = _norm(raw["roster_size"])

    # team_key -> pool index
    tk_index: Dict[str, int] = {k: i for i, k in enumerate(team_keys)}

    # ------------------------------------------------------------------
    # 30 variable definitions
    # ------------------------------------------------------------------
    VAR_DEFS = [
        # --- Form (9)
        ("v1",  "Points / match",            "form"),
        ("v2",  "Elim rate",                 "form"),
        ("v3",  "Victory Royale rate",       "form"),
        ("v4",  "Rank percentile",           "form"),
        ("v5",  "Consistency (1 − CV)",      "form"),
        ("v6",  "Placement trend",           "form"),
        ("v7",  "Elim trend",                "form"),
        ("v8",  "Best single-event rank",    "form"),
        ("v9",  "Qualifier cumulative pts",  "form"),
        # --- Volume / experience (4)
        ("v10", "Qualifier matches played",  "volume"),
        ("v11", "Events participated in",    "volume"),
        ("v12", "Total elims in qualifier",  "volume"),
        ("v13", "Total VRs in qualifier",    "volume"),
        # --- Risk / variance (5)
        ("v14", "Placement stdev",           "risk"),
        ("v15", "Pts/match stdev",           "risk"),
        ("v16", "Top-3 game rate",           "risk"),
        ("v17", "Top-10 game rate",          "risk"),
        ("v18", "Pts-floor (worst event)",   "risk"),
        # --- Pedigree / career (5)
        ("v19", "FNCS pedigree score",       "pedigree"),
        ("v20", "Grand-final appearances",   "pedigree"),
        ("v21", "Major top-10 finishes",     "pedigree"),
        ("v22", "Div-1 top-10 finishes",     "pedigree"),
        ("v23", "Best career finish",        "pedigree"),
        # --- Roster / meta (3)
        ("v24", "Roster stability",          "roster"),
        ("v25", "Roster size (duo/trio)",    "roster"),
        ("v26", "Recency (days since play)", "roster"),
        # --- Curated expert priors (4)
        ("v27", "Community power rank",      "curated"),
        ("v28", "Expert win probability %",  "curated"),
        ("v29", "Analyst tier",              "curated"),
        ("v30", "Clutch reputation",         "curated"),
        # --- External / soft data (6) — curated per-event from
        #     data/landing_spots.json + data/team_soft_data.json
        ("v31", "Landing spot quality",      "external"),
        ("v32", "Landing contest risk",      "external"),
        ("v33", "Injury / absence flag",     "external"),
        ("v34", "Scrim / ranked form (7d)",  "external"),
        ("v35", "LAN experience",            "external"),
        ("v36", "POI meta fit (patch)",      "external"),
    ]
    variables = [{"id": vid, "name": vname} for vid, vname, _cat in VAR_DEFS]

    # ------------------------------------------------------------------
    # Build players + scores
    # ------------------------------------------------------------------
    players: List[Dict[str, Any]] = []
    scores: Dict[str, Dict[str, float]] = {}
    account_to_pid: Dict[str, str] = {}
    next_pid = 1

    for aid, disp in account_display.items():
        pid = f"p{next_pid}"
        next_pid += 1
        account_to_pid[aid] = pid
        players.append({"id": pid, "name": disp})
        tk = account_team_keys.get(aid, [None])[0]
        i = tk_index.get(tk, -1) if tk else -1
        def _pull(name: str) -> float:
            arr = norms.get(name) or []
            return round(arr[i], 1) if 0 <= i < len(arr) else 0.0

        # per-player pedigree-derived z's (not per-team)
        s = {
            # Form (v1..v9)
            "v1":  _pull("n_pts"),
            "v2":  _pull("n_elim"),
            "v3":  _pull("n_vr"),
            "v4":  _pull("n_rank"),
            "v5":  _pull("n_cons"),
            "v6":  _pull("n_ptrend"),
            "v7":  _pull("n_etrend"),
            "v8":  _pull("n_best_rank"),
            "v9":  _pull("n_total_pts"),
            # Volume (v10..v13)
            "v10": _pull("n_sessions"),
            "v11": _pull("n_n_events"),
            "v12": _pull("n_total_elims"),
            "v13": _pull("n_total_wins"),
            # Risk (v14..v18)
            "v14": _pull("n_plc_stdev"),
            "v15": _pull("n_pts_stdev"),
            "v16": _pull("n_top3"),
            "v17": _pull("n_top10"),
            "v18": _pull("n_min_pts"),
            # Pedigree (v19..v23) — PER-PLAYER
            "v19": round(norm_ped_score.get(aid, 0.0),  1),
            "v20": round(norm_ped_finals.get(aid, 0.0), 1),
            "v21": round(norm_ped_majtop.get(aid, 0.0), 1),
            "v22": round(norm_ped_div1.get(aid, 0.0),   1),
            "v23": round(norm_ped_best.get(aid, 0.0),   1),
            # Roster / recency (v24..v26) — inherit from team
            "v24": _pull("n_roster_stb"),
            "v25": _pull("n_roster_sz"),
            "v26": _pull("n_recency"),
            # Curated priors (v27..v30) — per-player, seeded for known names
            "v27": round(curated_power.get(aid,  0.0),  1),
            "v28": round(curated_win.get(aid,    0.0),  1),
            "v29": round(curated_tier.get(aid,   0.0),  1),
            "v30": round(curated_clutch.get(aid, 0.0),  1),
            # External soft data (v31..v36) — team-level, same for both
            # players on a duo. Sourced from data/team_soft_data.json.
            "v31": round((team_soft_scores.get(tk) or {}).get("v31", 0.0), 1),
            "v32": round((team_soft_scores.get(tk) or {}).get("v32", 0.0), 1),
            "v33": round((team_soft_scores.get(tk) or {}).get("v33", 0.0), 1),
            "v34": round((team_soft_scores.get(tk) or {}).get("v34", 0.0), 1),
            "v35": round((team_soft_scores.get(tk) or {}).get("v35", 0.0), 1),
            "v36": round((team_soft_scores.get(tk) or {}).get("v36", 0.0), 1),
        }
        scores[pid] = s

    # ------------------------------------------------------------------
    # Build teams list
    # ------------------------------------------------------------------
    teams_out: List[Dict[str, Any]] = []
    next_tid = 1
    for k in team_keys:
        f = forms[k]
        aids = f.get("accountIds") or []
        memberIds = [account_to_pid[a] for a in aids if a in account_to_pid]
        if not memberIds:
            continue
        per = round(100.0 / len(memberIds), 2)
        mw: Dict[str, Dict[str, float]] = {}
        for vid, _, _ in VAR_DEFS:
            mw[vid] = {mid: per for mid in memberIds}
        tid = f"t{next_tid}"
        next_tid += 1
        teams_out.append(
            {
                "id": tid,
                "name": f.get("display") or " · ".join(
                    account_display.get(a, "?") for a in aids
                ),
                "memberIds": memberIds,
                "memberWeights": mw,
                # Stable identifiers the Lab's Drop Map + soft-overlay
                # machinery needs to link back to a team. Rating Builder
                # itself ignores unknown fields.
                "teamKey": k,
                "accountIds": list(aids),
            }
        )

    pedigreed_count = sum(1 for v in ped_score if v > 0)

    # ------------------------------------------------------------------
    # Default Builder category mapping (Mode / Spread / Skew / Kurtosis /
    # Bimodal / Mode-2) → variables with weights. Auto-populates the
    # purple/black Builder page so the user doesn't have to drag/drop.
    # Weights are signed -100..+100: positive = this variable pushes the
    # category UP, negative = DOWN.
    # ------------------------------------------------------------------
    builder_output = {
        "categories": [
            {
                "id": "mode",            # distribution peak location (1 = winner, 50 = last)
                "name": "Mode (Location)",
                "assignments": [
                    # Expert priors dominate; raw qualifier form (v1, v9) is
                    # intentionally excluded because pros like Peterbot only
                    # sweat ONE heat (low pts) yet remain the favorite.
                    {"varId": "v28", "weight":  100},  # Expert win %
                    {"varId": "v27", "weight":   90},  # Community power rank
                    {"varId": "v19", "weight":   60},  # FNCS pedigree score
                    {"varId": "v29", "weight":   55},  # Analyst tier
                    {"varId": "v20", "weight":   40},  # Grand-final appearances
                    {"varId": "v21", "weight":   30},  # Major top-10 finishes
                    {"varId": "v23", "weight":   25},  # Best career finish
                    {"varId": "v4",  "weight":   15},  # Rank percentile (light)
                    # External / soft data
                    {"varId": "v31", "weight":   30},  # Landing spot quality
                    {"varId": "v34", "weight":   25},  # Scrim / ranked form (7d)
                    {"varId": "v35", "weight":   20},  # LAN experience
                    {"varId": "v36", "weight":   15},  # POI meta fit
                    {"varId": "v33", "weight":  -80},  # Injury / absence flag (big negative)
                ],
            },
            {
                "id": "spread",          # wider = more variance
                "name": "Spread (Variance)",
                "assignments": [
                    # v14/v15 are ALREADY INVERTED (high score = consistent),
                    # so negative weights here produce a narrower distribution
                    # for consistent teams. v5 (consistency) is direct.
                    {"varId": "v14", "weight":  -60},  # Placement stdev (inverted)
                    {"varId": "v15", "weight":  -40},  # Pts/match stdev (inverted)
                    {"varId": "v5",  "weight":  -50},  # Consistency
                    {"varId": "v28", "weight":  -60},  # Expert prior → narrower σ for pros
                    {"varId": "v2",  "weight":   30},  # Elim rate (aggression = variance)
                    # External / soft data
                    {"varId": "v32", "weight":   25},  # Landing contest risk (contested = variance)
                    {"varId": "v33", "weight":   40},  # Injury / absence flag (wild card)
                ],
            },
            {
                "id": "upperSkew",       # overperformance / clutch upside
                "name": "Upper Skew",
                "assignments": [
                    {"varId": "v30", "weight":   70},  # Clutch reputation
                    {"varId": "v21", "weight":   45},  # Major top-10 finishes
                    {"varId": "v23", "weight":   40},  # Best career finish
                    {"varId": "v20", "weight":   30},  # Grand-final appearances
                    {"varId": "v35", "weight":   25},  # LAN experience (upside on stage)
                ],
            },
            {
                "id": "lowerSkew",       # downside / choke risk
                "name": "Lower Skew",
                "assignments": [
                    {"varId": "v18", "weight":  -30},  # Pts floor (high floor = less downside)
                    {"varId": "v5",  "weight":  -30},  # Consistency (consistent = less choke)
                    {"varId": "v24", "weight":  -25},  # Roster stability (stable = less choke)
                    {"varId": "v33", "weight":  -30},  # Injury flag (heavier left tail)
                    {"varId": "v32", "weight":  -15},  # Contest risk (worst-case early out)
                ],
            },
            {
                "id": "kurtosis",        # fat tails
                "name": "Tail Weight",
                "assignments": [
                    {"varId": "v2",  "weight":   35},  # Elim rate
                    {"varId": "v5",  "weight":  -30},  # Consistency (negative = thin tails)
                ],
            },
            {
                "id": "bimodalStrength", # feast-or-famine (w-key / contested POI)
                "name": "Bimodal Strength",
                "assignments": [
                    {"varId": "v2",  "weight":   50},  # Elim rate = w-key factor
                    {"varId": "v12", "weight":   30},  # Total elims
                    {"varId": "v16", "weight":   20},  # Top-3 game rate
                ],
            },
            {
                "id": "mode2",           # pop-off scenario peak
                "name": "Mode 2 Location",
                "assignments": [
                    {"varId": "v30", "weight":   70},  # Clutch reputation
                    {"varId": "v21", "weight":   40},  # Major top-10
                ],
            },
        ],
        "manualParams": {
            # Everyone in the Final can theoretically win (ceiling=1) and
            # theoretically last-place (floor=50). User can tighten per team.
            "ceiling": 1,
            "floor": 50,
            "deadZoneEnabled": False,
            "deadZoneStart": 0,
            "deadZoneEnd": 0,
        },
    }

    return {
        "version": 2,
        "builderOutput": builder_output,
        "__source": {
            "eventId": eventId,
            "region": target_region,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "params": {
                "halfLifeDays": halfLifeDays,
                "minEvents": minEvents,
                "restrictToQualifiers": restrictToQualifiers,
                "sourceMode": source_mode,
                "topTeams": topTeams if source_mode == "eventTop" else None,
                "windowId": windowId,
                "scale": scale,
                "seedCurated": seedCurated,
            },
            "stats": {
                "teamsInPool": len(team_keys),
                "playersInPool": len(players),
                "pedigreedPlayers": pedigreed_count,
                "curatedMatches": curated_hits,
                "sourceEvents": len(pool),
                "variableCount": len(VAR_DEFS),
                "expectedRosterSize": expected_roster_size,
                "rosterSizeCounts": roster_size_counts,
            },
            "variableCategories": {cat: [vid for vid, _, c in VAR_DEFS if c == cat] for cat in {c for _, _, c in VAR_DEFS}},
            "variableLegend": {
                "v1":  "recency-weighted pts/match (percentile)",
                "v2":  "recency-weighted elim rate",
                "v3":  "recency-weighted VR rate",
                "v4":  "recency-weighted rank percentile",
                "v5":  "1 − min(1, placementCV/2.5)",
                "v6":  "placement trend (older → newer slope)",
                "v7":  "elim trend",
                "v8":  "best rank in any source event (inverted)",
                "v9":  "cumulative points across qualifier stage",
                "v10": "matches played in qualifier stage",
                "v11": "source events participated in",
                "v12": "total elims in qualifier",
                "v13": "total Victory Royales in qualifier",
                "v14": "stdev of placement across events (inverted)",
                "v15": "stdev of pts/match across events (inverted)",
                "v16": "avg top-3 match rate",
                "v17": "avg top-10 match rate",
                "v18": "worst-event pts/match (floor)",
                "v19": "per-player FNCS pedigree score",
                "v20": "lifetime grand-final appearances",
                "v21": "lifetime major top-10 finishes",
                "v22": "lifetime division-1 top-10 finishes",
                "v23": "best career finish rank (inverted)",
                "v24": "roster stability across qualifier",
                "v25": "roster size (2=duo, 3=trio)",
                "v26": "days since most recent appearance (inverted)",
                "v27": "curated community power rank (seeded for known names)",
                "v28": "curated expert win probability % (seeded)",
                "v29": "curated analyst tier S/A/B/C (seeded)",
                "v30": "curated clutch / LAN reputation (seeded)",
                "v31": "landing spot quality from data/landing_spots.json",
                "v32": "landing contest risk (negative = contested drop)",
                "v33": "injury / absence flag (-100 = out, 0 = nominal)",
                "v34": "scrim / ranked form (7d) from team_soft_data.json",
                "v35": "LAN / big-stage experience",
                "v36": "POI meta fit (current patch)",
            },
            "softData": {
                "resolvedTeams": soft_hits,
                "totalTeams": len(team_keys),
                "landingSpotCount": len(_landing_quality),
                "teamSoftRows": len(_team_soft_raw),
                "activeOverlays": [
                    {
                        "storageKey": k,
                        "appliedBy": v.get("appliedBy"),
                        "appliedAt": v.get("appliedAt"),
                        "expiresAt": v.get("expiresAt"),
                        "subject": v.get("subject"),
                    }
                    for k, v in _active_overlays.items()
                ],
                "overlayRev": _overlay_rev(),
                "resolved": [
                    {
                        "teamKey": k,
                        "display": v.get("display"),
                        "landing_spot": v.get("landing_spot"),
                        "landing_quality": v.get("landing_quality"),
                        "contested_by": v.get("contested_by"),
                        "injury_flag": v.get("injury_flag"),
                        "scrim_form": v.get("scrim_form"),
                        "lan_experience": v.get("lan_experience"),
                        "poi_meta_fit": v.get("poi_meta_fit"),
                        "note": v.get("note"),
                        "overlayApplied": v.get("overlayApplied") or False,
                        "overlaySource": v.get("overlaySource"),
                        "overlayExpiresAt": v.get("overlayExpiresAt"),
                    }
                    for k, v in team_soft_resolved.items()
                ],
            },
        },
        "variables": variables,
        "players": players,
        "teams": teams_out,
        "scores": scores,
        "nextVariableId": len(VAR_DEFS) + 1,
        "nextPlayerId": next_pid,
        "nextTeamId": next_tid,
    }


@app.get("/api/predictions/soft-data")
async def api_predictions_soft_data(
    eventId: str = Query(..., description="Target event, e.g. epicgames_S40_FNCSMajor1_Final_NAC"),
    region: Optional[str] = Query(None),
):
    """
    Read-only preview of the curated landing-spot + team soft-data overlay
    for the current 50-team pool. Used by the Prediction tab's soft-data
    pill/table so you can see at a glance which teams still need a row
    in data/team_soft_data.json.

    Returns:
        {
          "totalTeams": 50,
          "resolvedTeams": 3,
          "landingSpots": [...],       # full ranked list
          "teams": [                   # one row per pool team
            { "teamKey": "...", "display": "...",
              "landing_spot": "Castle Pleasant", "landing_quality": 85,
              "contested_by": null, "injury_flag": false,
              "scrim_form": 85, "lan_experience": 90, "poi_meta_fit": 70,
              "note": "peaking form", "resolved": true }
          ]
        }
    """
    all_snaps = _load_all_cached_snapshots()
    pool = resolve_prediction_pool(eventId, all_snaps, target_region=region)
    pool = [p for p in pool if p.get("entries")]
    if not pool:
        raise HTTPException(400, f"No qualifying-pool data cached for {eventId}.")

    restrict_keys = qualifier_team_keys(pool, event_id=eventId, region=region)
    forms = build_team_forms(pool, half_life_days=7.0, restrict_to_keys=restrict_keys)
    expected_roster_size = _expected_roster_size(eventId, forms)
    forms, roster_size_counts = _filter_forms_by_roster_size(forms, expected_roster_size)

    # Load curated files
    DATA_DIR = ROOT / "data"
    landing_spots: List[Dict[str, Any]] = []
    landing_quality: Dict[str, float] = {}
    try:
        lp = DATA_DIR / "landing_spots.json"
        if lp.exists():
            lj = json.loads(lp.read_text(encoding="utf-8"))
            landing_spots = lj.get("spots") or []
            for spot in landing_spots:
                nm = str(spot.get("name") or "").strip().lower()
                if nm:
                    landing_quality[nm] = float(spot.get("quality") or 0.0)
    except Exception as e:
        logger.warning(f"[soft-data] failed to load landing_spots.json: {e}")

    team_soft_raw: Dict[str, Dict[str, Any]] = {}
    try:
        tp = DATA_DIR / "team_soft_data.json"
        if tp.exists():
            tj = json.loads(tp.read_text(encoding="utf-8"))
            if isinstance(tj, dict):
                for k, v in tj.items():
                    if k.startswith("__") or not isinstance(v, dict):
                        continue
                    team_soft_raw[k.lower().strip()] = v
    except Exception as e:
        logger.warning(f"[soft-data] failed to load team_soft_data.json: {e}")

    # Local fold (duplicate of the helper inside rating-builder-export
    # so this endpoint is self-contained).
    _UNFOLD_SINGLE = {
        "\u0430": "a", "\u0435": "e", "\u043e": "o", "\u0440": "p",
        "\u0441": "c", "\u0445": "x", "\u0443": "y", "\u0456": "i",
        "\u0451": "e", "\u043a": "k", "\u0455": "s",
    }
    _UNFOLD_DOUBLE = {
        "\u00d0\u00b0": "a", "\u00d0\u00b5": "e", "\u00d0\u00be": "o",
        "\u00d1\u0080": "p", "\u00d1\u0081": "c", "\u00d1\u0083": "y",
        "\u00d1\u0085": "x",
    }
    def _fold(s: str) -> str:
        s = (s or "").lower()
        for k, v in _UNFOLD_DOUBLE.items():
            s = s.replace(k, v)
        for k, v in _UNFOLD_SINGLE.items():
            s = s.replace(k, v)
        return s

    def _candidates(aids: List[str], names: List[str]) -> List[str]:
        out: List[str] = []
        ids = [str(a).lower().strip() for a in (aids or []) if a]
        if ids:
            out.append("aid:" + "+".join(sorted(ids)))
        folded = [_fold(n).strip() for n in (names or []) if n]
        folded = [f for f in folded if f]
        if folded:
            out.append("+".join(folded))
            out.append("+".join(sorted(folded)))
            if len(folded) == 2:
                out.append(f"{folded[1]}+{folded[0]}")
        for fname in folded:
            out.append(fname)
        seen, uniq = set(), []
        for k in out:
            if k and k not in seen:
                uniq.append(k); seen.add(k)
        return uniq

    def _lookup(aids: List[str], names: List[str]) -> Optional[Dict[str, Any]]:
        for k in _candidates(aids, names):
            if k in team_soft_raw:
                return team_soft_raw[k]
            for stored in team_soft_raw:
                if "+" in stored and all(part in k for part in stored.split("+")):
                    return team_soft_raw[stored]
        return None

    teams_out: List[Dict[str, Any]] = []
    resolved = 0
    for k, f in forms.items():
        aids = f.get("accountIds") or []
        last_apps = f.get("appearances") or []
        names = (last_apps[-1].get("playerList") if last_apps else []) or []
        display = f.get("display") or " · ".join(names) or "?"
        row = _lookup(aids, names)
        if row:
            resolved += 1
            landing_name = str(row.get("landing_spot") or "").strip().lower()
            teams_out.append({
                "teamKey": k,
                "display": display,
                "landing_spot":    row.get("landing_spot"),
                "landing_quality": landing_quality.get(landing_name, 0.0) if landing_name else 0.0,
                "contested_by":    row.get("contested_by"),
                "injury_flag":     bool(row.get("injury_flag") or False),
                "scrim_form":      row.get("scrim_form"),
                "lan_experience":  row.get("lan_experience"),
                "poi_meta_fit":    row.get("poi_meta_fit"),
                "note":            row.get("note"),
                "resolved":        True,
            })
        else:
            teams_out.append({
                "teamKey": k,
                "display": display,
                "landing_spot": None,
                "landing_quality": 0.0,
                "contested_by": None,
                "injury_flag": False,
                "scrim_form": None,
                "lan_experience": None,
                "poi_meta_fit": None,
                "note": None,
                "resolved": False,
            })

    # Sort: resolved first, then display name
    teams_out.sort(key=lambda t: (not t["resolved"], (t.get("display") or "").lower()))

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "eventId": eventId,
        "totalTeams": len(teams_out),
        "resolvedTeams": resolved,
        "landingSpots": landing_spots,
        "teams": teams_out,
    }


# ---------------------------------------------------------------------------
# Lightweight data-health dashboard for the Data Prep tab.
# Returns just enough aggregate info to render metric cards + the source list,
# without paying the cost of building full rating-builder-export JSON.
# ---------------------------------------------------------------------------
@app.get("/api/predictions/pool-health")
async def api_predictions_pool_health(
    eventId: str = Query(..., description="Target event, e.g. epicgames_S40_FNCSMajor1_Final_NAC"),
    region: Optional[str] = Query(None),
):
    """
    Return a compact data-health snapshot for the qualifier pool of an
    upcoming event. Used by the Lab's Data Prep tab to light up metric
    cards (teams in pool, players, pedigree coverage, soft-data coverage,
    source event count, cache freshness).
    """
    all_snaps = _load_all_cached_snapshots()
    pool = resolve_prediction_pool(eventId, all_snaps, target_region=region)
    pool = [p for p in pool if p.get("entries")]
    if not pool:
        raise HTTPException(
            400,
            f"No qualifying-pool data cached for {eventId}. Backfill heats/LCQ first.",
        )

    target_region = region
    if target_region is None:
        for r in REGIONS:
            if eventId.endswith(f"_{r}"):
                target_region = r
                break

    restrict_keys = qualifier_team_keys(pool, event_id=eventId, region=target_region)
    breakdown = qualifier_pool_breakdown(pool, restrict_keys, event_id=eventId, region=target_region) if restrict_keys is not None else None

    # build_team_forms gives us per-team accountId sets so we can count
    # unique players and cross-reference pedigree coverage.
    forms = build_team_forms(pool, half_life_days=7.0, restrict_to_keys=restrict_keys)
    account_ids: set = set()
    for f in forms.values():
        for aid in (f.get("accountIds") or []):
            if aid:
                account_ids.add(str(aid).lower().strip())

    pedigree = build_pedigree(all_snaps, region=target_region)
    pedigreed_players = sum(
        1 for aid in account_ids if (pedigree.get(aid) or {}).get("score", 0) > 0
    )

    # Soft-data coverage: count how many teams resolve in team_soft_data.json.
    # Keep this in sync with rating-builder-export's _soft_for_team resolver.
    DATA_DIR = ROOT / "data"
    team_soft_raw: Dict[str, Dict[str, Any]] = {}
    try:
        tp = DATA_DIR / "team_soft_data.json"
        if tp.exists():
            tj = json.loads(tp.read_text(encoding="utf-8"))
            if isinstance(tj, dict):
                for k, v in tj.items():
                    if k.startswith("__") or not isinstance(v, dict):
                        continue
                    team_soft_raw[k.lower().strip()] = v
    except Exception as e:
        logger.warning(f"[pool-health] team_soft_data.json load failed: {e}")

    _UNFOLD_SINGLE = {
        "\u0430": "a", "\u0435": "e", "\u043e": "o", "\u0440": "p",
        "\u0441": "c", "\u0445": "x", "\u0443": "y", "\u0456": "i",
        "\u0451": "e", "\u043a": "k", "\u0455": "s",
    }
    def _fold(s: str) -> str:
        s = (s or "").lower()
        for k, v in _UNFOLD_SINGLE.items():
            s = s.replace(k, v)
        return s

    def _soft_for_team(aids: List[str], names: List[str]) -> bool:
        ids = sorted([str(a).lower().strip() for a in (aids or []) if a])
        if ids and ("aid:" + "+".join(ids)) in team_soft_raw:
            return True
        folded = [_fold(n).strip() for n in (names or []) if n]
        folded = [f for f in folded if f]
        for combo in ["+".join(folded), "+".join(sorted(folded))]:
            if combo and combo in team_soft_raw:
                return True
        for fn in folded:
            if fn and fn in team_soft_raw:
                return True
        for stored in team_soft_raw:
            if "+" in stored and all(part in "+".join(folded) for part in stored.split("+")):
                return True
        return False

    soft_resolved = 0
    for k, f in forms.items():
        aids = f.get("accountIds") or []
        last_apps = f.get("appearances") or []
        names = (last_apps[-1].get("playerList") if last_apps else []) or []
        if _soft_for_team(aids, names):
            soft_resolved += 1

    # Cache freshness — newest/oldest snapshot age in the pool.
    now_ts = _now()
    ages_sec: List[float] = []
    source_events = []
    for s in pool:
        up = s.get("updatedAt")
        try:
            if up:
                dt = datetime.fromisoformat(str(up).replace("Z", "+00:00"))
                age = now_ts - dt.timestamp()
                ages_sec.append(max(0.0, age))
        except Exception:
            pass
        source_events.append({
            "eventId": s.get("eventId"),
            "windowId": s.get("windowId"),
            "region": s.get("region"),
            "updatedAt": s.get("updatedAt"),
            "totalEntries": s.get("totalEntries") or len(s.get("entries") or []),
        })

    cache_oldest_h = (max(ages_sec) / 3600.0) if ages_sec else 0.0
    cache_newest_h = (min(ages_sec) / 3600.0) if ages_sec else 0.0

    return {
        "eventId": eventId,
        "region": target_region,
        "teamsInPool": len(forms),
        "playersInPool": len(account_ids),
        "pedigreedPlayers": pedigreed_players,
        "softDataResolved": soft_resolved,
        "softDataTotal": len(forms),
        "sourceEvents": source_events,
        "cacheAgeHoursOldest": round(cache_oldest_h, 2),
        "cacheAgeHoursNewest": round(cache_newest_h, 2),
        "qualifierBreakdown": breakdown,
        "expectedRosterSize": expected_roster_size,
        "rosterSizeCounts": roster_size_counts,
    }


def _stage_for_snapshot_id(event_id: str) -> str:
    if "PlayInStage" in event_id:
        return "play_in"
    if "HeatsStage" in event_id:
        return "heat"
    if "LastChanceQualifier" in event_id or "LastChanceLobby" in event_id:
        return "lcq"
    if "_Final" in event_id:
        return "final"
    return "source"


def _entry_display(entry: Dict[str, Any]) -> str:
    names = entry.get("playerList") or entry.get("players") or []
    if isinstance(names, list) and names:
        return " · ".join(str(n) for n in names if n)
    return str(entry.get("displayName") or entry.get("team") or entry.get("name") or "")


def _expected_roster_size(event_id: Optional[str], forms: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Infer target roster size and guard against mixed duo/trio pools."""
    eid = (event_id or "").lower()
    # Current S40 Major 1 final is duos. Keep this explicit so trio pedigree
    # snapshots cannot leak into the active qualification pool.
    if "s40_fncsmajor1" in eid or "duo" in eid or "duos" in eid:
        return 2
    if "solo" in eid or "solos" in eid:
        return 1
    if "trio" in eid or "trios" in eid:
        return 3
    if "squad" in eid or "squads" in eid:
        return 4
    if forms:
        counts: Dict[int, int] = {}
        for f in forms.values():
            n = len(f.get("accountIds") or [])
            if n:
                counts[n] = counts.get(n, 0) + 1
        if counts:
            return max(counts.items(), key=lambda kv: kv[1])[0]
    return None


def _filter_forms_by_roster_size(
    forms: Dict[str, Any],
    expected_size: Optional[int],
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    if not expected_size:
        sizes: Dict[str, int] = {}
        for f in forms.values():
            n = len(f.get("accountIds") or [])
            sizes[str(n)] = sizes.get(str(n), 0) + 1
        return forms, sizes
    sizes: Dict[str, int] = {}
    out: Dict[str, Any] = {}
    for k, f in forms.items():
        n = len(f.get("accountIds") or [])
        sizes[str(n)] = sizes.get(str(n), 0) + 1
        if n == expected_size:
            out[k] = f
    return out, sizes


class QualificationWorkspacePayload(BaseModel):
    eventId: str
    region: Optional[str] = None
    rules: Dict[str, Any] = {}
    overrides: Dict[str, Any] = {}
    graph: Dict[str, Any] = {}
    notes: Optional[str] = ""


def _qualification_workspace_key(event_id: str, region: Optional[str]) -> str:
    return f"{event_id}|{(region or '').upper()}"


def _safe_int(value: Any, fallback: Optional[int] = None) -> Optional[int]:
    try:
        if value in (None, ""):
            return fallback
        return int(value)
    except Exception:
        return fallback


def _summit_slots_for_region(region_hint: Optional[str]) -> Optional[int]:
    slots = {
        "EU": 20,
        "NAC": 13,
        "BR": 5,
        "NAW": 3,
        "ASIA": 3,
        "ME": 3,
        "OCE": 3,
    }
    return slots.get((region_hint or "").upper())


def _default_qualification_workspace(event_id: str, region: Optional[str]) -> Dict[str, Any]:
    caps = _pool_caps_for(event_id, region)
    summit_slots = _summit_slots_for_region(region or caps.get("region"))
    return {
        "eventId": event_id,
        "region": region or caps.get("region"),
        "rules": {
            "mode": "auto",
            "cap": caps["cap"],
            "heatCut": caps["heat_cut"],
            "lcqCut": caps["lcq_cut"],
            "summitSlots": summit_slots,
            "expectedRosterSize": None,
            "rosterGuard": "strict",
            "regionLock": bool(region),
        },
        "overrides": {
            "includeKeys": [],
            "excludeKeys": [],
            "disqualifiedKeys": [],
        },
        "graph": {
            "nodes": [],
            "edges": [],
        },
        "notes": "",
    }


def _load_qualification_workspace(event_id: str, region: Optional[str]) -> Dict[str, Any]:
    default = _default_qualification_workspace(event_id, region)
    try:
        stored = _m_qualification_workspace_get(_qualification_workspace_key(event_id, region))
    except Exception as e:
        logger.warning(f"qualification workspace load failed: {e}")
        stored = None
    if not stored:
        return default
    merged = dict(default)
    merged.update({k: v for k, v in stored.items() if k not in {"rules", "overrides", "graph"}})
    merged["rules"] = {**default["rules"], **dict(stored.get("rules") or {})}
    merged["overrides"] = {**default["overrides"], **dict(stored.get("overrides") or {})}
    merged["graph"] = {**default["graph"], **dict(stored.get("graph") or {})}
    return merged


def _store_qualification_workspace(payload: QualificationWorkspacePayload) -> Dict[str, Any]:
    default = _default_qualification_workspace(payload.eventId, payload.region)
    doc = {
        "eventId": payload.eventId,
        "region": payload.region or default.get("region"),
        "rules": {**default["rules"], **dict(payload.rules or {})},
        "overrides": {**default["overrides"], **dict(payload.overrides or {})},
        "graph": {**default["graph"], **dict(payload.graph or {})},
        "notes": payload.notes or "",
    }
    try:
        return _m_qualification_workspace_put(_qualification_workspace_key(payload.eventId, payload.region), doc)
    except Exception as e:
        logger.warning(f"qualification workspace save failed: {e}")
        return {**doc, "storageWarning": str(e)}


def _as_key_set(value: Any) -> set:
    if isinstance(value, str):
        return {k.strip().lower() for k in value.split(",") if k.strip()}
    if isinstance(value, list):
        return {str(k).strip().lower() for k in value if str(k).strip()}
    return set()


def _graph_item_data(item: Any) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    data = item.get("data")
    if isinstance(data, dict):
        merged = dict(data)
        if "id" not in merged and item.get("id") is not None:
            merged["id"] = item.get("id")
        return merged
    return dict(item)


def _graph_stage_from_text(*parts: Any) -> Optional[str]:
    text = " ".join(str(p or "") for p in parts).lower()
    if "heat" in text:
        return "heat"
    if "lastchance" in text or "last chance" in text or "lcq" in text:
        return "lcq"
    if "play" in text:
        return "play_in"
    if "final" in text or "field" in text:
        return "final"
    return None


def _graph_target_node_ids(workspace: Dict[str, Any], event_id: str, region: Optional[str]) -> set:
    graph = workspace.get("graph") or {}
    nodes = graph.get("nodes") if isinstance(graph, dict) else []
    target_ids: set = set()
    for raw in nodes or []:
        data = _graph_item_data(raw)
        node_id = str(data.get("id") or "")
        node_event = str(data.get("eventId") or "")
        if node_event and node_event == event_id:
            target_ids.add(node_id)
        elif node_id and node_id == event_id:
            target_ids.add(node_id)

    if target_ids:
        return target_ids

    # Default FNCS graphs do not know the selected event id, but their final
    # field node is the target for Final-event qualification edges.
    if "final" in (event_id or "").lower():
        region_token = (region or "").upper()
        for raw in nodes or []:
            data = _graph_item_data(raw)
            node_id = str(data.get("id") or "")
            label = str(data.get("label") or "")
            stage = _graph_stage_from_text(node_id, label)
            if stage == "final" and (
                not region_token
                or region_token in node_id.upper()
                or region_token in str(data.get("region") or "").upper()
                or node_id.lower() in {"field", "finalfield"}
            ):
                target_ids.add(node_id)
    return target_ids


def _qualification_graph_rule_edges(workspace: Dict[str, Any], event_id: str, region: Optional[str]) -> List[Dict[str, Any]]:
    graph = workspace.get("graph") or {}
    if not isinstance(graph, dict):
        return []
    nodes = [_graph_item_data(raw) for raw in (graph.get("nodes") or [])]
    nodes_by_id = {str(node.get("id") or ""): node for node in nodes if node.get("id") is not None}
    target_ids = _graph_target_node_ids(workspace, event_id, region)
    if not target_ids:
        return []

    rules: List[Dict[str, Any]] = []
    for raw_edge in graph.get("edges") or []:
        edge = _graph_item_data(raw_edge)
        source_id = str(edge.get("source") or edge.get("from") or "")
        target_id = str(edge.get("target") or edge.get("to") or "")
        if target_id not in target_ids:
            continue
        source_node = nodes_by_id.get(source_id) or {}
        edge_type = str(edge.get("edgeType") or edge.get("kind") or "qualify")
        rule_mode = str(edge.get("ruleMode") or "").strip()
        has_explicit_rule = bool(rule_mode or edge.get("cut") not in (None, "") or edge.get("scope"))
        if not has_explicit_rule:
            continue
        if edge_type in {"check", "evidence", "override"} and rule_mode not in {"top", "all", "exclude"}:
            continue
        source_stage = _graph_stage_from_text(
            source_id,
            source_node.get("label"),
            source_node.get("nodeType"),
            source_node.get("eventId"),
        )
        rules.append(
            {
                "edgeId": edge.get("id") or f"{source_id}->{target_id}",
                "sourceId": source_id,
                "targetId": target_id,
                "sourceLabel": source_node.get("label") or source_id,
                "sourceEventId": source_node.get("eventId"),
                "sourceWindowId": source_node.get("windowId"),
                "sourceStage": source_stage,
                "edgeType": edge_type,
                "ruleMode": rule_mode or "top",
                "cut": _safe_int(edge.get("cut")),
                "scope": edge.get("scope") or "perSource",
                "excludeAlreadyQualified": edge.get("excludeAlreadyQualified") is not False,
                "label": edge.get("label") or edge.get("rule") or "",
            }
        )
    return rules


def _snapshot_matches_graph_rule(snapshot: Dict[str, Any], rule: Dict[str, Any]) -> bool:
    event_id = snapshot.get("eventId") or ""
    window_id = snapshot.get("windowId") or ""
    source_event = rule.get("sourceEventId")
    if source_event:
        if event_id != source_event:
            return False
        source_window = rule.get("sourceWindowId")
        return not source_window or window_id == source_window
    source_stage = rule.get("sourceStage")
    return bool(source_stage and _stage_for_snapshot_id(event_id) == source_stage)


def _prediction_pool_for_workspace(
    event_id: str,
    all_snaps: List[Dict[str, Any]],
    *,
    target_region: Optional[str],
    workspace: Dict[str, Any],
) -> List[Dict[str, Any]]:
    base = resolve_prediction_pool(event_id, all_snaps, target_region=target_region)
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {
        (s.get("eventId") or "", s.get("windowId") or ""): s
        for s in base
    }
    graph_rules = _qualification_graph_rule_edges(workspace, event_id, target_region)
    if not graph_rules:
        return list(by_key.values())
    for snap in all_snaps:
        if target_region and (snap.get("region") or "") != target_region:
            continue
        if any(_snapshot_matches_graph_rule(snap, rule) for rule in graph_rules):
            by_key[(snap.get("eventId") or "", snap.get("windowId") or "")] = snap
    return list(by_key.values())


def _qualification_audit(
    pool: List[Dict[str, Any]],
    *,
    event_id: str,
    region: Optional[str],
    workspace: Dict[str, Any],
    include_keys: str = "",
    exclude_keys: str = "",
) -> Dict[str, Any]:
    rules = dict(workspace.get("rules") or {})
    caps = _pool_caps_for(event_id, region)
    cap = _safe_int(rules.get("cap"), caps["cap"]) or caps["cap"]
    heat_cut = _safe_int(rules.get("heatCut"), caps["heat_cut"]) or caps["heat_cut"]
    lcq_cut = _safe_int(rules.get("lcqCut"), caps["lcq_cut"]) or caps["lcq_cut"]
    summit_slots = _safe_int(rules.get("summitSlots"), _summit_slots_for_region(region or caps.get("region")))
    roster_guard = str(rules.get("rosterGuard") or "strict")

    forced_include = _as_key_set((workspace.get("overrides") or {}).get("includeKeys")) | _as_key_set(include_keys)
    forced_exclude = _as_key_set((workspace.get("overrides") or {}).get("excludeKeys")) | _as_key_set(exclude_keys)
    forced_dq = _as_key_set((workspace.get("overrides") or {}).get("disqualifiedKeys"))
    graph_excluded_keys: set = set()

    selected: Dict[str, Dict[str, Any]] = {}
    candidates: Dict[str, Dict[str, Any]] = {}
    lcq_candidates: Dict[str, Dict[str, Any]] = {}
    raw_key_sequence: List[str] = []
    source_windows: List[Dict[str, Any]] = []
    stage_counts: Dict[str, int] = {}
    heat_source_count = sum(1 for snap in pool if _stage_for_snapshot_id(snap.get("eventId") or "") == "heat")
    lcq_source_count = sum(1 for snap in pool if _stage_for_snapshot_id(snap.get("eventId") or "") == "lcq")
    has_qualifier_sources = bool(heat_source_count or lcq_source_count)
    graph_rule_edges = _qualification_graph_rule_edges(workspace, event_id, region)

    def ensure_team(tk: str, entry: Dict[str, Any], stage: str) -> Dict[str, Any]:
        row = candidates.setdefault(
            tk,
            {
                "teamKey": tk,
                "display": _entry_display(entry) or tk,
                "bestRank": None,
                "sources": set(),
                "evidenceSources": set(),
                "sourceDetails": [],
                "rosterSize": len(entry.get("accountIds") or []),
            },
        )
        rank = _safe_int(entry.get("rank"), 10**9) or 10**9
        row["evidenceSources"].add(stage)
        row["sourceDetails"].append({
            "stage": stage,
            "rank": rank,
            "eventId": entry.get("eventId"),
        })
        if row.get("bestRank") is None or rank < int(row.get("bestRank") or 10**9):
            row["bestRank"] = rank
            row["display"] = _entry_display(entry) or row["display"]
        return row

    def select_team(tk: str, base: Dict[str, Any], reason: str, source_stage: Optional[str] = None) -> None:
        existing = selected.get(tk)
        if existing and int(existing.get("bestRank") or 10**9) <= int(base.get("bestRank") or 10**9):
            existing.setdefault("qualificationReasons", set()).add(reason)
            if source_stage:
                existing.setdefault("sources", set()).add(source_stage)
            return
        selected[tk] = {**base, "qualificationReason": reason, "qualificationReasons": {reason}}
        if source_stage:
            selected[tk].setdefault("sources", set()).add(source_stage)

    if graph_rule_edges:
        for rule in graph_rule_edges:
            matching_snaps = [snap for snap in pool if _snapshot_matches_graph_rule(snap, rule)]
            rule_mode = str(rule.get("ruleMode") or "top")
            scope = str(rule.get("scope") or "perSource")
            source_stage = str(rule.get("sourceStage") or rule.get("sourceLabel") or "graph")
            default_cut = cap if rule_mode == "all" else heat_cut if source_stage == "heat" else lcq_cut if source_stage == "lcq" else cap
            cut = _safe_int(rule.get("cut"), default_cut) or default_cut
            rule_label = str(rule.get("label") or "").strip() or (
                "move all teams" if rule_mode == "all" else f"top {cut} from {rule.get('sourceLabel') or source_stage}"
            )
            for snap in matching_snaps:
                entries = sorted(snap.get("entries") or [], key=lambda e: e.get("rank") or 10**9)
                source_windows.append({
                    "eventId": snap.get("eventId"),
                    "windowId": snap.get("windowId"),
                    "stage": source_stage,
                    "entries": len(entries),
                    "cut": "all" if rule_mode == "all" else cut,
                    "rule": rule_label,
                    "updatedAt": snap.get("updatedAt"),
                })
                stage_counts[source_stage] = stage_counts.get(source_stage, 0) + 1
                for entry in entries:
                    tk = _prediction_team_key(entry.get("accountIds") or [])
                    if not tk:
                        continue
                    raw_key_sequence.append(tk)
                    ensure_team(tk, {**entry, "eventId": snap.get("eventId")}, source_stage)

            if rule_mode == "exclude":
                excluded_added = 0
                for snap in matching_snaps:
                    for entry in sorted(snap.get("entries") or [], key=lambda e: e.get("rank") or 10**9):
                        if rule_mode != "all" and excluded_added >= cut:
                            break
                        tk = _prediction_team_key(entry.get("accountIds") or [])
                        if tk:
                            graph_excluded_keys.add(tk)
                            excluded_added += 1
                continue

            if rule_mode == "evidence":
                continue

            if scope == "poolWide":
                best_rows: Dict[str, Dict[str, Any]] = {}
                for snap in matching_snaps:
                    for entry in sorted(snap.get("entries") or [], key=lambda e: e.get("rank") or 10**9):
                        tk = _prediction_team_key(entry.get("accountIds") or [])
                        if not tk:
                            continue
                        row = ensure_team(tk, {**entry, "eventId": snap.get("eventId")}, source_stage)
                        current = best_rows.get(tk)
                        if current is None or int(row.get("bestRank") or 10**9) < int(current.get("bestRank") or 10**9):
                            best_rows[tk] = row
                added = 0
                for tk, row in sorted(best_rows.items(), key=lambda item: int(item[1].get("bestRank") or 10**9)):
                    if rule.get("excludeAlreadyQualified") and tk in selected:
                        selected[tk].setdefault("qualificationReasons", set()).add("duplicate_graph_edge")
                        continue
                    if rule_mode != "all" and added >= cut:
                        break
                    select_team(tk, row, f"qualified_by_edge:{rule.get('edgeId')}", source_stage)
                    added += 1
                continue

            for snap in matching_snaps:
                added = 0
                entries = sorted(snap.get("entries") or [], key=lambda e: e.get("rank") or 10**9)
                for entry in entries:
                    tk = _prediction_team_key(entry.get("accountIds") or [])
                    if not tk:
                        continue
                    if rule_mode != "all" and added >= cut:
                        break
                    row = ensure_team(tk, {**entry, "eventId": snap.get("eventId")}, source_stage)
                    if rule.get("excludeAlreadyQualified") and tk in selected:
                        selected[tk].setdefault("qualificationReasons", set()).add("duplicate_graph_edge")
                        continue
                    select_team(tk, row, f"qualified_by_edge:{rule.get('edgeId')}", source_stage)
                    added += 1
    else:
        for snap in pool:
            sid = snap.get("eventId") or ""
            stage = _stage_for_snapshot_id(sid)
            entries = sorted(snap.get("entries") or [], key=lambda e: e.get("rank") or 10**9)
            if stage == "heat":
                cut = heat_cut
                rule_label = f"top {heat_cut} per heat"
            elif stage == "lcq":
                cut = f"pool-wide {lcq_cut}"
                rule_label = f"top {lcq_cut} total across all LCQ source windows; skip teams already qualified via heats"
            elif stage == "final":
                cut = len(entries) if not has_qualifier_sources else 0
                rule_label = "actual final field" if not has_qualifier_sources else "final evidence only"
            else:
                cut = 0
                rule_label = "source only"
            source_windows.append({
                "eventId": sid,
                "windowId": snap.get("windowId"),
                "stage": stage,
                "entries": len(entries),
                "cut": cut,
                "rule": rule_label,
                "updatedAt": snap.get("updatedAt"),
            })
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
            heat_added = 0
            for idx, entry in enumerate(entries, start=1):
                tk = _prediction_team_key(entry.get("accountIds") or [])
                if not tk:
                    continue
                raw_key_sequence.append(tk)
                row = ensure_team(tk, {**entry, "eventId": sid}, stage)
                if stage == "heat" and heat_added < heat_cut:
                    if tk in selected:
                        selected[tk].setdefault("qualificationReasons", set()).add("duplicate_heat_appearance")
                        continue
                    select_team(tk, row, "qualified_by_heat_cut", "heat")
                    heat_added += 1
                elif isinstance(cut, int) and idx <= cut:
                    if stage == "heat":
                        select_team(tk, row, "qualified_by_heat_cut", "heat")
                    elif stage == "final":
                        select_team(tk, row, "actual_final_field", "final")
                if stage == "lcq":
                    current = lcq_candidates.get(tk)
                    if current is None or int(row.get("bestRank") or 10**9) < int(current.get("bestRank") or 10**9):
                        lcq_candidates[tk] = row

        lcq_added = 0
        for tk, row in sorted(lcq_candidates.items(), key=lambda item: int(item[1].get("bestRank") or 10**9)):
            if lcq_added >= lcq_cut:
                break
            if tk in selected:
                selected[tk].setdefault("qualificationReasons", set()).add("duplicate_lcq_appearance")
                continue
            select_team(tk, row, "qualified_by_lcq_cut", "lcq")
            lcq_added += 1

    for tk in forced_include:
        base = candidates.get(tk) or {
            "teamKey": tk,
            "display": tk,
            "bestRank": None,
            "sources": set(),
            "evidenceSources": set(),
            "sourceDetails": [],
            "rosterSize": None,
        }
        select_team(tk, base, "manual_include", "manual")

    cap_trimmed: set = set()
    if len(selected) > cap:
        ranked = sorted(selected, key=lambda k: int(selected[k].get("bestRank") or 10**9))
        keep = set(ranked[:cap])
        cap_trimmed = set(selected) - keep
        selected = {k: v for k, v in selected.items() if k in keep}

    roster_size_counts: Dict[str, int] = {}
    for row in candidates.values():
        n = row.get("rosterSize")
        if n:
            roster_size_counts[str(n)] = roster_size_counts.get(str(n), 0) + 1
    expected_roster_size = _safe_int(rules.get("expectedRosterSize"))
    if expected_roster_size is None:
        expected_roster_size = _expected_roster_size(event_id)
        if expected_roster_size is None and roster_size_counts:
            expected_roster_size = int(max(roster_size_counts.items(), key=lambda kv: kv[1])[0])

    roster_dropped: set = set()
    if expected_roster_size and roster_guard == "strict":
        for tk, row in list(selected.items()):
            if tk in forced_include:
                continue
            if row.get("rosterSize") and int(row["rosterSize"]) != int(expected_roster_size):
                roster_dropped.add(tk)
                selected.pop(tk, None)

    excluded = (forced_exclude | forced_dq | graph_excluded_keys) & set(selected)
    for tk in excluded:
        selected.pop(tk, None)

    selected_keys = set(selected)
    raw_keys = set(raw_key_sequence)
    heat_keys = {k for k, row in selected.items() if "heat" in row.get("sources", set())}
    lcq_keys = {k for k, row in selected.items() if "lcq" in row.get("sources", set())}
    final_keys = {k for k, row in selected.items() if "final" in row.get("sources", set())}
    final_evidence_keys = {k for k, row in selected.items() if "final" in row.get("evidenceSources", set())}
    size = len(selected_keys)
    if graph_rule_edges:
        expected_heats = sum(
            (_safe_int(rule.get("cut"), heat_cut) or heat_cut)
            for rule in graph_rule_edges
            if rule.get("sourceStage") == "heat" and rule.get("ruleMode") != "all"
        )
        expected_lcq = sum(
            (_safe_int(rule.get("cut"), lcq_cut) or lcq_cut)
            for rule in graph_rule_edges
            if rule.get("sourceStage") == "lcq" and rule.get("ruleMode") != "all"
        )
        explicit_total = sum(
            (_safe_int(rule.get("cut"), cap) or cap)
            for rule in graph_rule_edges
            if rule.get("ruleMode") not in {"all", "evidence", "exclude"}
        )
        expected_total = min(cap, explicit_total) if explicit_total else cap
        ok = bool(size and size <= cap)
        reason = "ok" if ok else f"resolved {size}; expected up to {expected_total}"
    else:
        expected_heats = heat_cut * stage_counts.get("heat", 0)
        expected_lcq = lcq_cut if stage_counts.get("lcq", 0) else 0
        expected_total = cap if stage_counts.get("heat", 0) and stage_counts.get("lcq", 0) else expected_heats + expected_lcq + len(final_keys)
        ok = bool(size and (size == expected_total or size == cap))
        reason = "ok" if ok else f"resolved {size}; expected {expected_total or cap}"

    def serialize(row: Dict[str, Any]) -> Dict[str, Any]:
        reasons = sorted(row.get("qualificationReasons") or {row.get("qualificationReason")})
        return {
            "teamKey": row.get("teamKey"),
            "display": row.get("display"),
            "bestRank": row.get("bestRank"),
            "sources": sorted(row.get("sources") or []),
            "evidenceSources": sorted(row.get("evidenceSources") or []),
            "sourceDetails": row.get("sourceDetails") or [],
            "rosterSize": row.get("rosterSize"),
            "qualificationReason": row.get("qualificationReason"),
            "qualificationReasons": [r for r in reasons if r],
            "override": "include" if row.get("teamKey") in forced_include else None,
        }

    dropped: List[Dict[str, Any]] = []
    for tk, row in candidates.items():
        if tk in selected_keys:
            continue
        reason_code = "not_inside_cut"
        if tk in forced_dq:
            reason_code = "official_disqualification"
        elif tk in graph_excluded_keys:
            reason_code = "graph_edge_exclude"
        elif tk in forced_exclude:
            reason_code = "manual_exclude"
        elif tk in cap_trimmed:
            reason_code = "cap_trim"
        elif tk in roster_dropped:
            reason_code = "roster_mismatch"
        dropped.append({**serialize(row), "reason": reason_code})

    warnings: List[str] = []
    if not ok:
        warnings.append(reason)
    if forced_include or forced_exclude or forced_dq:
        warnings.append("manual include/exclude overrides applied")
    if graph_rule_edges:
        warnings.append(f"{len(graph_rule_edges)} graph edge rule(s) applied")
    if forced_dq:
        warnings.append(f"{len(forced_dq)} disqualified team override(s) applied")
    if expected_roster_size and any(str(k) != str(expected_roster_size) and v for k, v in roster_size_counts.items()):
        warnings.append(f"mixed roster sizes detected {roster_size_counts}; roster guard is {roster_guard}")
    if cap_trimmed:
        warnings.append(f"cap trim removed {len(cap_trimmed)} team(s)")

    graph = workspace.get("graph") or {}
    if not graph.get("nodes") and "fncs" not in (event_id or "").lower():
        event_label = (event_id or "Selected event").replace("epicgames_", "").replace("_", " ")
        graph = {
            "nodes": [
                {
                    "id": "source_events",
                    "label": "Source events / snapshots",
                    "count": len(source_windows),
                },
                {
                    "id": "selected_event",
                    "label": event_label,
                    "eventId": event_id,
                    "region": region,
                    "count": len(selected_keys),
                    "cap": cap,
                },
                {
                    "id": "manual_overrides",
                    "label": "Manual overrides",
                    "count": len(forced_include) + len(forced_exclude) + len(forced_dq),
                },
            ],
            "edges": [
                {
                    "from": "source_events",
                    "to": "selected_event",
                    "kind": "qualify",
                    "label": "Attach source events and set edge rules for this cup",
                },
                {
                    "from": "manual_overrides",
                    "to": "selected_event",
                    "kind": "override",
                    "label": "force include/exclude/DQ",
                },
            ],
        }
    if not graph.get("nodes"):
        summit_label = f"top {summit_slots} duos qualify to Düsseldorf" if summit_slots else "regional slots qualify to Düsseldorf"
        graph = {
            "nodes": [
                {"id": "open_division", "label": "Open Division / FNCS Trial", "count": 0},
                {"id": "divisional_cups", "label": "Divisional Cups", "count": 0},
                {"id": "division_1", "label": "Division 1 Eligible Pool", "count": 0},
                {"id": "play_in", "label": "Play-In", "count": stage_counts.get("play_in", 0)},
                {"id": "heat_1", "label": "Heat 1", "count": stage_counts.get("heat", 0), "cut": heat_cut},
                {"id": "heat_2", "label": "Heat 2", "count": stage_counts.get("heat", 0), "cut": heat_cut},
                {"id": "heat_3", "label": "Heat 3", "count": stage_counts.get("heat", 0), "cut": heat_cut},
                {"id": "lcq_eligible", "label": "LCQ Eligible Pool", "count": 0},
                {"id": "lcq", "label": "LCQ", "count": stage_counts.get("lcq", 0), "cut": lcq_cut},
                {"id": "final_evidence", "label": "Final Snapshot", "count": stage_counts.get("final", 0)},
                {"id": "manual_overrides", "label": "Manual / DQ Overrides", "count": len(forced_include) + len(forced_exclude) + len(forced_dq)},
                {"id": "field", "label": "Major 1 Finals Field", "count": len(selected_keys), "cap": cap},
                {"id": "dusseldorf", "label": "Düsseldorf Summit LAN", "count": summit_slots},
            ],
            "edges": [
                {"from": "open_division", "to": "divisional_cups", "kind": "qualify", "label": "earn divisional placement / access"},
                {"from": "divisional_cups", "to": "division_1", "kind": "qualify", "label": "reach Division 1 eligibility"},
                {"from": "division_1", "to": "play_in", "kind": "qualify", "label": "Division 1 duos enter FNCS Play-In"},
                {"from": "play_in", "to": "heat_1", "kind": "qualify", "label": "advance to Heat 1 by Play-In rule"},
                {"from": "play_in", "to": "heat_2", "kind": "qualify", "label": "advance to Heat 2 by Play-In rule"},
                {"from": "play_in", "to": "heat_3", "kind": "qualify", "label": "advance to Heat 3 by Play-In rule"},
                {"from": "play_in", "to": "lcq_eligible", "kind": "qualify", "label": "non-heat / fallback eligibility feed"},
                {"from": "heat_1", "to": "lcq_eligible", "kind": "check", "label": "exclude already-final-qualified teams"},
                {"from": "heat_2", "to": "lcq_eligible", "kind": "check", "label": "exclude already-final-qualified teams"},
                {"from": "heat_3", "to": "lcq_eligible", "kind": "check", "label": "exclude already-final-qualified teams"},
                {"from": "lcq_eligible", "to": "lcq", "kind": "qualify", "label": "eligible remaining duos enter LCQ"},
                {"from": "heat_1", "to": "field", "kind": "qualify", "label": f"top {heat_cut}; skip duplicates"},
                {"from": "heat_2", "to": "field", "kind": "qualify", "label": f"top {heat_cut}; skip duplicates"},
                {"from": "heat_3", "to": "field", "kind": "qualify", "label": f"top {heat_cut}; skip duplicates"},
                {"from": "lcq", "to": "field", "kind": "qualify", "label": f"top {lcq_cut} total, skip already qualified"},
                {"from": "final_evidence", "to": "field", "kind": "evidence", "label": "evidence only, not a qual path"},
                {"from": "manual_overrides", "to": "field", "kind": "override", "label": "force include/exclude/DQ"},
                {"from": "field", "to": "dusseldorf", "kind": "qualify", "label": summit_label},
            ],
        }

    return {
        "rules": {
            **rules,
            "cap": cap,
            "heatCut": heat_cut,
            "lcqCut": lcq_cut,
            "summitSlots": summit_slots,
            "expectedRosterSize": expected_roster_size,
            "rosterSizeCounts": roster_size_counts,
        },
        "breakdown": {
            "size": size,
            "cap": cap,
            "heats": len(heat_keys),
            "lcq": len(lcq_keys),
            "final": len(final_keys),
            "finalEvidence": len(final_evidence_keys),
            "expectedHeats": expected_heats,
            "expectedLcq": expected_lcq,
            "expectedTotal": expected_total,
            "ok": ok,
            "reason": reason,
        },
        "graph": graph,
        "sourceWindows": source_windows,
        "qualifiedTeams": [serialize(row) for row in sorted(selected.values(), key=lambda r: int(r.get("bestRank") or 10**9))],
        "droppedTeams": sorted(dropped, key=lambda r: int(r.get("bestRank") or 10**9)),
        "duplicates": max(0, len(raw_key_sequence) - len(raw_keys)),
        "warnings": warnings,
    }


def _leaderboard_preview_from_snapshots(
    pool: List[Dict[str, Any]],
    *,
    event_id: str,
    region: Optional[str],
    workspace: Dict[str, Any],
    top_teams: int,
) -> Dict[str, Any]:
    """Return a lightweight ranked leaderboard preview for one selected event window."""
    limit = max(1, min(500, _safe_int(top_teams, 50) or 50))
    rows_by_key: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    roster_size_counts: Dict[str, int] = {}
    source_windows: List[Dict[str, Any]] = []

    for snap in pool:
        entries = snap.get("entries") or []
        source_windows.append({
            "eventId": snap.get("eventId"),
            "windowId": snap.get("windowId"),
            "stage": "leaderboard",
            "entries": len(entries),
            "cut": limit,
            "rule": f"top {limit} leaderboard rows",
            "updatedAt": snap.get("updatedAt"),
        })
        for entry in entries:
            tk = _prediction_team_key(entry.get("accountIds") or [])
            if not tk:
                continue
            rank = _safe_int(entry.get("rank"), 10**9) or 10**9
            roster_size = len(entry.get("accountIds") or [])
            if roster_size:
                roster_size_counts[str(roster_size)] = roster_size_counts.get(str(roster_size), 0) + 1
            current = rows_by_key.get(tk)
            if current and current[0] <= rank:
                continue
            rows_by_key[tk] = (
                rank,
                {
                    "teamKey": tk,
                    "display": _entry_display(entry) or tk,
                    "bestRank": rank,
                    "sources": ["leaderboard"],
                    "evidenceSources": ["leaderboard"],
                    "sourceDetails": [{
                        "stage": "leaderboard",
                        "rank": rank,
                        "eventId": snap.get("eventId"),
                    }],
                    "rosterSize": roster_size or None,
                    "qualificationReason": "leaderboard_rank",
                    "qualificationReasons": ["leaderboard_rank"],
                    "override": None,
                },
            )

    rows = [row for _rank, row in sorted(rows_by_key.values(), key=lambda item: item[0])[:limit]]
    expected_roster_size = _expected_roster_size(event_id)
    if expected_roster_size is None and roster_size_counts:
        expected_roster_size = int(max(roster_size_counts.items(), key=lambda kv: kv[1])[0])
    rules = dict(workspace.get("rules") or {})
    graph = workspace.get("graph") or {}
    if not graph.get("nodes"):
        graph = {
            "nodes": [
                {"id": "selected_event", "label": event_id.replace("epicgames_", "").replace("_", " "), "eventId": event_id, "region": region, "count": len(rows)},
            ],
            "edges": [],
        }
    warnings = [] if rows else [f"No cached leaderboard snapshot found for {event_id}"]

    return {
        "rules": {
            **rules,
            "cap": limit,
            "expectedRosterSize": expected_roster_size,
            "rosterSizeCounts": roster_size_counts,
        },
        "breakdown": {
            "size": len(rows),
            "cap": limit,
            "expectedTotal": min(limit, len(rows_by_key)),
            "ok": bool(rows),
            "reason": "ok" if rows else "no cached leaderboard rows",
        },
        "graph": graph,
        "sourceWindows": source_windows,
        "qualifiedTeams": rows,
        "droppedTeams": [],
        "duplicates": max(0, sum(len(snap.get("entries") or []) for snap in pool) - len(rows_by_key)),
        "warnings": warnings,
    }


@app.get("/api/qualification/workspace")
async def api_qualification_workspace(eventId: str = Query(...), region: Optional[str] = Query(None)):
    """Return persisted qualification rules/graph/overrides for an event."""
    return _load_qualification_workspace(eventId, region)


@app.post("/api/qualification/workspace")
async def api_qualification_workspace_save(payload: QualificationWorkspacePayload):
    """Persist analyst-edited qualification rules, overrides, and graph."""
    return _store_qualification_workspace(payload)


@app.get("/api/qualification/preview")
async def api_qualification_preview(
    eventId: str = Query(...),
    region: Optional[str] = Query(None),
    windowId: Optional[str] = Query(None),
    sourceMode: str = Query("qualification"),
    topTeams: int = Query(50, ge=1, le=500),
    includeKeys: str = Query("", description="Comma-separated team keys to force include"),
    excludeKeys: str = Query("", description="Comma-separated team keys to force exclude"),
):
    """Graph-shaped, auditable qualification preview for Data Prep."""
    target_region = region
    if target_region is None:
        for r in REGIONS:
            if eventId.endswith(f"_{r}"):
                target_region = r
                break
    workspace = _load_qualification_workspace(eventId, target_region)
    source_mode = (sourceMode or "qualification").strip()
    if source_mode == "eventTop":
        pool = [
            snap for snap in _load_cached_snapshots_for_event(eventId, window_id=windowId, target_region=target_region)
            if snap.get("entries")
        ]
        if not pool and windowId:
            try:
                pool = await _fetch_leaderboard_snapshot_for_preview(
                    eventId,
                    windowId,
                    target_region=target_region,
                    top_teams=topTeams,
                )
            except Exception as e:
                logger.warning(f"leaderboard preview backfill failed for {eventId}/{windowId}: {e}")
        audit = _leaderboard_preview_from_snapshots(
            pool,
            event_id=eventId,
            region=target_region,
            workspace=workspace,
            top_teams=topTeams,
        )
        return {
            "eventId": eventId,
            "region": target_region,
            "workspace": workspace,
            **audit,
        }

    all_snaps = _load_all_cached_snapshots()
    pool = _prediction_pool_for_workspace(eventId, all_snaps, target_region=target_region, workspace=workspace)
    pool = [p for p in pool if p.get("entries")]
    if not pool:
        if "fncs" in (eventId or "").lower():
            raise HTTPException(400, f"No qualifying-pool data cached for {eventId}")
        pool = []
    audit = _qualification_audit(
        pool,
        event_id=eventId,
        region=target_region,
        workspace=workspace,
        include_keys=includeKeys,
        exclude_keys=excludeKeys,
    )

    return {
        "eventId": eventId,
        "region": target_region,
        "workspace": workspace,
        **audit,
    }


# ---------------------------------------------------------------------------
# Soft-data upsert endpoint used by the Data Prep tab's inline editor.
# Patches data/team_soft_data.json by teamKey (pipe-joined lowercased
# accountIds) OR by a composite "aid:<sorted+joined>" key. Overwrites any
# existing row for that key.
# ---------------------------------------------------------------------------
class SoftDataUpsert(BaseModel):
    teamKey: str
    row: Dict[str, Any]


@app.post("/api/soft-data/upsert")
async def api_soft_data_upsert(payload: SoftDataUpsert):
    """
    Patch `data/team_soft_data.json` with a new / updated row for the
    given team. `teamKey` may be:
      - a pipe-joined accountIds string (from build_team_forms)
      - a composite "aid:<sorted+joined>" key
      - a folded display-name combo (e.g. "peterbot+pollo")
    Missing files are created; unknown row keys are preserved as-is.
    """
    row = dict(payload.row or {})
    tk = (payload.teamKey or "").strip()
    if not tk:
        raise HTTPException(400, "teamKey required")

    # Convert the Lab's teamKey (pipe-joined accountIds) into the same
    # aid:<ids> form used by the rating-builder-export resolver so the
    # new row is automatically found on the next request.
    storage_key = tk
    if "|" in tk and not tk.startswith("aid:"):
        parts = [p.strip().lower() for p in tk.split("|") if p.strip()]
        storage_key = "aid:" + "+".join(sorted(parts))

    DATA_DIR = ROOT / "data"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / "team_soft_data.json"
    try:
        existing = {}
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                existing = {}
    except Exception as e:
        logger.warning(f"[soft-data upsert] failed to read existing file: {e}")
        existing = {}

    existing[storage_key] = row
    try:
        path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        raise HTTPException(500, f"Failed to write team_soft_data.json: {e}")

    return {"ok": True, "teamKey": tk, "storageKey": storage_key, "row": row}


# ===========================================================================
# Signals subsystem (Phase 4 + 5)
# ===========================================================================


class SignalIngestBody(BaseModel):
    text: str
    source: Optional[str] = "discord_paste"
    sourceLabel: Optional[str] = None
    sourceUrl: Optional[str] = None


class SignalApplyBody(BaseModel):
    # Optional overrides the reviewer can tweak before applying.
    flag: Optional[str] = None
    magnitude: Optional[int] = None
    ttlSeconds: Optional[int] = None
    teamKey: Optional[str] = None  # override auto-linked subject's teamKey


class SignalDismissBody(BaseModel):
    note: Optional[str] = None


class SignalPatchBody(BaseModel):
    """Partial update for analyst edits (wiki / lab). Merges subject and extracted dicts."""

    text: Optional[str] = None
    sourceLabel: Optional[str] = None
    sourceUrl: Optional[str] = None
    subject: Optional[Dict[str, Any]] = None
    extracted: Optional[Dict[str, Any]] = None


@app.get("/api/signals/recent")
async def api_signals_recent(
    since: Optional[str] = Query(None, description="Only return signals with ts > this ISO timestamp"),
    source: Optional[str] = Query(None, description="Filter to one source"),
    limit: int = Query(50, ge=1, le=500),
):
    """Feed for the Lab's Signals tab + Live Feed sidebar."""
    store = _get_signal_store()
    sigs = store.recent(limit=limit, since_iso=since, source=source)
    return {
        "total": store.total(),
        "count": len(sigs),
        "overlayRev": _overlay_rev(),
        "signals": [s.to_dict() for s in sigs],
    }


@app.post("/api/signals/ingest")
async def api_signals_ingest(payload: SignalIngestBody):
    """
    Manual / paste-relay ingest. Runs the text through the extractor and
    persists it as a Signal. Used by the Lab UI's paste textarea (for
    Discord announcements / tweet screenshots) and by other tools that
    want to push into the feed.
    """
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(400, "text required")

    ex = _signal_runner.get_extractor()
    # Refresh subject index on demand — cheap to run, and ensures newly
    # cached qualifier pools are matchable before we wait for the next
    # restart.
    try:
        if not ex.subject_index:
            ex.refresh_subjects()
    except Exception:
        pass

    fields = ex.extract(text)
    src = (payload.source or "discord_paste").strip()
    label = payload.sourceLabel or ({
        "discord_paste": "Discord paste",
        "twitter_paste": "Twitter paste",
        "liquipedia_paste": "Liquipedia paste",
        "manual": "Manual",
    }.get(src, src))

    sig = SignalRow(
        id=str(__import__("uuid").uuid4()),
        ts=datetime.now(timezone.utc).isoformat(),
        source=src,
        sourceLabel=label,
        sourceUrl=payload.sourceUrl,
        text=text,
        subject=fields["subject"],
        extracted=fields["extracted"],
    )
    _get_signal_store().add(sig)
    logger.info(f"[signals] ingested {sig.id[:8]} / {src}: "
                f"{fields['extracted']['flag']} / "
                f"{fields['subject'].get('display') or '-'}")
    return sig.to_dict()


@app.post("/api/signals/{sig_id}/apply")
async def api_signals_apply(sig_id: str, payload: Optional[SignalApplyBody] = None):
    """
    Apply a signal to the soft-data overlay. Writes a TTL-bounded row
    keyed by the subject's teamKey (or aid:sorted+joined) into
    data/soft_overlay.json. The rating-builder-export endpoint merges the
    overlay on top of team_soft_data.json, so the next Builder refresh
    picks up the effect immediately.
    """
    store = _get_signal_store()
    sig = store.by_id(sig_id)
    if sig is None:
        raise HTTPException(404, f"signal {sig_id} not found")
    if sig.status == "applied":
        return {"ok": True, "alreadyApplied": True, "signal": sig.to_dict()}

    body = payload or SignalApplyBody()
    subject = dict(sig.subject or {})
    team_key = body.teamKey or subject.get("teamKey")
    account_ids = list(subject.get("accountIds") or [])

    if not team_key:
        # Best-effort: derive from accountIds if we have them.
        if account_ids:
            team_key = "|".join(sorted([a.lower().strip() for a in account_ids if a]))
        else:
            raise HTTPException(400, "Signal has no linked team/player — can't apply. Edit the signal or set teamKey explicitly.")

    # Normalize to the overlay storage key (same form rating-builder-export uses).
    storage_key = team_key
    if "|" in team_key and not team_key.startswith("aid:"):
        parts = [p.strip().lower() for p in team_key.split("|") if p.strip()]
        storage_key = "aid:" + "+".join(sorted(parts))

    flag = (body.flag or sig.extracted.get("flag") or "general")
    magnitude = body.magnitude if body.magnitude is not None else int(sig.extracted.get("magnitude") or 0)
    ttl_seconds = int(body.ttlSeconds or (4 * 3600))

    # Translate the signal into v31..v36 overlay deltas.
    row: Dict[str, Any] = {}
    if flag == "health_flag":
        row["injury_flag"] = True
        row["v33_override"] = -100
        row["note_overlay"] = f"signal: {flag} — {sig.text[:140]}"
    elif flag == "roster_change":
        row["roster_change_flag"] = True
        row["v34_override"] = max(-50, min(0, magnitude))
        row["note_overlay"] = f"signal: roster — {sig.text[:140]}"
    elif flag == "form_boost":
        row["v34_override"] = max(0, min(50, abs(magnitude)))
        row["note_overlay"] = f"signal: form+ — {sig.text[:140]}"
    elif flag == "form_concern":
        row["v34_override"] = -abs(magnitude)
        row["note_overlay"] = f"signal: form- — {sig.text[:140]}"
    else:
        row["note_overlay"] = f"signal: {flag} — {sig.text[:140]}"
        row["generic_magnitude"] = magnitude

    entry = _write_overlay_entry(
        storage_key=storage_key,
        row=row,
        ttl_seconds=ttl_seconds,
        applied_by=sig.id,
        subject={"teamKey": team_key, "accountIds": account_ids, "display": subject.get("display")},
    )
    store.update(
        sig.id,
        status="applied",
        appliedAt=datetime.now(timezone.utc).isoformat(),
        overlayKey=storage_key,
        overlayTtlSeconds=ttl_seconds,
    )
    return {
        "ok": True,
        "overlayKey": storage_key,
        "overlay": entry,
        "overlayRev": _overlay_rev(),
    }


@app.post("/api/signals/{sig_id}/dismiss")
async def api_signals_dismiss(sig_id: str, payload: Optional[SignalDismissBody] = None):
    store = _get_signal_store()
    sig = store.by_id(sig_id)
    if sig is None:
        raise HTTPException(404, f"signal {sig_id} not found")
    store.update(sig.id, status="dismissed")
    return {"ok": True}


@app.patch("/api/signals/{sig_id}")
async def api_signals_patch(sig_id: str, body: SignalPatchBody):
    """Update signal text, labels, subject, or extracted flags (persisted to JSONL)."""
    store = _get_signal_store()
    sig = store.by_id(sig_id)
    if sig is None:
        raise HTTPException(404, f"signal {sig_id} not found")
    updates: Dict[str, Any] = {}
    if body.text is not None:
        updates["text"] = body.text.strip()
    if body.sourceLabel is not None:
        updates["sourceLabel"] = body.sourceLabel.strip()
    if body.sourceUrl is not None:
        url = (body.sourceUrl or "").strip()
        updates["sourceUrl"] = url or None
    if body.subject is not None:
        merged = dict(sig.subject or {})
        for k, v in body.subject.items():
            if v is None and k in merged:
                del merged[k]
            elif v is not None:
                merged[k] = v
        updates["subject"] = merged
    if body.extracted is not None:
        merged_ex = dict(sig.extracted or {})
        for k, v in body.extracted.items():
            if v is None and k in merged_ex:
                del merged_ex[k]
            elif v is not None:
                merged_ex[k] = v
        updates["extracted"] = merged_ex
    if not updates:
        return sig.to_dict()
    updated = store.update(sig_id, **updates)
    if updated is None:
        raise HTTPException(404, f"signal {sig_id} not found")
    return updated.to_dict()


@app.get("/api/signals/overlay-rev")
async def api_signals_overlay_rev(target: Optional[str] = Query(None)):
    """
    Polled by the Rating Builder / Drop Map. Returns a monotonic revision
    counter that bumps every time a signal is applied or an overlay row
    expires. Clients compare against their last-seen rev; when it changes
    they refetch rating-builder-export and re-run their sim.
    """
    # Prune first so the rev bumps include expirations.
    _ = _read_overlay()
    return {
        "rev": _overlay_rev(),
        "target": target,
    }


@app.get("/api/signals/config")
async def api_signals_config():
    cfg = _signal_runner.read_config()
    status = _signal_runner.get_status()
    return {"config": cfg, "status": status}


@app.put("/api/signals/config")
async def api_signals_config_put(cfg: Dict[str, Any]):
    if not isinstance(cfg, dict):
        raise HTTPException(400, "expected JSON object")
    saved = _signal_runner.write_config(cfg)
    return {"ok": True, "config": saved, "note": "restart the server to apply enabled/disabled changes"}


@app.get("/api/signals/subjects")
async def api_signals_subjects():
    """Debug endpoint — shows the size of the entity-linker subject index."""
    ex = _signal_runner.get_extractor()
    return {"subjects": len(ex.subject_index)}


# ===========================================================================
# Builder <-> Drop Map bridge (Phase 7 pieces that live server-side)
# ===========================================================================
# Legacy file paths kept only for one-time migration. Runtime state is now
# persisted to MongoDB via the ``storage`` module.
LEGACY_BUILDER_PROBS_DIR = EXPORTS_DIR / "builder_probs"
LEGACY_DROPMAP_DIR = ROOT / "data" / "dropmap"
LEGACY_APPROVED_OFFERS_PATH = ROOT / "data" / "approved_offers.json"
LEGACY_SOFT_OVERLAY_PATH = ROOT / "data" / "soft_overlay.json"
POI_COORDS_PATH = ROOT / "data" / "poi_coordinates.json"
ASSETS_DIR = ROOT / "assets"
ASSETS_MAPS_DIR = ASSETS_DIR / "maps"
ASSETS_MAPS_DIR.mkdir(parents=True, exist_ok=True)

if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR), html=False), name="assets")


class PublishProbsBody(BaseModel):
    eventId: str
    teams: List[Dict[str, Any]]
    generatedAt: Optional[str] = None
    note: Optional[str] = None
    simulation: Optional[Dict[str, Any]] = None


class SimulationSweepBody(BaseModel):
    eventId: str
    region: Optional[str] = None
    nIter: int = 800
    configs: Optional[List[Dict[str, Any]]] = None


def _builder_rows_from_export(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    categories = ((data.get("builderOutput") or {}).get("categories") or [])
    cat_by_id = {c.get("id"): c for c in categories}
    scores = data.get("scores") or {}
    teams = data.get("teams") or []

    def cat_value(cat_id: str, team: Dict[str, Any]) -> float:
        cat = cat_by_id.get(cat_id) or {}
        weighted = 0.0
        total = 0.0
        for a in cat.get("assignments") or []:
            vals: List[float] = []
            for mid in team.get("memberIds") or []:
                val = (scores.get(mid) or {}).get(a.get("varId"))
                if val is not None:
                    vals.append(float(val))
            if not vals:
                continue
            avg = (sum(vals) / len(vals)) / 100.0
            w = float(a.get("weight") or 0.0) / 100.0
            weighted += avg * w
            total += abs(w)
        return weighted / total if total else 0.0

    def dist_params(team: Dict[str, Any]) -> Dict[str, float]:
        mode_val = cat_value("mode", team)
        spread_val = cat_value("spread", team)
        upper_val = cat_value("upperSkew", team)
        lower_val = cat_value("lowerSkew", team)
        kurt_val = cat_value("kurtosis", team)
        bimodal_val = cat_value("bimodalStrength", team)
        mode2_val = cat_value("mode2", team)
        return {
            "mode": max(1.0, min(50.0, 25.0 - mode_val * 20.0)),
            "spread": max(1.0, min(18.0, 8.0 + spread_val * 8.0)),
            "upperSkew": max(0.0, min(5.0, upper_val * 4.0)),
            "lowerSkew": max(0.0, min(5.0, lower_val * 4.0)),
            "kurtosis": kurt_val * 4.0,
            "bimodalStrength": max(0.0, min(1.0, bimodal_val)),
            "mode2": max(1.0, min(50.0, 35.0 - mode2_val * 15.0)),
            "ceiling": 1.0,
            "floor": 50.0,
            "deadZoneStart": 0.0,
            "deadZoneEnd": 0.0,
        }

    return [
        {
            "teamKey": t.get("teamKey") or t.get("id"),
            "teamDisplay": t.get("name") or t.get("teamDisplay") or t.get("id"),
            "accountIds": t.get("accountIds") or [],
            "distParams": dist_params(t),
        }
        for t in teams
    ]


def _run_builder_points_simulation(payload: PublishProbsBody) -> Dict[str, Any]:
    """Convert Builder priors into simulated PMFs/offers without persisting."""
    from pricing.bookmaker import build_market_offer_pack
    from simulation.points_simulator import (
        PointsSimulationConfig,
        build_team_inputs_from_builder_rows,
        run_points_event,
    )

    generated_at = payload.generatedAt or datetime.now(timezone.utc).isoformat()
    simulation_cfg = payload.simulation or {}
    teams_out = list(payload.teams)
    simulation_artifact: Optional[Dict[str, Any]] = None
    offer_pack: Optional[Dict[str, Any]] = None
    simulation_error: Optional[str] = None

    try:
        pool_size = int(simulation_cfg.get("poolSize") or max(50, len(payload.teams)))
        team_inputs = build_team_inputs_from_builder_rows(payload.teams, pool_size=pool_size)
        config = PointsSimulationConfig(
            num_matches=int(simulation_cfg.get("numMatches") or 10),
            pool_size=pool_size,
            elim_point_value=float(simulation_cfg.get("elimPointValue") or 1.0),
            zone_volatility=float(simulation_cfg.get("zoneVolatility") or 0.30),
            patch_volatility=float(simulation_cfg.get("patchVolatility") or 0.15),
            lobby_strength=float(simulation_cfg.get("lobbyStrength") or 0.50),
            sample_path_limit=int(simulation_cfg.get("samplePathLimit") or 5),
        )
        n_iter = int(simulation_cfg.get("nIter") or 3000)
        seed = simulation_cfg.get("seed")
        if seed is not None:
            seed = int(seed)
        else:
            seed_payload = {
                "eventId": payload.eventId,
                "teams": [
                    {
                        "teamKey": t.get("teamKey"),
                        "teamDisplay": t.get("teamDisplay") or t.get("name"),
                        "distParams": t.get("distParams"),
                        "teamPrior": t.get("teamPrior") or t.get("performancePrior"),
                        "contest": t.get("contest") or t.get("contestProfile"),
                    }
                    for t in payload.teams
                ],
                "simulation": {
                    "numMatches": config.num_matches,
                    "poolSize": config.pool_size,
                    "elimPointValue": config.elim_point_value,
                    "zoneVolatility": config.zone_volatility,
                    "patchVolatility": config.patch_volatility,
                    "lobbyStrength": config.lobby_strength,
                    "nIter": n_iter,
                },
            }
            seed = int(
                hashlib.sha1(
                    json.dumps(seed_payload, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()[:8],
                16,
            )
        simulation_artifact = run_points_event(
            team_inputs,
            config,
            n_iter=n_iter,
            seed=seed,
        )
        offer_pack = build_market_offer_pack(
            simulation_artifact,
            base_limit=float(simulation_cfg.get("baseLimit") or 100.0),
        )

        projected: List[Dict[str, Any]] = []
        for row, team_input in zip(payload.teams, team_inputs):
            tk = team_input.team_key
            pmf = (simulation_artifact.get("pmfs") or {}).get(tk) or row.get("placementDist") or []
            markets = ((offer_pack.get("pmfMarkets") or {}).get(tk) or {}) if offer_pack else {}
            projected_row = dict(row)
            projected_row.update({
                "teamKey": tk,
                "teamDisplay": row.get("teamDisplay") or team_input.display_name,
                "placementDist": pmf,
                "winProbPrior": row.get("winProbPrior", 0),
                "winProb": (markets.get("win") or {}).get("fairProbability", (pmf[0] if pmf else 0)),
                "top3": (markets.get("top3") or {}).get("fairProbability", sum(pmf[:3]) if pmf else 0),
                "top10": (markets.get("top10") or {}).get("fairProbability", sum(pmf[:10]) if pmf else 0),
                "top25": (markets.get("top25") or {}).get("fairProbability", sum(pmf[:25]) if pmf else 0),
                "modelVersion": simulation_artifact.get("modelVersion"),
                "commercialOffers": markets,
                "pointsLine": ((offer_pack.get("pointsLines") or {}).get(tk) if offer_pack else None),
                "simulationPrior": team_input.prior.to_dict(),
                "contestProfile": team_input.contest.to_dict(),
            })
            projected.append(projected_row)
        teams_out = projected
    except Exception as e:
        simulation_error = str(e)
        logger.exception(f"[builder-sim] server-side points simulation failed: {e}")

    return {
        "eventId": payload.eventId,
        "teams": teams_out,
        "generatedAt": generated_at,
        "note": payload.note or "",
        "simulationArtifact": simulation_artifact,
        "offerPack": offer_pack,
        "simulationError": simulation_error,
    }


@app.post("/api/builder/simulate")
async def api_builder_simulate(payload: PublishProbsBody):
    """Run the hosted server-side model from the Builder without persisting."""
    if not isinstance(payload.teams, list) or not payload.teams:
        raise HTTPException(400, "teams must be a non-empty list")
    return _run_builder_points_simulation(payload)


@app.post("/api/builder/simulation-sweep")
async def api_builder_simulation_sweep(payload: SimulationSweepBody):
    """Run a deterministic event-level sweep over points-simulation knobs."""
    export = await api_predictions_rating_builder_export(
        eventId=payload.eventId,
        region=payload.region,
        halfLifeDays=7.0,
        minEvents=1,
        restrictToQualifiers=True,
        scale="percentile",
        seedCurated=True,
    )
    rows = _builder_rows_from_export(export)
    if not rows:
        raise HTTPException(400, "rating-builder export produced no teams")
    configs = payload.configs or [
        {"name": "current", "zoneVolatility": 0.30, "patchVolatility": 0.15, "lobbyStrength": 0.50},
        {"name": "more-chaos", "zoneVolatility": 0.45, "patchVolatility": 0.22, "lobbyStrength": 0.55},
        {"name": "tighter-field", "zoneVolatility": 0.35, "patchVolatility": 0.18, "lobbyStrength": 0.70},
    ]
    results: List[Dict[str, Any]] = []
    for idx, cfg in enumerate(configs):
        sim = {
            "model": "points-sim.v1",
            "nIter": int(cfg.get("nIter") or payload.nIter),
            "numMatches": int(cfg.get("numMatches") or 10),
            "poolSize": int(cfg.get("poolSize") or max(50, len(rows))),
            "elimPointValue": float(cfg.get("elimPointValue") or 1.0),
            "zoneVolatility": float(cfg.get("zoneVolatility") or 0.30),
            "patchVolatility": float(cfg.get("patchVolatility") or 0.15),
            "lobbyStrength": float(cfg.get("lobbyStrength") or 0.50),
            "samplePathLimit": 0,
            "baseLimit": float(cfg.get("baseLimit") or 100.0),
            "seed": int(cfg.get("seed") or (12345 + idx)),
        }
        out = _run_builder_points_simulation(PublishProbsBody(
            eventId=payload.eventId,
            teams=rows,
            note=f"sweep:{cfg.get('name') or idx}",
            simulation=sim,
        ))
        teams_sorted = sorted(out.get("teams") or [], key=lambda t: t.get("winProb", 0), reverse=True)
        pmfs = (out.get("simulationArtifact") or {}).get("pmfs") or {}
        max_fav = float(teams_sorted[0].get("winProb") or 0.0) if teams_sorted else 0.0
        avg_top10 = 0.0
        if pmfs:
            avg_top10 = sum(sum((p or [])[:10]) for p in pmfs.values()) / max(1, len(pmfs))
        results.append({
            "name": cfg.get("name") or f"config-{idx+1}",
            "config": sim,
            "runtimeHint": f"{sim['nIter']} iterations",
            "maxFavoriteWinProbability": max_fav,
            "favorite": teams_sorted[0].get("teamDisplay") if teams_sorted else None,
            "averageTop10Probability": avg_top10,
            "fieldSize": len(rows),
            "modelVersion": (out.get("simulationArtifact") or {}).get("modelVersion"),
        })
    return {
        "eventId": payload.eventId,
        "region": payload.region,
        "teams": len(rows),
        "results": results,
        "recommendation": "Prefer configs that reduce max favorite win probability without flattening top10/top25 too far.",
    }


@app.post("/api/builder/publish-probs")
async def api_builder_publish_probs(payload: PublishProbsBody):
    """
    Receive Builder priors and publish a server-side points-simulation pack.

    The browser still sends its visual distribution snapshot for auditability,
    but server output is now the source of truth: Builder priors are replayed
    through the correlated lobby simulator, then markets/limits are derived
    from the stored sample artifact.
    """
    if not payload.eventId:
        raise HTTPException(400, "eventId required")
    if not isinstance(payload.teams, list):
        raise HTTPException(400, "teams must be a list")

    out = _run_builder_points_simulation(payload)
    try:
        _m_probs_upsert(payload.eventId, out)
    except Exception as e:
        raise HTTPException(500, f"Failed to persist probs: {e}")

    # Freeze-dry the full model-input bundle so future backtests have
    # perfect point-in-time data even after team_soft_data.json gets
    # edited or the signals JSONL log rolls.
    # -------------------------------------------------------------
    # This is additive: failures must not break the publish. See
    # ``storage/collections_snapshots.py`` for the schema.
    try:
        from storage import collections_snapshots as _cs
        overlay_snapshot: Dict[str, Any] = {}
        try:
            from storage import overlay_read_all as _ov_read
            overlay_snapshot = _ov_read(prune_expired=False)
        except Exception:
            pass

        signal_rev = 0
        try:
            signal_rev = int(_overlay_rev())
        except Exception:
            pass

        _cs.snapshot_model_state(
            payload.eventId,
            trigger="publish-probs",
            builder_probs=out,
            source_inputs=None,
            overlay_snapshot=overlay_snapshot,
            signal_rev=signal_rev,
        )
    except Exception as e:
        logger.warning(f"[publish-probs] freeze-dry snapshot failed: {e}")

    # Bump the overlay rev so the Drop Map's poll notices the fresh data
    # even without an overlay write.
    try:
        _m_bump_rev()
    except Exception as e:
        logger.warning(f"[publish-probs] rev bump failed: {e}")
    return {"ok": True, "eventId": payload.eventId, "count": len(payload.teams)}


@app.get("/api/builder/market-probs")
async def api_builder_market_probs(eventId: str = Query(...)):
    """Drop Map polls this every few seconds."""
    try:
        doc = _m_probs_get(eventId)
    except Exception as e:
        raise HTTPException(500, f"Failed to read probs: {e}")
    if not doc:
        return {"eventId": eventId, "teams": [], "published": False}
    doc["published"] = True
    return doc


# ===========================================================================
# Wiki endpoints — consolidated event/player views for the wiki UI.
# Mongo collections used (read-only): builder_probs, approved_offers,
# qualification_workspaces, catalogue_players, catalogue_snapshots.
# Disk: cached leaderboard snapshots under exports/public for resolved
# placements & cross-event aggregations. Signals come from the JSONL store.
# ===========================================================================


def _wiki_event_state(begin_iso: Optional[str], end_iso: Optional[str], has_leaderboard: bool) -> str:
    """Best-effort 'live'/'done'/'future' classification."""
    now = datetime.now(timezone.utc)
    begin = _catalogue_parse_ts(begin_iso)
    end = _catalogue_parse_ts(end_iso)
    if has_leaderboard:
        if end and end < now:
            return "done"
        if begin and begin <= now and end and end >= now:
            return "live"
        return "done"
    if begin and begin > now:
        return "future"
    if begin and end and begin <= now <= end:
        return "live"
    if end and end < now:
        return "done"
    return "unknown"


def _wiki_settle_offer(offer: Dict[str, Any], rank_lookup: Dict[str, int]) -> Dict[str, Any]:
    """Mark an offer as won/lost/open relative to a team's resolved placement.

    The bookmaker wins (collects juice) when the team did NOT clear the
    market line, and loses (pays out the price) when it did. Markets are
    matched by name with a tolerant lower-cased substring rule so the
    offer pack's varied labelling ("Top 10", "top10", etc.) all work.
    """
    team_key = (offer.get("teamKey") or "").strip().lower()
    market = str(offer.get("market") or offer.get("name") or "").lower()
    rank = rank_lookup.get(team_key)
    settlement: Dict[str, Any] = {"settled": False}

    if rank is None or not market:
        return settlement

    target_rank: Optional[int] = None
    if "win" in market and "winner" in market or market.strip() == "win" or market.startswith("winner"):
        target_rank = 1
    elif "top 3" in market or "top3" in market:
        target_rank = 3
    elif "top 5" in market or "top5" in market:
        target_rank = 5
    elif "top 10" in market or "top10" in market:
        target_rank = 10
    elif "top 25" in market or "top25" in market:
        target_rank = 25
    elif "top 50" in market or "top50" in market:
        target_rank = 50
    if target_rank is None:
        return settlement

    hit = bool(rank <= target_rank)
    fair = float(offer.get("fairProbability") or offer.get("fair") or offer.get("trueProbability") or 0.0)
    juiced = float(
        offer.get("juicedProbability")
        or offer.get("priceProbability")
        or (fair + float(offer.get("juicePct") or 0.0))
    )
    juiced = max(0.0001, min(0.9999, juiced))
    decimal_odds = 1.0 / juiced if juiced else 0.0
    max_stake = float(offer.get("maxStake") or offer.get("stakeCap") or offer.get("limit") or 100.0)
    if hit:
        # Bettor wins. Bookmaker pays profit on top of returning stake.
        bookmaker_pnl = -max_stake * (decimal_odds - 1.0)
    else:
        # Bettor loses. Bookmaker keeps the stake.
        bookmaker_pnl = max_stake
    expected_pnl = max_stake * (1.0 - juiced) - max_stake * juiced * (decimal_odds - 1.0)
    return {
        "settled": True,
        "hit": hit,
        "rank": rank,
        "targetRank": target_rank,
        "decimalOdds": round(decimal_odds, 4),
        "fair": round(fair, 4),
        "juiced": round(juiced, 4),
        "maxStake": max_stake,
        "bookmakerPnl": round(bookmaker_pnl, 2),
        "expectedPnl": round(expected_pnl, 2),
    }


def _wiki_offer_unsettled_metrics(offer: Dict[str, Any]) -> Dict[str, Any]:
    """Pre-resolution view: expected risk + worst case payout for an offer."""
    fair = float(offer.get("fairProbability") or offer.get("fair") or 0.0)
    juiced = float(
        offer.get("juicedProbability")
        or offer.get("priceProbability")
        or (fair + float(offer.get("juicePct") or 0.0))
    )
    juiced = max(0.0001, min(0.9999, juiced))
    decimal_odds = 1.0 / juiced if juiced else 0.0
    max_stake = float(offer.get("maxStake") or offer.get("stakeCap") or offer.get("limit") or 100.0)
    expected_pnl = max_stake * (1.0 - juiced) - max_stake * juiced * (decimal_odds - 1.0)
    return {
        "expectedPnl": round(expected_pnl, 2),
        "worstCasePayout": round(max_stake * (decimal_odds - 1.0), 2),
        "maxStake": max_stake,
        "decimalOdds": round(decimal_odds, 4),
    }


def _wiki_load_event_leaderboard(event_id: str, window_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Find the freshest cached leaderboard snapshot for (eventId, windowId)."""
    best: Optional[Dict[str, Any]] = None
    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception:
            continue
        if data.get("eventId") != event_id:
            continue
        if window_id and data.get("windowId") and data.get("windowId") != window_id:
            continue
        if best is None or (data.get("totalEntries") or 0) > (best.get("totalEntries") or 0):
            best = data
        elif (data.get("totalEntries") or 0) == (best.get("totalEntries") or 0) and \
             (data.get("fetchedAt") or 0) > (best.get("fetchedAt") or 0):
            best = data
    return best


def _wiki_signals_for_subjects(subject_keys: set, account_ids: set, max_results: int = 25) -> List[Dict[str, Any]]:
    """Collect any signals whose subject overlaps with the supplied teamKeys/accountIds.

    Used for both event ('does any qualified team have a flag?') and player
    ('what posts mention this player?') wiki views.
    """
    if not subject_keys and not account_ids:
        return []
    try:
        store = _get_signal_store()
        sigs = store.recent(limit=max(50, max_results * 5))
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for sig in sigs:
        subject = sig.subject or {}
        team_key = (subject.get("teamKey") or "").strip().lower()
        sig_account_ids = {str(a).lower() for a in (subject.get("accountIds") or []) if a}
        hit = False
        if subject_keys and team_key and team_key in subject_keys:
            hit = True
        if not hit and account_ids and (sig_account_ids & account_ids):
            hit = True
        if not hit:
            continue
        out.append(sig.to_dict())
        if len(out) >= max_results:
            break
    return out


async def _wiki_build_event_response(
    eventId: str,
    windowId: Optional[str],
    region: Optional[str],
) -> Dict[str, Any]:
    """Compute full wiki payload (hits caches / external APIs)."""
    target_region = region
    if target_region is None:
        for r in REGIONS:
            if eventId.endswith(f"_{r}"):
                target_region = r
                break

    # Event metadata via the existing tournaments cache
    info: Optional[Dict[str, Any]] = None
    title: Optional[str] = None
    begin_iso: Optional[str] = None
    end_iso: Optional[str] = None
    if windowId:
        regions_to_try: List[str] = [target_region] if target_region else REGIONS
        for r in regions_to_try:
            try:
                payload = await _get_tournaments_raw(r, include_historic=False)
                info = extract_event_info(
                    payload, leaderboard_event_id=eventId, leaderboard_window_id=windowId,
                )
                if not info:
                    payload_h = await _get_tournaments_raw(r, include_historic=True)
                    info = extract_event_info(
                        payload_h, leaderboard_event_id=eventId, leaderboard_window_id=windowId,
                    )
                if info:
                    target_region = info.get("region") or target_region
                    title = info.get("title")
                    begin_iso = info.get("beginTime")
                    end_iso = info.get("endTime")
                    break
            except Exception as e:
                logger.warning(f"[wiki/event] info lookup failed for {r}: {e}")
    if not title:
        title = eventId.replace("epicgames_", "").replace("_", " ") or eventId

    # Workspace + qualification audit (best-effort; thin pools may fail).
    workspace = _load_qualification_workspace(eventId, target_region)
    audit: Optional[Dict[str, Any]] = None
    pool_account_ids: set = set()
    pool_team_keys: set = set()
    try:
        all_snaps = _load_all_cached_snapshots()
        pool = resolve_prediction_pool(eventId, all_snaps, target_region=target_region)
        pool = [p for p in pool if p.get("entries")]
        if pool:
            audit = _qualification_audit(
                pool,
                event_id=eventId,
                region=target_region,
                workspace=workspace,
                include_keys="",
                exclude_keys="",
            )
            for snap in pool:
                for entry in snap.get("entries") or []:
                    for aid in entry.get("accountIds") or []:
                        if aid:
                            pool_account_ids.add(str(aid).lower())
            for team in (audit or {}).get("qualifiedTeams") or []:
                tk = (team.get("teamKey") or "").strip().lower()
                if tk:
                    pool_team_keys.add(tk)
    except Exception as e:
        logger.warning(f"[wiki/event] qualification audit failed: {e}")

    # Resolved leaderboard (if cached on disk).
    leaderboard_doc = _wiki_load_event_leaderboard(eventId, windowId)
    leaderboard_section: Optional[Dict[str, Any]] = None
    rank_lookup: Dict[str, int] = {}
    if leaderboard_doc:
        entries = leaderboard_doc.get("entries") or []
        top_entries = []
        for entry in entries[:60]:
            rank = entry.get("rank")
            account_ids = [str(a).lower() for a in entry.get("accountIds") or [] if a]
            for aid in account_ids:
                if aid and aid not in rank_lookup:
                    rank_lookup[aid] = int(rank) if rank else 999999
            tk_seed = "|".join(sorted(account_ids))
            if tk_seed:
                rank_lookup[tk_seed] = int(rank) if rank else 999999
                rank_lookup[f"aid:{'+'.join(sorted(account_ids))}"] = int(rank) if rank else 999999
            top_entries.append({
                "rank": rank,
                "players": entry.get("players"),
                "playerList": entry.get("playerList") or [],
                "accountIds": account_ids,
                "pointsEarned": entry.get("pointsEarned"),
                "elims": entry.get("elims"),
                "wins": entry.get("wins"),
                "sessions": entry.get("sessions"),
                "bestPlacement": entry.get("bestPlacement"),
                "placementMean": entry.get("placementMean"),
                "placementCv": entry.get("placementCv"),
                "vrRate": entry.get("vrRate"),
            })
        leaderboard_section = {
            "totalEntries": leaderboard_doc.get("totalEntries"),
            "updatedAt": leaderboard_doc.get("updatedAt"),
            "fetchedAt": leaderboard_doc.get("fetchedAt"),
            "windowId": leaderboard_doc.get("windowId"),
            "topEntries": top_entries,
            "distribution": leaderboard_doc.get("distribution"),
        }

    state = _wiki_event_state(begin_iso, end_iso, leaderboard_section is not None)
    resolved = bool(leaderboard_section and (leaderboard_section.get("totalEntries") or 0) > 0)

    # Builder-published probabilities (server-side simulation snapshot).
    probs_doc: Optional[Dict[str, Any]] = None
    try:
        probs_doc = _m_probs_get(eventId)
    except Exception as e:
        logger.warning(f"[wiki/event] probs read failed: {e}")
    builder_probs_section: Optional[Dict[str, Any]] = None
    if probs_doc and isinstance(probs_doc.get("teams"), list):
        teams = probs_doc.get("teams") or []
        # Sort by win prob if present, else preserve order.
        teams_sorted = sorted(teams, key=lambda t: float(t.get("winProb") or 0.0), reverse=True)
        builder_probs_section = {
            "generatedAt": probs_doc.get("generatedAt"),
            "modelVersion": (
                (probs_doc.get("simulationArtifact") or {}).get("modelVersion")
                if isinstance(probs_doc.get("simulationArtifact"), dict) else None
            ),
            "note": probs_doc.get("note") or "",
            "teamCount": len(teams_sorted),
            "teams": [
                {
                    "teamKey": t.get("teamKey"),
                    "teamDisplay": t.get("teamDisplay") or t.get("name"),
                    "memberIds": t.get("memberIds") or t.get("accountIds") or [],
                    "winProb": t.get("winProb"),
                    "top3": t.get("top3"),
                    "top10": t.get("top10"),
                    "top25": t.get("top25"),
                    "placementDist": (t.get("placementDist") or [])[:50],
                }
                for t in teams_sorted
            ],
        }

    # Approved offers + PnL settlement.
    offers_section: Optional[Dict[str, Any]] = None
    pnl_section: Optional[Dict[str, Any]] = None
    try:
        offers = _m_offers_get(eventId)
    except Exception as e:
        logger.warning(f"[wiki/event] offers read failed: {e}")
        offers = []
    if offers:
        # Pre-resolution snapshot: expected risk + worst case payout per offer.
        active_offers = [o for o in offers if (o.get("decision") or o.get("state") or "Offer") in {"Offer", "offer", "Open", "open", None}]
        worst_total = 0.0
        expected_total = 0.0
        cumulative_worst: List[float] = []
        cumulative_expected: List[float] = []
        for o in active_offers:
            metrics = _wiki_offer_unsettled_metrics(o)
            worst_total += metrics["worstCasePayout"]
            expected_total += metrics["expectedPnl"]
            cumulative_worst.append(round(worst_total, 2))
            cumulative_expected.append(round(expected_total, 2))
        offers_section = {
            "count": len(offers),
            "activeCount": len(active_offers),
            "expectedRisk": round(worst_total, 2),
            "expectedPnl": round(expected_total, 2),
            "liabilityWorstCaseSeries": cumulative_worst,
            "expectedPnlSeries": cumulative_expected,
            "offers": offers,
        }

        if rank_lookup and active_offers:
            settlements = []
            cumulative_actual: List[float] = []
            actual_total = 0.0
            won = 0
            lost = 0
            unsettled = 0
            for o in active_offers:
                # Try every plausible key shape: teamKey, accountId-joined, etc.
                team_key = (o.get("teamKey") or "").strip().lower()
                rank: Optional[int] = None
                if team_key in rank_lookup:
                    rank = rank_lookup[team_key]
                else:
                    # Synthesize teamKey from member ids.
                    member_ids = [str(a).lower() for a in (o.get("memberIds") or o.get("accountIds") or []) if a]
                    if member_ids:
                        candidates = [
                            "|".join(sorted(member_ids)),
                            "aid:" + "+".join(sorted(member_ids)),
                        ]
                        for cand in candidates:
                            if cand in rank_lookup:
                                rank = rank_lookup[cand]
                                break
                        if rank is None:
                            for aid in member_ids:
                                if aid in rank_lookup:
                                    rank = rank_lookup[aid]
                                    break
                lookup = {team_key: rank} if rank is not None and team_key else {}
                if rank is not None and team_key:
                    settle = _wiki_settle_offer(o, lookup)
                else:
                    settle = {"settled": False}
                if settle.get("settled"):
                    actual_total += float(settle.get("bookmakerPnl") or 0.0)
                    if settle.get("hit"):
                        lost += 1
                    else:
                        won += 1
                else:
                    unsettled += 1
                cumulative_actual.append(round(actual_total, 2))
                settlements.append({
                    "teamKey": o.get("teamKey"),
                    "teamDisplay": o.get("teamDisplay") or o.get("teamName") or o.get("display"),
                    "market": o.get("market") or o.get("name"),
                    "line": o.get("line"),
                    "fairProbability": o.get("fairProbability") or o.get("fair"),
                    "juicedProbability": o.get("juicedProbability") or o.get("priceProbability"),
                    "decision": o.get("decision") or o.get("state"),
                    **settle,
                })
            pnl_section = {
                "settledOffers": settlements,
                "wonCount": won,
                "lostCount": lost,
                "unsettledCount": unsettled,
                "totalPnl": round(actual_total, 2),
                "actualLiabilitySeries": cumulative_actual,
                "expectedRisk": round(worst_total, 2),
                "actualRisk": round(max(0.0, -actual_total), 2),
            }

    # Signals that touch any team in the event pool.
    signals = _wiki_signals_for_subjects(pool_team_keys, pool_account_ids, max_results=30)
    insider_suspected = any(
        (sig.get("extracted") or {}).get("flag") in {"insider", "insider_info", "leaked", "scrim_form"}
        or "insider" in str(sig.get("text") or "").lower()
        for sig in signals
    )

    return {
        "eventId": eventId,
        "windowId": windowId,
        "region": target_region,
        "title": title,
        "label": title,
        "state": state,
        "resolved": resolved,
        "info": info,
        "begin": begin_iso,
        "end": end_iso,
        "qualification": audit,
        "workspace": workspace,
        "leaderboard": leaderboard_section,
        "builderProbs": builder_probs_section,
        "offers": offers_section,
        "pnl": pnl_section,
        "signals": signals,
        "insiderSuspected": insider_suspected,
    }


def _wiki_payload_to_directory_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Derive a lightweight directory row from a full wiki payload."""
    offers = payload.get("offers") or {}
    oc = int(offers.get("count") or 0) if isinstance(offers, dict) else 0
    lb = payload.get("leaderboard") or {}
    return {
        "eventId": payload.get("eventId"),
        "windowId": payload.get("windowId") or "",
        "region": payload.get("region"),
        "title": payload.get("title"),
        "label": payload.get("label"),
        "begin": payload.get("begin"),
        "end": payload.get("end"),
        "state": payload.get("state"),
        "resolved": payload.get("resolved"),
        "totalEntries": lb.get("totalEntries"),
        "hasProbs": bool(payload.get("builderProbs")),
        "hasOffers": oc > 0,
        "offerCount": oc,
    }


@app.get("/api/wiki/event")
async def api_wiki_event(
    eventId: str = Query(...),
    windowId: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    refresh: bool = Query(False),
):
    """Wiki event detail — served from Mongo snapshot when possible."""
    if not refresh:
        try:
            cached = _wiki_event_snap_get(eventId, windowId)
            pl = cached.get("payload") if cached else None
            if isinstance(pl, dict) and pl.get("eventId"):
                return pl
        except Exception as e:
            logger.warning(f"[wiki/event] mongo read failed: {e}")
    payload = await _wiki_build_event_response(eventId, windowId, region)
    try:
        _wiki_event_snap_upsert(eventId, windowId, payload)
        _wiki_dir_bulk_upsert([_wiki_payload_to_directory_row(payload)])
    except Exception as e:
        logger.warning(f"[wiki/event] mongo persist failed: {e}")
    return payload


def _wiki_player_appearances(account_id: str, all_snaps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return every leaderboard appearance for a single accountId across cached snapshots."""
    aid = (account_id or "").strip().lower()
    out: List[Dict[str, Any]] = []
    for snap in all_snaps:
        entries = snap.get("entries") or []
        total = snap.get("totalEntries") or len(entries)
        for entry in entries:
            entry_aids = [str(a).lower() for a in entry.get("accountIds") or [] if a]
            if aid not in entry_aids:
                continue
            rank = entry.get("rank") or 0
            try:
                rank_int = int(rank)
            except Exception:
                rank_int = 0
            rank_pct = 0.0
            if total and rank_int:
                rank_pct = max(0.0, min(1.0, 1.0 - (rank_int - 1) / max(1, total - 1)))
            out.append({
                "eventId": snap.get("eventId"),
                "windowId": snap.get("windowId"),
                "event": _catalogue_event_label(str(snap.get("eventId") or "")),
                "region": snap.get("region"),
                "updatedAt": snap.get("updatedAt") or snap.get("fetchedAt"),
                "rank": rank_int,
                "total": total,
                "rankPct": round(rank_pct, 4),
                "points": entry.get("pointsEarned"),
                "elims": entry.get("elims"),
                "sessions": entry.get("sessions"),
                "team": entry.get("players") or ", ".join(entry.get("playerList") or []),
                "playerList": entry.get("playerList") or [],
                "accountIds": entry_aids,
            })
    out.sort(key=lambda a: str(a.get("updatedAt") or ""))
    return out


def _wiki_player_popularity_volatility(
    account_id: str,
    appearances: List[Dict[str, Any]],
    region: Optional[str],
) -> Dict[str, Any]:
    """Compute popularity (event count) + volatility (placement spread).

    Popularity = fraction of recent published-probs events that include this
    player as a member of any team. Volatility = stdev of placement
    percentiles across cached appearances. Bet exposure = number of approved
    offers whose memberIds include this player and the cumulative max stake.
    """
    aid = (account_id or "").strip().lower()
    rank_pcts = [float(a.get("rankPct") or 0.0) for a in appearances]
    if rank_pcts:
        mean_pct = sum(rank_pcts) / len(rank_pcts)
        variance = sum((p - mean_pct) ** 2 for p in rank_pcts) / len(rank_pcts)
        stdev_pct = math.sqrt(variance)
    else:
        mean_pct = 0.0
        stdev_pct = 0.0

    # Popularity: how many published events include this player.
    published_total = 0
    published_with_player: List[str] = []
    try:
        for entry in _m_probs_list():
            published_total += 1
            ev_id = entry.get("eventId")
            if not ev_id:
                continue
            try:
                doc = _m_probs_get(ev_id)
            except Exception:
                doc = None
            if not doc or not isinstance(doc.get("teams"), list):
                continue
            for team in doc["teams"]:
                member_ids = {str(m).lower() for m in (team.get("memberIds") or team.get("accountIds") or []) if m}
                if aid in member_ids:
                    published_with_player.append(ev_id)
                    break
    except Exception as e:
        logger.warning(f"[wiki/player] popularity scan failed: {e}")

    popularity = (
        len(published_with_player) / published_total
        if published_total
        else 0.0
    )

    # Bet exposure: scan approved offers across all events that include this player.
    bet_count = 0
    total_max_stake = 0.0
    bet_event_ids: List[str] = []
    try:
        from storage import get_db as _get_db
        db = _get_db()
        cursor = db.approved_offers.find({}, {"eventId": 1, "offers": 1})
        for doc in cursor:
            ev_id = doc.get("eventId") or doc.get("_id")
            event_hits = 0
            for offer in doc.get("offers") or []:
                member_ids = {str(m).lower() for m in (offer.get("memberIds") or offer.get("accountIds") or []) if m}
                team_key = (offer.get("teamKey") or "").lower()
                # `team_key` is often pipe-joined accountIds. Membership check
                # falls through to the fast string match if member_ids isn't
                # populated on legacy rows.
                if (member_ids and aid in member_ids) or (team_key and aid in team_key):
                    event_hits += 1
                    total_max_stake += float(offer.get("maxStake") or offer.get("stakeCap") or offer.get("limit") or 100.0)
            if event_hits:
                bet_count += event_hits
                bet_event_ids.append(str(ev_id))
    except Exception as e:
        logger.debug(f"[wiki/player] bet exposure scan skipped: {e}")

    return {
        "popularity": round(popularity, 4),
        "popularityEvents": published_with_player,
        "publishedTotal": published_total,
        "volatility": round(stdev_pct, 4),
        "averagePlacementPercentile": round(mean_pct, 4),
        "betCount": bet_count,
        "betEventIds": bet_event_ids,
        "totalMaxStakeExposure": round(total_max_stake, 2),
    }


@app.get("/api/wiki/player")
async def api_wiki_player(
    region: str = Query("EU"),
    playerId: str = Query(...),
    refresh: bool = Query(False),
):
    """Aggregate everything the player wiki page needs in one call."""
    region_key = (region or "EU").upper()
    if not refresh:
        try:
            cached = _wiki_player_snap_get(region_key, playerId)
            pl = cached.get("payload") if cached else None
            if isinstance(pl, dict) and pl.get("id"):
                return pl
        except Exception as e:
            logger.warning(f"[wiki/player] mongo read failed: {e}")
    profile: Optional[Dict[str, Any]] = None
    try:
        profile = _m_catalogue_profile_get(region_key, playerId)
    except Exception as e:
        logger.warning(f"[wiki/player] catalogue read failed: {e}")
    # Fall back to a freshly-built catalogue profile when nothing has been
    # persisted yet (e.g. first time the wiki page is visited for a player).
    if not profile:
        try:
            built = _catalogue_build(region_key, 200, 36)
            profile = next(
                (p for p in built.get("players") or [] if str(p.get("id")) == str(playerId).lower().strip()),
                None,
            )
        except Exception as e:
            logger.warning(f"[wiki/player] catalogue rebuild failed: {e}")
    if not profile:
        raise HTTPException(404, f"No catalogue data for player {playerId} in {region_key}.")

    aid = str(profile.get("id") or playerId).lower().strip()
    name = profile.get("name") or playerId
    all_snaps = _load_all_cached_snapshots()
    appearances = _wiki_player_appearances(aid, all_snaps)
    metrics = _wiki_player_popularity_volatility(aid, appearances, region_key)

    # Top-line cross-event stats
    if appearances:
        ranks = [a.get("rank") for a in appearances if a.get("rank")]
        best_rank = min(ranks) if ranks else None
        avg_rank = round(sum(ranks) / len(ranks), 2) if ranks else None
        top_100 = sum(1 for a in appearances if (a.get("rank") or 999999) <= 100)
        top_200 = sum(1 for a in appearances if (a.get("rank") or 999999) <= 200)
        wins = sum(1 for a in appearances if (a.get("rank") or 999999) == 1)
        total_elims = sum(int(a.get("elims") or 0) for a in appearances)
        last_seen_at = appearances[-1].get("updatedAt") if appearances else None
    else:
        best_rank = None
        avg_rank = None
        top_100 = 0
        top_200 = 0
        wins = 0
        total_elims = 0
        last_seen_at = None

    # Recent score-changing events (as already pre-computed by the catalogue)
    recent_changes = profile.get("eventImpacts") or []
    decisions = profile.get("eventDecisions") or {}
    manual_overrides = profile.get("manualScores") or {}

    # Signals that mention this account
    signals = _wiki_signals_for_subjects(set(), {aid}, max_results=15)

    payload = {
        "id": aid,
        "name": name,
        "region": region_key,
        "catalogue": profile,
        "stats": {
            "eventsPlayed": len(appearances),
            "bestRank": best_rank,
            "averageRank": avg_rank,
            "averagePlacementPercentile": metrics["averagePlacementPercentile"],
            "top100Count": top_100,
            "top200Count": top_200,
            "winsCount": wins,
            "totalElims": total_elims,
            "lastSeenAt": last_seen_at,
            "popularity": metrics["popularity"],
            "popularityEvents": metrics["popularityEvents"],
            "publishedTotal": metrics["publishedTotal"],
            "volatility": metrics["volatility"],
            "betCount": metrics["betCount"],
            "betEventIds": metrics["betEventIds"],
            "totalMaxStakeExposure": metrics["totalMaxStakeExposure"],
        },
        "appearances": appearances,
        "recentChanges": recent_changes,
        "decisions": decisions,
        "manualOverrides": manual_overrides,
        "signals": signals,
    }
    try:
        _wiki_player_snap_upsert(region_key, playerId, payload)
    except Exception as e:
        logger.warning(f"[wiki/player] mongo persist failed: {e}")
    return payload


@app.get("/api/wiki/players/search")
async def api_wiki_players_search(
    q: str = Query(..., min_length=1),
    region: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """Lightweight typeahead — pulled from cached catalogue snapshots."""
    needle = (q or "").strip().lower()
    if not needle:
        return {"query": q, "results": []}
    regions = [region] if region else REGIONS
    seen: set = set()
    results: List[Dict[str, Any]] = []
    for r in regions:
        try:
            snap = _m_catalogue_snapshot_get(r)
        except Exception:
            snap = None
        if not snap:
            continue
        for p in snap.get("players") or []:
            name = str(p.get("name") or "").lower()
            pid = str(p.get("id") or "").lower()
            if needle not in name and needle not in pid:
                continue
            key = (r, pid)
            if key in seen:
                continue
            seen.add(key)
            results.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "region": r,
                "baselineRank": p.get("baselineRank"),
                "baselineScore": p.get("baselineScore"),
                "catalogueType": p.get("catalogueType"),
            })
            if len(results) >= limit:
                break
        if len(results) >= limit:
            break
    return {"query": q, "results": results[:limit]}


async def _wiki_merge_directory_rows_live() -> List[Dict[str, Any]]:
    """Merge tournaments feed + disk leaderboard cache + prob/offer flags (slow)."""
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for r in REGIONS:
        try:
            payload = await _get_tournaments_raw(r, include_historic=False)
            payload_h = await _get_tournaments_raw(r, include_historic=True)
        except Exception as e:
            logger.warning(f"[wiki/events] tournament load failed for {r}: {e}")
            continue
        for src in (payload, payload_h):
            for row in summarize_tournaments_response(src):
                ev_id = row.get("leaderboardEventId") or row.get("eventId")
                wi_id = row.get("leaderboardEventWindowId") or row.get("windowId") or ""
                if not ev_id:
                    continue
                key = (str(ev_id), str(wi_id))
                cur_state = (row.get("state") or "").lower()
                if cur_state not in {"done", "live", "future"}:
                    cur_state = _wiki_event_state(row.get("begin"), row.get("end"), False)
                doc = out.get(key) or {}
                doc.update({
                    "eventId": ev_id,
                    "windowId": wi_id,
                    "region": row.get("region") or r,
                    "title": row.get("title") or row.get("eventGroup") or ev_id,
                    "label": row.get("title") or row.get("eventGroup") or ev_id,
                    "begin": row.get("begin"),
                    "end": row.get("end"),
                    "state": cur_state,
                    "roundLabel": row.get("roundLabel"),
                    "format": row.get("format"),
                    "cap": row.get("poolSize"),
                })
                out[key] = doc

    for path in sorted(PUBLIC_CACHE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception:
            continue
        ev_id = data.get("eventId") or ""
        wi_id = data.get("windowId") or ""
        if not ev_id:
            continue
        key = (ev_id, wi_id)
        existing = out.get(key) or {}
        existing.setdefault("eventId", ev_id)
        existing.setdefault("windowId", wi_id)
        existing.setdefault("title", _catalogue_event_label(ev_id))
        existing.setdefault("label", _catalogue_event_label(ev_id))
        existing["state"] = "done" if (data.get("totalEntries") or 0) > 0 else existing.get("state", "unknown")
        existing["resolved"] = (data.get("totalEntries") or 0) > 0
        existing["totalEntries"] = data.get("totalEntries")
        existing.setdefault("region", existing.get("region"))
        out[key] = existing

    try:
        prob_events = {row.get("eventId") for row in _m_probs_list() if row.get("eventId")}
    except Exception:
        prob_events = set()
    for row in out.values():
        row["hasProbs"] = bool(row.get("eventId") in prob_events)
        try:
            offers = _m_offers_get(row.get("eventId"))
            row["hasOffers"] = bool(offers)
            row["offerCount"] = len(offers or [])
        except Exception:
            row["hasOffers"] = False
            row["offerCount"] = 0

    rows = list(out.values())
    rows.sort(key=lambda r: str(r.get("begin") or r.get("end") or ""), reverse=True)
    return rows


@app.get("/api/wiki/events")
async def api_wiki_events(
    region: Optional[str] = Query(None),
    limit: int = Query(60, ge=1, le=200),
    state: Optional[str] = Query(None, description="'done' | 'live' | 'future' filter"),
    refresh: bool = Query(False),
):
    """Directory listing — stored in Mongo; live merge only when empty or refresh=true."""
    try:
        if refresh:
            _wiki_dir_clear()
            merged = await _wiki_merge_directory_rows_live()
            _wiki_dir_bulk_upsert(merged)
        elif _wiki_dir_count() == 0:
            merged = await _wiki_merge_directory_rows_live()
            _wiki_dir_bulk_upsert(merged)
    except Exception as e:
        logger.warning(f"[wiki/events] directory rebuild failed: {e}")
    rows, total = _wiki_dir_find(region, state, limit)
    return {"count": len(rows), "total": total, "events": rows}


@app.post("/api/wiki/sync-directory")
async def api_wiki_sync_directory():
    """Force a full directory rebuild from tournaments + disk cache."""
    deleted = 0
    try:
        deleted = _wiki_dir_clear()
    except Exception as e:
        logger.warning(f"[wiki/sync-directory] clear failed: {e}")
    merged = await _wiki_merge_directory_rows_live()
    upserted = _wiki_dir_bulk_upsert(merged)
    return {"ok": True, "cleared": deleted, "upserted": upserted, "total": len(merged)}


# ===========================================================================
# Drop Map endpoints (Phase 7c)
# ===========================================================================


class DropmapAssignmentsBody(BaseModel):
    assignments: List[Dict[str, Any]]


class DropmapPasteBody(BaseModel):
    text: str


def _load_poi_coords() -> List[Dict[str, Any]]:
    if not POI_COORDS_PATH.exists():
        return []
    try:
        d = json.loads(POI_COORDS_PATH.read_text(encoding="utf-8"))
        if isinstance(d, dict):
            return d.get("pois") or []
        if isinstance(d, list):
            return d
    except Exception as e:
        logger.warning(f"[dropmap] failed to read POI coords: {e}")
    return []


def _available_map_images() -> List[str]:
    if not ASSETS_MAPS_DIR.exists():
        return []
    return sorted(p.name for p in ASSETS_MAPS_DIR.iterdir() if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp"))


# NOTE: _dropmap_file() used to point at data/dropmap/*.json. Removed now
# that assignments live in db.dropmap_assignments.


def _compute_contested(assignments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Mark any POI with 2+ teams as contested on each assignment."""
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for a in assignments:
        poi = (a.get("poiName") or "").strip()
        if not poi:
            continue
        buckets.setdefault(poi.lower(), []).append(a)
    for rows in buckets.values():
        contested = len(rows) >= 2
        for r in rows:
            r["contested"] = contested
    return assignments


@app.get("/api/dropmap/{eventId}")
async def api_dropmap_get(eventId: str):
    """
    Return everything the Drop Map needs to render: the background image
    URL, POI coordinates, current team-to-POI assignments, and the latest
    published market probabilities.

    All mutable state is backed by MongoDB (``dropmap_assignments`` and
    ``builder_probs`` collections).
    """
    try:
        assignments = _m_dropmap_get(eventId)
    except Exception as e:
        logger.warning(f"[dropmap] mongo read failed: {e}")
        assignments = []

    pois = _load_poi_coords()
    maps = _available_map_images()
    default_map = maps[0] if maps else None

    # Pull latest published probs (best-effort).
    probs = None
    try:
        probs = _m_probs_get(eventId)
    except Exception as e:
        logger.warning(f"[dropmap] probs read failed: {e}")

    # Also list what events DO have published probs so the UI can tell the
    # user "hey, you published for NAC but this map is EU".
    try:
        published_events = _m_probs_list()
    except Exception as e:
        logger.warning(f"[dropmap] probs list failed: {e}")
        published_events = []

    contested_groups: Dict[str, List[str]] = {}
    for a in assignments:
        poi = (a.get("poiName") or "").strip().lower()
        if not poi:
            continue
        contested_groups.setdefault(poi, []).append(a.get("teamKey") or a.get("teamDisplay") or "?")

    return {
        "eventId": eventId,
        "mapImage": f"/assets/maps/{default_map}" if default_map else None,
        "availableMaps": [f"/assets/maps/{m}" for m in maps],
        "pois": pois,
        "assignments": assignments,
        "contestedGroups": {k: v for k, v in contested_groups.items() if len(v) >= 2},
        "marketProbs": probs,
        "publishedEvents": published_events,
        "overlayRev": _overlay_rev(),
    }


@app.put("/api/dropmap/{eventId}/assignments")
async def api_dropmap_put(eventId: str, payload: DropmapAssignmentsBody):
    """Bulk upsert. Recomputes contested flags from scratch."""
    rows = list(payload.assignments or [])
    rows = _compute_contested(rows)
    try:
        _m_dropmap_put(eventId, rows)
    except Exception as e:
        raise HTTPException(500, f"Failed to persist dropmap: {e}")

    # Bump rev so the Builder re-pulls soft-data-linked state (contest risk v32).
    try:
        _m_bump_rev()
    except Exception:
        pass
    return {"ok": True, "count": len(rows)}


@app.post("/api/dropmap/{eventId}/import-paste")
async def api_dropmap_import_paste(eventId: str, payload: DropmapPasteBody):
    """
    Accept a blob of text pasted from nobleprac or similar. Each non-empty
    line should be `Player Player : POI Name`. Returns the parsed rows.
    """
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(400, "text required")

    # Build a display-name -> teamKey map from cached pool snapshots.
    from signals.extractor import load_subject_index, _fold
    def _snaps():
        return _load_all_cached_snapshots()
    idx = load_subject_index(_snaps)

    rows: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if ":" not in line:
            # Allow "POI — Team Name" style too; split on em-dash if no colon.
            if "—" in line:
                poi_part, _, team_part = line.partition("—")
            elif "\t" in line:
                poi_part, _, team_part = line.partition("\t")
            else:
                continue
        else:
            team_part, _, poi_part = line.partition(":")
        team_part = team_part.strip()
        poi_part = poi_part.strip()
        if not team_part or not poi_part:
            continue

        # Try to link each individual name in team_part to a subject.
        names = [n for n in team_part.replace(",", " ").split() if n]
        accountIds: List[str] = []
        teamKeys: set = set()
        display_names: List[str] = []
        for nm in names:
            key = _fold(nm).strip()
            if key in idx:
                s = idx[key]
                for a in (s.get("accountIds") or []):
                    if a not in accountIds:
                        accountIds.append(a)
                if s.get("teamKey"):
                    teamKeys.add(s["teamKey"])
                if s.get("display"):
                    display_names.append(s["display"])

        team_key = None
        if len(teamKeys) == 1:
            team_key = next(iter(teamKeys))
        elif accountIds:
            team_key = "|".join(sorted(a.lower() for a in accountIds))

        rows.append({
            "teamKey": team_key,
            "accountIds": [a.lower() for a in accountIds],
            "teamDisplay": " · ".join(display_names) if display_names else team_part,
            "poiName": poi_part,
            "source": "paste",
            "raw": line,
        })

    rows = _compute_contested(rows)
    # Merge with any existing assignments so an import that's missing a
    # team doesn't blow away the one that was assigned manually.
    try:
        existing = _m_dropmap_get(eventId)
    except Exception as e:
        logger.warning(f"[dropmap-import] mongo read failed: {e}")
        existing = []
    by_key: Dict[str, Dict[str, Any]] = {
        (a.get("teamKey") or a.get("teamDisplay") or ""): a for a in existing if a
    }
    for r in rows:
        key = r.get("teamKey") or r.get("teamDisplay") or ""
        if not key:
            continue
        by_key[key] = r
    merged = _compute_contested(list(by_key.values()))
    try:
        _m_dropmap_put(eventId, merged)
    except Exception as e:
        raise HTTPException(500, f"Failed to persist dropmap: {e}")
    try:
        _m_bump_rev()
    except Exception:
        pass
    return {"ok": True, "importedRows": len(rows), "totalAssignments": len(merged)}


@app.get("/api/dropmap/pois")
async def api_dropmap_pois():
    """Return the POI coordinate catalog (hand-built asset)."""
    return {"pois": _load_poi_coords(), "maps": [f"/assets/maps/{m}" for m in _available_map_images()]}


class PoiCoordsBody(BaseModel):
    pois: List[Dict[str, Any]]


@app.put("/api/dropmap/pois")
async def api_dropmap_pois_put(payload: PoiCoordsBody):
    """Overwrite the POI coordinate catalog."""
    try:
        POI_COORDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        POI_COORDS_PATH.write_text(
            json.dumps({"pois": payload.pois}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to write POI coords: {e}")
    return {"ok": True, "count": len(payload.pois)}


class ApprovedOffersBody(BaseModel):
    eventId: str
    offers: List[Dict[str, Any]]


@app.post("/api/dropmap/approved-offers")
async def api_dropmap_approved_offers_save(payload: ApprovedOffersBody):
    """
    Persist the current Offer/Hold/Pass state for the given event so the
    user's selections survive a reload. The per-user-friendly export is
    handled client-side — this is just durability.
    """
    try:
        _m_offers_put(payload.eventId, payload.offers)
    except Exception as e:
        raise HTTPException(500, f"Failed to write approved offers: {e}")
    return {"ok": True, "count": len(payload.offers)}


@app.get("/api/dropmap/approved-offers")
async def api_dropmap_approved_offers_get(eventId: str = Query(...)):
    try:
        offers = _m_offers_get(eventId)
    except Exception as e:
        raise HTTPException(500, f"Failed to read approved offers: {e}")
    return {"eventId": eventId, "offers": offers}


@app.post("/api/markets/backfill")
async def api_markets_backfill(
    region: str = Query(..., description="Region to backfill, e.g. NAC, EU"),
    limit: int = Query(10, ge=1, le=60, description="Max events to fetch"),
    pages: int = Query(5, ge=1, le=20, description="Leaderboard depth per event"),
    historic: bool = Query(
        False,
        description="If true, pull includeHistoricData=true so past-season events show up.",
    ),
):
    """
    Auto-fetch the N most recent 'done' events in a region so the markets
    analyzer has enough history. Existing cached events are skipped unless
    they have expired. Returns a per-event status list.
    """
    if region not in REGIONS:
        raise HTTPException(400, f"Unknown region '{region}'. Valid: {REGIONS}")

    try:
        if historic:
            raw = await public_client.list_tournaments(region=region, include_historic=True)
        else:
            raw = await _get_tournaments_raw(region)
        events = summarize_tournaments_response(raw)
    except Exception as e:
        raise HTTPException(502, f"Public API error: {e}")

    done = [
        e for e in events
        if e.get("state") == "done"
        and e.get("leaderboardEventId")
        and e.get("leaderboardEventWindowId")
    ][:limit]

    results: List[Dict[str, Any]] = []
    for ev in done:
        ev_id = ev["leaderboardEventId"]
        win_id = ev["leaderboardEventWindowId"]
        disk_path = _disk_cache_path(ev_id, win_id, pages)
        cached = _disk_cache_read(disk_path, Config.LEADERBOARD_TTL_SECONDS * 288)  # ~1 day reuse for backfill
        if cached is not None:
            results.append(
                {
                    "eventId": ev_id,
                    "windowId": win_id,
                    "title": ev.get("title"),
                    "status": "cached",
                    "totalEntries": cached.get("totalEntries"),
                }
            )
            continue
        try:
            data = await public_client.get_full_leaderboard(ev_id, win_id, max_pages=pages)
            lb = data.get("leaderboard") or {}
            entries = normalize_leaderboard_entries(lb.get("entries") or [])
            distribution = summarize_leaderboard_distribution(entries)
            payload = {
                "eventId": ev_id,
                "windowId": win_id,
                "updatedAt": lb.get("updatedAt"),
                "fetchedAt": _now(),
                "totalPages": lb.get("totalPages"),
                "pagesFetched": lb.get("pagesFetched", 1),
                "entries": entries,
                "totalEntries": len(entries),
                "distribution": distribution,
                "statLabels": STAT_LABELS,
            }
            _cache_set(_leaderboard_cache, f"{ev_id}|{win_id}|{pages}", payload, _LEADERBOARD_CACHE_MAX)
            _disk_cache_write(disk_path, payload)
            results.append(
                {
                    "eventId": ev_id,
                    "windowId": win_id,
                    "title": ev.get("title"),
                    "status": "fetched",
                    "totalEntries": len(entries),
                }
            )
        except Exception as e:
            results.append(
                {
                    "eventId": ev_id,
                    "windowId": win_id,
                    "title": ev.get("title"),
                    "status": "failed",
                    "error": str(e)[:200],
                }
            )

    return {
        "region": region,
        "pages": pages,
        "eventsConsidered": len(done),
        "results": results,
    }


@app.get("/api/leaderboard.csv")
async def api_leaderboard_csv(
    eventId: str,
    windowId: str,
    pages: int = Query(10, ge=1, le=100),
):
    data = await api_leaderboard(
        eventId=eventId,
        windowId=windowId,
        pages=pages,
        minSessions=0,
        search=None,
        force=False,
        includeSeries=False,
    )
    buf = io.StringIO()
    cols = [
        "rank",
        "players",
        "pointsEarned",
        "sessions",
        "unscoredSessions",
        "elims",
        "elimsMean",
        "elimsStdev",
        "elimsCv",
        "elimsMax",
        "placementMean",
        "placementMedian",
        "placementStdev",
        "placementCv",
        "bestPlacement",
        "worstPlacement",
        "wins",
        "vrRate",
        "top1Pct",
        "top5Pct",
        "top10Pct",
        "top25Pct",
        "top50Pct",
        "tiebreakerSum",
        "timeAliveTotal",
        "placementTrend",
        "elimsTrend",
        "percentile",
        "teamId",
    ]
    buf.write(",".join(cols) + "\n")
    for e in data.get("entries", []):
        row = []
        for c in cols:
            v = e.get(c)
            if v is None:
                row.append("")
            elif isinstance(v, str):
                row.append('"' + v.replace('"', '""') + '"')
            else:
                row.append(str(v))
        buf.write(",".join(row) + "\n")
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{windowId}.csv"'},
    )


# ---------------------------------------------------------------------------
# Paid-API explorer (legacy; available at /explorer)
# ---------------------------------------------------------------------------
class APIRequest(BaseModel):
    endpoint: str
    method: str = "GET"
    params: Dict[str, Any] = {}
    path_params: Dict[str, str] = {}
    body: Dict[str, Any] = None


@app.get("/api/endpoints")
async def get_endpoints():
    return API_ENDPOINTS


@app.post("/api/request")
async def make_request(req: APIRequest):
    result = await paid_client.request(
        req.method, req.endpoint, req.params, req.path_params, req.body
    )
    if req.endpoint == "/tournaments/stats" and result.get("success"):
        window_id = req.params.get("eventWindowId", "unknown")
        try:
            proc = _get_processor()
            processed = proc.process_leaderboard(
                {"eventWindowId": window_id, "players": result["data"].get("players", [])}
            )
            if len(paid_cache) >= _PAID_CACHE_MAX:
                oldest = next(iter(paid_cache))
                paid_cache.pop(oldest, None)
            paid_cache[window_id] = {
                "data": processed,
                "df": proc.to_dataframe(processed),
            }
        except Exception as e:
            logger.error(f"Failed to process for cache: {e}")
    return result


@app.get("/api/export/excel/{window_id}")
async def export_excel(window_id: str):
    if window_id in paid_cache:
        df = paid_cache[window_id]["df"]
        buf = _get_exporter().export_leaderboard(df, window_id)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={window_id}.xlsx"},
        )

    xlsx_path = EXPORTS_DIR / f"{window_id}.xlsx"
    if xlsx_path.exists():
        return StreamingResponse(
            iter([xlsx_path.read_bytes()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={window_id}.xlsx"},
        )

    stats_path = EXPORTS_DIR / f"{window_id}_stats.json"
    if stats_path.exists():
        try:
            with open(stats_path, "r", encoding="utf-8-sig") as fp:
                payload = json.load(fp)
            players = (payload.get("data") or {}).get("players") or []
            proc = _get_processor()
            processed = proc.process_leaderboard(
                {"eventWindowId": window_id, "players": players}
            )
            df = proc.to_dataframe(processed)
            buf = _get_exporter().export_leaderboard(df, window_id)
            return StreamingResponse(
                buf,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={window_id}.xlsx"},
            )
        except Exception as e:
            logger.error(f"Excel export regenerate failed for {window_id}: {e}")

    raise HTTPException(404, f"No leaderboard data for '{window_id}'.")


@app.get("/api/logs")
async def get_logs():
    return {"logs": paid_client.get_logs()}


@app.post("/api/logs/clear")
async def clear_logs():
    paid_client.clear_logs()
    return {"ok": True}


@app.get("/api/ping")
async def ping():
    return await paid_client.ping()


@app.get("/api/credits")
async def credits():
    c = await paid_client.get_credits()
    return {"credits": c}


# Mount React at site root last so /api/* and other routes take precedence.
if REACT_BUILDER_DIST.exists():
    app.mount(
        "/",
        StaticFiles(directory=str(REACT_BUILDER_DIST), html=True),
        name="builder-root",
    )
    logger.info(f"Mounted React Rating Builder at / (and /builder) from {REACT_BUILDER_DIST}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=Config.HOST, port=Config.PORT, reload=True)
