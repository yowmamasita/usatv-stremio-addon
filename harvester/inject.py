"""Inject working harvested streams into existing Stremio addon catalogs."""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlparse

CATALOG_PATH = Path(__file__).resolve().parent.parent / "catalog" / "tv" / "all.json"
META_DIR = Path(__file__).resolve().parent.parent / "meta" / "tv"
GENRE_DIR = Path(__file__).resolve().parent.parent / "catalog" / "tv" / "all"

_NON_US_SUFFIXES = [
    "international", "italia", "indonesia", "finland", "arabic", "uk ",
    "canada", "brasil", "brazil", "india", "japan", "korea", "turkish",
    "turkey", "europe", "asia", "africa", "españa", "france", "french",
    "german", "deutsch", "portugal", "chinese", "russian", "australia",
    "philippines", "pakistan", "middle east", "baltia", "baltic",
    "français", "pусский", "español", " sub", " ava",
]

_NON_US_URL_PATTERNS = ["qvcuk", "tvkaista.net"]


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _quality_label(result: dict) -> str:
    res = (result.get("codecs") or {}).get("resolution", "")
    if not res:
        codec = (result.get("codecs") or {}).get("video", "")
        if not codec:
            return "Audio"
        return "SD"
    w = int(res.split("x")[0]) if "x" in res else 0
    if w >= 1920:
        return "FHD"
    if w >= 1280:
        return "HD"
    if w >= 720:
        return "SD"
    return "SD"


def _source_tag(url: str) -> str:
    host = urlparse(url).hostname or ""
    parts = host.replace("www.", "").split(".")
    if len(parts) >= 2:
        return parts[0][:12].upper()
    return "HV"


def _make_stream_entry(result: dict) -> dict:
    quality = _quality_label(result)
    tag = _source_tag(result["url"])
    return {
        "url": result["url"],
        "behaviorHints": {"notWebReady": True},
        "name": quality,
        "description": f"HV:{tag}",
    }


def _match_streams(channels: list[dict], working: list[dict], catalog_norms: set[str]) -> dict[str, list[dict]]:
    """Match working streams to catalog channels. Returns {channel_id: [results]}."""
    matches: dict[str, list[dict]] = {}

    for ch in channels:
        ch_name = ch["name"]
        ch_norm = _normalize(ch_name)
        ch_lower = ch_name.lower().strip()
        ch_id = ch["id"]

        for r in working:
            stream_name = (r.get("channel_name") or "").strip()
            if not stream_name:
                continue
            s_norm = _normalize(stream_name)
            s_lower = stream_name.lower().strip()

            if s_norm == ch_norm:
                matches.setdefault(ch_id, []).append(r)
                continue

            if len(ch_norm) < 3:
                continue
            if not s_norm.startswith(ch_norm):
                continue
            if not s_lower.startswith(ch_lower):
                continue
            if len(s_lower) > len(ch_lower) and s_lower[len(ch_lower)] not in " ([-/":
                continue

            suffix = stream_name[len(ch_name):].lower()
            if any(ind in suffix for ind in _NON_US_SUFFIXES):
                continue

            url_lower = r.get("url", "").lower()
            if any(pat in url_lower for pat in _NON_US_URL_PATTERNS):
                continue

            more_specific = any(
                other != ch_norm and other.startswith(ch_norm) and s_norm.startswith(other)
                for other in catalog_norms
            )
            if more_specific:
                continue

            matches.setdefault(ch_id, []).append(r)

    return matches


def inject(test_results_path: str = "data/test_results.json") -> dict:
    results_path = Path(__file__).resolve().parent.parent / test_results_path
    results = json.load(open(results_path))
    working = [r for r in results if r["status"] == "working"]

    catalog = json.load(open(CATALOG_PATH))
    channels = catalog["metas"]

    catalog_norms = {_normalize(ch["name"]) for ch in channels}

    channel_matches = _match_streams(channels, working, catalog_norms)

    stats = {"channels_updated": 0, "streams_added": 0}

    for ch in channels:
        streams = ch.get("streams", [])
        existing_urls = {s["url"] for s in streams}

        new_streams = [
            r for r in channel_matches.get(ch["id"], [])
            if r["url"] not in existing_urls
        ]

        if new_streams:
            for r in new_streams:
                entry = _make_stream_entry(r)
                streams.append(entry)
                existing_urls.add(r["url"])
                stats["streams_added"] += 1
            stats["channels_updated"] += 1

        ch["streams"] = streams

    json.dump(catalog, open(CATALOG_PATH, "w"), separators=(",", ":"))

    genre_channels: dict[str, list] = {}
    for ch in channels:
        genre = ch.get("genre", "")
        if genre:
            genre_channels.setdefault(genre, []).append(ch)

    GENRE_DIR.mkdir(parents=True, exist_ok=True)
    for genre, chs in genre_channels.items():
        genre_file = GENRE_DIR / f"genre={genre}.json"
        json.dump({"metas": chs}, open(genre_file, "w"), separators=(",", ":"))

    for ch in channels:
        meta_file = META_DIR / f"{ch['id']}.json"
        if meta_file.exists():
            json.dump({"meta": ch}, open(meta_file, "w"), separators=(",", ":"))

    return stats


if __name__ == "__main__":
    stats = inject()
    print(f"Channels updated: {stats['channels_updated']}")
    print(f"Streams added: {stats['streams_added']}")
