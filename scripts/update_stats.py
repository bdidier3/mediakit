#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL

ROOT = Path(__file__).resolve().parents[1]
STATS_DIR = ROOT / "stats"
OUTPUT_COMPACT = STATS_DIR / "stats_compact.json"
OUTPUT_FULL = STATS_DIR / "stats.json"

YOUTUBE_CHANNEL_ID = "UCMxkVdDL3CkI9tog4m_Nf4g"

SOURCES = [
    {
        "platform": "tiktok",
        "urls": [
            "https://www.tiktok.com/@clip2ep.fan",
        ],
    },
    {
        "platform": "youtube",
        "urls": [
            "https://www.youtube.com/@clip2ep-fan/shorts",
            "https://www.youtube.com/@clip2ep-fan/videos",
            "https://www.youtube.com/@clip2ep-fan/streams",
            "https://www.youtube.com/@clip2ep-fan/live",
        ],
    },
]

MAX_ITEMS_PER_SOURCE = 1200


def _to_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _clean_text(value: Any) -> str:
    if not value:
        return ""
    return str(value).strip()


def _http_get_json(base_url: str, params: dict[str, Any]) -> dict[str, Any]:
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base_url}?{query}"
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_youtube_via_api(api_key: str, channel_id: str) -> list[dict[str, Any]]:
    collected_ids: list[str] = []
    next_page_token = None

    while True:
        payload = _http_get_json(
            "https://www.googleapis.com/youtube/v3/search",
            {
                "key": api_key,
                "part": "id",
                "channelId": channel_id,
                "maxResults": 50,
                "order": "date",
                "type": "video",
                "pageToken": next_page_token,
            },
        )

        items = payload.get("items", [])
        for item in items:
            video_id = _clean_text(((item or {}).get("id") or {}).get("videoId"))
            if video_id:
                collected_ids.append(video_id)

        next_page_token = payload.get("nextPageToken")
        if not next_page_token:
            break

    # Keep insertion order while removing duplicates.
    unique_ids = list(dict.fromkeys(collected_ids))
    videos: list[dict[str, Any]] = []

    for i in range(0, len(unique_ids), 50):
        batch_ids = unique_ids[i : i + 50]
        payload = _http_get_json(
            "https://www.googleapis.com/youtube/v3/videos",
            {
                "key": api_key,
                "part": "snippet,statistics",
                "id": ",".join(batch_ids),
                "maxResults": 50,
            },
        )

        for item in payload.get("items", []):
            video_id = _clean_text(item.get("id"))
            snippet = item.get("snippet") or {}
            stats = item.get("statistics") or {}

            if not video_id:
                continue

            videos.append(
                {
                    "platform": "youtube",
                    "id": video_id,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "title": _clean_text(snippet.get("title") or "Video"),
                    "description": _clean_text(snippet.get("description") or ""),
                    "views": _to_int(stats.get("viewCount")),
                    "likes": _to_int(stats.get("likeCount")),
                    "comments": _to_int(stats.get("commentCount")),
                }
            )

    return videos


def _entry_to_video(entry: dict[str, Any], platform: str) -> dict[str, Any]:
    video_id = _clean_text(entry.get("id"))
    webpage_url = _clean_text(entry.get("webpage_url"))
    if not webpage_url:
        webpage_url = _clean_text(entry.get("url"))

    title = _clean_text(entry.get("title") or entry.get("description") or "Video")
    description = _clean_text(entry.get("description") or title)

    return {
        "platform": platform,
        "id": video_id,
        "url": webpage_url,
        "title": title,
        "description": description,
        "views": _to_int(entry.get("view_count") or entry.get("views")),
        "likes": _to_int(entry.get("like_count") or entry.get("likes")),
        "comments": _to_int(entry.get("comment_count") or entry.get("comments")),
    }


def _extract_from_source(platform: str, url: str) -> list[dict[str, Any]]:
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
        "noplaylist": False,
        "playlistend": MAX_ITEMS_PER_SOURCE,
        "ignoreerrors": True,
        "extractor_retries": 2,
        "socket_timeout": 25,
    }

    videos: list[dict[str, Any]] = []

    with YoutubeDL(ydl_opts) as ydl:
        data = ydl.extract_info(url, download=False)

    if not data:
        return videos

    entries = data.get("entries") if isinstance(data, dict) else None
    if not entries and isinstance(data, dict):
        entries = [data]

    for item in entries or []:
        if not isinstance(item, dict):
            continue
        video = _entry_to_video(item, platform)
        if not video["url"] and video["id"]:
            if platform == "youtube":
                video["url"] = f"https://www.youtube.com/watch?v={video['id']}"
            elif platform == "tiktok":
                video["url"] = f"https://www.tiktok.com/@clip2ep.fan/video/{video['id']}"
        if not video["id"] and not video["url"]:
            continue
        videos.append(video)

    return videos


def _dedupe(videos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}

    for video in videos:
        key = video.get("url") or f"{video.get('platform')}:{video.get('id')}"
        if not key:
            continue

        existing = by_key.get(key)
        if existing is None or _to_int(video.get("views")) > _to_int(existing.get("views")):
            by_key[key] = video

    return list(by_key.values())


def _video_key(video: dict[str, Any]) -> str:
    url = _clean_text(video.get("url"))
    if url:
        return url
    return f"{_clean_text(video.get('platform'))}:{_clean_text(video.get('id'))}"


def _merge_with_previous(
    extracted: list[dict[str, Any]],
    previous: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}

    for old in previous:
        if not isinstance(old, dict):
            continue
        key = _video_key(old)
        if key:
            by_key[key] = old

    for fresh in extracted:
        key = _video_key(fresh)
        if not key:
            continue

        old = by_key.get(key)
        if old is None:
            by_key[key] = fresh
            continue

        merged = {
            "platform": _clean_text(fresh.get("platform") or old.get("platform")),
            "id": _clean_text(fresh.get("id") or old.get("id")),
            "url": _clean_text(fresh.get("url") or old.get("url")),
            "title": _clean_text(fresh.get("title") or old.get("title") or "Video"),
            "description": _clean_text(
                fresh.get("description") or old.get("description") or fresh.get("title") or old.get("title") or ""
            ),
            # Keep the best known counters to avoid temporary drops when a source omits a metric.
            "views": max(_to_int(old.get("views")), _to_int(fresh.get("views"))),
            "likes": max(_to_int(old.get("likes")), _to_int(fresh.get("likes"))),
            "comments": max(_to_int(old.get("comments")), _to_int(fresh.get("comments"))),
        }
        by_key[key] = merged

    return list(by_key.values())


def _load_previous_if_exists() -> list[dict[str, Any]]:
    if not OUTPUT_COMPACT.exists():
        return []

    try:
        data = json.loads(OUTPUT_COMPACT.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []

    videos = data.get("videos") if isinstance(data, dict) else []
    if isinstance(videos, list):
        return [v for v in videos if isinstance(v, dict)]
    return []


def main() -> None:
    STATS_DIR.mkdir(parents=True, exist_ok=True)

    previous = _load_previous_if_exists()
    youtube_api_key = _clean_text(os.getenv("YOUTUBE_API_KEY"))

    extracted: list[dict[str, Any]] = []
    for source in SOURCES:
        platform = source["platform"]

        if platform == "youtube" and youtube_api_key:
            try:
                yt_videos = _extract_youtube_via_api(youtube_api_key, YOUTUBE_CHANNEL_ID)
                extracted.extend(yt_videos)
                print(f"[ok] youtube api channel scan: {len(yt_videos)} videos")
                continue
            except Exception as exc:
                print(f"[warn] youtube api scan failed, fallback to yt-dlp: {exc}")

        for url in source.get("urls", []):
            try:
                batch = _extract_from_source(platform, url)
                extracted.extend(batch)
                print(f"[ok] {platform} from {url}: {len(batch)} videos")
            except Exception as exc:
                print(f"[warn] extraction failed for {platform} ({url}): {exc}")

    extracted = _dedupe(extracted)

    if not extracted:
        print("[warn] no fresh data extracted, keeping previous data")
        extracted = previous
    else:
        extracted = _merge_with_previous(extracted, previous)
        extracted = _dedupe(extracted)

    extracted.sort(
        key=lambda item: (
            _to_int(item.get("views")),
            _to_int(item.get("likes")),
            _to_int(item.get("comments")),
        ),
        reverse=True,
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "videos": extracted,
    }

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    OUTPUT_COMPACT.write_text(text + "\n", encoding="utf-8")
    OUTPUT_FULL.write_text(text + "\n", encoding="utf-8")

    print(f"[ok] wrote {len(extracted)} videos to {OUTPUT_COMPACT}")


if __name__ == "__main__":
    main()
