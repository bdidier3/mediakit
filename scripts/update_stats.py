#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL

ROOT = Path(__file__).resolve().parents[1]
STATS_DIR = ROOT / "stats"
OUTPUT_COMPACT = STATS_DIR / "stats_compact.json"
# Vignettes TikTok : leurs adresses d'origine expirent au bout de quelques jours,
# on en garde donc une petite copie dans le site.
THUMBS_DIR = ROOT / "thumbs" / "tiktok"
THUMB_WIDTH = 240

YOUTUBE_CHANNEL_ID = "UCMxkVdDL3CkI9tog4m_Nf4g"
TIKTOK_PROFILE_URL = "https://www.tiktok.com/@clip2ep.fan"
YOUTUBE_SHORTS_URL = "https://www.youtube.com/@clip2ep-fan/shorts"
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0 Safari/537.36"
)

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


def _youtube_channel_stats(api_key: str) -> dict[str, int]:
    """Abonnés et vues totales de la chaîne : exacts via l'API, sinon abonnés arrondis via yt-dlp."""
    if api_key:
        payload = _http_get_json(
            "https://www.googleapis.com/youtube/v3/channels",
            {"key": api_key, "part": "statistics", "id": YOUTUBE_CHANNEL_ID},
        )
        stats = ((payload.get("items") or [{}])[0] or {}).get("statistics") or {}
        result = {"subscribers": _to_int(stats.get("subscriberCount")), "views": _to_int(stats.get("viewCount"))}
        return {key: value for key, value in result.items() if value > 0}

    with YoutubeDL({"quiet": True, "skip_download": True, "extract_flat": "in_playlist", "playlistend": 1}) as ydl:
        data = ydl.extract_info(YOUTUBE_SHORTS_URL, download=False) or {}
    subscribers = _to_int(data.get("channel_follower_count"))
    return {"subscribers": subscribers} if subscribers > 0 else {}


def _tiktok_profile_stats() -> dict[str, int]:
    """Abonnés et likes du profil, lus dans les données publiques de la page TikTok."""
    request = urllib.request.Request(TIKTOK_PROFILE_URL, headers={"User-Agent": BROWSER_UA, "Accept-Language": "fr-FR,fr;q=0.9"})
    with urllib.request.urlopen(request, timeout=30) as response:
        html = response.read().decode("utf-8", errors="replace")
    match = re.search(r'"stats":\{[^{}]*"followerCount":(\d+)[^{}]*\}', html)
    if not match:
        return {}
    block = match.group(0)
    likes = re.search(r'"heartCount":(\d+)', block)
    result = {"followers": int(match.group(1)), "likes": int(likes.group(1)) if likes else 0}
    return {key: value for key, value in result.items() if value > 0}


def _load_previous_channels() -> dict[str, Any]:
    if not OUTPUT_COMPACT.exists():
        return {}
    try:
        data = json.loads(OUTPUT_COMPACT.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    channels = data.get("channels") if isinstance(data, dict) else None
    return channels if isinstance(channels, dict) else {}


def _collect_channels(api_key: str) -> dict[str, Any]:
    """Chiffres des comptes ; en cas d'échec, on garde la dernière valeur connue."""
    channels = _load_previous_channels()
    now = datetime.now(timezone.utc).isoformat()
    for platform, fetch in (("youtube", lambda: _youtube_channel_stats(api_key)), ("tiktok", _tiktok_profile_stats)):
        try:
            fresh = fetch()
        except Exception as exc:
            print(f"[warn] {platform} channel stats failed, keeping previous values: {exc}")
            continue
        if not fresh:
            print(f"[warn] {platform} channel stats empty, keeping previous values")
            continue
        channels[platform] = {**(channels.get(platform) or {}), **fresh, "updated_at": now}
        print(f"[ok] {platform} channel stats: {fresh}")
    return channels


def _entry_to_video(entry: dict[str, Any], platform: str) -> dict[str, Any]:
    video_id = _clean_text(entry.get("id"))
    webpage_url = _clean_text(entry.get("webpage_url"))
    if not webpage_url:
        webpage_url = _clean_text(entry.get("url"))
    # yt-dlp renvoie /shorts/ID alors que l'API renvoie watch?v=ID : même vidéo, une seule URL.
    if platform == "youtube" and video_id:
        webpage_url = f"https://www.youtube.com/watch?v={video_id}"

    title = _clean_text(entry.get("title") or entry.get("description") or "Video")
    # Pas de repli sur le titre : une description vide laisse la fusion garder
    # l'ancienne (et ses hashtags, qui rattachent la vidéo à un parc).
    description = _clean_text(entry.get("description"))

    video = {
        "platform": platform,
        "id": video_id,
        "url": webpage_url,
        "title": title,
        "description": description,
        "views": _to_int(entry.get("view_count") or entry.get("views")),
        "likes": _to_int(entry.get("like_count") or entry.get("likes")),
        "comments": _to_int(entry.get("comment_count") or entry.get("comments")),
    }
    # Adresse de la couverture, utilisée pour la vignette puis retirée avant l'écriture.
    covers = {thumb.get("id"): thumb.get("url") for thumb in entry.get("thumbnails") or [] if isinstance(thumb, dict)}
    cover = covers.get("cover") or covers.get("originCover") or entry.get("thumbnail")
    if platform == "tiktok" and cover:
        video["_cover"] = cover
    return video


def _save_tiktok_thumbs(covers: dict[str, str]) -> None:
    """Télécharge les vignettes TikTok manquantes (les déjà présentes ne bougent pas)."""
    from io import BytesIO

    from PIL import Image

    THUMBS_DIR.mkdir(parents=True, exist_ok=True)
    saved = 0
    for video_id, url in covers.items():
        target = THUMBS_DIR / f"{video_id}.webp"
        if target.exists() or not re.fullmatch(r"\d+", video_id):
            continue
        try:
            request = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA, "Referer": "https://www.tiktok.com/"})
            with urllib.request.urlopen(request, timeout=20) as response:
                image = Image.open(BytesIO(response.read())).convert("RGB")
            image.thumbnail((THUMB_WIDTH, THUMB_WIDTH * 2))
            image.save(target, "WEBP", quality=62, method=6)
            saved += 1
        except Exception as exc:
            print(f"[warn] tiktok thumb {video_id}: {exc}")
    print(f"[ok] {saved} new tiktok thumbs")


def _mark_thumbs(videos: list[dict[str, Any]]) -> None:
    """Indique au site quelles vidéos ont une vignette enregistrée."""
    for video in videos:
        video.pop("thumb", None)
        if video.get("platform") == "tiktok" and (THUMBS_DIR / f"{video.get('id')}.webp").exists():
            video["thumb"] = 1


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
        key = _video_key(video)
        if not key:
            continue

        existing = by_key.get(key)
        if existing is None or _to_int(video.get("views")) > _to_int(existing.get("views")):
            by_key[key] = video

    return list(by_key.values())


def _video_key(video: dict[str, Any]) -> str:
    # Clé par plateforme + identifiant : une vidéo n'est comptée qu'une fois,
    # quelle que soit la forme de son URL (watch, shorts, paramètres de partage).
    platform = _clean_text(video.get("platform"))
    video_id = _clean_text(video.get("id"))
    if video_id:
        return f"{platform}:{video_id}"
    return _clean_text(video.get("url"))


def _merge_with_previous(
    extracted: list[dict[str, Any]],
    previous: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}

    for old in _dedupe([v for v in previous if isinstance(v, dict)]):
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

    covers = {video["id"]: video.pop("_cover") for video in extracted if video.get("_cover")}
    extracted = _dedupe(extracted)

    if not extracted:
        print("[warn] no fresh data extracted, keeping previous data")
        extracted = previous
    else:
        extracted = _merge_with_previous(extracted, previous)
        extracted = _dedupe(extracted)

    try:
        _save_tiktok_thumbs(covers)
    except Exception as exc:
        print(f"[warn] tiktok thumbs skipped: {exc}")
    _mark_thumbs(extracted)

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
        "channels": _collect_channels(youtube_api_key),
        "videos": extracted,
    }

    # Sans indentation : environ 20 % plus léger à télécharger pour le site.
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    OUTPUT_COMPACT.write_text(text + "\n", encoding="utf-8")

    print(f"[ok] wrote {len(extracted)} videos to {OUTPUT_COMPACT}")


if __name__ == "__main__":
    main()
