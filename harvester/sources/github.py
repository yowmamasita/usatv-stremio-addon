from __future__ import annotations

import asyncio
import fnmatch
import logging
import os

import aiohttp

from harvester.models import ParsedStream, SourceConfig
from harvester.parser import parse_m3u
from harvester.sources.base import BaseSource

logger = logging.getLogger(__name__)

_GLOB_CHARS = frozenset("*?[")
_M3U_EXTENSIONS = (".m3u", ".m3u8")
_BRANCH_FALLBACKS = ("main", "master")

COMMON_M3U_PATHS = [
    "output/result.m3u", "output/result.m3u8", "output/result.txt",
    "output/all.m3u", "output/all.m3u8",
    "output/live.m3u", "output/live.m3u8", "output/live.txt",
    "output/output.m3u", "output/output.txt",
    "output/ipv4/result.m3u", "output/ipv6/result.m3u",
    "output/user_result.m3u", "output/xp_result.m3u",
    "output/live_ipv4.m3u", "output/live_ipv6.m3u",
    "index.m3u", "index.m3u8",
    "playlist.m3u", "playlist.m3u8",
    "live.m3u", "live.m3u8",
    "iptv.m3u", "iptv.m3u8",
    "tv.m3u", "tv.m3u8",
    "all.m3u", "all.m3u8",
    "channels.m3u", "channels.m3u8",
    "result.m3u", "result.m3u8",
    "list.m3u", "list.m3u8",
    "IPTV.m3u", "IPTV.m3u8",
    "cn.m3u", "us.m3u", "hk.m3u",
    "m3u/iptv.m3u", "m3u/result.m3u",
    "live_ipv4.m3u", "live_ipv6.m3u",
    "live.txt", "tv.txt", "iptv.txt", "result.txt",
    "itv.m3u", "itv.m3u8",
    "combined.m3u", "combined.m3u8",
    "global.m3u", "world.m3u",
    "tv/m3u/index.m3u", "radio/m3u/index.m3u",
    "tv/iptv4.m3u", "tv/iptv6.m3u",
    "simple.m3u", "eng.m3u",
    "Gather.m3u", "Migu.m3u",
    "hk_live.m3u", "my_tv.m3u",
    "iptvit.m3u", "radioita.m3u8",
    "SU.m3u", "BD.m3u",
]


def _has_glob(pattern: str) -> bool:
    return any(c in pattern for c in _GLOB_CHARS)


def _is_m3u_path(path: str) -> bool:
    return path.lower().endswith(_M3U_EXTENSIONS)


def _glob_match(path: str, pattern: str) -> bool:
    if "**" not in pattern:
        return fnmatch.fnmatch(path, pattern)
    parts = pattern.split("**/")
    prefix = parts[0]
    suffix = "**/".join(parts[1:])
    for depth in range(path.count("/") + 1):
        mid = "/".join(["*"] * depth) + "/" if depth else ""
        candidate = prefix + mid + suffix
        if fnmatch.fnmatch(path, candidate):
            return True
    return False


class GitHubSource(BaseSource):
    async def fetch(self, session: aiohttp.ClientSession) -> list[ParsedStream]:
        repo = self.config.repo
        branch = self.config.branch
        patterns = self.config.paths
        source_id = self.config.source_id()

        # Strategy 1: Try literal (non-glob) patterns directly via raw URLs
        literal_paths = [p for p in patterns if not _has_glob(p)]
        glob_patterns = [p for p in patterns if _has_glob(p)]

        streams: list[ParsedStream] = []
        sem = asyncio.Semaphore(5)
        resolved_branch = branch

        async def fetch_raw(path: str, br: str) -> tuple[str, list[ParsedStream]]:
            async with sem:
                raw_url = f"https://raw.githubusercontent.com/{repo}/{br}/{path}"
                content = await self.fetch_url(session, raw_url)
                if content and (content.strip().startswith("#EXT") or "://" in content[:500]):
                    return path, parse_m3u(content, source_url=raw_url, source_id=source_id)
                return path, []

        # Try literal paths on configured branch, then fallback
        if literal_paths:
            tasks = [fetch_raw(p, branch) for p in literal_paths]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            found_any = False
            for r in results:
                if isinstance(r, tuple) and r[1]:
                    streams.extend(r[1])
                    found_any = True

            if not found_any and branch != "master":
                tasks = [fetch_raw(p, "master") for p in literal_paths]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, tuple) and r[1]:
                        streams.extend(r[1])
                        resolved_branch = "master"

        # Strategy 2: If we have glob patterns, try tree API first, then brute-force common paths
        if glob_patterns:
            tree_paths = await self._find_via_tree(session, repo, branch, glob_patterns)
            if tree_paths:
                tasks = [fetch_raw(p, tree_paths[1]) for p in tree_paths[0]]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, tuple) and r[1]:
                        streams.extend(r[1])
            else:
                # Tree API failed (rate limit) — brute-force common M3U filenames
                logger.info("github:%s — tree API unavailable, trying common paths", repo)
                brute_streams = await self._brute_force_common_paths(session, repo, branch, source_id)
                streams.extend(brute_streams)

        # Strategy 3: If nothing found yet, brute-force common paths
        if not streams and not literal_paths:
            brute_streams = await self._brute_force_common_paths(session, repo, branch, source_id)
            streams.extend(brute_streams)

        if streams:
            logger.info("github:%s — found %d streams", repo, len(streams))
        else:
            logger.warning("github:%s — 0 streams found", repo)

        return streams

    async def _brute_force_common_paths(
        self, session: aiohttp.ClientSession, repo: str, branch: str, source_id: str
    ) -> list[ParsedStream]:
        sem = asyncio.Semaphore(10)
        streams: list[ParsedStream] = []
        found_branch = branch

        async def try_path(path: str, br: str) -> list[ParsedStream]:
            async with sem:
                raw_url = f"https://raw.githubusercontent.com/{repo}/{br}/{path}"
                content = await self.fetch_url(session, raw_url)
                if content and (content.strip().startswith("#EXT") or "://" in content[:500]):
                    return parse_m3u(content, source_url=raw_url, source_id=source_id)
                return []

        # Try configured branch first
        tasks = [try_path(p, branch) for p in COMMON_M3U_PATHS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list) and r:
                streams.extend(r)

        # If nothing found, try fallback branch
        if not streams:
            alt_branch = "master" if branch == "main" else "main"
            tasks = [try_path(p, alt_branch) for p in COMMON_M3U_PATHS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list) and r:
                    streams.extend(r)

        return streams

    async def _find_via_tree(
        self, session: aiohttp.ClientSession, repo: str, branch: str, glob_patterns: list[str]
    ) -> tuple[list[str], str] | None:
        token = os.environ.get("GITHUB_TOKEN", "")
        headers = {"Accept": "application/vnd.github.v3+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        branches_to_try = [branch] + [b for b in _BRANCH_FALLBACKS if b != branch]

        for candidate in branches_to_try:
            url = f"https://api.github.com/repos/{repo}/git/trees/{candidate}?recursive=1"
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    tree = data.get("tree", [])
            except (aiohttp.ClientError, asyncio.TimeoutError):
                continue

            expanded_globs = list(glob_patterns)
            for pat in glob_patterns:
                if pat.endswith(".m3u") and not pat.endswith(".m3u8"):
                    expanded_globs.append(pat + "8")

            allowed_extensions = set(_M3U_EXTENSIONS)
            for pat in glob_patterns:
                if pat.endswith(".txt"):
                    allowed_extensions.add(".txt")
                    break

            matches = []
            for item in tree:
                if item.get("type") != "blob":
                    continue
                path = item["path"]
                if not any(path.lower().endswith(ext) for ext in allowed_extensions):
                    continue
                basename = path.split("/")[-1]
                if any(_glob_match(path, pat) or _glob_match(basename, pat) for pat in expanded_globs):
                    matches.append(path)

            if matches:
                return matches[:100], candidate

        return None
