from __future__ import annotations

import asyncio
import json
import socket
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn

from harvester.models import CodecInfo, ParsedStream, StreamStatus, StreamTestResult


async def _resolve_host(host: str, timeout: float = 3.0) -> bool:
    loop = asyncio.get_event_loop()
    try:
        await asyncio.wait_for(
            loop.run_in_executor(None, socket.getaddrinfo, host, None),
            timeout=timeout,
        )
        return True
    except Exception:
        return False


async def test_stream(url: str, timeout: float = 8.0) -> StreamTestResult:
    start = time.monotonic()
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            "-analyzeduration", "2000000",
            "-probesize", "500000",
            "-timeout", str(int(timeout * 1_000_000)),
            "-i", url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout + 5)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        if proc.returncode != 0:
            return StreamTestResult(
                url=url,
                status=StreamStatus.DEAD,
                response_time_ms=elapsed_ms,
                tested_at=datetime.now(timezone.utc).isoformat(),
            )

        codecs = CodecInfo()
        try:
            data = json.loads(stdout)
            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video" and not codecs.video:
                    codecs.video = stream.get("codec_name", "")
                    w = stream.get("width", 0)
                    h = stream.get("height", 0)
                    if w and h:
                        codecs.resolution = f"{w}x{h}"
                elif stream.get("codec_type") == "audio" and not codecs.audio:
                    codecs.audio = stream.get("codec_name", "")
            fmt = data.get("format", {})
            if fmt.get("bit_rate"):
                codecs.bitrate = fmt["bit_rate"]
        except (json.JSONDecodeError, KeyError):
            pass

        return StreamTestResult(
            url=url,
            status=StreamStatus.WORKING,
            response_time_ms=elapsed_ms,
            codecs=codecs,
            tested_at=datetime.now(timezone.utc).isoformat(),
        )

    except asyncio.TimeoutError:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return StreamTestResult(
            url=url,
            status=StreamStatus.TIMEOUT,
            response_time_ms=elapsed_ms,
            tested_at=datetime.now(timezone.utc).isoformat(),
        )
    except OSError as e:
        return StreamTestResult(
            url=url,
            status=StreamStatus.DEAD,
            response_time_ms=0,
            tested_at=datetime.now(timezone.utc).isoformat(),
            error=str(e),
        )


async def test_streams(
    streams: list[ParsedStream],
    timeout: float = 8.0,
    concurrency: int = 50,
    tested_urls: dict[str, str] | None = None,
    on_result: callable = None,
) -> list[StreamTestResult]:
    tested = tested_urls or {}
    results: list[StreamTestResult] = []

    urls_to_test = [s for s in streams if s.url not in tested]

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[green]{task.fields[working]}W[/] [red]{task.fields[dead]}D[/] [yellow]{task.fields[timeout]}T[/]"),
    ) as progress:

        # Phase 1: DNS pre-filter — resolve unique hosts to kill dead domains in bulk
        hosts_by_stream: dict[str, str] = {}
        unique_hosts: set[str] = set()
        for s in urls_to_test:
            try:
                host = urlparse(s.url).hostname or ""
            except Exception:
                host = ""
            hosts_by_stream[s.url] = host
            if host:
                unique_hosts.add(host)

        dns_task = progress.add_task("DNS resolve", total=len(unique_hosts), working=0, dead=0, timeout=0)
        dns_sem = asyncio.Semaphore(200)
        live_hosts: set[str] = set()
        dead_host_count = 0

        async def resolve_one(host: str) -> tuple[str, bool]:
            nonlocal dead_host_count
            async with dns_sem:
                alive = await _resolve_host(host)
                if not alive:
                    dead_host_count += 1
                progress.update(dns_task, advance=1, dead=dead_host_count, working=0, timeout=0)
                return host, alive

        dns_results = await asyncio.gather(*[resolve_one(h) for h in unique_hosts])
        for host, alive in dns_results:
            if alive:
                live_hosts.add(host)

        alive_streams = []
        for s in urls_to_test:
            host = hosts_by_stream[s.url]
            if host in live_hosts:
                alive_streams.append(s)
            else:
                r = StreamTestResult(
                    url=s.url,
                    status=StreamStatus.DEAD,
                    channel_name=s.channel_name,
                    group=s.group,
                    sources=s.source_id.split(",") if s.source_id else [],
                    tested_at=datetime.now(timezone.utc).isoformat(),
                )
                results.append(r)
                if on_result:
                    on_result(r)

        progress.update(dns_task, description=(
            f"DNS done — {len(live_hosts)} hosts alive, {dead_host_count} dead, "
            f"{len(alive_streams)} streams to probe"
        ))

        # Phase 2: ffprobe
        probe_sem = asyncio.Semaphore(concurrency)
        probe_task = progress.add_task("ffprobe testing", total=len(alive_streams), working=0, dead=0, timeout=0)
        counts = {"working": 0, "dead": 0, "timeout": 0}

        async def probe_one(stream: ParsedStream) -> StreamTestResult:
            async with probe_sem:
                result = await test_stream(stream.url, timeout=timeout)
                result.channel_name = stream.channel_name
                result.group = stream.group
                result.sources = stream.source_id.split(",") if stream.source_id else []
                counts[result.status.value] += 1
                progress.update(probe_task, advance=1, **counts)
                if on_result:
                    on_result(result)
                return result

        probe_results = await asyncio.gather(*[probe_one(s) for s in alive_streams])
        results.extend(probe_results)

    return [r for r in results if r is not None]
