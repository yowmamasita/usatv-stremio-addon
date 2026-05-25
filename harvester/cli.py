from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.table import Table

from harvester.config import DATA_DIR, DEFAULT_HARVEST_CONCURRENCY, DEFAULT_TEST_CONCURRENCY, DEFAULT_TIMEOUT, SOURCES_FILE
from harvester.dedup import deduplicate
from harvester.models import HarvestState, ParsedStream, SourceConfig, SourceType, StreamTestResult, TestState
from harvester.report import generate_report, print_summary, save_report
from harvester.state import load_harvest_state, load_streams, load_test_state, save_harvest_state, save_results, save_streams, save_test_state
from harvester.tester import test_streams

console = Console()


def _load_sources(path: Path, filter_type: str | None = None, filter_name: str | None = None) -> list[SourceConfig]:
    with open(path) as f:
        data = yaml.safe_load(f)
    sources = [SourceConfig(**s) for s in data.get("sources", [])]
    if filter_type:
        sources = [s for s in sources if s.type.value == filter_type]
    if filter_name:
        sources = [s for s in sources if filter_name.lower() in s.source_id().lower()]
    return sources


def _get_scraper(config: SourceConfig):
    from harvester.sources.direct import DirectSource
    from harvester.sources.github import GitHubSource
    from harvester.sources.paste import PasteSource
    from harvester.sources.telegram import TelegramSource
    from harvester.sources.website import WebsiteSource

    return {
        SourceType.GITHUB: GitHubSource,
        SourceType.WEBSITE: WebsiteSource,
        SourceType.TELEGRAM: TelegramSource,
        SourceType.PASTE: PasteSource,
        SourceType.DIRECT: DirectSource,
    }[config.type](config)


async def _harvest(sources: list[SourceConfig], concurrency: int, resume: bool) -> list[ParsedStream]:
    import aiohttp

    state = load_harvest_state() if resume else HarvestState()
    state.run_id = state.run_id or datetime.now(timezone.utc).isoformat()
    all_streams: list[ParsedStream] = []
    sem = asyncio.Semaphore(concurrency)

    completed = set(state.sources_completed)
    pending = [s for s in sources if s.source_id() not in completed]

    console.print(f"[bold]Harvesting from {len(pending)} sources[/] ({len(completed)} already done)")

    async with aiohttp.ClientSession() as session:
        async def fetch_source(src: SourceConfig) -> tuple[str, list[ParsedStream]]:
            async with sem:
                sid = src.source_id()
                try:
                    scraper = _get_scraper(src)
                    streams = await scraper.fetch(session)
                    console.print(f"  [green]OK[/] {sid}: {len(streams)} streams")
                    return sid, streams
                except Exception as e:
                    console.print(f"  [red]FAIL[/] {sid}: {e}")
                    return sid, []

        tasks = [fetch_source(s) for s in pending]
        results = await asyncio.gather(*tasks)

    for sid, streams in results:
        if streams:
            all_streams.extend(streams)
            state.sources_completed.append(sid)
        else:
            state.sources_failed.append(sid)

    deduped = deduplicate(all_streams)
    state.streams_collected = len(deduped)
    save_harvest_state(state)
    save_streams(deduped)

    console.print(f"\n[bold green]Harvested {len(all_streams)} streams, {len(deduped)} unique after dedup[/]")
    return deduped


async def _test(streams: list[ParsedStream], timeout: float, concurrency: int, resume: bool) -> list[StreamTestResult]:
    state = load_test_state() if resume else TestState()
    state.run_id = state.run_id or datetime.now(timezone.utc).isoformat()

    tested = state.tested_urls if resume else {}
    console.print(f"[bold]Testing {len(streams)} streams[/] ({len(tested)} already tested, concurrency={concurrency})")

    def on_result(r: StreamTestResult):
        state.tested_urls[r.url] = r.status.value
        if len(state.tested_urls) % 100 == 0:
            save_test_state(state)

    results = await test_streams(streams, timeout=timeout, concurrency=concurrency, tested_urls=tested, on_result=on_result)

    save_test_state(state)
    save_results(results)
    return results


@click.group()
def main():
    """IPTV Stream Harvester — discover, test, and report on live streams."""
    pass


@main.command()
@click.option("--sources-file", type=click.Path(exists=True), default=str(SOURCES_FILE))
@click.option("--filter-type", type=click.Choice(["github", "website", "telegram", "paste", "direct"]))
@click.option("--filter-name", type=str, default=None)
@click.option("--concurrency", type=int, default=DEFAULT_HARVEST_CONCURRENCY)
@click.option("--resume/--no-resume", default=True)
def harvest(sources_file, filter_type, filter_name, concurrency, resume):
    """Fetch M3U playlists from all configured sources."""
    sources = _load_sources(Path(sources_file), filter_type, filter_name)
    if not sources:
        console.print("[red]No sources matched filters[/]")
        return
    asyncio.run(_harvest(sources, concurrency, resume))


@main.command()
@click.option("--input", "input_file", type=str, default="harvested_streams.json")
@click.option("--timeout", type=float, default=DEFAULT_TIMEOUT)
@click.option("--concurrency", type=int, default=DEFAULT_TEST_CONCURRENCY)
@click.option("--resume/--no-resume", default=True)
@click.option("--limit", type=int, default=None, help="Max streams to test")
def test(input_file, timeout, concurrency, resume, limit):
    """Test collected streams with ffprobe."""
    raw = load_streams(input_file)
    if not raw:
        console.print("[red]No streams found. Run 'harvest' first.[/]")
        return
    streams = [ParsedStream(**s) for s in raw]
    if limit:
        streams = streams[:limit]
    results = asyncio.run(_test(streams, timeout, concurrency, resume))
    console.print(f"\n[bold green]Tested {len(results)} streams[/]")


@main.command()
@click.option("--input", "input_file", type=str, default="test_results.json")
@click.option("--working-only", is_flag=True, default=False)
def report(input_file, working_only):
    """Generate report from test results."""
    path = DATA_DIR / input_file
    if not path.exists():
        console.print("[red]No test results found. Run 'test' first.[/]")
        return
    raw = json.loads(path.read_text())
    results = [StreamTestResult(**r) for r in raw]
    if working_only:
        results = [r for r in results if r.status == "working"]

    rep = generate_report(results)
    save_report(rep)
    print_summary(rep)

    table = Table(title="Top Working Streams")
    table.add_column("Channel", style="cyan")
    table.add_column("URL", style="blue", max_width=60)
    table.add_column("Codec", style="green")
    table.add_column("Resolution", style="yellow")
    table.add_column("Time (ms)", style="magenta")

    for s in rep.streams[:50]:
        if s.status == "working":
            table.add_row(
                s.channel_name or "-",
                s.url[:60],
                s.codecs.video or "-",
                s.codecs.resolution or "-",
                str(s.response_time_ms),
            )
    console.print(table)


@main.command()
@click.option("--sources-file", type=click.Path(exists=True), default=str(SOURCES_FILE))
@click.option("--filter-type", type=click.Choice(["github", "website", "telegram", "paste", "direct"]))
@click.option("--filter-name", type=str, default=None)
@click.option("--timeout", type=float, default=DEFAULT_TIMEOUT)
@click.option("--harvest-concurrency", type=int, default=DEFAULT_HARVEST_CONCURRENCY)
@click.option("--test-concurrency", type=int, default=DEFAULT_TEST_CONCURRENCY)
@click.option("--resume/--no-resume", default=True)
def run(sources_file, filter_type, filter_name, timeout, harvest_concurrency, test_concurrency, resume):
    """Execute harvest + test + report in sequence."""
    sources = _load_sources(Path(sources_file), filter_type, filter_name)
    if not sources:
        console.print("[red]No sources matched filters[/]")
        return

    async def pipeline():
        streams = await _harvest(sources, harvest_concurrency, resume)
        results = await _test(streams, timeout, test_concurrency, resume)
        rep = generate_report(results, sources_total=len(sources))
        save_report(rep)
        print_summary(rep)

    asyncio.run(pipeline())
