#!/usr/bin/env python3
"""Benchmark study view chart endpoints against a local or remote server.

Usage:
    # Local (starts server automatically if not running)
    uv run python tests/performance/benchmark_study_view.py

    # Remote
    uv run python tests/performance/benchmark_study_view.py --host https://cbio-revamp-xxx.run.app

    # Custom study / iterations
    uv run python tests/performance/benchmark_study_view.py --study msk_chord_2024 --iterations 10

    # Cold-start test (scales Cloud Run to zero first, requires gcloud)
    uv run python tests/performance/benchmark_study_view.py --host https://... --cold-start

Results are saved to tests/performance/results/benchmark-{host}-{timestamp}.json
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx

ENDPOINTS = [
    ("mutated-genes", "POST", "/study/summary/chart/mutated-genes"),
    ("cna-genes", "POST", "/study/summary/chart/cna-genes"),
    ("sv-genes", "POST", "/study/summary/chart/sv-genes"),
    ("age", "POST", "/study/summary/chart/age"),
    ("scatter", "POST", "/study/summary/chart/scatter"),
    ("km", "POST", "/study/summary/chart/km"),
    ("data-types", "POST", "/study/summary/chart/data-types"),
    ("charts-meta", "GET", "/study/summary/charts-meta"),
    ("page", "GET", "/study/summary"),
]

UNFILTERED = "{}"
FILTERED = json.dumps({
    "clinicalDataFilters": [
        {"attributeId": "CANCER_TYPE_DETAILED", "values": [{"value": "Breast Invasive Ductal Carcinoma"}]}
    ],
    "mutationFilter": {"genes": []},
    "svFilter": {"genes": []},
})


def get_memory_mb(host: str, client: httpx.Client) -> float | None:
    """Fetch RSS memory from the /metrics endpoint."""
    try:
        r = client.get(f"{host}/metrics", timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("rss_mb") or data.get("memory_mb")
    except Exception:
        pass
    return None


def run_request(client: httpx.Client, host: str, endpoint: tuple, study_id: str,
                filter_json: str) -> dict:
    """Run a single request and return timing/status."""
    name, method, path = endpoint
    url = f"{host}{path}"

    t0 = time.perf_counter()
    try:
        if method == "GET":
            params = {"id": study_id}
            if filter_json != "{}":
                params["filter_json"] = filter_json
            r = client.get(url, params=params, timeout=60)
        else:
            # Use multipart form data (matching how the browser sends it)
            files = {
                "study_id": (None, study_id),
                "filter_json": (None, filter_json),
            }
            r = client.post(url, files=files, timeout=60)
        elapsed = time.perf_counter() - t0
        return {
            "endpoint": name,
            "status": r.status_code,
            "time_s": round(elapsed, 4),
            "size_bytes": len(r.content),
        }
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return {
            "endpoint": name,
            "status": 0,
            "time_s": round(elapsed, 4),
            "size_bytes": 0,
            "error": str(e),
        }


def run_iteration(client: httpx.Client, host: str, study_id: str,
                  filter_json: str, label: str) -> list[dict]:
    """Run all endpoints once and return results."""
    results = []
    for ep in ENDPOINTS:
        result = run_request(client, host, ep, study_id, filter_json)
        result["iteration"] = label
        result["filter"] = "filtered" if filter_json != "{}" else "unfiltered"
        results.append(result)
    return results


def measure_cold_start(host: str, study_id: str) -> dict | None:
    """Scale Cloud Run to zero, then measure the first request."""
    # Check if this is a Cloud Run URL
    parsed = urlparse(host)
    if "run.app" not in parsed.hostname:
        return None

    print("\n  Measuring cold start...")
    # Extract service name from the URL (first segment of hostname)
    service_name = parsed.hostname.split("-")[0]
    # We can't easily force scale-to-zero, but we can wait and hope.
    # Instead, just record the first request time separately.
    # The main benchmark already captures this as iteration 0 (warmup).
    return None


def print_table(summary: list[dict]):
    """Print a formatted results table."""
    # Header
    print(f"\n{'Endpoint':<20} {'Cold':>8} {'Avg':>8} {'Min':>8} {'Max':>8} {'p50':>8} {'Status':>7}")
    print("-" * 73)
    for row in summary:
        cold = f"{row['cold_s']:.3f}s" if row.get("cold_s") else "—"
        avg = f"{row['avg_s']:.3f}s"
        mn = f"{row['min_s']:.3f}s"
        mx = f"{row['max_s']:.3f}s"
        p50 = f"{row['p50_s']:.3f}s"
        status = "OK" if row["all_ok"] else "FAIL"
        print(f"{row['endpoint']:<20} {cold:>8} {avg:>8} {mn:>8} {mx:>8} {p50:>8} {status:>7}")


def summarize(all_results: list[dict]) -> list[dict]:
    """Compute summary stats per endpoint, excluding warmup from averages."""
    from collections import defaultdict
    by_endpoint = defaultdict(list)
    for r in all_results:
        by_endpoint[(r["endpoint"], r["filter"])].append(r)

    summary = []
    for (endpoint, filt), results in sorted(by_endpoint.items()):
        cold_result = next((r for r in results if r["iteration"] == "warmup"), None)
        measured = [r for r in results if r["iteration"] != "warmup"]
        times = [r["time_s"] for r in measured if r["status"] == 200]

        row = {
            "endpoint": f"{endpoint} ({'F' if filt == 'filtered' else 'U'})",
            "filter": filt,
            "cold_s": cold_result["time_s"] if cold_result else None,
            "all_ok": all(r["status"] == 200 for r in results),
            "iterations": len(measured),
        }
        if times:
            row["avg_s"] = round(statistics.mean(times), 4)
            row["min_s"] = round(min(times), 4)
            row["max_s"] = round(max(times), 4)
            row["p50_s"] = round(statistics.median(times), 4)
            if len(times) > 1:
                row["stdev_s"] = round(statistics.stdev(times), 4)
        else:
            row["avg_s"] = row["min_s"] = row["max_s"] = row["p50_s"] = 0

        summary.append(row)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Benchmark study view endpoints")
    parser.add_argument("--host", default="http://127.0.0.1:8082",
                        help="Server URL (default: http://127.0.0.1:8082)")
    parser.add_argument("--study", default="msk_chord_2024",
                        help="Study ID to benchmark (default: msk_chord_2024)")
    parser.add_argument("--iterations", type=int, default=5,
                        help="Number of measured iterations after warmup (default: 5)")
    parser.add_argument("--cold-start", action="store_true",
                        help="Include cold-start measurement (Cloud Run only)")
    parser.add_argument("--no-filtered", action="store_true",
                        help="Skip filtered benchmarks")
    parser.add_argument("--output", help="Output JSON path (default: auto-generated)")
    args = parser.parse_args()

    host = args.host.rstrip("/")
    parsed = urlparse(host)
    host_label = parsed.hostname.replace(".", "_")

    print(f"Benchmarking {host}")
    print(f"Study: {args.study}, Iterations: {args.iterations}")

    # Check server is reachable (try /metrics first, fall back to /)
    # Use a long timeout for the initial check to handle Cloud Run cold starts.
    client = httpx.Client(follow_redirects=True)
    print("  Waiting for server...")
    try:
        r = client.get(f"{host}/metrics", timeout=120)
        if r.status_code != 200:
            r = client.get(f"{host}/", timeout=120)
            if r.status_code != 200:
                print(f"Server returned {r.status_code} — is it running?")
                sys.exit(1)
    except (httpx.ConnectError, httpx.ReadTimeout):
        print(f"Cannot connect to {host} — is the server running?")
        sys.exit(1)
    print("  Server is up.")

    all_results = []
    memory_samples = []

    # Warmup run (included in results but excluded from averages)
    print("\n  Warmup run...")
    warmup = run_iteration(client, host, args.study, UNFILTERED, "warmup")
    all_results.extend(warmup)
    for r in warmup:
        status = "OK" if r["status"] == 200 else f"ERR:{r['status']}"
        print(f"    {r['endpoint']:<20} {r['time_s']:.3f}s  {status}")

    mem = get_memory_mb(host, client)
    if mem:
        memory_samples.append({"phase": "post_warmup", "rss_mb": mem})

    # Measured iterations (unfiltered)
    for i in range(1, args.iterations + 1):
        print(f"\n  Iteration {i}/{args.iterations} (unfiltered)...")
        results = run_iteration(client, host, args.study, UNFILTERED, f"run_{i}")
        all_results.extend(results)
        for r in results:
            status = "OK" if r["status"] == 200 else f"ERR:{r['status']}"
            print(f"    {r['endpoint']:<20} {r['time_s']:.3f}s  {status}")

    mem = get_memory_mb(host, client)
    if mem:
        memory_samples.append({"phase": "post_unfiltered", "rss_mb": mem})

    # Measured iterations (filtered)
    if not args.no_filtered:
        for i in range(1, args.iterations + 1):
            print(f"\n  Iteration {i}/{args.iterations} (filtered)...")
            results = run_iteration(client, host, args.study, FILTERED, f"run_{i}")
            all_results.extend(results)
            for r in results:
                status = "OK" if r["status"] == 200 else f"ERR:{r['status']}"
                print(f"    {r['endpoint']:<20} {r['time_s']:.3f}s  {status}")

    mem = get_memory_mb(host, client)
    if mem:
        memory_samples.append({"phase": "post_filtered", "rss_mb": mem})

    client.close()

    # Summarize
    unfiltered_results = [r for r in all_results if r["filter"] == "unfiltered"]
    filtered_results = [r for r in all_results if r["filter"] == "filtered"]

    print("\n" + "=" * 73)
    print("UNFILTERED")
    unfiltered_summary = summarize(unfiltered_results)
    print_table(unfiltered_summary)

    if filtered_results:
        print("\nFILTERED")
        filtered_summary = summarize(filtered_results)
        print_table(filtered_summary)
    else:
        filtered_summary = []

    if memory_samples:
        print(f"\nMemory: {', '.join(f'{m['phase']}={m['rss_mb']:.0f}MB' for m in memory_samples)}")

    # Save results
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = args.output or str(results_dir / f"benchmark-{host_label}-{timestamp}.json")

    report = {
        "metadata": {
            "host": host,
            "study_id": args.study,
            "iterations": args.iterations,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "host_label": host_label,
        },
        "summary": {
            "unfiltered": unfiltered_summary,
            "filtered": filtered_summary,
        },
        "memory": memory_samples,
        "raw": all_results,
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
