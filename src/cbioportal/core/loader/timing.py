"""Loader phase timing instrumentation.

Usage:
    timer = LoadTimer()
    with timer.phase("clinical_patient"):
        ...  # timed work
    timer.report()

Phases accumulate — if the same phase is used multiple times (e.g. in a
multi-study load), the times are summed so the report shows totals.
"""
import time
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class PhaseStats:
    name: str
    total_seconds: float = 0.0
    calls: int = 0

    @property
    def avg_seconds(self) -> float:
        return self.total_seconds / self.calls if self.calls else 0.0


class LoadTimer:
    """Accumulate per-phase wall-clock times across one or many study loads."""

    def __init__(self):
        self._phases: dict[str, PhaseStats] = {}
        self._wall_start = time.perf_counter()

    @contextmanager
    def phase(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - t0
            if name not in self._phases:
                self._phases[name] = PhaseStats(name)
            self._phases[name].total_seconds += elapsed
            self._phases[name].calls += 1

    def report(self, study_id: str = "") -> str:
        """Return a formatted timing report and print it."""
        wall = time.perf_counter() - self._wall_start
        header = f"=== Loader timing {'for ' + study_id if study_id else ''} ==="
        lines = [header]

        if not self._phases:
            lines.append("  (no phases recorded)")
        else:
            # Sort by total time descending
            sorted_phases = sorted(self._phases.values(), key=lambda p: p.total_seconds, reverse=True)
            name_w = max(len(p.name) for p in sorted_phases)
            lines.append(f"  {'Phase':<{name_w}}   {'Total':>8}   {'Calls':>6}   {'Avg':>8}   {'%wall':>6}")
            lines.append(f"  {'-'*name_w}   {'--------':>8}   {'------':>6}   {'--------':>8}   {'------':>6}")
            for p in sorted_phases:
                pct = (p.total_seconds / wall * 100) if wall > 0 else 0
                lines.append(
                    f"  {p.name:<{name_w}}   {p.total_seconds:>7.3f}s   {p.calls:>6}   {p.avg_seconds:>7.3f}s   {pct:>5.1f}%"
                )

        accounted = sum(p.total_seconds for p in self._phases.values())
        unaccounted = wall - accounted
        lines.append(f"  {'--- unaccounted ---':<{max(10, max((len(p.name) for p in self._phases.values()), default=10))}}   {unaccounted:>7.3f}s")
        lines.append(f"  Wall time: {wall:.3f}s")
        report_str = "\n".join(lines)
        print(report_str)
        return report_str
