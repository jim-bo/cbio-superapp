"""
cBioPortal load test scenarios.

Four user classes:
  StudyViewUser  (weight 3) — realistic dashboard session
  HomepageUser   (weight 2) — homepage browsing
  HeavyQueryUser (weight 1) — stress: heavy endpoints back-to-back
  MetricsUser    (weight 0) — polls /metrics every 5 s; memory appears in report

Usage:
    # Headless (CI / inv tasks)
    locust -f tests/load/locustfile.py --host http://localhost:8082 \
        --headless -u 20 -r 2 -t 120s --html tests/load/load-report.html

    # Interactive web UI
    locust -f tests/load/locustfile.py --host http://localhost:8082
    # then open http://localhost:8089
"""
import random

from locust import HttpUser, between, constant, task

from studies import (
    ACTIVE_FILTER,
    ALL_CHART_ENDPOINTS,
    EMPTY_FILTER,
    HEAVY_ENDPOINTS,
    STUDIES,
)

# The largest study — used by HeavyQueryUser to maximise query pressure
_LARGE_STUDY = "msk_impact_50k_2026"


class StudyViewUser(HttpUser):
    """
    Simulates a researcher opening a study dashboard and waiting for all
    charts to render. Picks a random study each iteration and alternates
    between an empty filter and an active TP53/age filter.

    Weight 3 — the most common user pattern.
    """

    weight = 3
    wait_time = between(2, 5)  # think time between requests

    def _study(self) -> dict:
        return random.choice(STUDIES)

    def _filter(self) -> str:
        return random.choice([EMPTY_FILTER, ACTIVE_FILTER])

    @task(1)
    def load_dashboard(self):
        study = self._study()
        study_id = study["id"]
        flt = self._filter()
        form = {"study_id": study_id, "filter_json": flt}

        # 1. Page HTML
        self.client.get(
            f"/study/summary?id={study_id}",
            name="/study/summary",
        )

        # 2. Chart metadata (layout)
        self.client.get(
            f"/study/summary/charts-meta?id={study_id}",
            name="/study/summary/charts-meta",
        )

        # 3. Navbar counts (sample/patient totals after filters)
        self.client.post(
            "/study/summary/navbar-counts",
            data=form,
            name="/study/summary/navbar-counts",
        )

        # 4. All chart widgets — mirrors what the browser fires in parallel
        for endpoint in ALL_CHART_ENDPOINTS:
            chart_form = dict(form)
            if endpoint == "/study/summary/chart/clinical":
                chart_form["attribute_id"] = study["clinical_attr"]
            self.client.post(endpoint, data=chart_form, name=endpoint)


class HomepageUser(HttpUser):
    """
    Simulates browsing the homepage: landing, filtering by cancer type,
    then resetting.

    Weight 2 — moderate frequency.
    """

    weight = 2
    wait_time = between(1, 3)

    @task(1)
    def browse_homepage(self):
        self.client.get("/", name="/")
        self.client.post(
            "/studies",
            data={"cancer_type": "Breast", "data_types": ["mutations"]},
            name="/studies (filtered)",
        )
        self.client.post(
            "/studies",
            data={"cancer_type": "", "data_types": []},
            name="/studies (reset)",
        )


class HeavyQueryUser(HttpUser):
    """
    Hits the three most expensive chart endpoints back-to-back against the
    largest study with no filter applied. Designed to find the breaking point
    under concurrent heavy load.

    Weight 1 — minority of traffic, but drives maximum server load.
    """

    weight = 1
    wait_time = between(0, 1)

    @task(1)
    def hammer_heavy_endpoints(self):
        form = {"study_id": _LARGE_STUDY, "filter_json": EMPTY_FILTER}
        for endpoint in HEAVY_ENDPOINTS:
            self.client.post(endpoint, data=form, name=endpoint)


class MetricsUser(HttpUser):
    """
    Polls GET /metrics every 5 s and reports RSS memory (MiB) as a custom
    "request" so it appears in the Locust HTML report alongside latency rows.

    The p50/p95/p99 columns will show MiB values — read them as memory, not ms.

    weight=0 means Locust never auto-spawns this user. It is always spawned
    exactly once via fixed_count=1, regardless of the -u flag, so memory is
    sampled continuously without inflating the user count.
    """

    weight = 0
    fixed_count = 1
    wait_time = constant(5)

    @task
    def sample_memory(self):
        with self.client.get("/metrics", catch_response=True) as resp:
            if resp.status_code == 200:
                data = resp.json()
                rss = data.get("rss_mib", 0)
                resp.success()
                # Fire a synthetic event with rss_mib as "response_time" so it
                # appears as its own row in the HTML report with MiB values.
                # The /metrics HTTP latency is suppressed (not named, not tracked).
                self.environment.events.request.fire(
                    request_type="mem_mib",
                    name="rss",
                    response_time=rss,
                    response_length=0,
                    exception=None,
                    context={},
                )
                print(f"[mem] rss={rss} MiB  vms={data.get('vms_mib', 0)} MiB")
            else:
                resp.failure(f"/metrics returned {resp.status_code}")
