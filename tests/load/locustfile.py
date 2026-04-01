"""
cBioPortal load test scenarios.

Three user classes with different weights:
  StudyViewUser  (weight 3) — realistic dashboard session
  HomepageUser   (weight 2) — homepage browsing
  HeavyQueryUser (weight 1) — stress: heavy endpoints back-to-back

Usage:
    # Headless (CI / inv tasks)
    locust -f tests/load/locustfile.py --host http://localhost:8082 \
        --headless -u 20 -r 2 -t 120s --html tests/load/load-report.html

    # Interactive web UI
    locust -f tests/load/locustfile.py --host http://localhost:8082
    # then open http://localhost:8089
"""
import random

from locust import HttpUser, between, task

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
