"""
/metrics — process memory endpoint for load testing and operational monitoring.

Returns RSS and VMS for the current uvicorn worker process. Intended for use by
the Locust MetricsUser during load tests; also useful for Cloud Run dashboards.
"""
import os

import psutil
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

_process = psutil.Process(os.getpid())


@router.get("/metrics")
def get_metrics() -> JSONResponse:
    """Return current process memory usage in MiB."""
    mem = _process.memory_info()
    return JSONResponse({
        "rss_mib": round(mem.rss / 1024 / 1024, 1),
        "vms_mib": round(mem.vms / 1024 / 1024, 1),
    })
