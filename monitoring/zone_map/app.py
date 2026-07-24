"""map dashboard: European DAY_AHEAD bidding zones, colored once tomorrow's price has landed.

run with: poetry run uvicorn monitoring.zone_map.app:app --reload
"""

import datetime as dt
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

from monitoring.zone_map.zones import build_zone_summary

STATIC_DIR = Path(__file__).resolve().parent / "static"


class CachedStaticFiles(StaticFiles):
    """StaticFiles with a fixed Cache-Control header - how aggressively a given mount can be
    cached depends entirely on how often its files actually change (see mounts below)."""

    def __init__(self, *args, cache_control: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_control = cache_control

    def file_response(self, *args, **kwargs):
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = self._cache_control
        return response


app = FastAPI(title="Day-ahead zone map")
app.add_middleware(GZipMiddleware, minimum_size=500)

# most specific mounts first - Starlette matches in registration order, so /static/geo and
# /static/vendor need to be checked before the catch-all /static mount below.
app.mount(
    "/static/geo",
    CachedStaticFiles(directory=STATIC_DIR / "geo", cache_control="public, max-age=3600"),
    name="geo",
)
app.mount(
    "/static/vendor",
    CachedStaticFiles(directory=STATIC_DIR / "vendor", cache_control="public, max-age=604800, immutable"),
    name="vendor",
)
# index.html/app.js/style.css change during active development - no-cache (not "no caching",
# but "always revalidate") so a refresh reliably picks up the latest version instead of the
# stale-until-hard-refresh behavior seen earlier in this project.
app.mount("/static", CachedStaticFiles(directory=STATIC_DIR, cache_control="no-cache"), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/prices")
def get_prices(date: str | None = None) -> dict:
    """DAY_AHEAD price summary per in-scope bidding zone. `date` is the delivery day
    (YYYY-MM-DD); defaults to tomorrow's delivery day, same default as
    monitoring/day_ahead_completeness.py's run()."""
    target_date = dt.date.fromisoformat(date) if date else dt.date.today() + dt.timedelta(days=1)
    return {"date": target_date.isoformat(), "zones": build_zone_summary(target_date)}
