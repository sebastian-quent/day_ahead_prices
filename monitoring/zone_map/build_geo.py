"""one-off script: builds geo/{zones,context,grid}.geojson from public sources.

not run by the FastAPI app - run manually (`poetry run python -m monitoring.zone_map.build_geo`)
whenever the zone list changes or upstream shapes are updated, then commit the output files.

sources:
- bidding-zone polygons: EnergieID/entsoe-py `entsoe/geo/geojson/` (MIT license), covers every
  IN_SCOPE_ZONES entry except GB/IE (matches project-overview.md - GB has no ENTSO-E area at all).
- GB/IE + the "everything else, greyed out" context layer (the whole rest of the world, not just
  Europe - see build_context_geojson): Natural Earth 1:50m admin-0 countries (public domain),
  via the nvkelso/natural-earth-vector GitHub mirror.
- grid.geojson: Europe's high-voltage transmission lines, a purely decorative background layer
  (see build_grid_geojson) from GridKit (github.com/PyPSA/GridKit), an OpenStreetMap `power=line`
  extraction published under ODbL 1.0 on Zenodo.
"""

import csv
import io
import json
import logging
import re
import zipfile
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GEO_DIR = Path(__file__).resolve().parent / "static" / "geo"

ENTSOE_RAW_BASE = "https://raw.githubusercontent.com/EnergieID/entsoe-py/master/entsoe/geo/geojson"
NE_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
    "ne_50m_admin_0_countries.geojson"
)
GRIDKIT_EUROPE_ZIP_URL = "https://zenodo.org/api/records/47317/files/gridkit_euorpe.zip/content"

# our bidding_zone -> entsoe-py's geojson filename (without extension). entsoe-py's DE_LU covers
# the merged DE/LU bidding zone, close enough to our plain "DE" zone; non-2020 IT files match the
# current (post-2021) zone config used everywhere else in this repo.
BIDDING_ZONE_TO_ENTSOE_FILE = {
    "AT": "AT", "BE": "BE", "BG": "BG", "CH": "CH", "CZ": "CZ", "DE": "DE_LU",
    "DK1": "DK_1", "DK2": "DK_2", "EE": "EE", "ES": "ES", "FI": "FI", "FR": "FR",
    "GR": "GR", "HR": "HR", "HU": "HU",
    "IT_NORD": "IT_NORD", "IT_CNOR": "IT_CNOR", "IT_CSUD": "IT_CSUD", "IT_SUD": "IT_SUD",
    "IT_SICI": "IT_SICI", "IT_SARD": "IT_SARD", "IT_CALA": "IT_CALA",
    "LT": "LT", "LV": "LV", "NL": "NL",
    "NO1": "NO_1", "NO2": "NO_2", "NO3": "NO_3", "NO4": "NO_4", "NO5": "NO_5",
    "PL": "PL", "PT": "PT", "RO": "RO",
    "SE1": "SE_1", "SE2": "SE_2", "SE3": "SE_3", "SE4": "SE_4",
    "SI": "SI", "SK": "SK",
}

# GB/IE aren't in entsoe-py (no ENTSO-E area) - plain country outlines from Natural Earth instead.
BIDDING_ZONE_TO_ISO_A2 = {"GB": "GB", "IE": "IE"}

# country-level ISO_A2 codes already covered (directly or via sub-zones) by BIDDING_ZONE_TO_ENTSOE_FILE
# / BIDDING_ZONE_TO_ISO_A2 above - excluded from the grey context layer so they don't double-draw
# underneath their own colored zone shapes.
COVERED_ISO_A2 = {
    "AT", "BE", "BG", "CH", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU",
    "IT", "LT", "LV", "NL", "NO", "PL", "PT", "RO", "SE", "SI", "SK", "GB", "IE",
}

def build_zones_geojson() -> dict:
    features = []
    for bidding_zone, entsoe_file in BIDDING_ZONE_TO_ENTSOE_FILE.items():
        url = f"{ENTSOE_RAW_BASE}/{entsoe_file}.geojson"
        logger.info("fetching zone %s from %s", bidding_zone, url)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        source_fc = resp.json()
        for feature in source_fc["features"]:
            features.append(
                {"type": "Feature", "properties": {"bidding_zone": bidding_zone}, "geometry": feature["geometry"]}
            )

    logger.info("fetching country outlines for %s", sorted(BIDDING_ZONE_TO_ISO_A2))
    ne_resp = requests.get(NE_COUNTRIES_URL, timeout=60)
    ne_resp.raise_for_status()
    ne_countries = ne_resp.json()

    iso_to_bidding_zone = {iso: bz for bz, iso in BIDDING_ZONE_TO_ISO_A2.items()}
    for feature in ne_countries["features"]:
        iso_a2 = feature["properties"].get("ISO_A2")
        if iso_a2 in iso_to_bidding_zone:
            features.append(
                {
                    "type": "Feature",
                    "properties": {"bidding_zone": iso_to_bidding_zone[iso_a2]},
                    "geometry": feature["geometry"],
                }
            )

    found = {f["properties"]["bidding_zone"] for f in features}
    expected = set(BIDDING_ZONE_TO_ENTSOE_FILE) | set(BIDDING_ZONE_TO_ISO_A2)
    missing = expected - found
    if missing:
        raise RuntimeError(f"missing geometry for zones: {sorted(missing)}")

    return {"type": "FeatureCollection", "features": features}, ne_countries


def build_context_geojson(ne_countries: dict) -> dict:
    """every country worldwide except the ones we draw as colored zones - not clipped to a
    Europe bounding box. the map's own maxBounds (see static/app.js) keeps the user from ever
    panning/zooming far enough to reach the far side of antimeridian-crossing countries (Russia's
    Far East, USA/Alaska) where an unclipped polygon would otherwise render as a stray line
    across the whole map - so there's no need to clip the data itself, just restrict the camera.
    """
    features = []
    for feature in ne_countries["features"]:
        iso_a2 = feature["properties"].get("ISO_A2")
        if iso_a2 in COVERED_ISO_A2:
            continue
        features.append(
            {
                "type": "Feature",
                "properties": {"name": feature["properties"].get("NAME", "")},
                "geometry": feature["geometry"],
            }
        )
    return {"type": "FeatureCollection", "features": features}


_WKT_LINESTRING_RE = re.compile(r"LINESTRING\s*\((.*)\)", re.IGNORECASE)


def _parse_wkt_linestring(wkt: str) -> list[list[float]] | None:
    match = _WKT_LINESTRING_RE.search(wkt)
    if not match:
        return None
    coords = [[float(v) for v in pair.strip().split(" ")] for pair in match.group(1).split(",")]
    return coords if len(coords) >= 2 else None


def build_grid_geojson() -> dict:
    """Europe's high-voltage transmission lines - a faint decorative background layer, not
    analytical data (see module docstring: 2016 extract, ODbL 1.0). every link already ships
    its own ready-to-use WKT LINESTRING, so this just needs parsing, no vertex-table join.
    """
    logger.info("fetching GridKit Europe high-voltage grid dataset")
    resp = requests.get(GRIDKIT_EUROPE_ZIP_URL, timeout=60)
    resp.raise_for_status()

    features = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("gridkit_europe-highvoltage-links.csv") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
                coords = _parse_wkt_linestring(row.get("wkt_srid_4326", ""))
                if coords is None:
                    continue
                features.append({"type": "Feature", "properties": {}, "geometry": {"type": "LineString", "coordinates": coords}})
    return {"type": "FeatureCollection", "features": features}


def main() -> None:
    GEO_DIR.mkdir(parents=True, exist_ok=True)

    zones_fc, ne_countries = build_zones_geojson()
    context_fc = build_context_geojson(ne_countries)
    grid_fc = build_grid_geojson()

    (GEO_DIR / "zones.geojson").write_text(json.dumps(zones_fc), encoding="utf-8")
    (GEO_DIR / "context.geojson").write_text(json.dumps(context_fc), encoding="utf-8")
    (GEO_DIR / "grid.geojson").write_text(json.dumps(grid_fc), encoding="utf-8")
    logger.info(
        "wrote %d zone features, %d context features, %d grid features to %s",
        len(zones_fc["features"]), len(context_fc["features"]), len(grid_fc["features"]), GEO_DIR,
    )


if __name__ == "__main__":
    main()
