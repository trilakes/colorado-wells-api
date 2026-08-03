"""
Drought context — current US Drought Monitor status for a well's county.

Free, no API key. USDM publishes one map per week (Thursdays), so results are
cached for 12 hours; this must never sit on the critical path of a search.

The percentages USDM returns are CUMULATIVE: d2=100 means 100% of the county is
in D2 *or worse*, not that 100% is exactly D2. The headline category is therefore
the most severe level that still covers a majority of the county.
"""
import threading
import time
from collections import OrderedDict

import requests

USDM_URL = ("https://usdmdataservices.unl.edu/api/CountyStatistics/"
            "GetDroughtSeverityStatisticsByAreaPercent")

CATEGORIES = [
    ("d4", "D4", "Exceptional Drought"),
    ("d3", "D3", "Extreme Drought"),
    ("d2", "D2", "Severe Drought"),
    ("d1", "D1", "Moderate Drought"),
    ("d0", "D0", "Abnormally Dry"),
]

# A county has to be majority-covered before we call it by that category.
COVERAGE_THRESHOLD = 50.0

_TTL = 12 * 3600
_CACHE_MAX = 300
_cache = OrderedDict()
_cache_lock = threading.Lock()

# The wells table stores county NAMES, not FIPS, so we map here.
CO_FIPS = {
    "ADAMS": "08001", "ALAMOSA": "08003", "ARAPAHOE": "08005", "ARCHULETA": "08007",
    "BACA": "08009", "BENT": "08011", "BOULDER": "08013", "BROOMFIELD": "08014",
    "CHAFFEE": "08015", "CHEYENNE": "08017", "CLEAR CREEK": "08019", "CONEJOS": "08021",
    "COSTILLA": "08023", "CROWLEY": "08025", "CUSTER": "08027", "DELTA": "08029",
    "DENVER": "08031", "DOLORES": "08033", "DOUGLAS": "08035", "EAGLE": "08037",
    "ELBERT": "08039", "EL PASO": "08041", "FREMONT": "08043", "GARFIELD": "08045",
    "GILPIN": "08047", "GRAND": "08049", "GUNNISON": "08051", "HINSDALE": "08053",
    "HUERFANO": "08055", "JACKSON": "08057", "JEFFERSON": "08059", "KIOWA": "08061",
    "KIT CARSON": "08063", "LAKE": "08065", "LA PLATA": "08067", "LARIMER": "08069",
    "LAS ANIMAS": "08071", "LINCOLN": "08073", "LOGAN": "08075", "MESA": "08077",
    "MINERAL": "08079", "MOFFAT": "08081", "MONTEZUMA": "08083", "MONTROSE": "08085",
    "MORGAN": "08087", "OTERO": "08089", "OURAY": "08091", "PARK": "08093",
    "PHILLIPS": "08095", "PITKIN": "08097", "PROWERS": "08099", "PUEBLO": "08101",
    "RIO BLANCO": "08103", "RIO GRANDE": "08105", "ROUTT": "08107", "SAGUACHE": "08109",
    "SAN JUAN": "08111", "SAN MIGUEL": "08113", "SEDGWICK": "08115", "SUMMIT": "08117",
    "TELLER": "08119", "WASHINGTON": "08121", "WELD": "08123", "YUMA": "08125",
}

AZ_FIPS = {
    "APACHE": "04001", "COCHISE": "04003", "COCONINO": "04005", "GILA": "04007",
    "GRAHAM": "04009", "GREENLEE": "04011", "LA PAZ": "04012", "MARICOPA": "04013",
    "MOHAVE": "04015", "NAVAJO": "04017", "PIMA": "04019", "PINAL": "04021",
    "SANTA CRUZ": "04023", "YAVAPAI": "04025", "YUMA_AZ": "04027",
}


def county_fips(county, state=None):
    """County name (+ optional state) -> 5-digit FIPS, or None if unknown."""
    if not county:
        return None
    key = str(county).strip().upper().replace(" COUNTY", "")
    st = (state or "CO").strip().upper()
    if st in ("AZ", "ARIZONA"):
        # Yuma exists in both states; the AZ table keys it separately.
        return AZ_FIPS.get("YUMA_AZ") if key == "YUMA" else AZ_FIPS.get(key)
    return CO_FIPS.get(key)


def _classify(row):
    for field, code, label in CATEGORIES:
        try:
            pct = float(row.get(field) or 0)
        except (TypeError, ValueError):
            pct = 0.0
        if pct >= COVERAGE_THRESHOLD:
            return code, label, pct
    return None, "No drought designation", 0.0


def _fetch(fips):
    # USDM wants M/D/YYYY. A 5-week window guarantees we catch the latest map
    # even if this week's has not posted yet.
    now = time.time()
    end = time.strftime("%m/%d/%Y", time.localtime(now))
    start = time.strftime("%m/%d/%Y", time.localtime(now - 35 * 86400))
    r = requests.get(USDM_URL,
                     params={"aoi": fips, "startdate": start, "enddate": end,
                             "statisticsType": "1"},
                     headers={"Accept": "application/json"}, timeout=15)
    rows = r.json() or []
    if not rows:
        return {"ok": False, "reason": "No USDM data for this county."}
    latest = max(rows, key=lambda x: str(x.get("mapDate") or ""))
    code, label, pct = _classify(latest)
    return {
        "ok": True,
        "fips": fips,
        "county": latest.get("county"),
        "state": latest.get("state"),
        "map_date": str(latest.get("mapDate") or "")[:10],
        "category": code,
        "label": label,
        "coverage_pct": round(pct, 1),
        "severe": code in ("D3", "D4"),
        "source": "U.S. Drought Monitor (NDMC / USDA / NOAA)",
    }


def get_drought(county, state=None):
    """Cached USDM lookup. Always returns a dict; callers treat ok=False as
    'render the panel without drought context' rather than as an error."""
    fips = county_fips(county, state)
    if not fips:
        return {"ok": False, "reason": "County not recognized."}
    now = time.time()
    with _cache_lock:
        hit = _cache.get(fips)
        if hit and now - hit[0] < _TTL:
            _cache.move_to_end(fips)
            return hit[1]
    try:
        result = _fetch(fips)
    except Exception as e:
        return {"ok": False, "reason": "Drought lookup unavailable: %s" % e.__class__.__name__}
    if result.get("ok"):
        with _cache_lock:
            _cache[fips] = (now, result)
            while len(_cache) > _CACHE_MAX:
                _cache.popitem(last=False)
    return result
