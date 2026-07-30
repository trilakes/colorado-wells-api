"""
Septic Outlook — free soil-based septic feasibility snapshot for a searched point.

Lifted from the Tri-Lakes Colorado Site Planner septic estimator
(services/septic_estimate.py): USDA SSURGO point query -> limiting-horizon
LTAR -> Reg 43 soil type -> likely system + Reg 43 field sizing.
Only needs `requests` + stdlib; no API key (USDA SDA is public).
"""
import math
import threading
from collections import OrderedDict

import requests

SDA_URL = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/post.rest"

# Reg 43 sizing constants (calibrated to stamped Teller County plans):
# A = design flow / application rate; chambers = A / 12 sf (Infiltrator Quick4).
GPD_PER_BEDROOM = 150
CHAMBER_SF = 12.0

# Retail installed-cost ranges by configuration (mountain-market, 3 BR baseline).
CONFIG_COST = {
    "gravity":  (18000, 32000),
    "pressure": (22000, 36000),
    "atu":      (30000, 48000),
}
MOUND_ADDER = (12000, 20000)

# Reg 43 Table 10-1 LTAR (gal/day/sf), TL1 design rate by soil texture.
# Fast sands capped at 0.8 — too-rapid percolation doesn't treat effluent.
TEXTURE_LTAR = {
    "sand": 0.8, "coarse sand": 0.8, "fine sand": 0.7, "very fine sand": 0.6,
    "loamy sand": 0.8, "loamy coarse sand": 0.8, "loamy fine sand": 0.7,
    "loamy very fine sand": 0.6,
    "sandy loam": 0.8, "coarse sandy loam": 0.8, "fine sandy loam": 0.6,
    "very fine sandy loam": 0.5,
    "loam": 0.6, "silt loam": 0.5, "silt": 0.45,
    "sandy clay loam": 0.45, "clay loam": 0.35, "silty clay loam": 0.3,
    "sandy clay": 0.2, "silty clay": 0.15, "clay": 0.0,
}

# SSURGO results only change when the soil map unit changes, so a rounded
# point key is safe. ~500 entries keeps a Render starter dyno comfortable.
_CACHE_MAX = 500
_cache = OrderedDict()
_cache_lock = threading.Lock()


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _texture_ltar(tex):
    return TEXTURE_LTAR.get(tex.strip().lower()) if tex else None


def _reg43_type(ltar):
    if ltar >= 0.8:
        return "Type 0 — Sand / loamy sand"
    if ltar >= 0.6:
        return "Type 1 — Sandy loam / loam"
    if ltar >= 0.45:
        return "Type 2 — Loam / sandy clay loam"
    if ltar >= 0.3:
        return "Type 3 — Clay loam / silty clay loam"
    if ltar >= 0.15:
        return "Type 4 — Sandy clay / silty clay"
    return "Type 5 — Clay (engineered)"


def _tank_gal(bedrooms):
    if bedrooms <= 3:
        return 1250
    if bedrooms == 4:
        return 1500
    if bedrooms == 5:
        return 2000
    return 2500


def _soil(lat, lon):
    """USDA Soil Data Access point query: dominant component, horizon textures,
    drainage, slope, hydric flag, and bedrock restriction depth."""
    q = (
        "SELECT mu.muname, c.compname, c.comppct_r, c.drainagecl, c.slope_r, "
        "c.hydricrating, c.taxorder, ch.hzdept_r, ch.hzdepb_r, "
        "(SELECT TOP 1 t.texcl FROM chtexturegrp tg JOIN chtexture t ON t.chtgkey=tg.chtgkey "
        "WHERE tg.chkey=ch.chkey AND tg.rvindicator='Yes') AS texture, "
        "(SELECT TOP 1 cr.reskind FROM corestrictions cr WHERE cr.cokey=c.cokey) AS restriction, "
        "(SELECT TOP 1 cr.resdept_r FROM corestrictions cr WHERE cr.cokey=c.cokey) AS restrict_depth "
        "FROM mapunit mu JOIN component c ON c.mukey=mu.mukey JOIN chorizon ch ON ch.cokey=c.cokey "
        "WHERE mu.mukey IN (SELECT mukey FROM "
        "SDA_Get_Mukey_from_intersection_with_WktWgs84('POINT(%s %s)')) "
        "AND c.comppct_r=(SELECT MAX(comppct_r) FROM component WHERE mukey=mu.mukey) "
        "ORDER BY ch.hzdept_r" % (lon, lat)
    )
    r = requests.post(SDA_URL, json={"query": q, "format": "JSON"}, timeout=20)
    rows = r.json().get("Table") or []
    if not rows:
        return None
    f = rows[0]
    horizons = [{"top_in": _num(x[7]), "bot_in": _num(x[8]), "texture": x[9]} for x in rows]
    return {
        "map_unit": f[0] or "", "component": f[1] or "",
        "drainage": f[3] or "Unknown", "slope": _num(f[4]),
        "hydric": f[5] or "No", "restriction": f[10],
        "restrict_depth_in": _num(f[11]), "horizons": horizons,
    }


def _outlook(lat, lon, bedrooms):
    soil = _soil(lat, lon)
    if not soil:
        return {"ok": False, "reason": "No USDA soil survey data at this location."}

    drainage = (soil.get("drainage") or "").lower()
    hydric = (soil.get("hydric") or "No").lower() == "yes"
    series = soil.get("component") or soil.get("map_unit") or "Soil"

    # Reg 43 sizes off the most restrictive horizon in the drainfield zone (top 36 in).
    zone = [h for h in soil["horizons"]
            if h.get("top_in") is not None and h["top_in"] <= 36
            and _texture_ltar(h.get("texture")) is not None]
    if not zone:
        zone = [h for h in soil["horizons"] if _texture_ltar(h.get("texture")) is not None][:2]
    if zone:
        hz = min(zone, key=lambda h: _texture_ltar(h["texture"]))
        ltar, texture = _texture_ltar(hz["texture"]), hz["texture"]
    else:
        ltar, texture = 0.5, None

    bedrock = soil.get("restrict_depth_in")
    slope = soil.get("slope")
    constraint, note = None, ""
    if hydric or "poorly drained" in drainage or "very poorly" in drainage:
        constraint = "drainage"
        ltar = min(ltar, 0.2)
        note = ("Poor drainage / wetland indicators — plan on an engineered or "
                "advanced-treatment system.")
    elif bedrock is not None and bedrock <= 48:
        constraint = "bedrock"
        note = ("USDA maps shallow bedrock (~%d in). A profile pit often finds deeper "
                "weathered soil that allows a conventional system — a soil test settles it."
                % int(bedrock))

    steep = slope is not None and slope >= 15
    mound = constraint is not None
    if mound:
        system = "Engineered / advanced-treatment system"
        cost_kind = "atu"
        detail = "Pressure-dosed, likely on an engineered mound"
    elif steep:
        system = "Pressure-dosed drainfield"
        cost_kind = "pressure"
        detail = "Septic tank + pump/dose tank (slope calls for pressure distribution)"
    else:
        system = "Conventional gravity trench system"
        cost_kind = "gravity"
        detail = "Septic tank + gravity drainfield — the simplest, most affordable OWTS"

    flow = max(2, bedrooms) * GPD_PER_BEDROOM
    area = flow / max(ltar, 0.1)
    chambers = max(1, int(math.ceil(area / CHAMBER_SF)))
    lo, hi = CONFIG_COST[cost_kind]
    if mound:
        lo, hi = lo + MOUND_ADDER[0], hi + MOUND_ADDER[1]

    return {
        "ok": True,
        "soil": {
            "series": series,
            "map_unit": soil.get("map_unit"),
            "texture": (texture or "Unknown").capitalize(),
            "drainage": soil.get("drainage") or "Unknown",
            "slope_pct": slope,
            "ltar": ltar,
            "reg43_type": _reg43_type(ltar),
            "bedrock_in": bedrock,
            "constraint": constraint,
            "note": note,
        },
        "system": {"name": system, "detail": detail, "mound": mound,
                   "cost_low": lo, "cost_high": hi},
        "sizing": {"bedrooms": bedrooms, "design_flow_gpd": flow,
                   "field_sqft": int(round(area)), "chambers": chambers,
                   "tank_gal": _tank_gal(bedrooms)},
        "source": "USDA SSURGO soil survey + CO Reg 43 Table 10-1",
    }


def get_septic_outlook(lat, lon, bedrooms=3):
    """Cached entry point. Rounds the point to ~11 m so repeat searches on the
    same parcel don't re-hit SSURGO."""
    key = (round(lat, 4), round(lon, 4), bedrooms)
    with _cache_lock:
        if key in _cache:
            _cache.move_to_end(key)
            return _cache[key]
    try:
        result = _outlook(lat, lon, bedrooms)
    except Exception as e:
        return {"ok": False, "reason": "Soil lookup unavailable: %s" % e.__class__.__name__}
    if result.get("ok"):  # only cache successes — let transient failures retry
        with _cache_lock:
            _cache[key] = result
            while len(_cache) > _CACHE_MAX:
                _cache.popitem(last=False)
    return result
