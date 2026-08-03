"""
Well Resilience — how one well compares to its neighbors, and what that implies.

Every fact here already lives in the `wells` table. The product is the
comparison, not the data: a well is vulnerable when it is shallow relative to
its peers AND has little or no reserve below its screen. A 110 ft well screened
70-110 on a road where the median is 182 ft does not lose pressure gradually
when the water table drops past its screen - it stops.

Two things this module is deliberately careful about:

1. A single peer median is misleading where two aquifers exist. On Eby Creek
   (Eagle County) the shallow alluvial wells run 40-85 ft and the bedrock wells
   run 160-400 ft; averaging them describes no well that was ever drilled.
   `_regimes()` splits the peers first, and static-water comparisons are made
   only against wells in the same regime.

2. Permit yield is a RATING, not a measurement. Exempt household wells in
   Colorado are capped at 15 gpm, so a 15.0 in the data usually means "permitted
   to 15" rather than "tested at 15". Where most of the neighborhood reads
   exactly 15.0 we say so and refuse to imply a decline.
"""
import math
import threading
import time
from collections import OrderedDict

from drought_context import get_drought

# Peer search: start tight, widen only if the neighborhood is sparse.
RADIUS_START = 2.0
RADIUS_WIDE = 5.0
MIN_PEERS = 8

# Below this many peers even the wide search is not a sample worth scoring.
MIN_PEERS_ABSOLUTE = 4

# A regime split has to be a real gap, not just the largest of many small ones.
REGIME_MIN_GAP_FT = 40.0
REGIME_GAP_RATIO = 1.5
REGIME_MIN_SIDE = 3
# Producing zones are local; detect them on the nearest neighbours, not the
# whole radius, or the shallow/deep split blurs into a continuum.
REGIME_SAMPLE = 30

EXEMPT_YIELD_CAP = 15.0

_TTL = 6 * 3600
_CACHE_MAX = 400
_cache = OrderedDict()
_cache_lock = threading.Lock()

PEER_COLS = ("receipt, permit, latitude, longitude, depth_total, top_perforated, "
             "bottom_perforated, pump_yield_gpm, static_water_level, date_completed, "
             "aquifers, county, well_state, owner_name, address, status, uses")


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _median(vals):
    s = sorted(vals)
    n = len(s)
    if not n:
        return None
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _pct(vals, p):
    s = sorted(vals)
    if not s:
        return None
    k = (len(s) - 1) * (p / 100.0)
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _year(d):
    if not d:
        return None
    try:
        return int(str(d)[:4])
    except ValueError:
        return None


def _regimes(depths):
    """Split peer depths into producing zones on the largest real gap.

    Returns a list of {lo, hi, count} - one entry when the neighborhood draws
    from a single zone, two when there is a clean shallow/deep separation.
    """
    s = sorted(depths)
    if len(s) < 2 * REGIME_MIN_SIDE:
        return [{"lo": s[0], "hi": s[-1], "count": len(s)}] if s else []

    gaps = [(s[i + 1] - s[i], i) for i in range(len(s) - 1)]
    span = s[-1] - s[0]
    typical = span / float(len(s) - 1) if len(s) > 1 else 0
    biggest, idx = max(gaps)

    lower, upper = s[:idx + 1], s[idx + 1:]
    if (biggest >= REGIME_MIN_GAP_FT and biggest >= REGIME_GAP_RATIO * typical
            and len(lower) >= REGIME_MIN_SIDE and len(upper) >= REGIME_MIN_SIDE):
        return [{"lo": lower[0], "hi": lower[-1], "count": len(lower)},
                {"lo": upper[0], "hi": upper[-1], "count": len(upper)}]
    return [{"lo": s[0], "hi": s[-1], "count": len(s)}]


def _fetch_peers(conn, lat, lng, receipt, radius):
    """Bounding-box prefilter then true distance - same shape as wells_nearby()."""
    lat_range = radius / 69.0
    lng_range = radius / (69.0 * max(math.cos(math.radians(lat)), 0.01))
    cur = conn.cursor()
    cur.execute(
        "SELECT " + PEER_COLS + ", "
        "SQRT(POW((latitude - %s) * 69.0, 2) + "
        "     POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)) AS distance_miles "
        "FROM wells "
        "WHERE latitude BETWEEN %s AND %s AND longitude BETWEEN %s AND %s "
        "  AND latitude IS NOT NULL AND longitude IS NOT NULL "
        "  AND receipt <> %s AND depth_total IS NOT NULL AND depth_total > 0 "
        "ORDER BY distance_miles LIMIT 400",
        (lat, lng, lat, lat - lat_range, lat + lat_range,
         lng - lng_range, lng + lng_range, receipt))
    rows = [dict(r) for r in cur.fetchall()]
    return [r for r in rows if (r.get("distance_miles") or 0) <= radius]


def _area_stats(peers, regimes, radius):
    """The database view behind the Area Well Statistics panel."""
    depths = [_num(p.get("depth_total")) for p in peers]
    depths = [d for d in depths if d]
    tops = [_num(p.get("top_perforated")) for p in peers]
    tops = [t for t in tops if t]
    bots = [_num(p.get("bottom_perforated")) for p in peers]
    bots = [b for b in bots if b]
    statics = [_num(p.get("static_water_level")) for p in peers]
    statics = [s for s in statics if s]

    old, new = [], []
    for p in peers:
        y, d = _year(p.get("date_completed")), _num(p.get("depth_total"))
        if y and d:
            (old if y < 1995 else new).append(d)

    yields = [_num(p.get("pump_yield_gpm")) for p in peers]
    yields = [y for y in yields if y]
    capped = sum(1 for y in yields if abs(y - EXEMPT_YIELD_CAP) < 0.01)

    return {
        "count": len(peers),
        "depth": {
            "median": round(_median(depths), 0) if depths else None,
            "p25": round(_pct(depths, 25), 0) if depths else None,
            "p75": round(_pct(depths, 75), 0) if depths else None,
            "min": round(min(depths), 0) if depths else None,
            "max": round(max(depths), 0) if depths else None,
        },
        "screen": {
            "top_median": round(_median(tops), 0) if tops else None,
            "bottom_median": round(_median(bots), 0) if bots else None,
            "reported": len(bots),
        },
        "static": {
            "median": round(_median(statics), 0) if statics else None,
            "min": round(min(statics), 0) if statics else None,
            "max": round(max(statics), 0) if statics else None,
            "reported": len(statics),
        },
        "radius": radius,
        "vintage": {
            "pre1995_median_depth": round(_median(old), 0) if old else None,
            "post1995_median_depth": round(_median(new), 0) if new else None,
            "pre1995_count": len(old),
            "post1995_count": len(new),
        },
        "yield": {
            "reported": len(yields),
            "at_permit_cap": capped,
            "cap_dominated": bool(yields) and capped >= 0.5 * len(yields),
        },
        "regimes": regimes,
    }


def _analyze(well, peers, radius):
    depths = [_num(p.get("depth_total")) for p in peers]
    depths = [d for d in depths if d]
    # Producing zones are a LOCAL structure. Across 80+ wells spanning a whole
    # valley the shallow/deep separation washes out into a continuum, so detect
    # regimes on the nearest neighbours only (peers arrive distance-sorted).
    local = [d for d in (_num(p.get("depth_total")) for p in peers[:REGIME_SAMPLE]) if d]
    regimes = _regimes(local)
    stats = _area_stats(peers, regimes, radius)
    peer_median = stats["depth"]["median"]

    depth = _num(well.get("depth_total"))
    top = _num(well.get("top_perforated"))
    bot = _num(well.get("bottom_perforated"))
    year = _year(well.get("date_completed"))

    deeper = sum(1 for d in depths if d > depth) if depth else 0
    percentile = round(100.0 * sum(1 for d in depths if d < depth) / len(depths)) \
        if (depth and depths) else None

    reserve = round(depth - bot, 1) if (depth and bot) else None
    screen_len = round(bot - top, 1) if (top and bot) else None

    # Same-regime peers only - a shallow well must not be judged against bedrock
    # wells whose static level is naturally far deeper.
    my_regime = None
    for r in regimes:
        if depth and r["lo"] <= depth <= r["hi"]:
            my_regime = r
            break
    if my_regime and len(regimes) > 1:
        same = [p for p in peers
                if (_num(p.get("depth_total")) or 0) >= my_regime["lo"]
                and (_num(p.get("depth_total")) or 0) <= my_regime["hi"]]
    else:
        same = peers
    regime_statics = [_num(p.get("static_water_level")) for p in same]
    regime_statics = [s for s in regime_statics if s]
    static_median = _median(regime_statics)
    # Positive = screen bottom sits below the water table (submerged, good).
    margin = round(bot - static_median, 1) if (bot and static_median) else None

    shallow_regime = bool(my_regime and len(regimes) > 1 and my_regime is regimes[0])

    # ── score ────────────────────────────────────────────────────────────────
    points, findings = 0, []

    if percentile is not None and peer_median:
        if percentile <= 25:
            points += 2
            findings.append(
                "At %d ft, this well is shallower than %d of the %d wells within %g miles "
                "(neighborhood median %d ft)."
                % (depth, deeper, len(depths), radius, peer_median))
        elif percentile <= 40:
            points += 1
            findings.append(
                "At %d ft, this well is on the shallow side for the area "
                "(median %d ft)." % (depth, peer_median))

    # Screening to the bottom of the hole is normal practice, so zero reserve is
    # only a warning when the well is ALSO near the water table or shallow for the
    # area. A 530 ft well screened 430-530 with 350 ft of head above it is fine.
    reserve_matters = (margin is None or margin < 50) or (percentile is not None
                                                          and percentile <= 50)
    if reserve is not None and reserve_matters:
        if reserve <= 0:
            points += 2
            where = "The screen runs %d-%d ft" % (top, bot) if top else \
                    "The screen bottoms at %d ft" % bot
            findings.append(
                "%s and the well bottoms at %d ft, so there is no reserve below the "
                "producing zone. Once the water table falls past %d ft this well does "
                "not weaken, it stops." % (where, depth, bot))
        elif reserve <= 5:
            points += 1
            findings.append(
                "Only about %g ft of sump sits below the screen, so there is very little "
                "reserve if the water table drops." % reserve)

    if margin is not None:
        if margin < 0:
            points += 3
            findings.append(
                "Nearby wells in the same zone report water around %d ft down - already "
                "below this well's screen bottom of %d ft." % (static_median, bot))
        elif margin < 15:
            points += 2
            findings.append(
                "Nearby wells in the same zone report water around %d ft down, leaving "
                "roughly %g ft above this well's screen bottom." % (static_median, margin))
        elif margin < 30:
            points += 1

    if shallow_regime:
        points += 1
        deep = regimes[-1]
        findings.append(
            "Two producing zones show up around here: a shallow one (%d-%d ft, %d wells) "
            "and a deeper one (%d-%d ft, %d wells). This well appears to draw from the "
            "shallow zone, which responds much faster to dry years."
            % (regimes[0]["lo"], regimes[0]["hi"], regimes[0]["count"],
               deep["lo"], deep["hi"], deep["count"]))

    v = stats["vintage"]
    if (year and v["pre1995_median_depth"] and v["post1995_median_depth"]
            and v["post1995_count"] >= 3
            and v["post1995_median_depth"] > v["pre1995_median_depth"] * 1.25):
        findings.append(
            "Wells drilled here since 1995 average %d ft versus %d ft before - drillers "
            "have been going deeper in this area."
            % (v["post1995_median_depth"], v["pre1995_median_depth"]))

    # Drought amplifies an existing weakness; it does not by itself make a deep,
    # well-submerged well vulnerable. Only escalate a well already showing signal.
    drought = get_drought(well.get("county"), well.get("well_state"))
    if drought.get("ok") and drought.get("severe") and points > 0:
        points += 1
        findings.append(
            "%s is currently in %s (%s), which pulls shallow water-bearing zones down "
            "hardest." % (drought.get("county") or "This county",
                          drought.get("label"), drought.get("category")))

    if points >= 6:
        tier, label = "high", "High vulnerability"
    elif points >= 4:
        tier, label = "elevated", "Elevated vulnerability"
    elif points >= 2:
        tier, label = "moderate", "Moderate vulnerability"
    else:
        tier, label = "low", "Low vulnerability"

    if not findings:
        findings.append(
            "This well's depth and construction are typical for the area, with no "
            "obvious vulnerability in the state records.")

    # ── caveats - stated, never omitted ──────────────────────────────────────
    caveats = ["Based on state permit records for nearby wells, not an inspection "
               "of this well."]
    if stats["yield"]["cap_dominated"]:
        caveats.append(
            "Yield figures here are unreliable: %d of %d nearby wells report exactly "
            "15 gpm, which is the permitted ceiling for exempt household wells rather "
            "than a measured test. Treat a permit yield as a rating, not a measurement."
            % (stats["yield"]["at_permit_cap"], stats["yield"]["reported"]))
    if len(regimes) > 1:
        caveats.append("Which zone a well draws from is inferred from its depth, not "
                       "from a geologic log.")
    if stats["static"]["reported"] < 3:
        caveats.append("Few nearby wells report a static water level, so the water-table "
                       "comparison is weak here.")

    return {
        "ok": True,
        "well": {
            "receipt": well.get("receipt"), "permit": well.get("permit"),
            "address": well.get("address"), "county": well.get("county"),
            "depth_ft": depth, "screen_top_ft": top, "screen_bottom_ft": bot,
            "screen_length_ft": screen_len, "reserve_below_screen_ft": reserve,
            "year_completed": year, "uses": well.get("uses"),
            "permit_yield_gpm": _num(well.get("pump_yield_gpm")),
            "aquifers": well.get("aquifers"),
        },
        "comparison": {
            "peer_count": len(depths), "radius_miles": radius,
            "depth_percentile": percentile, "deeper_peers": deeper,
            "peer_depth_median": stats["depth"]["median"],
            "regime_static_median_ft": round(static_median, 0) if static_median else None,
            "screen_margin_ft": margin,
            "in_shallow_regime": shallow_regime,
        },
        "risk": {"tier": tier, "label": label, "score": points, "findings": findings},
        "area_stats": stats,
        "drought": drought if drought.get("ok") else None,
        "caveats": caveats,
        "source": "Colorado DWR well permit records + U.S. Drought Monitor",
    }


def get_well_resilience(receipt, conn):
    """Cached entry point. Returns {'ok': False, 'reason': ...} whenever there is
    not enough nearby data to say something honest."""
    now = time.time()
    with _cache_lock:
        hit = _cache.get(receipt)
        if hit and now - hit[0] < _TTL:
            _cache.move_to_end(receipt)
            return hit[1]

    cur = conn.cursor()
    cur.execute("SELECT " + PEER_COLS + " FROM wells WHERE receipt = %s LIMIT 1", (receipt,))
    row = cur.fetchone()
    if not row:
        return {"ok": False, "reason": "Well not found."}
    well = dict(row)

    lat, lng = well.get("latitude"), well.get("longitude")
    if lat is None or lng is None:
        return {"ok": False, "reason": "This well has no mapped location."}
    if not _num(well.get("depth_total")):
        return {"ok": False, "reason": "No depth on file for this well."}

    radius = RADIUS_START
    peers = _fetch_peers(conn, float(lat), float(lng), receipt, radius)
    if len(peers) < MIN_PEERS:
        radius = RADIUS_WIDE
        peers = _fetch_peers(conn, float(lat), float(lng), receipt, radius)
    if len(peers) < MIN_PEERS_ABSOLUTE:
        return {"ok": False,
                "reason": "Too few nearby wells on record to compare against."}

    result = _analyze(well, peers, radius)

    with _cache_lock:
        _cache[receipt] = (now, result)
        while len(_cache) > _CACHE_MAX:
            _cache.popitem(last=False)
    return result
