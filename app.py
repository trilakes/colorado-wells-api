"""
Well Finder - Multi-State Wells & Environmental Data API
Serves 1.3M+ wells (CO, AZ, NM, WY) plus 1.5M+ overlay features
(contamination, oil/gas, dams, gauges, springs, water rights, aquifers)
from PostgreSQL on Render.
"""
import os
import math
import io
from datetime import datetime, timezone
import requests as http_requests
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app, origins=[
    "https://coloradowell.com",
    "https://www.coloradowell.com",
    "http://localhost:*",
    "http://127.0.0.1:*"
])

DATABASE_URL = os.environ.get('DATABASE_URL', '')
STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY', '')
OWNER_EMAILS = ['kyle@trilakes.co']
VALID_STATES = {'CO', 'AZ', 'NM', 'WY'}

# ─── Overlay Layer Definitions ───────────────────────────────────────────────

OVERLAY_LAYERS = {
    'contamination': {
        'table': 'epa_sites',
        'label': 'EPA Contamination Sites',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'site_id, site_name, latitude, longitude, state, county, category, npl_status, superfund, site_url',
    },
    'superfund': {
        'table': 'superfund_sites',
        'label': 'Superfund Sites',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'epa_id, site_name, latitude, longitude, state, county, npl_status, site_url',
    },
    'oilgas': {
        'table': 'oil_gas_wells',
        'label': 'Oil & Gas Wells',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'api_number, well_name, operator, latitude, longitude, state, county, well_type, well_status, depth',
    },
    'gauges': {
        'table': 'stream_gauges',
        'label': 'USGS Stream Gauges',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'station_id, name, latitude, longitude, state, stage_ft, flow_cfs, status, url',
    },
    'dams': {
        'table': 'dams',
        'label': 'Dams & Reservoirs',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'dam_id, dam_name, latitude, longitude, state, county, river, dam_height_ft, max_storage_acre_ft, primary_purpose, hazard_potential, year_completed',
    },
    'springs': {
        'table': 'springs',
        'label': 'Springs',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'site_id, name, latitude, longitude, state, county, source',
    },
    'waterrights': {
        'table': 'water_rights',
        'label': 'Water Rights',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'record_number, latitude, longitude, basin, status, use_type, total_diversion, url',
    },
    'groundwater': {
        'table': 'groundwater_sites',
        'label': 'Groundwater Monitoring',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'site_id, latitude, longitude, well_type, water_use, well_depth, water_level_depth, county, basin',
    },
    'aquifers': {
        'table': 'aquifer_boundaries',
        'label': 'Aquifer Boundaries',
        'lat': 'center_lat', 'lng': 'center_lng',
        'bbox_cols': 'aquifer_name, aquifer_code, rock_name, rock_type, min_lat, max_lat, min_lng, max_lng, center_lat, center_lng',
    },
    'tri': {
        'table': 'tri_facilities',
        'label': 'Toxic Release Inventory',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'facility_id, facility_name, latitude, longitude, state, county, city, address, parent_company, closed',
    },
    'radon': {
        'table': 'radon_zones',
        'label': 'Radon Zones',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'county, state, radon_zone, predicted_level, risk_level, latitude, longitude',
    },
    'wqstations': {
        'table': 'wq_stations',
        'label': 'Water Quality Monitoring',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'station_id, name, latitude, longitude, state, county, organization, site_type, result_count, site_url',
    },
    'brownfields': {
        'table': 'brownfield_sites',
        'label': 'Brownfield Sites',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'registry_id, site_name, latitude, longitude, state, county, city, epa_id, property_id, cleanup_ind, assess_ind, site_url',
    },
    'rcra': {
        'table': 'rcra_sites',
        'label': 'RCRA Corrective Action',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'handler_id, handler_name, latitude, longitude, state, county, city, epa_id, gpra_ca, site_url',
    },
    'fedfac': {
        'table': 'federal_facilities',
        'label': 'Federal Facilities',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'registry_id, site_name, latitude, longitude, state, county, city, epa_id, agency_owner, federal_agency, ff_superfund, ff_rcra, ff_brac, site_url',
    },
    'eparesponse': {
        'table': 'epa_response_sites',
        'label': 'EPA Emergency Response',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'registry_id, site_name, latitude, longitude, state, county, city, epa_id, osc_site_id, response_type, response_status, site_url',
    },
    'mines': {
        'table': 'mine_sites',
        'label': 'Mine Sites',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'id, site_name, mine_type, latitude, longitude, county, topo_name',
    },
    'pfas': {
        'table': 'pfas_sites',
        'label': 'PFAS Contamination',
        'lat': 'latitude', 'lng': 'longitude',
        'bbox_cols': 'id, site_name, source_type, latitude, longitude, state, county, city, facility_id, contaminant',
    },
}


def get_db():
    """Get a database connection."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn


def parse_state_filter():
    """Parse ?state= parameter. Returns (condition_sql, params) or (None, [])."""
    state = request.args.get('state', '').strip().upper()
    if state:
        states = [s.strip() for s in state.split(',') if s.strip() in VALID_STATES]
        if states:
            placeholders = ','.join(['%s'] * len(states))
            return f"well_state IN ({placeholders})", states
    return None, []


# ─── Explore Map: Bounding Box Query ────────────────────────────────────────

@app.route('/api/wells/bbox')
def wells_bbox():
    """
    Get wells within a bounding box for the explore map.
    Returns lightweight payload (lat, lon, depth, receipt) for fast rendering.

    Query params:
      minLat, maxLat, minLng, maxLng  (required)
      state  (optional — CO, AZ, NM or comma-separated)
      limit  (optional, default 10000, max 50000)
      hasDepth (optional — if 'true', only return wells with depth_total > 0)
    """
    try:
        min_lat = float(request.args.get('minLat', 0))
        max_lat = float(request.args.get('maxLat', 0))
        min_lng = float(request.args.get('minLng', 0))
        max_lng = float(request.args.get('maxLng', 0))
        limit = min(int(request.args.get('limit', 10000)), 50000)
        has_depth = request.args.get('hasDepth', 'false').lower() == 'true'
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid parameters: {e}"}), 400

    if min_lat == 0 and max_lat == 0:
        return jsonify({"error": "Bounding box required"}), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        state_cond, state_params = parse_state_filter()
        extra_where = f"AND {state_cond}" if state_cond else ""
        depth_filter = "AND depth_total > 0" if has_depth else ""

        # At wide zoom, use hash-sampling backed by partial index idx_wells_hash_sample.
        # The partial index (WHERE depth_total>0 AND hashtext(receipt)%40=0) covers ~23K rows
        # so the query touches only ~10K rows instead of 600K → sub-second instead of 24s.
        lat_span = max_lat - min_lat
        lng_span = max_lng - min_lng
        bbox_area = lat_span * lng_span

        if bbox_area > 4:
            cur.execute(f"""
                SELECT receipt, permit, latitude, longitude, depth_total,
                       status, county, uses, pump_yield_gpm, static_water_level,
                       aquifers, driller_name, date_completed, address, city,
                       owner_name, category, elevation, well_state
                FROM wells
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND depth_total > 0
                  AND hashtext(receipt) %% 40 = 0
                  {extra_where}
                ORDER BY RANDOM()
                LIMIT %s
            """, (min_lat, max_lat, min_lng, max_lng, *state_params, limit))
        else:
            cur.execute(f"""
                SELECT receipt, permit, latitude, longitude, depth_total,
                       status, county, uses, pump_yield_gpm, static_water_level,
                       aquifers, driller_name, date_completed, address, city,
                       owner_name, category, elevation, well_state
                FROM wells
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  {depth_filter}
                  {extra_where}
                LIMIT %s
            """, (min_lat, max_lat, min_lng, max_lng, *state_params, limit))

        rows = cur.fetchall()
        return jsonify({
            "wells": rows,
            "count": len(rows),
            "truncated": len(rows) >= limit
        })
    finally:
        conn.close()


# ─── Clustered Wells for Zoomed-Out Views ────────────────────────────────────

@app.route('/api/wells/clusters')
def wells_clusters():
    """
    Get clustered well counts in a grid for zoomed-out views.
    Divides the bounding box into a grid and returns counts per cell.

    Query params:
      minLat, maxLat, minLng, maxLng  (required)
      state  (optional — CO, AZ, NM or comma-separated)
      gridSize  (optional, default 20 — number of cells per axis)
    """
    try:
        min_lat = float(request.args.get('minLat', 0))
        max_lat = float(request.args.get('maxLat', 0))
        min_lng = float(request.args.get('minLng', 0))
        max_lng = float(request.args.get('maxLng', 0))
        grid_size = int(request.args.get('gridSize', 20))
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid parameters: {e}"}), 400

    lat_step = (max_lat - min_lat) / grid_size
    lng_step = (max_lng - min_lng) / grid_size

    if lat_step <= 0 or lng_step <= 0:
        return jsonify({"clusters": [], "count": 0})

    conn = get_db()
    try:
        cur = conn.cursor()
        state_cond, state_params = parse_state_filter()
        extra_where = f"AND {state_cond}" if state_cond else ""
        cur.execute(f"""
            SELECT
                FLOOR((latitude - %s) / %s) AS lat_bin,
                FLOOR((longitude - %s) / %s) AS lng_bin,
                COUNT(*) AS count,
                AVG(depth_total) AS avg_depth
            FROM wells
            WHERE latitude BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              {extra_where}
            GROUP BY lat_bin, lng_bin
        """, (min_lat, lat_step, min_lng, lng_step,
              min_lat, max_lat, min_lng, max_lng, *state_params))

        clusters = []
        for row in cur.fetchall():
            lat_bin = int(row['lat_bin']) if row['lat_bin'] is not None else 0
            lng_bin = int(row['lng_bin']) if row['lng_bin'] is not None else 0
            clusters.append({
                "lat": min_lat + (lat_bin + 0.5) * lat_step,
                "lng": min_lng + (lng_bin + 0.5) * lng_step,
                "count": row['count'],
                "avgDepth": float(row['avg_depth']) if row['avg_depth'] else None
            })

        return jsonify({"clusters": clusters, "count": len(clusters)})
    finally:
        conn.close()


# ─── Well Detail ─────────────────────────────────────────────────────────────

@app.route('/api/wells/<receipt>')
def well_detail(receipt):
    """Get full details for a single well by receipt number."""
    conn = get_db()
    try:
        cur = conn.cursor()
        state_cond, state_params = parse_state_filter()
        if state_cond:
            cur.execute(f"SELECT * FROM wells WHERE receipt = %s AND {state_cond} LIMIT 1",
                        (receipt, *state_params))
        else:
            cur.execute("SELECT * FROM wells WHERE receipt = %s LIMIT 1", (receipt,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Well not found"}), 404
        return jsonify(dict(row))
    finally:
        conn.close()


# ─── Search Wells ────────────────────────────────────────────────────────────

@app.route('/api/wells/search')
def wells_search():
    """
    Search wells by various criteria.

    Query params:
      q         - free text (searches owner, address, receipt, permit, city)
      county    - county name
      division  - water division number
      uses      - well use type
      status    - well status
      state     - CO, AZ, NM or comma-separated
      minDepth  - minimum total depth
      maxDepth  - maximum total depth
      page      - page number (default 1)
      pageSize  - results per page (default 50, max 200)
    """
    q = request.args.get('q', '').strip()
    county = request.args.get('county', '').strip()
    division = request.args.get('division', '').strip()
    uses = request.args.get('uses', '').strip()
    status = request.args.get('status', '').strip()
    min_depth = request.args.get('minDepth', '')
    max_depth = request.args.get('maxDepth', '')
    page = max(1, int(request.args.get('page', 1)))
    page_size = min(int(request.args.get('pageSize', 50)), 200)

    conditions = []
    params = []

    if q:
        conditions.append("""
            (owner_name ILIKE %s OR address ILIKE %s OR receipt ILIKE %s
             OR permit ILIKE %s OR city ILIKE %s OR parcel_name ILIKE %s)
        """)
        like_q = f"%{q}%"
        params.extend([like_q] * 6)

    if county:
        conditions.append("UPPER(county) = UPPER(%s)")
        params.append(county)

    if division:
        conditions.append("division::text = %s")
        params.append(str(division))

    if uses:
        conditions.append("uses ILIKE %s")
        params.append(f"%{uses}%")

    if status:
        conditions.append("status ILIKE %s")
        params.append(f"%{status}%")

    if min_depth:
        conditions.append("depth_total >= %s")
        params.append(float(min_depth))

    if max_depth:
        conditions.append("depth_total <= %s")
        params.append(float(max_depth))

    state_cond, state_params = parse_state_filter()
    if state_cond:
        conditions.append(state_cond)
        params.extend(state_params)

    where = " AND ".join(conditions) if conditions else "TRUE"

    conn = get_db()
    try:
        cur = conn.cursor()

        # Count total
        cur.execute(f"SELECT COUNT(*) AS total FROM wells WHERE {where}", params)
        total = cur.fetchone()['total']

        # Fetch page
        offset = (page - 1) * page_size
        cur.execute(f"""
            SELECT receipt, permit, status, category, latitude, longitude,
                   county, division, depth_total, pump_yield_gpm,
                   static_water_level, uses, owner_name, city, state, zip_code,
                   aquifers, date_permit_issued, date_completed, more_info_url,
                   well_state
            FROM wells
            WHERE {where}
            ORDER BY receipt
            LIMIT %s OFFSET %s
        """, params + [page_size, offset])

        rows = cur.fetchall()

        return jsonify({
            "wells": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pageSize": page_size,
            "totalPages": math.ceil(total / page_size)
        })
    finally:
        conn.close()


# ─── Stats ───────────────────────────────────────────────────────────────────

@app.route('/api/wells/stats')
def wells_stats():
    """Get aggregate statistics about the wells database. Supports ?state= filter."""
    state_cond, state_params = parse_state_filter()
    extra_where = f"WHERE {state_cond}" if state_cond else ""
    county_extra = f"AND {state_cond}" if state_cond else ""

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT
                COUNT(*) AS total_wells,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) AS geocoded_wells,
                COUNT(CASE WHEN depth_total IS NOT NULL THEN 1 END) AS wells_with_depth,
                AVG(depth_total) AS avg_depth,
                MAX(depth_total) AS max_depth,
                COUNT(DISTINCT county) AS counties
            FROM wells
            {extra_where}
        """, state_params)
        stats = dict(cur.fetchone())
        stats['avg_depth'] = round(float(stats['avg_depth']), 1) if stats['avg_depth'] else None
        stats['max_depth'] = float(stats['max_depth']) if stats['max_depth'] else None

        # State breakdown
        cur.execute("SELECT well_state, COUNT(*) AS count FROM wells GROUP BY well_state ORDER BY well_state")
        stats['states'] = [dict(r) for r in cur.fetchall()]

        # County breakdown
        cur.execute(f"""
            SELECT county, COUNT(*) AS count
            FROM wells
            WHERE county IS NOT NULL
            {county_extra}
            GROUP BY county
            ORDER BY count DESC
            LIMIT 20
        """, state_params)
        stats['top_counties'] = [dict(r) for r in cur.fetchall()]

        return jsonify(stats)
    finally:
        conn.close()


# ─── County List ─────────────────────────────────────────────────────────────

@app.route('/api/wells/counties')
def wells_counties():
    """Get list of all counties with well counts. Supports ?state= filter."""
    state_cond, state_params = parse_state_filter()
    extra_where = f"AND {state_cond}" if state_cond else ""

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT county, COUNT(*) AS count
            FROM wells
            WHERE county IS NOT NULL
            {extra_where}
            GROUP BY county
            ORDER BY county
        """, state_params)
        return jsonify([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()


# ─── Stripe Email Verification ───────────────────────────────────────────────

@app.route('/api/verify', methods=['POST'])
def verify_email():
    """Verify Stripe subscription/payment by email.
    
    Checks in order:
    1. Customer-based: subscriptions, checkout sessions, payment intents
    2. Email-based fallback: scans checkout sessions from known Payment Links
       (handles payments made before customer_creation=always was enabled)
    
    Returns tier info for the new report model.
    """
    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()

    if not email:
        return jsonify({"valid": False, "message": "Email required"}), 400

    # Owner bypass
    if email in OWNER_EMAILS:
        return jsonify({"valid": True, "type": "unlimited", "tier": "unlimited",
                        "reports_remaining": 999999, "message": "Unlimited access verified"})

    if not STRIPE_SECRET_KEY:
        return jsonify({"valid": False, "message": "Payment verification unavailable"}), 503

    headers = {"Authorization": f"Bearer {STRIPE_SECRET_KEY}"}

    try:
        # ── Step 1: Customer-based lookup ──
        r = http_requests.get(
            f"https://api.stripe.com/v1/customers?email={email}&limit=1",
            headers=headers, timeout=10
        )
        customers = r.json()
        customer_id = customers['data'][0]['id'] if customers.get('data') else None

        if customer_id:
            cust = customers['data'][0]
            meta = cust.get('metadata', {})

            # Check if tier is already set on customer
            if meta.get('report_tier'):
                tier = meta['report_tier']
                used = int(meta.get('reports_used', '0'))
                limit = TIER_REPORT_LIMITS.get(tier, 0)
                return jsonify({
                    "valid": True,
                    "type": tier,
                    "tier": tier,
                    "reports_used": used,
                    "reports_remaining": max(0, limit - used),
                    "message": f"{tier.title()} access verified"
                })

            # 1a) Check active subscription (legacy monthly → treat as unlimited)
            r = http_requests.get(
                f"https://api.stripe.com/v1/subscriptions?customer={customer_id}&status=active&limit=1",
                headers=headers, timeout=10
            )
            subs = r.json()
            if subs.get('data'):
                return jsonify({
                    "valid": True,
                    "type": "unlimited",
                    "tier": "unlimited",
                    "reports_remaining": 999999,
                    "message": "Active subscription found — unlimited access",
                    "expiresAt": subs['data'][0].get('current_period_end', 0) * 1000
                })

            # 1b) Check checkout sessions for tier detection
            r = http_requests.get(
                f"https://api.stripe.com/v1/checkout/sessions?customer={customer_id}&limit=20",
                headers=headers, timeout=10
            )
            sessions = r.json()

            # Check new report payment links first
            for s in sessions.get('data', []):
                if s.get('payment_status') != 'paid':
                    continue
                plink = s.get('payment_link')
                if plink in REPORT_PAYMENT_LINKS:
                    idx = REPORT_PAYMENT_LINKS.index(plink)
                    detected_tier = ['single', 'pack', 'unlimited'][idx]
                    # Save to customer
                    http_requests.post(
                        f"https://api.stripe.com/v1/customers/{customer_id}",
                        headers=headers, timeout=10,
                        data={'metadata[report_tier]': detected_tier, 'metadata[reports_used]': '0'}
                    )
                    limit = TIER_REPORT_LIMITS.get(detected_tier, 0)
                    return jsonify({
                        "valid": True,
                        "type": detected_tier,
                        "tier": detected_tier,
                        "reports_used": 0,
                        "reports_remaining": limit,
                        "message": f"{detected_tier.title()} access verified"
                    })

            # Legacy one-time payment → treat as unlimited
            has_lifetime = any(
                s.get('payment_status') == 'paid' and s.get('mode') == 'payment'
                for s in sessions.get('data', [])
            )
            if has_lifetime:
                return jsonify({"valid": True, "type": "unlimited", "tier": "unlimited",
                                "reports_remaining": 999999, "message": "Lifetime access verified"})

            # 1c) Fallback — check payment_intents
            r = http_requests.get(
                f"https://api.stripe.com/v1/payment_intents?customer={customer_id}&limit=10",
                headers=headers, timeout=10
            )
            payments = r.json()
            has_payment = any(p.get('status') == 'succeeded' for p in payments.get('data', []))
            if has_payment:
                return jsonify({"valid": True, "type": "unlimited", "tier": "unlimited",
                                "reports_remaining": 999999, "message": "Payment verified"})

        # ── Step 2: Email-based fallback ──
        ALL_PAYMENT_LINKS = REPORT_PAYMENT_LINKS + [
            'plink_1T0XVOFiHBHcGzRNQXGAkacg',
            'plink_1T0XVTFiHBHcGzRNkd9H0TWL',
            'plink_1T0T2KFiHBHcGzRNUJfF6mTD',
            'plink_1T0SuoFiHBHcGzRNUcVpycOk',
            'plink_1T0SuoFiHBHcGzRN9NwMrpqm',
            'plink_1T0Rm2FiHBHcGzRNMDlLGOxZ',
            'plink_1T0RlwFiHBHcGzRNfHxd5vlh',
            'plink_1T0O6zFiHBHcGzRNs0fLtb2q',
            'plink_1T0O6sFiHBHcGzRNYinOZNvg',
            'plink_1T0O2WFiHBHcGzRNnCDNowrR',
            'plink_1SjAS0FiHBHcGzRNSfmm24E1',
        ]

        for plink_id in ALL_PAYMENT_LINKS:
            plink_id = plink_id.strip()
            if not plink_id:
                continue
            try:
                r = http_requests.get(
                    f"https://api.stripe.com/v1/checkout/sessions?payment_link={plink_id}&limit=100",
                    headers=headers, timeout=15
                )
                sessions = r.json()
                for s in sessions.get('data', []):
                    cd = s.get('customer_details') or {}
                    if cd.get('email', '').lower() == email and s.get('payment_status') == 'paid':
                        # New report links → detect tier
                        if plink_id in REPORT_PAYMENT_LINKS:
                            idx = REPORT_PAYMENT_LINKS.index(plink_id)
                            detected_tier = ['single', 'pack', 'unlimited'][idx]
                            limit = TIER_REPORT_LIMITS.get(detected_tier, 0)
                            return jsonify({
                                "valid": True,
                                "type": detected_tier,
                                "tier": detected_tier,
                                "reports_remaining": limit,
                                "message": f"{detected_tier.title()} access verified"
                            })
                        # Legacy links → unlimited
                        access_type = 'unlimited'
                        return jsonify({
                            "valid": True,
                            "type": access_type,
                            "tier": access_type,
                            "reports_remaining": 999999,
                            "message": f"Lifetime access verified (payment found)"
                        })
            except Exception:
                continue

        return jsonify({"valid": False, "message": "No active subscription found for this email"})

    except Exception:
        return jsonify({"valid": False, "message": "Verification error"}), 500


# ─── Overlay: Bounding Box Query ─────────────────────────────────────────────

@app.route('/api/overlay/<layer>/bbox')
def overlay_bbox(layer):
    """
    Get overlay features within a bounding box.

    Layers: contamination, superfund, oilgas, gauges, dams, springs,
            waterrights, groundwater, aquifers

    Query params:
      minLat, maxLat, minLng, maxLng  (required)
      limit  (optional, default 5000, max 20000)
    """
    if layer not in OVERLAY_LAYERS:
        return jsonify({
            "error": f"Unknown layer: {layer}",
            "available": list(OVERLAY_LAYERS.keys())
        }), 400

    config = OVERLAY_LAYERS[layer]

    try:
        min_lat = float(request.args.get('minLat', 0))
        max_lat = float(request.args.get('maxLat', 0))
        min_lng = float(request.args.get('minLng', 0))
        max_lng = float(request.args.get('maxLng', 0))
        limit = min(int(request.args.get('limit', 5000)), 20000)
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid parameters: {e}"}), 400

    if min_lat == 0 and max_lat == 0:
        return jsonify({"error": "Bounding box required"}), 400

    lat_col = config['lat']
    lng_col = config['lng']

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {config['bbox_cols']}
            FROM {config['table']}
            WHERE {lat_col} BETWEEN %s AND %s
              AND {lng_col} BETWEEN %s AND %s
              AND {lat_col} IS NOT NULL
              AND {lng_col} IS NOT NULL
            LIMIT %s
        """, (min_lat, max_lat, min_lng, max_lng, limit))

        rows = cur.fetchall()
        return jsonify({
            "layer": layer,
            "label": config['label'],
            "features": [dict(r) for r in rows],
            "count": len(rows),
            "truncated": len(rows) >= limit
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ─── Overlay: Feature Detail ────────────────────────────────────────────────

@app.route('/api/overlay/<layer>/<feature_id>')
def overlay_detail(layer, feature_id):
    """Get full details for a single overlay feature."""
    if layer not in OVERLAY_LAYERS:
        return jsonify({"error": f"Unknown layer: {layer}"}), 400

    config = OVERLAY_LAYERS[layer]
    # Determine the ID column based on layer
    id_cols = {
        'contamination': 'site_id', 'superfund': 'epa_id',
        'oilgas': 'api_number', 'gauges': 'station_id',
        'dams': 'dam_id', 'springs': 'site_id',
        'waterrights': 'record_number', 'groundwater': 'site_id',
        'aquifers': 'aquifer_code', 'tri': 'facility_id',
        'radon': 'county', 'wqstations': 'station_id',
        'brownfields': 'registry_id', 'rcra': 'handler_id',
        'fedfac': 'registry_id', 'eparesponse': 'registry_id',
        'mines': 'id', 'pfas': 'id',
    }
    id_col = id_cols.get(layer, 'id')

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {config['table']} WHERE {id_col} = %s LIMIT 1",
                    (feature_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Feature not found"}), 404
        return jsonify(dict(row))
    finally:
        conn.close()


# ─── Overlay: Statistics ─────────────────────────────────────────────────────

@app.route('/api/overlay/stats')
def overlay_stats():
    """Get total feature counts for all overlay layers and wells."""
    conn = get_db()
    try:
        cur = conn.cursor()
        layers = {}
        for key, config in OVERLAY_LAYERS.items():
            try:
                cur.execute(f"SELECT COUNT(*) AS count FROM {config['table']}")
                layers[key] = {
                    "label": config['label'],
                    "count": cur.fetchone()['count']
                }
            except Exception:
                layers[key] = {"label": config['label'], "count": 0}
                conn.rollback()

        # Wells breakdown
        cur.execute("SELECT well_state, COUNT(*) AS count FROM wells GROUP BY well_state ORDER BY well_state")
        wells_by_state = {r['well_state']: r['count'] for r in cur.fetchall()}

        return jsonify({
            "wells": wells_by_state,
            "wells_total": sum(wells_by_state.values()),
            "overlay_layers": layers,
            "overlay_total": sum(s['count'] for s in layers.values()),
        })
    finally:
        conn.close()


# ─── Nearby: Proximity Query Across All Layers ──────────────────────────────

@app.route('/api/nearby')
def nearby_features():
    """
    Get nearby features from all layers within a radius of a point.

    Query params:
      lat, lng     (required — center point)
      radius       (optional, miles, default 5)
      layers       (optional, comma-separated layer names, default all)
      limit        (optional, per-layer limit, default 10, max 50)
    """
    try:
        lat = float(request.args.get('lat'))
        lng = float(request.args.get('lng'))
    except (TypeError, ValueError):
        return jsonify({"error": "lat and lng required"}), 400

    radius_miles = float(request.args.get('radius', 5))
    per_limit = min(int(request.args.get('limit', 10)), 50)

    # Convert miles to approximate degrees
    lat_range = radius_miles / 69.0
    lng_range = radius_miles / (69.0 * max(math.cos(math.radians(lat)), 0.01))

    min_lat = lat - lat_range
    max_lat = lat + lat_range
    min_lng = lng - lng_range
    max_lng = lng + lng_range

    requested = request.args.get('layers', '').strip()
    if requested:
        target_layers = [l.strip() for l in requested.split(',') if l.strip() in OVERLAY_LAYERS]
    else:
        target_layers = list(OVERLAY_LAYERS.keys())

    conn = get_db()
    try:
        cur = conn.cursor()
        results = {}

        for key in target_layers:
            config = OVERLAY_LAYERS[key]
            lat_col = config['lat']
            lng_col = config['lng']

            try:
                cur.execute(f"""
                    SELECT {config['bbox_cols']},
                           SQRT(POW(({lat_col} - %s) * 69.0, 2) +
                                POW(({lng_col} - %s) * 69.0 * COS(RADIANS(%s)), 2)
                           ) AS distance_miles
                    FROM {config['table']}
                    WHERE {lat_col} BETWEEN %s AND %s
                      AND {lng_col} BETWEEN %s AND %s
                      AND {lat_col} IS NOT NULL
                    ORDER BY distance_miles
                    LIMIT %s
                """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng, per_limit))

                features = cur.fetchall()
                results[key] = {
                    "label": config['label'],
                    "count": len(features),
                    "nearest": [dict(f) for f in features],
                }
            except Exception as e:
                results[key] = {"label": config['label'], "count": 0, "error": str(e)}
                conn.rollback()

        # Also include nearest wells
        try:
            cur.execute("""
                SELECT receipt, latitude, longitude, depth_total, county, well_state,
                       owner_name, uses, status,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM wells
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                  AND latitude IS NOT NULL
                ORDER BY distance_miles
                LIMIT %s
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng, per_limit))

            wells = cur.fetchall()
            results['wells'] = {
                "label": "Water Wells",
                "count": len(wells),
                "nearest": [dict(w) for w in wells],
            }
        except Exception as e:
            results['wells'] = {"label": "Water Wells", "count": 0, "error": str(e)}

        return jsonify({
            "center": {"lat": lat, "lng": lng},
            "radius_miles": radius_miles,
            "layers": results,
        })
    finally:
        conn.close()


# ─── PDF Report Generation ───────────────────────────────────────────────────

# New payment link IDs for report verification
REPORT_PAYMENT_LINKS = [
    'plink_1T2OhpFiHBHcGzRNjrGsoYjT',   # Single $39
    'plink_1T2OhqFiHBHcGzRNjMc0JTth',   # Unlimited $49
    'plink_1T215ZCSH1YJHcuWVktIj4RW',   # Legacy Single $19
    'plink_1T215fCSH1YJHcuWd6wiCnrI',   # Legacy Pack $49
    'plink_1T215kCSH1YJHcuWxo8TKhcw',   # Legacy Unlimited $97
]

TIER_REPORT_LIMITS = {
    'single': 1,
    'unlimited': 999999,
}


def _verify_report_access(email):
    """Check if email has report access and return tier info."""
    if not STRIPE_SECRET_KEY:
        return None
    if email in OWNER_EMAILS:
        return {'tier': 'unlimited', 'reports_remaining': 999999}

    headers = {"Authorization": f"Bearer {STRIPE_SECRET_KEY}"}

    try:
        # Check customer-based first
        r = http_requests.get(
            f"https://api.stripe.com/v1/customers?email={email}&limit=1",
            headers=headers, timeout=10
        )
        customers = r.json()
        cust_id = customers['data'][0]['id'] if customers.get('data') else None

        if cust_id:
            # Get customer metadata for report tracking
            cust = customers['data'][0]
            meta = cust.get('metadata', {})
            tier = meta.get('report_tier')
            used = int(meta.get('reports_used', '0'))

            if tier:
                limit = TIER_REPORT_LIMITS.get(tier, 0)
                return {
                    'tier': tier,
                    'customer_id': cust_id,
                    'reports_used': used,
                    'reports_remaining': max(0, limit - used),
                }

            # No tier set yet — check checkout sessions to determine tier
            r = http_requests.get(
                f"https://api.stripe.com/v1/checkout/sessions?customer={cust_id}&limit=20",
                headers=headers, timeout=10
            )
            sessions = r.json()
            for s in sessions.get('data', []):
                if s.get('payment_status') != 'paid':
                    continue
                # Check payment link metadata for tier
                plink = s.get('payment_link')
                if plink in REPORT_PAYMENT_LINKS:
                    idx = REPORT_PAYMENT_LINKS.index(plink)
                    detected_tier = ['single', 'pack', 'unlimited'][idx]
                    # Set tier on customer for future lookups
                    http_requests.post(
                        f"https://api.stripe.com/v1/customers/{cust_id}",
                        headers=headers, timeout=10,
                        data={'metadata[report_tier]': detected_tier, 'metadata[reports_used]': '0'}
                    )
                    limit = TIER_REPORT_LIMITS.get(detected_tier, 0)
                    return {
                        'tier': detected_tier,
                        'customer_id': cust_id,
                        'reports_used': 0,
                        'reports_remaining': limit,
                    }

            # Fallback — check if any successful one-time payment (legacy lifetime buyers)
            has_payment = any(
                s.get('payment_status') == 'paid' and s.get('mode') == 'payment'
                for s in sessions.get('data', [])
            )
            if has_payment:
                return {'tier': 'unlimited', 'customer_id': cust_id, 'reports_remaining': 999999}

            # Check subscriptions (legacy monthly)
            r = http_requests.get(
                f"https://api.stripe.com/v1/subscriptions?customer={cust_id}&status=active&limit=1",
                headers=headers, timeout=10
            )
            if r.json().get('data'):
                return {'tier': 'unlimited', 'customer_id': cust_id, 'reports_remaining': 999999}

        # Email-based fallback for all payment links (old + new)
        all_plinks = REPORT_PAYMENT_LINKS + [
            'plink_1T0XVOFiHBHcGzRNQXGAkacg', 'plink_1T0XVTFiHBHcGzRNkd9H0TWL',
        ]
        for plink_id in all_plinks:
            try:
                r = http_requests.get(
                    f"https://api.stripe.com/v1/checkout/sessions?payment_link={plink_id}&limit=100",
                    headers=headers, timeout=15
                )
                for s in r.json().get('data', []):
                    cd = s.get('customer_details') or {}
                    if cd.get('email', '').lower() == email and s.get('payment_status') == 'paid':
                        # Legacy buyers get unlimited
                        if plink_id not in REPORT_PAYMENT_LINKS:
                            return {'tier': 'unlimited', 'reports_remaining': 999999}
                        idx = REPORT_PAYMENT_LINKS.index(plink_id)
                        detected_tier = ['single', 'pack', 'unlimited'][idx]
                        return {
                            'tier': detected_tier,
                            'reports_remaining': TIER_REPORT_LIMITS.get(detected_tier, 0),
                        }
            except Exception:
                continue

    except Exception:
        pass

    return None


def _increment_reports_used(customer_id, current_used):
    """Increment the reports_used counter on the Stripe customer."""
    if not customer_id or not STRIPE_SECRET_KEY:
        return
    headers = {"Authorization": f"Bearer {STRIPE_SECRET_KEY}"}
    try:
        http_requests.post(
            f"https://api.stripe.com/v1/customers/{customer_id}",
            headers=headers, timeout=10,
            data={'metadata[reports_used]': str(current_used + 1)}
        )
    except Exception:
        pass


def _compute_water_risk_score(hazards, radon_info=None):
    """Compute a 0-100 water risk score based on nearby hazards."""
    score = 0

    # Weight by hazard type and distance
    TYPE_WEIGHTS = {
        'superfund': 25, 'pfas': 22, 'epa': 15, 'eparesponse': 15,
        'tri': 12, 'rcra': 12, 'brownfield': 10, 'mine': 8, 'fedfac': 8,
    }

    for h in hazards:
        htype = h.get('_type', 'epa')
        dist = h.get('distance_miles', 5)
        weight = TYPE_WEIGHTS.get(htype, 8)

        # Closer = more dangerous (exponential decay)
        if dist < 1:
            score += weight * 1.0
        elif dist < 2:
            score += weight * 0.6
        elif dist < 3:
            score += weight * 0.3
        else:
            score += weight * 0.15

    # Radon zone bonus
    if radon_info:
        zone = radon_info.get('radon_zone')
        if zone == 1:
            score += 12
        elif zone == 2:
            score += 6

    return min(100, round(score))


def _generate_report_pdf(address, lat, lng, wells, hazards, area_stats, radon_info=None):
    """Generate an ultra-premium PDF property water & environmental risk report."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.colors import HexColor, Color
    from reportlab.lib.units import inch
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                     TableStyle, HRFlowable, KeepTogether, PageBreak,
                                     CondPageBreak)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    import hashlib

    buf = io.BytesIO()
    W, H = letter

    # ── Color Palette ──
    NAVY       = HexColor('#0a1628')
    SLATE      = HexColor('#1e293b')
    BLUE       = HexColor('#0284c7')
    LIGHT_BLUE = HexColor('#e0f2fe')
    ICE        = HexColor('#f0f9ff')
    GREEN      = HexColor('#16a34a')
    L_GREEN    = HexColor('#f0fdf4')
    RED        = HexColor('#dc2626')
    L_RED      = HexColor('#fef2f2')
    AMBER      = HexColor('#d97706')
    L_AMBER    = HexColor('#fffbeb')
    PURPLE     = HexColor('#7c3aed')
    L_PURPLE   = HexColor('#f5f3ff')
    BROWN      = HexColor('#78350f')
    L_BROWN    = HexColor('#fef3c7')
    GRAY       = HexColor('#64748b')
    LGRAY      = HexColor('#f1f5f9')
    BORDER     = HexColor('#e2e8f0')
    WHITE      = HexColor('#ffffff')

    # ── Generate Report ID ──
    report_hash = hashlib.md5(f"{address}{lat}{lng}{datetime.now().isoformat()}".encode()).hexdigest()[:8].upper()
    report_id = f"CWF-{report_hash}"
    now_str = datetime.now(timezone.utc).strftime('%B %d, %Y')

    styles = getSampleStyleSheet()

    # ── Typography System ──
    s_cover_title = ParagraphStyle('CoverTitle', fontName='Helvetica-Bold',
        fontSize=24, textColor=NAVY, leading=30, alignment=TA_CENTER)
    s_cover_sub = ParagraphStyle('CoverSub', fontName='Helvetica',
        fontSize=11, textColor=GRAY, leading=16, alignment=TA_CENTER, spaceAfter=6)
    s_cover_addr = ParagraphStyle('CoverAddr', fontName='Helvetica-Bold',
        fontSize=13, textColor=SLATE, leading=18, alignment=TA_CENTER, spaceAfter=4)
    s_cover_meta = ParagraphStyle('CoverMeta', fontName='Helvetica',
        fontSize=9, textColor=GRAY, leading=13, alignment=TA_CENTER)

    s_section = ParagraphStyle('Section', fontName='Helvetica-Bold',
        fontSize=15, textColor=NAVY, spaceBefore=18, spaceAfter=6, leading=19)
    s_subsection = ParagraphStyle('SubSection', fontName='Helvetica-Bold',
        fontSize=11, textColor=SLATE, spaceBefore=12, spaceAfter=4, leading=14)
    s_body = ParagraphStyle('Body', fontName='Helvetica',
        fontSize=9.5, textColor=SLATE, leading=14, spaceAfter=4)
    s_body_sm = ParagraphStyle('BodySm', fontName='Helvetica',
        fontSize=8.5, textColor=GRAY, leading=12)
    s_interp = ParagraphStyle('Interp', fontName='Helvetica-Oblique',
        fontSize=9.5, textColor=SLATE, leading=14, spaceAfter=6,
        borderColor=LIGHT_BLUE, borderWidth=0, borderPadding=8,
        leftIndent=10, rightIndent=10)
    s_label = ParagraphStyle('Label', fontName='Helvetica',
        fontSize=8, textColor=GRAY, alignment=TA_CENTER)
    s_value = ParagraphStyle('Value', fontName='Helvetica-Bold',
        fontSize=15, textColor=NAVY, alignment=TA_CENTER)
    s_tag = ParagraphStyle('Tag', fontName='Helvetica-Bold',
        fontSize=8.5, textColor=WHITE, alignment=TA_CENTER)
    s_footer = ParagraphStyle('Footer', fontName='Helvetica',
        fontSize=7.5, textColor=GRAY, alignment=TA_CENTER)
    s_disclaimer = ParagraphStyle('Disclaimer', fontName='Helvetica',
        fontSize=7.5, textColor=GRAY, leading=10)

    doc = SimpleDocTemplate(buf, pagesize=letter, topMargin=0.6*inch,
        bottomMargin=0.5*inch, leftMargin=0.65*inch, rightMargin=0.65*inch)
    pw = doc.width  # printable width

    elements = []

    def risk_color_fn(level):
        level = (level or '').lower()
        if level in ('high', 'elevated', 'significant'):
            return RED
        elif level in ('moderate', 'medium'):
            return AMBER
        return GREEN

    def section_divider():
        elements.append(Spacer(1, 8))
        elements.append(HRFlowable(width='100%', thickness=0.5, color=BORDER, spaceAfter=4))

    # ══════════════════════════════════════════════════════════════════════
    # PRECOMPUTE STATS (needed for cover gauge)
    # ══════════════════════════════════════════════════════════════════════

    avg_depth = area_stats.get('avg_depth')
    min_depth = area_stats.get('min_depth')
    max_depth = area_stats.get('max_depth')
    well_count = area_stats.get('total_nearby', 0)
    hazard_count = len(hazards)

    yields = [w['pump_yield_gpm'] for w in wells if w.get('pump_yield_gpm') and w['pump_yield_gpm'] > 0]
    avg_yield = round(sum(yields) / len(yields), 1) if yields else None

    aquifer_set = set()
    for w in wells:
        if w.get('aquifers'):
            for a in str(w['aquifers']).split(','):
                a = a.strip()
                if a and a != '\u2014':
                    aquifer_set.add(a)
    aquifer_list = sorted(aquifer_set)[:5]

    risk_score = _compute_water_risk_score(hazards, radon_info)

    def count_in_radius(items, radius):
        return sum(1 for x in items if x.get('distance_miles', 99) <= radius)

    wells_05 = count_in_radius(wells, 0.5)
    wells_1 = count_in_radius(wells, 1.0)
    wells_3 = count_in_radius(wells, 3.0)
    haz_05 = count_in_radius(hazards, 0.5)
    haz_1 = count_in_radius(hazards, 1.0)
    haz_3 = count_in_radius(hazards, 3.0)

    pfas_list = [h for h in hazards if h.get('_type') == 'pfas']
    mine_list = [h for h in hazards if h.get('_type') == 'mine']
    sf_list = [h for h in hazards if h.get('_type') == 'superfund']
    bf_list = [h for h in hazards if h.get('_type') == 'brownfield']
    tri_list = [h for h in hazards if h.get('_type') == 'tri']
    epa_list = [h for h in hazards if h.get('_type') == 'epa']
    rcra_list = [h for h in hazards if h.get('_type') == 'rcra']

    nearest_haz = hazards[0] if hazards else None
    nearest_haz_dist = f"{nearest_haz['distance_miles']:.1f}" if nearest_haz else None

    def depth_class(d):
        if d is None: return 'Unknown'
        if d < 150: return 'Shallow'
        if d < 400: return 'Moderate'
        return 'Deep'

    def yield_class(y):
        if y is None: return 'Unknown'
        if y >= 15: return 'Strong'
        if y >= 5: return 'Good'
        return 'Low'

    pfas_near = count_in_radius(pfas_list, 3)
    pfas_level = 'High' if count_in_radius(pfas_list, 1) > 0 else ('Moderate' if pfas_near > 2 else ('Low' if pfas_near > 0 else 'None Detected'))

    mines_near = count_in_radius(mine_list, 3)
    mine_level = 'High' if mines_near >= 5 else ('Elevated' if mines_near >= 2 else ('Low' if mines_near > 0 else 'None'))

    cleanup_1mi = count_in_radius([h for h in hazards if h.get('_type') in ('superfund', 'epa', 'eparesponse', 'brownfield')], 1.0)
    cleanup_label = f'{cleanup_1mi} site{"s" if cleanup_1mi != 1 else ""}' if cleanup_1mi > 0 else 'None within 1 mile'

    radon_zone = radon_info.get('radon_zone', '\u2014') if radon_info else '\u2014'
    radon_risk = radon_info.get('risk_level', 'Unknown') if radon_info else 'Unknown'

    # ══════════════════════════════════════════════════════════════════════
    # GAUGE FLOWABLE — semicircular speedometer for risk score
    # ══════════════════════════════════════════════════════════════════════

    from reportlab.platypus import Flowable as _Flowable
    import math as _math

    class RiskGauge(_Flowable):
        """Draws a semicircular speedometer gauge for 0-100 risk score."""
        def __init__(self, score, width=220, height=135):
            _Flowable.__init__(self)
            self.score = max(0, min(100, score))
            self.width = width
            self.height = height

        def draw(self):
            c = self.canv
            cx = self.width / 2
            cy = 30  # center of arc (bottom half)
            r_outer = 95
            r_inner = 65

            # Draw arc segments: green -> yellow -> orange -> red
            arc_colors = [
                (0, 25,   HexColor('#22c55e')),  # green
                (25, 45,  HexColor('#84cc16')),   # lime
                (45, 60,  HexColor('#eab308')),   # yellow
                (60, 75,  HexColor('#f97316')),   # orange
                (75, 100, HexColor('#ef4444')),    # red
            ]

            for lo, hi, color in arc_colors:
                a_start = 180 - (lo / 100) * 180
                a_end = 180 - (hi / 100) * 180
                # Draw thick arc via filled wedge pairs
                c.saveState()
                c.setFillColor(color)
                c.setStrokeColor(color)
                # Outer arc
                p = c.beginPath()
                # Walk the arc in small steps
                steps = max(int((a_start - a_end) / 2), 4)
                for i in range(steps + 1):
                    angle = _math.radians(a_start - (a_start - a_end) * i / steps)
                    x = cx + r_outer * _math.cos(angle)
                    y = cy + r_outer * _math.sin(angle)
                    if i == 0:
                        p.moveTo(x, y)
                    else:
                        p.lineTo(x, y)
                # Inner arc (reverse)
                for i in range(steps + 1):
                    angle = _math.radians(a_end + (a_start - a_end) * i / steps)
                    x = cx + r_inner * _math.cos(angle)
                    y = cy + r_inner * _math.sin(angle)
                    p.lineTo(x, y)
                p.close()
                c.drawPath(p, fill=1, stroke=0)
                c.restoreState()

            # Tick marks at 0, 25, 50, 75, 100
            c.setStrokeColor(HexColor('#0a1628'))
            c.setLineWidth(1.5)
            for val in [0, 25, 50, 75, 100]:
                angle = _math.radians(180 - (val / 100) * 180)
                x1 = cx + (r_outer + 2) * _math.cos(angle)
                y1 = cy + (r_outer + 2) * _math.sin(angle)
                x2 = cx + (r_outer + 8) * _math.cos(angle)
                y2 = cy + (r_outer + 8) * _math.sin(angle)
                c.line(x1, y1, x2, y2)

            # Tick labels
            c.setFont('Helvetica', 7)
            c.setFillColor(HexColor('#64748b'))
            for val, nudge_x, nudge_y in [(0, -8, -2), (25, -6, 6), (50, -4, 8), (75, 0, 6), (100, -2, -2)]:
                angle = _math.radians(180 - (val / 100) * 180)
                x = cx + (r_outer + 16) * _math.cos(angle) + nudge_x
                y = cy + (r_outer + 16) * _math.sin(angle) + nudge_y
                c.drawString(x, y, str(val))

            # Needle
            needle_angle = _math.radians(180 - (self.score / 100) * 180)
            needle_len = r_inner - 8
            nx = cx + needle_len * _math.cos(needle_angle)
            ny = cy + needle_len * _math.sin(needle_angle)

            # Needle body (tapered)
            c.setStrokeColor(HexColor('#0a1628'))
            c.setLineWidth(2.5)
            c.line(cx, cy, nx, ny)

            # Needle hub
            c.setFillColor(HexColor('#0a1628'))
            c.circle(cx, cy, 6, fill=1, stroke=0)
            c.setFillColor(HexColor('#ffffff'))
            c.circle(cx, cy, 3, fill=1, stroke=0)

            # Score text centered below
            rs_color = HexColor('#dc2626') if self.score >= 70 else HexColor('#d97706') if self.score >= 40 else HexColor('#16a34a')
            c.setFont('Helvetica-Bold', 28)
            c.setFillColor(rs_color)
            score_text = str(self.score)
            tw = c.stringWidth(score_text, 'Helvetica-Bold', 28)
            c.drawString(cx - tw / 2, cy - 28, score_text)

            # Label below score
            rs_label = 'HIGH RISK' if self.score >= 70 else 'MODERATE' if self.score >= 40 else 'LOW RISK'
            c.setFont('Helvetica-Bold', 8)
            c.setFillColor(rs_color)
            lw = c.stringWidth(rs_label, 'Helvetica-Bold', 8)
            c.drawString(cx - lw / 2, cy - 40, rs_label)

    # ══════════════════════════════════════════════════════════════════════
    # COVER PAGE
    # ══════════════════════════════════════════════════════════════════════

    elements.append(Spacer(1, 36))
    elements.append(HRFlowable(width='35%', thickness=2, color=BLUE, spaceAfter=14))
    elements.append(Paragraph('Colorado Property', s_cover_title))
    elements.append(Paragraph('Environmental &amp; Water Risk Report', s_cover_title))
    elements.append(Spacer(1, 14))
    elements.append(HRFlowable(width='18%', thickness=0.75, color=BORDER, spaceAfter=12))
    elements.append(Paragraph(address, s_cover_addr))
    elements.append(Paragraph(f'{lat:.5f}, {lng:.5f}', s_cover_meta))
    elements.append(Spacer(1, 22))

    # ── Risk Gauge centered ──
    gauge = RiskGauge(risk_score, width=220, height=135)
    gauge_tbl = Table([[gauge]], colWidths=[220])
    gauge_tbl.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ]))
    # Center the gauge table on the page
    outer_gauge = Table([[gauge_tbl]], colWidths=[pw])
    outer_gauge.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    elements.append(outer_gauge)

    elements.append(Paragraph('WATER RISK SCORE',
        ParagraphStyle('GaugeLabel', fontName='Helvetica',
            fontSize=8, textColor=GRAY, alignment=TA_CENTER, spaceBefore=2)))
    elements.append(Spacer(1, 18))

    # ── Cover quick-stats row ──
    avg_d_disp = f"{int(avg_depth)} ft" if avg_depth else 'N/A'
    yield_disp = f"{avg_yield} GPM" if avg_yield else 'N/A'
    haz_disp = str(hazard_count)
    wells_disp = str(well_count)

    cover_stat_label = ParagraphStyle('CSL', fontName='Helvetica', fontSize=7.5,
        textColor=GRAY, alignment=TA_CENTER, leading=10)
    cover_stat_val = ParagraphStyle('CSV', fontName='Helvetica-Bold', fontSize=13,
        textColor=NAVY, alignment=TA_CENTER, leading=16)

    cs_data = [
        [Paragraph(wells_disp, cover_stat_val),
         Paragraph(avg_d_disp, cover_stat_val),
         Paragraph(yield_disp, cover_stat_val),
         Paragraph(haz_disp, ParagraphStyle('CSVh', parent=cover_stat_val,
             textColor=RED if hazard_count > 5 else AMBER if hazard_count > 0 else GREEN))],
        [Paragraph('Wells Found', cover_stat_label),
         Paragraph('Avg Depth', cover_stat_label),
         Paragraph('Avg Yield', cover_stat_label),
         Paragraph('Hazard Sites', cover_stat_label)],
    ]
    cs_tbl = Table(cs_data, colWidths=[pw/4]*4)
    cs_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), ICE),
        ('BOX', (0, 0), (-1, -1), 0.5, LIGHT_BLUE),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, LIGHT_BLUE),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
        ('TOPPADDING', (0, 1), (-1, 1), 2),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 8),
    ]))
    elements.append(cs_tbl)
    elements.append(Spacer(1, 24))

    elements.append(HRFlowable(width='50%', thickness=0.5, color=BORDER, spaceAfter=8))
    elements.append(Paragraph('Water, Environmental &amp; Subsurface Risk Analysis', s_cover_sub))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f'Generated {now_str}  \u2014  Report {report_id}', s_cover_meta))
    elements.append(Paragraph('coloradowell.com',
        ParagraphStyle('url2', parent=s_cover_meta, textColor=BLUE, spaceBefore=3)))
    elements.append(Spacer(1, 20))

    # ══════════════════════════════════════════════════════════════════════
    # EXECUTIVE SUMMARY
    # ══════════════════════════════════════════════════════════════════════

    elements.append(Paragraph('Executive Summary', s_section))
    section_divider()

    # Property Risk Snapshot grid
    snap_items = [
        ('Water Depth', depth_class(avg_depth), depth_class(avg_depth)),
        ('Yield', yield_class(avg_yield), yield_class(avg_yield)),
        ('PFAS Exposure', pfas_level, pfas_level),
        ('Mine Density', mine_level, mine_level),
        ('Cleanup Sites', cleanup_label, 'High' if cleanup_1mi > 0 else 'None'),
        ('Radon Zone', str(radon_zone), 'High' if radon_zone == 1 else ('Moderate' if radon_zone == 2 else 'Low')),
    ]

    snap_data = [[], []]
    for label, value, rlevel in snap_items:
        rc = risk_color_fn(rlevel)
        snap_data[0].append(Paragraph(label, s_label))
        snap_data[1].append(Paragraph(f'<font color="{rc.hexval()}">{value}</font>',
            ParagraphStyle('sv_' + label, parent=s_body, alignment=TA_CENTER, fontName='Helvetica-Bold', fontSize=10)))

    snap_table = Table(snap_data, colWidths=[pw/6]*6)
    snap_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), ICE),
        ('BOX', (0, 0), (-1, -1), 1, LIGHT_BLUE),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, LIGHT_BLUE),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 3),
        ('TOPPADDING', (0, 1), (-1, 1), 3),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 8),
    ]))
    elements.append(snap_table)
    elements.append(Spacer(1, 12))

    # Interpretation paragraph
    depth_str = f"{int(avg_depth)} ft" if avg_depth else "insufficient data"
    interp_parts = []
    interp_parts.append(
        f'This property shows <b>{depth_class(avg_depth).lower()}</b> drilling depth variability '
        f'based on <b>{well_count} nearby wells</b> within a 5-mile radius'
        + (f' (average depth: {depth_str}).' if avg_depth else '.')
    )

    if sf_list:
        nearest_sf = min(sf_list, key=lambda x: x.get('distance_miles', 99))
        interp_parts.append(
            f' <b>{len(sf_list)} EPA Superfund site{"s" if len(sf_list) != 1 else ""}</b>'
            f' {"are" if len(sf_list) != 1 else "is"} located within 5 miles'
            f' (nearest: {nearest_sf["distance_miles"]:.1f} miles).'
        )
    else:
        interp_parts.append(' No major EPA Superfund sites are located within 5 miles.')

    if mine_list:
        nearest_mine = min(mine_list, key=lambda x: x.get('distance_miles', 99))
        interp_parts.append(
            f' Historical mining activity is present \u2014 <b>{len(mine_list)} mine site{"s" if len(mine_list) != 1 else ""}</b>'
            f' within {nearest_mine["distance_miles"]:.1f} miles.'
        )

    if pfas_list:
        nearest_pfas = min(pfas_list, key=lambda x: x.get('distance_miles', 99))
        interp_parts.append(
            f' <b>{len(pfas_list)} PFAS detection{"s" if len(pfas_list) != 1 else ""}</b>'
            f' recorded within 5 miles (nearest: {nearest_pfas["distance_miles"]:.1f} mi).'
            f' PFAS are persistent contaminants that do not break down in groundwater.'
        )

    if radon_info and radon_zone in (1, 2):
        interp_parts.append(
            f' The property falls in <b>EPA Radon Zone {radon_zone}</b>'
            f' ({radon_risk} risk) \u2014 radon testing is recommended.'
        )

    elements.append(Paragraph(''.join(interp_parts), s_interp))

    # ══════════════════════════════════════════════════════════════════════
    # WATER INTELLIGENCE
    # ══════════════════════════════════════════════════════════════════════

    elements.append(Paragraph('Water Intelligence', s_section))
    section_divider()

    avg_d_str = f"{int(avg_depth)} ft" if avg_depth else 'N/A'
    min_d_str = f"{int(min_depth)} ft" if min_depth else '\u2014'
    max_d_str = f"{int(max_depth)} ft" if max_depth else '\u2014'
    yield_str = f"{avg_yield} GPM" if avg_yield else 'N/A'
    cost_lo = f"${int(avg_depth * 50):,}" if avg_depth else '\u2014'
    cost_hi = f"${int(avg_depth * 75):,}" if avg_depth else '\u2014'

    water_data = [
        [Paragraph('Avg Depth', s_label), Paragraph('Depth Range', s_label),
         Paragraph('Avg Yield', s_label), Paragraph('Est. Cost', s_label),
         Paragraph('Wells Found', s_label)],
        [Paragraph(avg_d_str, ParagraphStyle('wv1', parent=s_value, fontSize=13)),
         Paragraph(f'{min_d_str} \u2013 {max_d_str}', ParagraphStyle('wv2', parent=s_body, alignment=TA_CENTER, fontName='Helvetica-Bold', fontSize=9)),
         Paragraph(yield_str, ParagraphStyle('wv3', parent=s_value, fontSize=13)),
         Paragraph(f'{cost_lo} \u2013 {cost_hi}', ParagraphStyle('wv4', parent=s_body, alignment=TA_CENTER, fontName='Helvetica-Bold', fontSize=9)),
         Paragraph(str(well_count), ParagraphStyle('wv5', parent=s_value, fontSize=13))],
    ]
    wt = Table(water_data, colWidths=[pw/5]*5)
    wt.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), ICE),
        ('BOX', (0, 0), (-1, -1), 0.5, BORDER),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, BORDER),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, 0), 6),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
        ('TOPPADDING', (0, 1), (-1, 1), 2),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 8),
    ]))
    elements.append(wt)
    elements.append(Spacer(1, 6))

    if aquifer_list:
        elements.append(Paragraph(f'<b>Aquifer types in area:</b> {", ".join(aquifer_list)}', s_body))
    elements.append(Spacer(1, 8))

    # Depth distribution
    if wells:
        shallow = sum(1 for w in wells if w.get('depth_total') and w['depth_total'] < 200)
        medium = sum(1 for w in wells if w.get('depth_total') and 200 <= w['depth_total'] < 500)
        deep_ = sum(1 for w in wells if w.get('depth_total') and w['depth_total'] >= 500)
        total_w = len([w for w in wells if w.get('depth_total')])
        if total_w > 0:
            elements.append(Paragraph('Well Depth Distribution', s_subsection))
            dd = [
                ['Depth Range', 'Count', '%'],
                ['Under 200 ft (shallow)', str(shallow), f"{shallow/total_w*100:.0f}%"],
                ['200\u2013500 ft (moderate)', str(medium), f"{medium/total_w*100:.0f}%"],
                ['Over 500 ft (deep)', str(deep_), f"{deep_/total_w*100:.0f}%"],
            ]
            dd_tbl = Table(dd, colWidths=[2.5*inch, 0.8*inch, 0.8*inch])
            dd_tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), NAVY),
                ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8.5),
                ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, LGRAY]),
                ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            elements.append(dd_tbl)
            elements.append(Spacer(1, 8))

    # Nearby wells table (top 15)
    if wells:
        elements.append(Paragraph('Nearby Well Records', s_subsection))
        elements.append(Paragraph('Official state well permit records, sorted by distance.', s_body_sm))
        elements.append(Spacer(1, 3))
        wh = ['Dist.', 'Depth', 'Water Lvl', 'Yield', 'Aquifer', 'Year']
        wd = [wh]
        for w in wells[:15]:
            dist = f"{w.get('distance_miles', 0):.1f} mi" if w.get('distance_miles') else '\u2014'
            depth = f"{int(w['depth_total'])} ft" if w.get('depth_total') else '\u2014'
            swl = f"{int(w['static_water_level'])} ft" if w.get('static_water_level') else '\u2014'
            yld = f"{w['pump_yield_gpm']} GPM" if w.get('pump_yield_gpm') else '\u2014'
            aq = (w.get('aquifers') or '\u2014')[:18]
            yr = str(w.get('date_completed', ''))[:4] if w.get('date_completed') else '\u2014'
            wd.append([dist, depth, swl, yld, aq, yr])

        well_tbl = Table(wd, colWidths=[0.65*inch, 0.65*inch, 0.7*inch, 0.65*inch, 1.6*inch, 0.5*inch], repeatRows=1)
        well_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), SLATE),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 7.5),
            ('FONTSIZE', (0, 1), (-1, -1), 7.5),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, LGRAY]),
            ('GRID', (0, 0), (-1, -1), 0.25, BORDER),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(well_tbl)

    # ══════════════════════════════════════════════════════════════════════
    # DRILLING COST ESTIMATE
    # ══════════════════════════════════════════════════════════════════════

    elements.append(Paragraph('Drilling Cost Estimate', s_section))
    section_divider()

    if isinstance(avg_depth, (int, float)) and avg_depth > 0:
        elements.append(Paragraph(
            f'Based on <b>{well_count} nearby wells</b> with depth data, '
            f'using Colorado\'s typical drilling rate of <b>$50\u2013$75 per foot</b>:', s_body))
        elements.append(Spacer(1, 6))

        cd = [
            ['Scenario', 'Est. Depth', 'Cost @ $50/ft', 'Cost @ $75/ft'],
            ['Shallow (minimum)', f"{int(min_depth)} ft", f"${int(min_depth*50):,}", f"${int(min_depth*75):,}"],
            ['Average for area', f"{int(avg_depth)} ft", f"${int(avg_depth*50):,}", f"${int(avg_depth*75):,}"],
            ['Deep (maximum)', f"{int(max_depth)} ft", f"${int(max_depth*50):,}", f"${int(max_depth*75):,}"],
        ]
        cost_tbl = Table(cd, colWidths=[1.4*inch, 1.1*inch, 1.2*inch, 1.2*inch])
        cost_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), BLUE),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, ICE]),
            ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(cost_tbl)
        elements.append(Spacer(1, 4))
        elements.append(Paragraph(
            '<i>Note: Actual costs vary by driller, terrain, rock type, casing, and permit requirements. '
            'Pump, pressure tank, and hookup add $3,000\u2013$8,000.</i>', s_body_sm))
    else:
        elements.append(Paragraph('Insufficient well depth data to generate a cost estimate for this area.', s_body))

    # ══════════════════════════════════════════════════════════════════════
    # ENVIRONMENTAL EXPOSURE
    # ══════════════════════════════════════════════════════════════════════

    elements.append(Paragraph('Environmental Exposure Analysis', s_section))
    section_divider()

    # Proximity Radius Summary
    elements.append(Paragraph('Proximity Radius Summary', s_subsection))

    type_groups = [
        ('EPA / Cleanup', [h for h in hazards if h.get('_type') in ('epa', 'eparesponse', 'brownfield')]),
        ('Superfund (NPL)', sf_list),
        ('PFAS Detections', pfas_list),
        ('TRI Facilities', tri_list),
        ('Mine Sites', mine_list),
        ('RCRA Haz. Waste', rcra_list),
    ]

    prox_header = ['Hazard Type', '< 0.5 mi', '< 1 mi', '< 3 mi', '< 5 mi', 'Nearest']
    prox_data = [prox_header]
    for label, items in type_groups:
        c05 = count_in_radius(items, 0.5)
        c1 = count_in_radius(items, 1.0)
        c3 = count_in_radius(items, 3.0)
        c5 = len(items)
        nearest = f"{min(items, key=lambda x: x.get('distance_miles', 99))['distance_miles']:.1f} mi" if items else '\u2014'
        prox_data.append([label, str(c05) if c05 else '\u2014', str(c1) if c1 else '\u2014',
                         str(c3) if c3 else '\u2014', str(c5) if c5 else '\u2014', nearest])

    prox_data.append([
        'ALL HAZARDS',
        str(haz_05) if haz_05 else '\u2014', str(haz_1) if haz_1 else '\u2014',
        str(haz_3) if haz_3 else '\u2014', str(hazard_count) if hazard_count else '\u2014',
        f'{nearest_haz_dist} mi' if nearest_haz_dist else '\u2014'
    ])

    prox_tbl = Table(prox_data, colWidths=[1.3*inch, 0.7*inch, 0.7*inch, 0.7*inch, 0.7*inch, 0.8*inch], repeatRows=1)
    prox_styles = [
        ('BACKGROUND', (0, 0), (-1, 0), NAVY),
        ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -2), [WHITE, LGRAY]),
        ('BACKGROUND', (0, -1), (-1, -1), L_RED),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('GRID', (0, 0), (-1, -1), 0.25, BORDER),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]
    for row_idx in range(1, len(prox_data)):
        for col_idx in [1, 2]:
            val = prox_data[row_idx][col_idx]
            if val != '\u2014' and val != '0':
                prox_styles.append(('TEXTCOLOR', (col_idx, row_idx), (col_idx, row_idx), RED))
                prox_styles.append(('FONTNAME', (col_idx, row_idx), (col_idx, row_idx), 'Helvetica-Bold'))
    prox_tbl.setStyle(TableStyle(prox_styles))
    elements.append(prox_tbl)
    elements.append(Spacer(1, 10))

    # Hazard subsection helper
    def hazard_subsection(title, title_color, items, detail_fn, interp_text, bg_color):
        if not items:
            return
        elements.append(Paragraph(title, ParagraphStyle('HS_' + title[:8], parent=s_subsection, textColor=title_color)))
        elements.append(Paragraph(interp_text, s_interp))
        rows = [['Site Name', 'Distance', 'Detail']]
        for h in items[:8]:
            name = (h.get('site_name') or h.get('facility_name') or '\u2014')[:35]
            dist = f"{h.get('distance_miles', 0):.1f} mi" if h.get('distance_miles') else '\u2014'
            detail = detail_fn(h)
            rows.append([name, dist, detail])
        tbl = Table(rows, colWidths=[2.4*inch, 0.7*inch, 1.8*inch], repeatRows=1)
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), title_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7.5),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, bg_color]),
            ('GRID', (0, 0), (-1, -1), 0.25, BORDER),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(tbl)
        elements.append(Spacer(1, 6))

    # EPA & Cleanup
    epa_bf = [h for h in hazards if h.get('_type') in ('epa', 'eparesponse', 'brownfield')]
    if epa_bf:
        nearest_epa = min(epa_bf, key=lambda x: x.get('distance_miles', 99))
        interp = (
            f'{len(epa_bf)} EPA-tracked cleanup or brownfield site{"s" if len(epa_bf) != 1 else ""} '
            f'within 5 miles. Nearest is {nearest_epa["distance_miles"]:.1f} miles from the property.'
            + (' No sites within 1 mile.' if count_in_radius(epa_bf, 1) == 0 else
               f' {count_in_radius(epa_bf, 1)} site{"s" if count_in_radius(epa_bf, 1) != 1 else ""} within 1 mile \u2014 elevated groundwater risk.')
        )
        hazard_subsection('EPA &amp; Cleanup Sites', RED, epa_bf,
            lambda h: (h.get('npl_status') or h.get('category') or '\u2014')[:25], interp, L_RED)

    # Superfund
    if sf_list:
        nearest_sf = min(sf_list, key=lambda x: x.get('distance_miles', 99))
        interp = (
            f'{len(sf_list)} National Priorities List (Superfund) site{"s" if len(sf_list) != 1 else ""} '
            f'within 5 miles (nearest: {nearest_sf["distance_miles"]:.1f} mi). '
            f'Superfund sites represent the most serious contamination threats and can affect '
            f'groundwater quality for decades.'
        )
        hazard_subsection('Superfund Sites', HexColor('#b91c1c'), sf_list,
            lambda h: (h.get('npl_status') or '\u2014')[:25], interp, L_RED)

    # PFAS
    if pfas_list:
        nearest_pfas = min(pfas_list, key=lambda x: x.get('distance_miles', 99))
        interp = (
            f'{len(pfas_list)} PFAS "forever chemical" detection{"s" if len(pfas_list) != 1 else ""} '
            f'within 5 miles (nearest: {nearest_pfas["distance_miles"]:.1f} mi). '
            f'PFAS are persistent chemicals linked to cancer, thyroid disease, and immune disorders. '
            f'They do not break down in soil or groundwater.'
        )
        hazard_subsection('PFAS "Forever Chemical" Detections', PURPLE, pfas_list,
            lambda h: (h.get('contaminant') or h.get('source_type') or 'PFAS')[:25], interp, L_PURPLE)

    # TRI
    if tri_list:
        nearest_tri = min(tri_list, key=lambda x: x.get('distance_miles', 99))
        interp = (
            f'{len(tri_list)} Toxic Release Inventory facilit{"ies" if len(tri_list) != 1 else "y"} '
            f'within 5 miles (nearest: {nearest_tri["distance_miles"]:.1f} mi). '
            f'TRI facilities report annual chemical releases to air, water, and land.'
        )
        hazard_subsection('TRI Toxic Release Facilities', AMBER, tri_list,
            lambda h: (h.get('category') or '\u2014')[:25], interp, L_AMBER)

    # Mines
    if mine_list:
        nearest_mine = min(mine_list, key=lambda x: x.get('distance_miles', 99))
        interp = (
            f'{len(mine_list)} mine site{"s" if len(mine_list) != 1 else ""} '
            f'within 5 miles (nearest: {nearest_mine["distance_miles"]:.1f} mi). '
            f'Abandoned mines can leach arsenic, lead, and cadmium into groundwater. '
            + ('No documented acid mine drainage reports tied to this parcel.'
               if count_in_radius(mine_list, 1) == 0 else
               f'{count_in_radius(mine_list, 1)} mine{"s" if count_in_radius(mine_list, 1) != 1 else ""} within 1 mile \u2014 groundwater testing recommended.')
        )
        hazard_subsection('Mine Sites', HexColor('#78350f'), mine_list,
            lambda h: (h.get('mine_type') or h.get('county') or '\u2014')[:25], interp, L_BROWN)

    # RCRA
    if rcra_list:
        nearest_rcra = min(rcra_list, key=lambda x: x.get('distance_miles', 99))
        interp = (
            f'{len(rcra_list)} RCRA hazardous waste facilit{"ies" if len(rcra_list) != 1 else "y"} '
            f'within 5 miles (nearest: {nearest_rcra["distance_miles"]:.1f} mi). '
            f'RCRA facilities handle, treat, or dispose of hazardous waste under federal regulation.'
        )
        hazard_subsection('RCRA Hazardous Waste', HexColor('#c2410c'), rcra_list,
            lambda h: (h.get('category') or '\u2014')[:25], interp, L_AMBER)

    # No hazards
    if not hazards:
        elements.append(Spacer(1, 8))
        elements.append(Paragraph(
            '<font color="#16a34a"><b>No contamination sites found within 5 miles</b></font> \u2014 this property '
            'shows low environmental exposure risk based on available EPA, state, and federal databases.',
            s_body))

    # Radon
    if radon_info:
        elements.append(Paragraph('Radon Risk', s_subsection))
        zone = radon_info.get('radon_zone', '\u2014')
        rr = radon_info.get('risk_level', 'Unknown')
        rc_name = radon_info.get('county', 'Unknown')
        pred = radon_info.get('predicted_level', '')
        z_color = RED if zone == 1 else AMBER if zone == 2 else GREEN
        radon_data = [[
            Paragraph(f'<b>Zone {zone}</b>', ParagraphStyle('rz', parent=s_body, textColor=z_color, fontSize=14, fontName='Helvetica-Bold', alignment=TA_CENTER)),
            Paragraph(f'<b>{rc_name} County</b><br/>Risk: {rr}' + (f' | {pred}' if pred else ''), s_body),
            Paragraph('EPA recommends radon testing for all homes. Zone 1 = highest risk (>4 pCi/L predicted).', s_body_sm),
        ]]
        radon_tbl = Table(radon_data, colWidths=[1*inch, 2*inch, 2.5*inch])
        radon_tbl.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.5, BORDER),
            ('BACKGROUND', (0, 0), (0, 0), LGRAY),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ]))
        elements.append(radon_tbl)

    # ══════════════════════════════════════════════════════════════════════
    # DATA SOURCES & DISCLAIMER
    # ══════════════════════════════════════════════════════════════════════

    elements.append(Spacer(1, 20))
    elements.append(HRFlowable(width='100%', thickness=0.5, color=BORDER, spaceAfter=8))
    elements.append(Paragraph('Data Sources &amp; Disclaimer', ParagraphStyle('DiscHead',
        parent=s_subsection, fontSize=9, textColor=GRAY)))
    elements.append(Spacer(1, 4))

    sources = [
        'Colorado Division of Water Resources \u2014 Well Permit Database',
        'U.S. EPA Envirofacts \u2014 SEMS, Superfund, TRI, RCRA, Brownfields',
        'U.S. EPA \u2014 PFAS Analytic Tools &amp; Detection Data',
        'Colorado Division of Reclamation, Mining &amp; Safety \u2014 Mine Site Inventory',
        'U.S. EPA \u2014 Radon Zone Classification (county-level)',
        'USGS National Water Information System',
    ]
    for src in sources:
        elements.append(Paragraph(f'  \u2022  {src}', s_disclaimer))

    elements.append(Spacer(1, 8))
    elements.append(Paragraph(
        '<b>Disclaimer:</b> This report is generated from publicly available federal and state databases '
        'and is provided for informational purposes only. It does not constitute professional geological, '
        'environmental, or engineering advice. Well depths, water quality, and contamination risks vary '
        'by precise location and geology. Consult a licensed well driller and environmental professional '
        'for site-specific assessments before making land purchase or drilling decisions.',
        s_disclaimer))
    elements.append(Spacer(1, 12))
    elements.append(HRFlowable(width='30%', thickness=0.5, color=BORDER, spaceAfter=6))
    elements.append(Paragraph(
        f'\u00a9 {datetime.now().year} Colorado Well Finder  \u2014  coloradowell.com  \u2014  Report {report_id}',
        ParagraphStyle('FinalFooter', parent=s_footer, textColor=BLUE)))

    doc.build(elements)
    buf.seek(0)
    return buf


@app.route('/api/report', methods=['POST'])
def generate_report():
    """Generate a PDF well report for an address.
    
    POST body: { email, address, lat, lng }
    Returns: PDF file download
    """
    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    address = (data.get('address') or '').strip()
    
    try:
        lat = float(data.get('lat'))
        lng = float(data.get('lng'))
    except (TypeError, ValueError):
        return jsonify({"error": "lat and lng required"}), 400

    if not email:
        return jsonify({"error": "Email required for report generation"}), 400
    if not address:
        return jsonify({"error": "Address required"}), 400

    # Verify access
    access = _verify_report_access(email)
    if not access:
        return jsonify({"error": "No valid purchase found for this email", "needsPurchase": True}), 403

    if access.get('reports_remaining', 0) <= 0 and access['tier'] != 'unlimited':
        return jsonify({
            "error": "All report credits used",
            "tier": access['tier'],
            "reports_used": access.get('reports_used', 0),
            "needsUpgrade": True,
        }), 403

    # Query nearby wells (5 mile radius)
    radius_miles = 5
    lat_range = radius_miles / 69.0
    lng_range = radius_miles / (69.0 * max(math.cos(math.radians(lat)), 0.01))
    min_lat, max_lat = lat - lat_range, lat + lat_range
    min_lng, max_lng = lng - lng_range, lng + lng_range

    conn = get_db()
    try:
        cur = conn.cursor()

        # Get nearby wells with distance
        cur.execute("""
            SELECT receipt, permit, latitude, longitude, depth_total,
                   static_water_level, pump_yield_gpm, aquifers, county,
                   owner_name, uses, status, driller_name, date_completed,
                   address, city,
                   SQRT(POW((latitude - %s) * 69.0, 2) +
                        POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                   ) AS distance_miles
            FROM wells
            WHERE latitude BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
              AND depth_total > 0
              AND latitude IS NOT NULL
            ORDER BY distance_miles
            LIMIT 50
        """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
        wells = [dict(r) for r in cur.fetchall()]

        # Get nearby EPA sites
        cur.execute("""
            SELECT site_name, latitude, longitude, category, npl_status,
                   SQRT(POW((latitude - %s) * 69.0, 2) +
                        POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                   ) AS distance_miles
            FROM epa_sites
            WHERE latitude BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
            ORDER BY distance_miles
            LIMIT 20
        """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
        epa = [dict(r) for r in cur.fetchall()]
        for e in epa:
            e['_type'] = 'epa'

        # Get nearby Superfund sites
        cur.execute("""
            SELECT site_name, latitude, longitude, npl_status,
                   SQRT(POW((latitude - %s) * 69.0, 2) +
                        POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                   ) AS distance_miles
            FROM superfund_sites
            WHERE latitude BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
            ORDER BY distance_miles
            LIMIT 10
        """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
        superfund = [dict(r) for r in cur.fetchall()]
        for s in superfund:
            s['_type'] = 'superfund'

        # Get nearby TRI (Toxic Release Inventory) facilities
        tri = []
        try:
            cur.execute("""
                SELECT facility_name AS site_name, latitude, longitude, parent_company, city, county, closed,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM tri_facilities
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 15
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            tri = [dict(r) for r in cur.fetchall()]
            for t in tri:
                t['_type'] = 'tri'
                t['category'] = f"TRI — {t.get('parent_company') or t.get('city') or ''}"
        except Exception:
            pass  # table may not exist yet

        # Get nearby Brownfields
        brownfields = []
        try:
            cur.execute("""
                SELECT site_name, latitude, longitude, city, county, cleanup_ind, assess_ind,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM brownfield_sites
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 15
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            brownfields = [dict(r) for r in cur.fetchall()]
            for b in brownfields:
                b['_type'] = 'brownfield'
                b['category'] = 'Brownfield Site'
        except Exception:
            pass

        # Get nearby PFAS sites
        pfas = []
        try:
            cur.execute("""
                SELECT site_name, latitude, longitude, source_type, city, county, contaminant,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM pfas_sites
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 15
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            pfas = [dict(r) for r in cur.fetchall()]
            for p in pfas:
                p['_type'] = 'pfas'
                p['category'] = f"PFAS — {p.get('source_type') or ''}"
        except Exception:
            pass

        # Get nearby mines
        mines = []
        try:
            cur.execute("""
                SELECT site_name, mine_type, latitude, longitude, county,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM mine_sites
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 10
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            mines = [dict(r) for r in cur.fetchall()]
            for m in mines:
                m['_type'] = 'mine'
                m['category'] = f"Mine — {m.get('mine_type') or ''}"
        except Exception:
            pass

        # Get nearby RCRA hazardous waste facilities
        rcra = []
        try:
            cur.execute("""
                SELECT site_name, latitude, longitude, city, county,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM rcra_sites
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 10
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            rcra = [dict(r) for r in cur.fetchall()]
            for r in rcra:
                r['_type'] = 'rcra'
                r['category'] = 'RCRA Haz. Waste'
        except Exception:
            pass

        # Get nearby Federal Facilities
        fedfac = []
        try:
            cur.execute("""
                SELECT site_name, latitude, longitude, city, county,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM federal_facilities
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 10
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            fedfac = [dict(r) for r in cur.fetchall()]
            for f in fedfac:
                f['_type'] = 'fedfac'
                f['category'] = 'Federal Facility'
        except Exception:
            pass

        # Get nearby EPA Response sites
        eparesponse = []
        try:
            cur.execute("""
                SELECT site_name, latitude, longitude, city, county,
                       SQRT(POW((latitude - %s) * 69.0, 2) +
                            POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2)
                       ) AS distance_miles
                FROM epa_response_sites
                WHERE latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY distance_miles
                LIMIT 10
            """, (lat, lng, lat, min_lat, max_lat, min_lng, max_lng))
            eparesponse = [dict(r) for r in cur.fetchall()]
            for e in eparesponse:
                e['_type'] = 'eparesponse'
                e['category'] = 'EPA Response'
        except Exception:
            pass

        # Get radon zone for the area
        radon_info = None
        try:
            cur.execute("""
                SELECT county, radon_zone, predicted_level, risk_level
                FROM radon_zones
                WHERE latitude IS NOT NULL
                ORDER BY SQRT(POW((latitude - %s) * 69.0, 2) +
                             POW((longitude - %s) * 69.0 * COS(RADIANS(%s)), 2))
                LIMIT 1
            """, (lat, lng, lat))
            row = cur.fetchone()
            if row:
                radon_info = dict(row)
        except Exception:
            pass

        hazards = sorted(epa + superfund + tri + brownfields + pfas + mines + rcra + fedfac + eparesponse, key=lambda x: x.get('distance_miles', 99))

    finally:
        conn.close()

    # Calculate area stats
    depths = [w['depth_total'] for w in wells if w.get('depth_total')]
    area_stats = {
        'total_nearby': len(wells),
        'avg_depth': round(sum(depths) / len(depths), 1) if depths else None,
        'min_depth': min(depths) if depths else None,
        'max_depth': max(depths) if depths else None,
        'hazard_count': len(hazards),
    }

    # Generate PDF
    pdf_buf = _generate_report_pdf(address, lat, lng, wells, hazards, area_stats, radon_info=radon_info)

    # Increment report usage (non-blocking)
    if access.get('customer_id') and access['tier'] != 'unlimited':
        _increment_reports_used(access['customer_id'], access.get('reports_used', 0))

    # Create filename from address
    safe_addr = ''.join(c if c.isalnum() or c in ' -' else '' for c in address)[:50].strip()
    filename = f"Well_Report_{safe_addr}_{datetime.now().strftime('%Y%m%d')}.pdf"

    return send_file(
        pdf_buf,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=filename,
    )


@app.route('/api/report/status', methods=['POST'])
def report_status():
    """Check how many reports a user has remaining.
    
    POST body: { email }
    Returns: { tier, reports_used, reports_remaining }
    """
    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()

    if not email:
        return jsonify({"error": "Email required"}), 400

    access = _verify_report_access(email)
    if not access:
        return jsonify({"hasAccess": False})

    return jsonify({
        "hasAccess": True,
        "tier": access['tier'],
        "reports_used": access.get('reports_used', 0),
        "reports_remaining": access.get('reports_remaining', 0),
    })


# ─── Health Check ────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "service": "Well Finder - Multi-State Wells & Environmental Data API",
        "states": ["CO", "AZ", "NM", "WY"],
        "wells": "1.3M+",
        "overlay_layers": list(OVERLAY_LAYERS.keys()),
        "endpoints": [
            "GET /api/wells/bbox?minLat=&maxLat=&minLng=&maxLng=&state=CO",
            "GET /api/wells/clusters?minLat=&maxLat=&minLng=&maxLng=",
            "GET /api/wells/search?q=&county=&state=CO,AZ,NM,WY",
            "GET /api/wells/<receipt>",
            "GET /api/wells/stats?state=NM",
            "GET /api/wells/counties?state=CO",
            "GET /api/overlay/<layer>/bbox?minLat=&maxLat=&minLng=&maxLng=",
            "GET /api/overlay/<layer>/<id>",
            "GET /api/overlay/stats",
            "GET /api/nearby?lat=&lng=&radius=5&layers=contamination,dams",
            "POST /api/verify {email}",
            "POST /api/report {email, address, lat, lng}",
            "POST /api/report/status {email}"
        ],
        "overlay_info": {k: v['label'] for k, v in OVERLAY_LAYERS.items()},
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
