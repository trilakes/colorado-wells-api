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
    'plink_1T215ZCSH1YJHcuWVktIj4RW',   # Single $19
    'plink_1T215fCSH1YJHcuWd6wiCnrI',   # Pack $49
    'plink_1T215kCSH1YJHcuWxo8TKhcw',   # Unlimited $97
]

TIER_REPORT_LIMITS = {
    'single': 1,
    'pack': 10,
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


def _generate_report_pdf(address, lat, lng, wells, hazards, area_stats, radon_info=None):
    """Generate a professional PDF well report using ReportLab."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.colors import HexColor
    from reportlab.lib.units import inch
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                     TableStyle, HRFlowable, KeepTogether)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, topMargin=0.5*inch,
                            bottomMargin=0.6*inch, leftMargin=0.7*inch, rightMargin=0.7*inch)

    # Colors
    NAVY = HexColor('#0a1628')
    BLUE = HexColor('#00b4d8')
    DARK_BLUE = HexColor('#0096c7')
    LIGHT_BG = HexColor('#f0f9ff')
    GREEN = HexColor('#22c55e')
    RED = HexColor('#ef4444')
    AMBER = HexColor('#f59e0b')
    GRAY = HexColor('#64748b')
    DARK = HexColor('#1e293b')
    WHITE = HexColor('#ffffff')

    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle('ReportTitle', parent=styles['Title'],
        fontSize=22, textColor=NAVY, spaceAfter=4, fontName='Helvetica-Bold')
    subtitle_style = ParagraphStyle('ReportSubtitle', parent=styles['Normal'],
        fontSize=11, textColor=GRAY, spaceAfter=20)
    heading_style = ParagraphStyle('SectionHead', parent=styles['Heading2'],
        fontSize=14, textColor=NAVY, spaceBefore=16, spaceAfter=8,
        fontName='Helvetica-Bold', borderPadding=(0, 0, 4, 0))
    body_style = ParagraphStyle('Body', parent=styles['Normal'],
        fontSize=10, textColor=DARK, leading=14)
    small_style = ParagraphStyle('Small', parent=styles['Normal'],
        fontSize=8.5, textColor=GRAY, leading=11)
    stat_label = ParagraphStyle('StatLabel', parent=styles['Normal'],
        fontSize=9, textColor=GRAY)
    stat_value = ParagraphStyle('StatValue', parent=styles['Normal'],
        fontSize=16, textColor=NAVY, fontName='Helvetica-Bold')
    center_style = ParagraphStyle('Center', parent=styles['Normal'],
        fontSize=10, textColor=DARK, alignment=TA_CENTER)

    elements = []

    # ── Header ──
    now = datetime.now(timezone.utc).strftime('%B %d, %Y')
    elements.append(Paragraph('Colorado Well Finder', title_style))
    elements.append(Paragraph('PROPERTY WELL REPORT', ParagraphStyle('Badge',
        parent=styles['Normal'], fontSize=10, textColor=BLUE,
        fontName='Helvetica-Bold', spaceAfter=2, letterSpacing=2)))
    elements.append(Spacer(1, 4))
    elements.append(HRFlowable(width='100%', thickness=2, color=BLUE, spaceAfter=12))
    elements.append(Paragraph(f'<b>Property:</b> {address}', body_style))
    elements.append(Paragraph(f'<b>Coordinates:</b> {lat:.6f}, {lng:.6f}', body_style))
    elements.append(Paragraph(f'<b>Generated:</b> {now}', body_style))
    elements.append(Spacer(1, 16))

    # ── Key Stats Summary ──
    avg_depth = area_stats.get('avg_depth', 'N/A')
    well_count = area_stats.get('total_nearby', 0)
    min_depth = area_stats.get('min_depth', 'N/A')
    max_depth = area_stats.get('max_depth', 'N/A')
    hazard_count = area_stats.get('hazard_count', 0)

    cost_low = f"${int(float(avg_depth) * 50):,}" if isinstance(avg_depth, (int, float)) else 'N/A'
    cost_high = f"${int(float(avg_depth) * 75):,}" if isinstance(avg_depth, (int, float)) else 'N/A'
    avg_depth_str = f"{int(avg_depth)} ft" if isinstance(avg_depth, (int, float)) else 'N/A'

    stat_data = [
        [Paragraph('Nearby Wells', stat_label), Paragraph('Avg Depth', stat_label),
         Paragraph('Est. Cost', stat_label), Paragraph('Hazards', stat_label)],
        [Paragraph(str(well_count), stat_value),
         Paragraph(avg_depth_str, stat_value),
         Paragraph(f"{cost_low}–{cost_high}" if cost_low != 'N/A' else 'N/A', stat_value),
         Paragraph(str(hazard_count), ParagraphStyle('HazVal', parent=stat_value,
             textColor=RED if hazard_count > 0 else GREEN))],
    ]
    stat_table = Table(stat_data, colWidths=[doc.width/4]*4)
    stat_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), LIGHT_BG),
        ('BOX', (0, 0), (-1, -1), 1, HexColor('#e0f2fe')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
        ('TOPPADDING', (0, 1), (-1, 1), 2),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 10),
        ('ROUNDEDCORNERS', [6, 6, 6, 6]),
    ]))
    elements.append(stat_table)
    elements.append(Spacer(1, 16))

    # ── Nearby Wells Table ──
    elements.append(Paragraph('Nearby Well Records', heading_style))
    elements.append(Paragraph(
        f'Wells within 5 miles of the property, sorted by distance. '
        f'Based on official state well permit records.', small_style))
    elements.append(Spacer(1, 6))

    if wells:
        header = ['Distance', 'Depth', 'Water Lvl', 'Yield', 'Aquifer', 'County', 'Year']
        table_data = [header]
        for w in wells[:25]:  # Cap at 25 wells
            dist = f"{w.get('distance_miles', 0):.1f} mi" if w.get('distance_miles') else '—'
            depth = f"{int(w['depth_total'])} ft" if w.get('depth_total') else '—'
            swl = f"{int(w['static_water_level'])} ft" if w.get('static_water_level') else '—'
            yld = f"{w['pump_yield_gpm']} GPM" if w.get('pump_yield_gpm') else '—'
            aq = (w.get('aquifers') or '—')[:20]
            county = (w.get('county') or '—')[:15]
            year = ''
            if w.get('date_completed'):
                try:
                    year = str(w['date_completed'])[:4]
                except Exception:
                    year = '—'
            table_data.append([dist, depth, swl, yld, aq, county, year or '—'])

        col_widths = [0.7*inch, 0.7*inch, 0.7*inch, 0.7*inch, 1.5*inch, 1.1*inch, 0.6*inch]
        well_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        well_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), NAVY),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8.5),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, LIGHT_BG]),
            ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#cbd5e1')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(well_table)
    else:
        elements.append(Paragraph('No well records found within 5 miles of this property.', body_style))

    elements.append(Spacer(1, 16))

    # ── Drilling Cost Estimate ──
    elements.append(Paragraph('Drilling Cost Estimate', heading_style))
    if isinstance(avg_depth, (int, float)) and avg_depth > 0:
        elements.append(Paragraph(
            f'Based on <b>{well_count} nearby wells</b> with an average depth of '
            f'<b>{int(avg_depth)} ft</b>, using Colorado\'s typical rate of $50–$75 per foot:', body_style))
        elements.append(Spacer(1, 6))

        cost_data = [
            ['Scenario', 'Depth', 'Cost @ $50/ft', 'Cost @ $75/ft'],
            ['If shallow (minimum)', f"{int(min_depth)} ft",
             f"${int(float(min_depth)*50):,}", f"${int(float(min_depth)*75):,}"],
            ['Average for area', f"{int(avg_depth)} ft",
             f"${int(float(avg_depth)*50):,}", f"${int(float(avg_depth)*75):,}"],
            ['If deep (maximum)', f"{int(max_depth)} ft",
             f"${int(float(max_depth)*50):,}", f"${int(float(max_depth)*75):,}"],
        ]
        cost_table = Table(cost_data, colWidths=[1.5*inch, 1.2*inch, 1.3*inch, 1.3*inch])
        cost_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), DARK_BLUE),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, LIGHT_BG]),
            ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#cbd5e1')),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(cost_table)
    else:
        elements.append(Paragraph(
            'Insufficient depth data in the area to generate a cost estimate.', body_style))

    elements.append(Spacer(1, 16))

    # ── Environmental Hazard Scan ──
    elements.append(Paragraph('Environmental Hazard Scan', heading_style))
    elements.append(Paragraph(
        'EPA contamination sites, Superfund locations, and toxic release facilities within 5 miles of the property.', small_style))
    elements.append(Spacer(1, 6))

    if hazards:
        haz_header = ['Type', 'Name', 'Distance', 'Status']
        haz_data = [haz_header]
        for h in hazards[:15]:
            if h.get('_type') == 'superfund':
                htype = 'Superfund'
            elif h.get('_type') == 'tri':
                htype = 'TRI Facility'
            else:
                htype = 'EPA Site'
            name = (h.get('site_name') or '—')[:35]
            dist = f"{h.get('distance_miles', 0):.1f} mi" if h.get('distance_miles') else '—'
            status = (h.get('npl_status') or h.get('category') or '—')[:20]
            haz_data.append([htype, name, dist, status])

        haz_table = Table(haz_data, colWidths=[1*inch, 2.5*inch, 0.8*inch, 1.7*inch], repeatRows=1)
        haz_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), RED),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8.5),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, HexColor('#fef2f2')]),
            ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#fca5a5')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(haz_table)
    else:
        elements.append(Paragraph(
            '<font color="#22c55e">&#10003;</font> <b>No EPA contamination sites or Superfund locations found within 5 miles.</b>',
            body_style))

    # ── Radon Zone Info ──
    if radon_info:
        elements.append(Spacer(1, 10))
        zone = radon_info.get('radon_zone', '?')
        risk = radon_info.get('risk_level', 'Unknown')
        county = radon_info.get('county', 'Unknown')
        level = radon_info.get('predicted_level', '')
        zone_color = '#d50000' if zone == 1 else '#ff6d00' if zone == 2 else '#2e7d32'
        radon_text = (
            f'<b>Radon Zone {zone}</b> — {county} County<br/>'
            f'Risk Level: <b>{risk}</b>'
            + (f' | Predicted: {level}' if level else '')
            + '<br/><i>EPA recommends radon testing for all homes, especially in Zone 1 (highest risk).</i>'
        )
        elements.append(Paragraph(radon_text, body_style))

    elements.append(Spacer(1, 20))

    # ── Depth Distribution ──
    if wells:
        elements.append(Paragraph('Area Well Depth Distribution', heading_style))
        shallow = sum(1 for w in wells if w.get('depth_total') and w['depth_total'] < 200)
        medium = sum(1 for w in wells if w.get('depth_total') and 200 <= w['depth_total'] < 500)
        deep_ = sum(1 for w in wells if w.get('depth_total') and w['depth_total'] >= 500)
        total_w = len([w for w in wells if w.get('depth_total')])

        if total_w > 0:
            dist_data = [
                ['Depth Range', 'Count', 'Percentage'],
                ['Under 200 ft (shallow)', str(shallow), f"{shallow/total_w*100:.0f}%"],
                ['200–500 ft (moderate)', str(medium), f"{medium/total_w*100:.0f}%"],
                ['Over 500 ft (deep)', str(deep_), f"{deep_/total_w*100:.0f}%"],
            ]
            dist_table = Table(dist_data, colWidths=[2.5*inch, 1.2*inch, 1.2*inch])
            dist_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), NAVY),
                ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, LIGHT_BG]),
                ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#cbd5e1')),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ]))
            elements.append(dist_table)
            elements.append(Spacer(1, 16))

    # ── Footer / Disclaimer ──
    elements.append(HRFlowable(width='100%', thickness=1, color=HexColor('#e2e8f0'), spaceAfter=8))
    elements.append(Paragraph(
        '<b>Disclaimer:</b> This report is based on publicly available state well permit records and EPA data. '
        'Well depths and water levels vary by location and geology. Actual drilling costs depend on driller, '
        'terrain, rock type, and permit requirements. This report is for informational purposes only and does '
        'not constitute professional geological or engineering advice. Consult a licensed well driller for '
        'site-specific estimates.', small_style))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph(
        f'© {datetime.now().year} Colorado Well Finder — coloradowell.com', 
        ParagraphStyle('Footer', parent=small_style, alignment=TA_CENTER, textColor=BLUE)))

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

        hazards = sorted(epa + superfund + tri, key=lambda x: x.get('distance_miles', 99))

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
