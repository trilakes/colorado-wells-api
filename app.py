"""
Well Finder - Multi-State Wells & Environmental Data API
Serves 1.3M+ wells (CO, AZ, NM, WY) plus 1.5M+ overlay features
(contamination, oil/gas, dams, gauges, springs, water rights, aquifers)
from PostgreSQL on Render.
"""
import os
import math
import requests as http_requests
from flask import Flask, request, jsonify
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

        # At wide zoom (large bbox), random-sample for even spatial distribution.
        # PostgreSQL top-N heapsort makes ORDER BY RANDOM() LIMIT n efficient: O(N log n).
        lat_span = max_lat - min_lat
        lng_span = max_lng - min_lng
        bbox_area = lat_span * lng_span

        if bbox_area > 4:
            # Wide view — random sample spreads wells across all counties
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
    """
    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()

    if not email:
        return jsonify({"valid": False, "message": "Email required"}), 400

    # Owner bypass
    if email in OWNER_EMAILS:
        return jsonify({"valid": True, "type": "lifetime", "message": "Lifetime access verified"})

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
            # 1a) Check active subscription (monthly)
            r = http_requests.get(
                f"https://api.stripe.com/v1/subscriptions?customer={customer_id}&status=active&limit=1",
                headers=headers, timeout=10
            )
            subs = r.json()
            if subs.get('data'):
                return jsonify({
                    "valid": True,
                    "type": "monthly",
                    "message": "Active subscription found",
                    "expiresAt": subs['data'][0].get('current_period_end', 0) * 1000
                })

            # 1b) Check checkout sessions for one-time payment (lifetime)
            r = http_requests.get(
                f"https://api.stripe.com/v1/checkout/sessions?customer={customer_id}&limit=20",
                headers=headers, timeout=10
            )
            sessions = r.json()
            has_lifetime = any(
                s.get('payment_status') == 'paid' and s.get('mode') == 'payment'
                for s in sessions.get('data', [])
            )
            if has_lifetime:
                return jsonify({"valid": True, "type": "lifetime", "message": "Lifetime access verified"})

            # 1c) Fallback — check payment_intents
            r = http_requests.get(
                f"https://api.stripe.com/v1/payment_intents?customer={customer_id}&limit=10",
                headers=headers, timeout=10
            )
            payments = r.json()
            has_payment = any(p.get('status') == 'succeeded' for p in payments.get('data', []))
            if has_payment:
                return jsonify({"valid": True, "type": "lifetime", "message": "Payment verified"})

        # ── Step 2: Email-based fallback (no customer or customer had no payments) ──
        # Scan checkout sessions from all known Payment Links and match by email.
        # This catches payments made before customer_creation was set to "always".
        PAYMENT_LINKS = os.environ.get('PAYMENT_LINK_IDS', '').split(',')
        if not PAYMENT_LINKS or PAYMENT_LINKS == ['']:
            # Hardcoded fallback — all Colorado Well Finder payment links ever used
            PAYMENT_LINKS = [
                'plink_1T0XVOFiHBHcGzRNQXGAkacg',  # Lifetime $47 (active)
                'plink_1T0XVTFiHBHcGzRNkd9H0TWL',  # Monthly $19 (active)
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

        for plink_id in PAYMENT_LINKS:
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
                        access_type = 'monthly' if s.get('mode') == 'subscription' else 'lifetime'
                        return jsonify({
                            "valid": True,
                            "type": access_type,
                            "message": f"{access_type.title()} access verified (payment found)"
                        })
            except Exception:
                continue  # Skip this payment link if API call fails

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
        'aquifers': 'aquifer_code',
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
            "POST /api/verify {email}"
        ],
        "overlay_info": {k: v['label'] for k, v in OVERLAY_LAYERS.items()},
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
