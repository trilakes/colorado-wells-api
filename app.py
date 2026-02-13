"""
Colorado Well Finder - Wells API
Serves 591K+ Colorado wells from PostgreSQL on Render.
Replaces live DWR API calls with own database.
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


def get_db():
    """Get a database connection."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn


# ─── Explore Map: Bounding Box Query ────────────────────────────────────────

@app.route('/api/wells/bbox')
def wells_bbox():
    """
    Get wells within a bounding box for the explore map.
    Returns lightweight payload (lat, lon, depth, receipt) for fast rendering.

    Query params:
      minLat, maxLat, minLng, maxLng  (required)
      limit  (optional, default 10000, max 50000)
    """
    try:
        min_lat = float(request.args.get('minLat', 0))
        max_lat = float(request.args.get('maxLat', 0))
        min_lng = float(request.args.get('minLng', 0))
        max_lng = float(request.args.get('maxLng', 0))
        limit = min(int(request.args.get('limit', 10000)), 50000)
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid parameters: {e}"}), 400

    if min_lat == 0 and max_lat == 0:
        return jsonify({"error": "Bounding box required"}), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT receipt, permit, latitude, longitude, depth_total,
                   status, county, uses, pump_yield_gpm, static_water_level,
                   aquifers, driller_name, date_completed, address, city,
                   owner_name, category, elevation
            FROM wells
            WHERE latitude BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            LIMIT %s
        """, (min_lat, max_lat, min_lng, max_lng, limit))
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
        cur.execute("""
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
            GROUP BY lat_bin, lng_bin
        """, (min_lat, lat_step, min_lng, lng_step,
              min_lat, max_lat, min_lng, max_lng))

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
                   aquifers, date_permit_issued, date_completed, more_info_url
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
    """Get aggregate statistics about the wells database."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total_wells,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) AS geocoded_wells,
                COUNT(CASE WHEN depth_total IS NOT NULL THEN 1 END) AS wells_with_depth,
                AVG(depth_total) AS avg_depth,
                MAX(depth_total) AS max_depth,
                COUNT(DISTINCT county) AS counties
            FROM wells
        """)
        stats = dict(cur.fetchone())
        stats['avg_depth'] = round(float(stats['avg_depth']), 1) if stats['avg_depth'] else None
        stats['max_depth'] = float(stats['max_depth']) if stats['max_depth'] else None

        # County breakdown
        cur.execute("""
            SELECT county, COUNT(*) AS count
            FROM wells
            WHERE county IS NOT NULL
            GROUP BY county
            ORDER BY count DESC
            LIMIT 20
        """)
        stats['top_counties'] = [dict(r) for r in cur.fetchall()]

        return jsonify(stats)
    finally:
        conn.close()


# ─── County List ─────────────────────────────────────────────────────────────

@app.route('/api/wells/counties')
def wells_counties():
    """Get list of all counties with well counts."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT county, COUNT(*) AS count
            FROM wells
            WHERE county IS NOT NULL
            GROUP BY county
            ORDER BY county
        """)
        return jsonify([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()


# ─── Stripe Email Verification ───────────────────────────────────────────────

@app.route('/api/verify', methods=['POST'])
def verify_email():
    """Verify Stripe subscription/payment by email. Replaces Cloudflare Worker."""
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
        # Find customer by email
        r = http_requests.get(
            f"https://api.stripe.com/v1/customers?email={email}&limit=1",
            headers=headers, timeout=10
        )
        customers = r.json()
        if not customers.get('data'):
            return jsonify({"valid": False, "message": "No account found for this email"})

        customer_id = customers['data'][0]['id']

        # 1) Check active subscription (monthly)
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

        # 2) Check checkout sessions for one-time payment (lifetime)
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

        # 3) Fallback — check payment_intents
        r = http_requests.get(
            f"https://api.stripe.com/v1/payment_intents?customer={customer_id}&limit=10",
            headers=headers, timeout=10
        )
        payments = r.json()
        has_payment = any(p.get('status') == 'succeeded' for p in payments.get('data', []))
        if has_payment:
            return jsonify({"valid": True, "type": "lifetime", "message": "Payment verified"})

        return jsonify({"valid": False, "message": "No active subscription found"})

    except Exception:
        return jsonify({"valid": False, "message": "Verification error"}), 500


# ─── Health Check ────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "service": "Colorado Well Finder - Wells API",
        "endpoints": [
            "GET /api/wells/bbox?minLat=&maxLat=&minLng=&maxLng=",
            "GET /api/wells/clusters?minLat=&maxLat=&minLng=&maxLng=",
            "GET /api/wells/search?q=&county=&division=",
            "GET /api/wells/<receipt>",
            "GET /api/wells/stats",
            "GET /api/wells/counties",
            "POST /api/verify {email}"
        ]
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
