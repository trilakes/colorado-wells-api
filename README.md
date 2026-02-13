# Colorado Wells API

Self-hosted API serving 591,510+ Colorado wells from PostgreSQL. Replaces live DWR API calls.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/wells/bbox?minLat=&maxLat=&minLng=&maxLng=` | Wells in bounding box (explore map) |
| `GET /api/wells/clusters?minLat=&maxLat=&minLng=&maxLng=` | Clustered counts for zoomed-out view |
| `GET /api/wells/search?q=&county=&division=` | Search wells |
| `GET /api/wells/<receipt>` | Single well details |
| `GET /api/wells/stats` | Aggregate statistics |
| `GET /api/wells/counties` | County list with counts |

## Deploy to Render

### 1. Create PostgreSQL Database
- Go to Render Dashboard → New → PostgreSQL
- Plan: **Starter** ($7/mo, 1 GB storage)
- Name: `colorado-wells-db`
- Copy the **External Database URL**

### 2. Migrate Data
```bash
# Set the Render PostgreSQL URL
set DATABASE_URL=postgresql://wells_admin:PASSWORD@HOST:5432/wells_db

# Run migration (takes ~2-5 min)
python migrate.py "C:\path\to\wells.db"
```

### 3. Deploy Web Service
- Go to Render Dashboard → New → Web Service
- Connect your repo or push this folder
- Plan: **Starter** ($7/mo)
- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn app:app`
- Add environment variable:
  - `DATABASE_URL` → paste Internal Database URL from step 1

### Or use render.yaml (Blueprint)
Push to a Git repo and use Render Blueprints to auto-deploy both database + service.

## Cost
- PostgreSQL Starter: **$7/mo**
- Web Service Starter: **$7/mo**  
- **Total: $14/mo**

## Performance
- Bounding box query (10K wells): ~50-100ms
- Full-text search: ~100-200ms
- 591K wells with indexes: fits in <300 MB PostgreSQL
