# Otodom apartments scraper

## 🏡 About

Scrapes apartment listings from Otodom (Polish real estate site) into a PostgreSQL database. Tracks price changes over time, marks listings that disappear, and runs on a schedule via Docker or GitHub Actions.

## 🎯 What it does

- Two scraping modes: a full daily sync and a lightweight intra-day refresh for new offers
- Price history kept as a separate table - every price change is saved
- Listings that disappear get flagged, not deleted
- Normalized location schema (voivodeship -> city -> district -> street) so adding new cities later doesn't require migrations
- Unit tests for parsing and the repository layer (mocked DB)

## 🏗 Architecture

A few decisions worth mentioning:

- **Two listing models** (`ListingBasic` vs `ListingFull`) — full HTML is only parsed when we actually need the details, not on every list page
- **Normalization is its own layer** (`domain/normalize.py`) so cleaning logic isn't tangled with scraping or DB code
- **Two scraping modes**: `full` (daily) does everything; `latest` (intra-day) only fetches new offers and stops as soon as it hits something already in the DB
- **Price history + soft deletes**: makes time-series analysis possible without losing data on offers that get taken down

---

## 📦 Database structure

The schema stores listings, price history, photos and extracted features:

- `locations` — unique locations (city, district, street, …)
- `apartments_sale_listings` — main listings table
- `price_history` — historical price changes
- `photos` — image binaries (BYTEA)
- `features` — extracted flat features (AC, balcony, parking, …)

💡 Full schema in `db/schema.sql`.

⚠️ The schema is tuned for Katowice and the Silesian region. It will work for other cities, but if you want to scrape the whole country it's probably worth splitting `locations` into separate tables for voivodeships, cities and districts.

![Database structure](imgs/db_structure.png)

## 🚀 Quick start

**Requires:** [Docker Desktop](https://www.docker.com/products/docker-desktop)

```bash
# 1. Copy config and fill it in
cp .env.example .env

# 2. Run (builds PostgreSQL + scraper in one go)
docker compose up --build

# 3. Run later (database persists)
docker compose up

# 4. Stop everything
docker compose down
```

Database data lives in a Docker volume and survives `docker compose down`. To wipe it: `docker compose down -v`.

---

## 📦 Deployment options

### 1️⃣ Docker (easiest)

PostgreSQL + scraper in containers, no manual DB setup.

```bash
docker compose up --build
```

This starts:
- PostgreSQL container (`otodom_db`) on port 5432
- The scraper runs once and exits (no background daemon)
- Data persists in the `otodom_pgdata` volume

### 2️⃣ Local Python

If you'd rather skip Docker:

```bash
# 1. Create the database
psql -U postgres
CREATE DATABASE apartments_for_sale_otodom;
\q

# 2. Install and run
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

In `.env`, set `DB_HOST=localhost`.

### 3️⃣ GitHub Actions (scheduled)

Run the scraper on a schedule using GitHub Actions + an external PostgreSQL (e.g. [Neon](https://neon.tech)).

**Setup:**
1. Add repository secrets: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SSLMODE`
2. Workflows are defined in `.github/workflows/` (daily full sync + intra-day latest)
3. Logs are uploaded as artifacts after each run

**Manual trigger:** GitHub -> Actions -> pick the workflow -> Run workflow

## 🔄 Scraping modes

Controlled via the `SCRAPE_MODE` environment variable. Two tiers — one keeps the data fresh, the other keeps it complete:

| Mode | Schedule (UTC) | Scope | Use case |
|------|----------------|-------|----------|
| **`full`** (default) | Daily at 03:00 | All pages, full detail scrape, detects deleted offers | Complete daily sync |
| **`latest`** | Adaptive through the day — denser when more listings appear: every 30 min in peak (09–16), hourly in the morning (06–08), every 2h in the evening (18/20/22), plus a 02:00 run | First N pages, stops at the first offer already in DB | Fast delta for new listings |

**Configuration:**

```bash
# In .env:
SCRAPE_MODE=latest          # or "full"
LATEST_MAX_PAGES=1          # for latest mode: how many pages to scan

# Manual run:
docker compose run scraper_full      # force full sync
docker compose run scraper_latest    # force latest mode
```

## 📂 Project structure

```
otodom_scraper/
├── scraping/              # web scraping 
│   ├── client.py
│   ├── search_page.py
│   └── listing_page.py
│
├── domain/                # models + normalization
│   ├── models.py
│   └── normalize.py
│
├── services/              # sync strategies
│   ├── sync_listings.py   # full sync
│   └── sync_latest.py     # lightweight delta
│
├── db/                    # data layer
│   ├── listings_repo.py
│   ├── schema.sql
│   └── migration/
│
├── config/
│   └── logging_config.py
│
├── tests/
│   ├── test_normalize.py
│   ├── test_search_page.py
│   └── test_listing_page.py
│
├── main.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

The layers are isolated on purpose: when Otodom changes their HTML, the breakage stays in `scraping/` and doesn't leak into the DB schema.

---

## 🔑 Environment variables

Everything is configured via `.env`. See `.env.example` for the list.

> 💡 On first run, the required tables are created automatically if they don't exist.

## 🧪 Testing

Tests run on `pytest` with a mocked database — no PostgreSQL needed.

```bash
source venv/bin/activate
pytest tests/
```

**Covered:**
- ✅ Parsing (search pages, listing detail pages)
- ✅ Normalization
- ✅ Repository layer (CRUD)

**Not covered yet:**
- 🚧 Integration tests with a real DB
- 🚧 Contract tests that catch Otodom HTML changes in CI (right now I only find out when the scraper starts producing nulls)
- 🚧 Service layer (sync orchestration)

## 📝 Logging

Database operations are logged via Python's `logging` module. Logs go to `logs/`, configured in `config/logging_config.py`. On GitHub Actions, logs are uploaded as artifacts after each run.

---

## 📌 Good to know

- Otodom doesn't have a public API, but the data sits as JSON embedded in the HTML — no need for a real browser
- Nothing ever gets deleted from the DB - offers that disappear get a `detected_inactive_at` timestamp, so the history stays intact
- Re-runs are safe to interrupt - duplicates are blocked by `created_at` and price changes land in `price_history`
- The two scraping modes exist because hammering Otodom hourly with a full scrape is a waste of bandwidth and a fast way to get rate-limited
- The schema was built around Katowice but the `voivodeship -> city -> district -> street` hierarchy means adding new cities doesn't need a migration
- GitHub Actions logs are uploaded as artifacts, so you can actually debug a failed run instead of guessing