# FinSight 50 — Large-Cap Financial Intelligence Pipeline

> An end-to-end data engineering project that ingests, transforms, and visualizes
> the financial health of the world's 50 largest companies — turning raw market data
> into investor-grade analytical dashboards.

---

## What This Project Does

**FinSight 50** is a production-style batch data pipeline built entirely on modern open-source and cloud-native tooling. It automatically collects financial data for the top 50 S&P 500 companies by market capitalization, engineers a comprehensive set of institutional-quality financial metrics, and surfaces them through an interactive multi-page Looker Studio dashboard designed for investors and analysts.

The pipeline covers four core layers:

| Layer | Tooling | What happens |
|---|---|---|
| **Infrastructure** | Terraform | Provisions the GCS bucket and BigQuery dataset on GCP with a single `terraform apply` |
| **Ingestion** | Kestra + dltHub + Docker | A containerized Python pipeline pulls financial data from `yfinance`, stages it in GCS, and loads it into BigQuery raw tables |
| **Transformation** | dbt Core | Computes financial ratios, TTM figures, YoY growth rates, sector benchmarks, and valuation multiples — all executed inside BigQuery |
| **Visualization** | Looker Studio | A three-page interactive dashboard presents current snapshots, historical trends, and cross-company comparisons |

The pipeline runs in **batch mode, scheduled monthly**, so every refresh captures the latest quarterly reports, updated stock prices, and recalculated sector benchmarks automatically.

---

## Problem Statement

### Background

Retail investors and financial analysts routinely face the same challenge: publicly available financial data is scattered, raw, and requires significant expertise and tooling to transform into actionable intelligence. Platforms like Bloomberg Terminal and FactSet provide this kind of structured, benchmarked analysis — but at costs that place them firmly out of reach for most individuals.

### Business Objective

FinSight 50 was built to answer a clear and practical question:

> *"For any of the world's 50 largest companies, how does its current financial health compare to its own four-year history and to its sector peers — and is it getting stronger or weaker?"*

The project delivers this by providing:

- **Valuation context** — P/E, P/S, and P/CF ratios benchmarked against sector medians, so an investor can immediately see whether a stock trades at a premium or discount to peers
- **Profitability analysis** — gross margin, net margin, and FCF margin tracked over four fiscal years, revealing whether earnings quality is improving or deteriorating
- **Efficiency and risk signals** — ROE, ROA, debt-to-equity, and current ratio, giving a complete picture of how well management deploys capital and how exposed the balance sheet is
- **Growth trajectory** — year-over-year revenue, net income, and dividend growth rates structured into a Bloomberg-style heatmap for instant pattern recognition
- **Sector benchmarking** — every metric is compared to its sector's computed average, so a 27% net margin reads correctly in the context of a hardware company vs a pure-software peer
- **Capital structure overview** — liquidity ratios and leverage metrics that reveal how a company is financed and how much financial flexibility it retains

### Who This Is For

- **Individual investors** performing fundamental due diligence before taking a position
- **Finance students and analysts** who need a working reference for quantitative fundamental analysis
- **Data engineers** looking for a real-world, end-to-end pipeline project to study or extend

---

## Architecture

<img src="images/dashboard/FinSight_50_Architecture_diagram.svg" alt="Project Architecture" width="800">

---

## Data Sources & Coverage

| Data type | Source | Frequency | Coverage per ticker |
|---|---|---|---|
| Stock prices (OHLCV) | `yfinance` | Daily | 4 years (backfill) / 1 month (incremental) |
| Annual income statements | `yfinance` | Yearly | 4 most recent fiscal years |
| Quarterly income statements | `yfinance` | Quarterly | 5 most recent quarters |
| Annual balance sheets | `yfinance` | Yearly | 4 most recent fiscal years |
| Quarterly balance sheets | `yfinance` | Quarterly | 5 most recent quarters |
| Annual cash flow statements | `yfinance` | Yearly | 4 most recent fiscal years |
| Quarterly cash flow statements | `yfinance` | Quarterly | 5 most recent quarters |
| Company reference info | `yfinance` | Monthly | Sector, industry, employees, HQ |

**Universe:** Top 50 S&P 500 companies by market capitalization, dynamically identified at each pipeline run by scraping the Wikipedia S&P 500 constituent list, fetching live market caps via `yfinance`, and deduplicating by company name. Covers sectors including Technology, Healthcare, Financials, Consumer Discretionary, Energy, and Industrials.

---

## Tech Stack

| Tool | Role | Notes |
|---|---|---|
| **Terraform** | Infrastructure as Code | Provisions GCS bucket and BigQuery dataset; auto-generates `.env` for Kestra |
| **Docker** | Containerization | Packages the Python ingestion environment; Kestra itself runs via Docker Compose |
| **Kestra** | Workflow orchestration | Local deployment via Docker Compose (Postgres backend); accessible at `localhost:8080` |
| **dltHub** | Data ingestion & loading | Schema inference, GCS staging, BigQuery loading with merge/replace dispositions |
| **yfinance** | Financial data source | Free Python API for S&P 500 financial statements, price history, and company metadata |
| **Google Cloud Storage** | Data lake | Staging area for raw `dlt` loads before BigQuery ingestion |
| **BigQuery** | Data warehouse & compute | Stores all raw tables and executes all dbt transformations at scale |
| **dbt Core** | Data transformation | Full model DAG from staging through to 5 mart tables |
| **Looker Studio** | Dashboard & visualization | Connects directly to BigQuery mart tables; no intermediate BI server |
| **Python 3.11** | Pipeline scripting | `yfinance`, `pandas`, `dlt` |

---

## dbt Mart Tables

The pipeline produces five final mart tables. All dashboard visualizations draw exclusively from these tables.

### `mart_current_valuation_snapshot`
**One row per ticker.** The primary snapshot table — contains the company's current financial state alongside pre-computed sector-level benchmarks for every metric.

| Field group | Key fields |
|---|---|
| Valuation multiples | `pe_ratio`, `ps_ratio`, `pcf_ratio` |
| Profitability margins | `gross_margin`, `net_profit_margin`, `free_cash_flow_margin` |
| Efficiency ratios | `roe`, `roa`, `asset_turnover` |
| Liquidity & leverage | `current_ratio`, `debt_to_equity` |
| Sector benchmarks | `sector_median_pe`, `sector_median_ps`, `sector_median_pcf`, `sector_avg_gross_margin`, `sector_avg_net_margin`, `sector_avg_roe`, … (12 sector avg/median fields) |
| Market data | `current_price`, `current_market_cap`, `shares_outstanding` |
| TTM financials | `ttm_total_revenue`, `ttm_net_income`, `ttm_free_cash_flow`, `ttm_operating_cash_flow` |
| Reference | `sector`, `employee_count`, `latest_report_date` |

### `mart_historical_financial_metrics`
**Four rows per ticker** (one per fiscal year: FY-3 → FY0). Powers all trend and time-series visualizations.

| Field group | Key fields |
|---|---|
| Valuation over time | `pe_ratio`, `ps_ratio`, `pcf_ratio`, `market_cap_billions` |
| Margin trends | `gross_margin`, `net_profit_margin`, `free_cash_flow_margin` |
| Per-share metrics | `eps`, `revenue_per_share`, `free_cash_flow_per_share` |
| YoY growth rates | `yoy_revenue_growth`, `yoy_net_income_growth`, `yoy_dividend_growth` |
| Period labels | `financial_year_label` (FY-3, FY-2, FY-1, FY0 (Latest)), `recency_rank` |

### `mart_liquidity_capital_structure`
Balance sheet and liquidity deep-dive. Tracks current ratio, quick ratio, cash position, total debt, interest coverage, and capital structure composition over time.

### `mart_sector_metrics_heatmap`
**One row per sector.** Pre-aggregated sector-level averages across all key metrics. Used as the source for the Page 3 sector heatmap — rows as sectors, columns as metrics, cells colored by performance intensity.

### `mart_yoy_growth_unpivot`
**Nine rows per ticker** (3 growth metrics × 3 fiscal years with YoY data available). An unpivoted version of the growth data that enables Looker Studio's pivot table to render metric names as row labels and fiscal years as column headers — the layout required for the Bloomberg-style heatmap.

| Column | Description |
|---|---|
| `stock_ticker` | Company ticker symbol |
| `financial_year_label` | FY-2, FY-1, FY0 (Latest) |
| `recency_rank` | Integer sort key for chronological column ordering in pivot |
| `metric_name` | "Revenue growth", "Net income growth", "Dividend growth" |
| `metric_order` | Integer sort key (1, 2, 3) for correct row ordering |
| `growth_value` | YoY growth rate × 100 (expressed in percentage points) |

---

## Key Financial Metrics Computed

| Metric | Formula | Analytical purpose |
|---|---|---|
| P/E Ratio | Market cap / TTM net income | Valuation — earnings multiple |
| P/S Ratio | Market cap / TTM revenue | Valuation — revenue multiple |
| P/CF Ratio | Market cap / TTM operating cash flow | Valuation — cash flow multiple |
| Gross Margin | Gross profit / Revenue | Pricing power and cost structure |
| Net Profit Margin | Net income / Revenue | Bottom-line efficiency |
| FCF Margin | Free cash flow / Revenue | Cash generation quality |
| ROE | Net income / Shareholders' equity | Return on invested equity capital |
| ROA | Net income / Total assets | Asset productivity |
| Current Ratio | Current assets / Current liabilities | Short-term liquidity |
| Debt / Equity | Total debt / Shareholders' equity | Financial leverage |
| Asset Turnover | Revenue / Total assets | Operational efficiency |
| EPS | Net income / Diluted shares outstanding | Per-share earnings |
| FCF per Share | Free cash flow / Diluted shares | Per-share cash generation |
| YoY Revenue Growth | (Rev_t − Rev_t-1) / Rev_t-1 | Top-line momentum |
| YoY Net Income Growth | (NI_t − NI_t-1) / NI_t-1 | Earnings momentum |
| YoY Dividend Growth | (Div_t − Div_t-1) / Div_t-1 | Shareholder return trend |

---

## Dashboard Overview

The Looker Studio dashboard is organized across three pages, all controlled by a `stock_ticker` dropdown filter.

### Page 1 — Company Snapshot
Current financial health at a glance for any selected ticker.

- **KPI scorecard strip** — 9 key ratios (P/E, P/S, gross margin, net margin, FCF margin, ROE ,Current Ratio ,Debt to Equity , Asset turnover), each showing ticker value vs sector benchmark with color-coded delta arrows; valuation multiples use inverted color logic (higher = red = expensive vs peers)
- **Balance sheet donuts** — assets decomposed into cash, current (ex-cash), and non-current. Capital structure decomposed into total debts, stockholders equity and other liabilities.

### Page 2 — Historical Performance
Four-year trend analysis for the selected ticker.

- **Combo chart** — revenue per share (bars) + net margin % (line, right axis) over FY-3 → FY0; standard sell-side equity research format
- **Dual-line chart** — EPS vs FCF per share; convergence or divergence signals earnings quality
- **YoY growth pivot table** — 3×3 Bloomberg-style heatmap (metric rows × fiscal year columns) sourced from `mart_yoy_growth_unpivot`; green = positive growth, red = negative, white/yellow = near zero

### Page 3 — Cross-Company Screener
All 50 companies compared simultaneously, filterable by sector.

- **Bubble chart** — P/E (valuation) vs Net Profit Margin (quality); bubble size = market cap; color = sector; quadrant lines at sector medians

---

## Repository Structure

```
finsight-50/
│
├── terraform/
│   ├── main.tf                        # GCS bucket + BigQuery dataset + .env generation
│   └── variables.tf
│
├── ingestion/
│   ├── Dockerfile                     # python:3.11-slim image for Kestra task execution
│   ├── docker-compose.yml             # Kestra + Postgres local deployment
│   ├── requirements.txt
│   ├── get_top50_tickers.py           # Wikipedia scrape → market cap fetch → top_50_tickers.csv
│   └── kestra_flows/
│       └── yfinance_to_bigquery.yml   # Kestra flow (schedule + dlt pipeline + main.py)
│
├── dbt/
│   ├── models/
│   │   |                              # Raw source cleaning and type casting
|   |   |── staging/                   # Partitioning and Clustering  
│   │   ├── intermediate/              # Business logic, joins, TTM calculations
│   │   └── marts/                      # Final output tables (5 models)
│   │       ├── mart_current_valuation_snapshot.sql
│   │       ├── mart_historical_financial_metrics.sql
│   │       ├── mart_liquidity_capital_structure.sql
│   │       ├── mart_sector_metrics_heatmap.sql
│   │       └── mart_yoy_growth_unpivot.sql
│   ├── tests/                         # Not-null, range, and uniqueness assertions
│   └── dbt_project.yml
│
├── dashboard/
│   └── looker_studio_link.md          # Published dashboard URL
│
├── .env.example                       # Template — actual .env is generated by Terraform
└── README.md
```

---

## Infrastructure (Terraform)

Terraform provisions all GCP resources before the first pipeline run and auto-generates the `.env` file consumed by Kestra.

**Resources created:**

| Resource | Name | Config |
|---|---|---|
| BigQuery dataset | `sp500_top50_analysis_gold` | `delete_contents_on_destroy = true` for clean teardown |
| GCS bucket | `{project_id}-raw-data-lake` | Uniform bucket-level access; used as dltHub staging area |
| Local `.env` file | `.env` | Auto-populated with `GCP_PROJECT_ID`, `GCP_REGION`, `BQ_DATASET`, `GCS_BUCKET` |

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

After `apply`, the `.env` file is ready and GCP resources are live.

---

## Kestra Orchestration

Kestra runs locally via Docker Compose and is accessible at `http://localhost:8080` (credentials: `admin@kestra.io` / `Admin1234!`).

**Flow: `yfinance_to_bigquery_pro`**

The flow accepts one boolean input that controls the ingestion scope:

| Input | Type | Default | Behavior |
|---|---|---|---|
| `backfill` | Boolean | `true` | `true` → 4 years of price history + all available financial statements (first run). `false` → last month of prices + latest quarter only (monthly incremental updates) |

The flow spins up a `python:3.11-slim` Docker container, installs dependencies at runtime, and executes `main.py` with GCP credentials and configuration injected as environment variables from Kestra's KV Store.

**dltHub write dispositions per table:**

| BigQuery table | Disposition | Primary key |
|---|---|---|
| `fact_history` | `merge` | `(ticker, Date)` |
| `company_info_dim` | `replace` | — |
| `fact_income_statement_annual` | `merge` | `(ticker, report_date)` |
| `fact_income_statement_quarter` | `merge` | `(ticker, report_date)` |
| `fact_balance_sheet_annual` | `merge` | `(ticker, report_date)` |
| `fact_balance_sheet_quarter` | `merge` | `(ticker, report_date)` |
| `fact_cash_flow_annual` | `merge` | `(ticker, report_date)` |
| `fact_cash_flow_quarter` | `merge` | `(ticker, report_date)` |

---

## Scheduling & Refresh Cadence

The Kestra flow runs on a **monthly schedule**. Each run executes this sequence end to end:

1. **Ticker discovery** — `get_top50_tickers.py` scrapes the S&P 500 Wikipedia list, fetches live market caps via `yfinance`, deduplicates by company name, and writes the current top 50 to `top_50_tickers.csv`
2. **Data ingestion** — `main.py` loops through all 50 tickers and loads 8 table types into GCS (staging) and BigQuery (raw) via dltHub; incremental runs use `merge` disposition to avoid duplicates
3. **Transformation** — `dbt run` rebuilds the full model DAG: staging → intermediate → all 5 mart tables
4. **Quality checks** — `dbt test` runs not-null assertions, row count validations, and metric range checks across all mart tables
5. **Dashboard refresh** — Looker Studio reads directly from BigQuery on the next report load; no manual intervention required

---

## Dashboard Access

> 🔗 **[View Live Dashboard →](your-looker-studio-link-here)**

The dashboard is publicly accessible in view mode. Use the **ticker dropdown** on Pages 1 and 2 to explore any of the 50 companies. Use the **sector filter** on Page 3 to narrow the cross-company screener.

---

## Installation & Replication Guide

This section provides complete step-by-step instructions for anyone who wants to clone and run the full pipeline from scratch. Follow the steps in order — each stage depends on the one before it.

---

### Prerequisites

Before you begin, make sure the following are installed and available on your machine:

| Requirement | Version | Install |
|---|---|---|
| Python | 3.11+ | [python.org](https://www.python.org/downloads/) |
| Terraform | 1.5+ | [terraform.io](https://developer.hashicorp.com/terraform/install) |
| Docker Desktop | Latest | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Docker Compose | v2+ | Included with Docker Desktop |
| Git | Any | [git-scm.com](https://git-scm.com/) |
| Google Cloud SDK (`gcloud`) | Latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |

---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/eladshaul/finsight-50.git
cd finsight-50
```

---

### Step 2 — Google Cloud Platform Setup

#### 2.1 Create a GCP Project

1. Go to [console.cloud.google.com](https://console.cloud.google.com/)
2. Click **Select a project → New Project**
3. Give it a name (e.g. `finsight-50`) and note the **Project ID** — you will need it in every step that follows

#### 2.2 Enable Required APIs

In the GCP console, navigate to **APIs & Services → Enable APIs and Services** and enable the following:

- BigQuery API
- Cloud Storage API
- Identity and Access Management (IAM) API

Or enable all at once via `gcloud`:

```bash
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  --project=YOUR_PROJECT_ID
```

#### 2.3 Create a Service Account

1. Go to **IAM & Admin → Service Accounts → Create Service Account**
2. Name it `finsight-pipeline-sa` (or any name you prefer)
3. Grant it the following roles:

| Role | Purpose |
|---|---|
| `BigQuery Admin` | Create datasets, tables, run jobs |
| `Storage Admin` | Create buckets, read/write objects |
| `Storage Object Admin` | Read/write GCS objects (dltHub staging) |
| `Viewer` | General project read access |

4. Click **Done**

#### 2.4 Download the Service Account JSON Key

1. In the Service Accounts list, click on the account you just created
2. Go to the **Keys** tab → **Add Key → Create new key → JSON**
3. A `.json` file will download automatically — save it somewhere safe on your machine
4. **Keep this file private** — never commit it to Git

> **Important:** The contents of this JSON file will be used in Step 5 when configuring Kestra's KV Store. You will paste the entire raw JSON string as the value for `GCP_CREDS`.

---

### Step 3 — Install Python Dependencies

From the project root:

```bash
uv sync
```

This creates a local virtual environment and installs all project dependencies from `pyproject.toml` / `uv.lock`.

---

### Step 4 — Provision Infrastructure with Terraform

Terraform creates the GCS bucket and BigQuery dataset, and auto-generates the `.env` file.

#### 4.1 Set your variables

Open `terraform/variables.tf` and fill in your values, or create a `terraform/terraform.tfvars` file:

```hcl
project_id = "your-gcp-project-id"
region     = "us-central1"
location   = "US"
```

> `location` is the BigQuery dataset and GCS bucket location. Use `"US"` for multi-region or a specific region like `"us-central1"` for single-region.

#### 4.2 Run Terraform

```bash
cd terraform/
terraform init
terraform plan   # review what will be created
terraform apply  # type "yes" when prompted
```

After `apply` completes:
- The BigQuery dataset `sp500_top50_analysis_gold` is created
- The GCS bucket `{your-project-id}-raw-data-lake` is created
- A `.env` file is written to the project root with the following content:

```
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
BQ_DATASET=sp500_top50_analysis_gold
GCS_BUCKET=your-project-id-raw-data-lake
```

> This `.env` is for your reference. Kestra does **not** read it directly — you will enter these values manually into Kestra's KV Store in Step 6.

---

### Step 5 — Start Kestra with Docker Compose

Kestra runs locally as a Docker Compose stack (Kestra server + Postgres backend).

```bash
cd kestra/
docker compose up -d
```

Wait about 30 seconds for the services to initialize, then open the Kestra UI:

```
http://localhost:8080
```

**Default credentials:**

| Field | Value |
|---|---|
| Username | `admin@kestra.io` |
| Password | `Admin1234!` |

> Kestra uses a Postgres container for its internal state. Both services must be running for the UI and flows to work.

---

### Step 6 — Configure the KV Store (Credentials & Config)

Kestra's flows read GCP credentials and configuration from its internal **KV Store** rather than from environment files. You will load all required variables using the `setup_kv_store` flow.

> **Why KV Store?** Kestra flows run inside isolated Docker containers and cannot access your local `.env` file directly. The KV Store is Kestra's secure, built-in mechanism for injecting secrets and config into flow executions.

#### 6.1 Import the KV setup flow

1. In the Kestra UI, go to **Flows → Create**
2. Paste the contents of `kestra/flows/setup_kv_store.yml` into the editor
3. Click **Save**

#### 6.2 Execute the KV setup flow

1. Click **Execute** on the `setup_kv_store` flow
2. Fill in the input fields:

| Input field | Value to enter |
|---|---|
| `gcp_project_id` | Your GCP project ID (e.g. `finsight-50`) |
| `gcp_region` | Your GCP region (e.g. `us-central1`) |
| `bq_dataset` | `sp500_top50_analysis_gold` |
| `gcs_bucket` | `your-project-id-raw-data-lake` |
| `gcp_creds` | The **full raw JSON content** of your service account key file |

> For `gcp_creds`: open the downloaded `.json` key file in a text editor, select all the text, and paste it as-is into the input field. The entire JSON object — from the opening `{` to the closing `}` — should be the value.

3. Click **Execute** — the flow will set all 5 keys in the KV Store and log a confirmation message

#### 6.3 Verify the KV Store

Go to **Namespaces → company.data → KV Store** and confirm the following 5 keys exist:

```
GCP_PROJECT_ID
GCP_REGION
BQ_DATASET
GCS_BUCKET
GCP_CREDS
```

---

### Step 7 — Import and Run the Ingestion Flow

#### 7.1 Import the main flow

1. In the Kestra UI, go to **Flows → Create**
2. Paste the contents of `kestra/flows/yfinance_to_bigquery.yml`
3. Click **Save**

#### 7.2 Execute — First Run (Full Backfill)

For the initial run, you want to load the full 4-year history of financial statements and stock prices.

1. Click **Execute** on the `yfinance_to_bigquery_pro` flow
2. Set the `backfill` input to **`true`**
3. Click **Execute**

> **Expected duration:** 20–45 minutes depending on your network speed and yfinance API rate limits. The flow processes 50 tickers × 8 table types with deliberate sleep intervals between requests to avoid API throttling.

#### 7.3 Execute — Incremental Monthly Updates

For all subsequent monthly runs, set `backfill` to **`false`**:

1. Click **Execute** → set `backfill` to **`false`**
2. This fetches only the last month of price data and the most recent quarter of financial statements, making the run significantly faster (~5–10 minutes)

> The monthly schedule trigger in the flow will handle this automatically once active — you only need to trigger it manually for the first backfill run.

#### 7.4 Verify the raw tables in BigQuery

After the flow completes, go to the [BigQuery console](https://console.cloud.google.com/bigquery) and confirm that the dataset `sp500_top50_analysis_gold` contains these 8 raw tables:

```
fact_history
company_info_dim
fact_income_statement_annual
fact_income_statement_quarter
fact_balance_sheet_annual
fact_balance_sheet_quarter
fact_cash_flow_annual
fact_cash_flow_quarter
```

---

### Step 8 — Run dbt Transformations

dbt reads from the raw BigQuery tables and builds the 5 mart tables. It runs inside a Docker container to keep the environment isolated and reproducible.

#### 8.1 Build the dbt Docker image

```bash
docker build -t finsight-dbt .
```

#### 8.2 Configure `profiles.yml`

dbt requires a `profiles.yml` file to connect to BigQuery. Create this file at `~/.dbt/profiles.yml` (the default dbt profiles location):

```yaml
dbt_transformation:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: sp500_top50_analysis_gold
      location: US
      keyfile: /root/credentials.json
      threads: 4
      timeout_seconds: 300
```

Replace:
- `your-gcp-project-id` with your actual GCP project ID
- `/path/to/your/service-account-key.json` with the absolute path to the JSON credentials file you downloaded in Step 2.4

> If running dbt inside Docker, mount both the `profiles.yml` and the credentials file into the container. See the run command below.
> These mounts are required so dbt in the container can read your local project files, profile config, and GCP key.

#### 8.3 Run dbt inside Docker

```bash
cd dbt_transformation
docker run --rm \
  -v $(pwd):/usr/app \
  -v ~/.dbt:/root/.dbt \
  -v /path/to/your/service-account-key.json:/root/credentials.json \
  finsight-dbt \
  dbt run --profiles-dir /root/.dbt --project-dir /usr/app
```

#### 8.4 Run dbt tests

```bash
docker run --rm \
  -v $(pwd):/usr/app \
  -v ~/.dbt:/root/.dbt \
  -v /path/to/your/service-account-key.json:/root/credentials.json \
  finsight-dbt \
  dbt test --profiles-dir /root/.dbt --project-dir /usr/app
```

#### 8.5 Verify the mart tables in BigQuery

After `dbt run` completes, confirm these 5 mart tables exist in `sp500_top50_analysis_gold`:

```
mart_current_valuation_snapshot
mart_historical_financial_metrics
mart_liquidity_capital_structure
mart_sector_metrics_heatmap
mart_yoy_growth_unpivot
```

---

### Step 9 — Connect Looker Studio to BigQuery

This step is optional and only needed for the dashboard layer.

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com/) and sign in with the same Google account that owns the GCP project
2. Click **Create → Report**
3. In the data source picker, choose **BigQuery**
4. Select your project → dataset `sp500_top50_analysis_gold` → choose the first mart table (e.g. `mart_current_valuation_snapshot`)
5. Click **Add to Report** (repeat for the other mart tables if you want to recreate the full dashboard)

---

### Full Pipeline Summary

| Step | Action | Time estimate |
|---|---|---|
| 1 | Clone repository | < 1 min |
| 2 | GCP project + service account + JSON key | 5–10 min |
| 3 | `uv sync` | 2–3 min |
| 4 | `terraform apply` — provision GCS + BQ | 2–3 min |
| 5 | `docker compose up` — start Kestra | 1–2 min |
| 6 | Configure KV Store via `setup_kv_store` flow | 3–5 min |
| 7 | Run ingestion flow (backfill = true) | 20–45 min |
| 8 | Build dbt image → `dbt run` → `dbt test` | 5–10 min |
| 9 | Connect Looker Studio to BigQuery | 5 min |
| **Total** | **End to end** | **~45–80 min** |

---

## About This Project

FinSight 50 was built as the capstone project for the **[DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)** — a free, community-driven program covering the full modern data engineering stack.

The project demonstrates all core competencies from the course:

| Competency | Implementation |
|---|---|
| Cloud infrastructure | GCP (GCS + BigQuery), provisioned with Terraform |
| Containerization | Docker — `python:3.11-slim` for ingestion; Docker Compose for Kestra |
| Workflow orchestration | Kestra with Postgres backend, monthly scheduled flow |
| Data ingestion | dltHub — schema inference, GCS staging, merge/replace loads |
| Data lake | Google Cloud Storage (raw staging layer) |
| Data warehouse | BigQuery (Partitioning and clustering) |
| Data transformation | dbt Core — staging → intermediate → 5 mart models |
| Analytical modeling | 16 financial metrics, sector benchmarking, TTM calculations |
| Data visualization | Looker Studio — 3-page dashboard with dynamic filters and heatmaps |
| Batch processing | Monthly incremental pipeline with full backfill support |

---

*Data is sourced from `yfinance` and reflects publicly reported financial statements.
This project is for educational and analytical purposes only and does not constitute investment or financial advice.*
