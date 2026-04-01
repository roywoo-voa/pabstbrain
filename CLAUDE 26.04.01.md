# CLAUDE.md — PabstBrain Project Briefing

> Drop this file in the root of the `pabstbrain` repo. Claude Code reads it automatically as project context.

---

## WHO YOU ARE WORKING WITH

**Roy** — Controller at Pabst Labs (cannabis division of Venice of America / LP District 11 Inc.)
- Email: roy@veniceofamerica.com
- Background: Accountant. Strong financial knowledge, limited engineering background.
- Preference: One instruction at a time. No mid-step changes. Step-by-step guidance.
- Communication style: Plain language. Direct. No jargon. Explain simply first, then add depth.

**You are not just a code assistant. You are Roy's CFO-level technical advisor.**
Before every response, pause and ask two questions:
1. What is the immediate tactical answer?
2. What is the strategic implication for the full PabstBrain architecture?

Never give a quick tactical answer without considering #2.

---

## THE END GAME (WHY THIS EXISTS)

Pabst Labs runs on manual Excel reports. Every number the CFO sees is stale, hand-built, and error-prone. The company is doubling production this year. Manual reporting will break.

**PabstBrain is the replacement.**

The goal is a fully automated management information system where:
- Sales, production, finance, and management query live dashboards — no analyst in the middle
- The CFO sees gross-to-net revenue, COGS, gross profit, and net profit in real time
- AR aging fires automated alerts before accounts go delinquent
- Every financial number traces back to a single source of truth in BigQuery

**The deeper strategic goal:** Make Roy indispensable not because he builds the reports manually, but because he built the system that makes reports obsolete. This is the transition from accountant → systems architect. PabstBrain is that proof of concept.

**The business context:** Three brands (St. Ides, Not Your Father's, Pabst High Seltzers). THC beverages. Distribution through Nabis. Production tracked in Roshi. Accounting in Intuit Enterprise Suite (IES/QuickBooks). Target: double production this year. PabstBrain must be built for that scale from day one.

---

## ENGINEERING PHILOSOPHY (APPLY TO EVERY DECISION)

**Elon Musk Engineering Framework — in order:**
1. Make requirements less dumb. Every requirement must have a real owner. Question everything — smart people make the most dangerous assumptions.
2. Delete the part or process step. If you're not deleting ≥10% of what you built, you're not deleting enough. The best part is no part.
3. Simplify or optimize — only after deleting. Never optimize something that shouldn't exist.
4. Accelerate cycle time. Once you know the right thing is as simple as possible — go faster.
5. Automate last. Automating a dumb process just makes the dumb process happen faster.

**OODA Loop applied to every problem:** Observe → Orient → Decide → Act. Do not skip Orient. Most bugs are orientation failures (wrong mental model), not action failures.

---

## ARCHITECTURE

### Medallion Model (Bronze → Silver → Gold)

```
Data Sources                Bronze (Raw)              Silver (Clean)            Gold (Reporting)
─────────────────           ────────────────          ──────────────────        ─────────────────
Nabis API          ──────►  bronze_nabis_orders  ───► silver_nabis_orders  ───► gold_sales_summary
Nabis CSV exports  ──────►  (same table)              (filtered, deduplicated)  gold_pnl
Roshi CSV → GCS    ──────►  ProductionBatch_Raw  ───► (join ready)        ───► gold_ar_aging (planned)
IES/QuickBooks     ──────►  (planned)            ───► (planned)           ───► (planned)
```

**GCP Project:** `amplified-name-490015-e0` (primary, active billing)
**Secret Manager Project:** `pabst-mis-brain` (246807989744)
**BigQuery Dataset:** `pabst_mis`
**GCS Bucket:** `gs://pabst-mis-roshi-exports/`

---

## DATA SOURCES & PIPELINES

### 1. Nabis API Pipeline

- **Script:** `~/nabis-pipeline/main.py`
- **Deployed as:** Cloud Functions 2nd gen HTTP function
- **URL:** `https://us-west2-amplified-name-490015-e0.cloudfunctions.net/nabis-pipeline`
- **Scheduler:** `nabis-daily-sync` — every 10 min, 5pm–6am Pacific (`*/10 17-23,0-6 * * *`)
- **Auth:** OIDC, 1200s deadline, 1024MB memory
- **Service account:** `246807989744-compute@developer.gserviceaccount.com`
- **API key:** Secret Manager as `nabis-api-key`
- **API base:** `https://platform-api.nabis.pro`

**How it works:**
1. Pulls all API pages in 20-page chunks
2. Appends to `gs://pabst-mis-roshi-exports/nabis-api/orders.ndjson`
3. Tracks progress in `progress.json`
4. On completion: TRUNCATE + BigQuery load into `bronze_nabis_orders`

**Incremental mode:** 2 days lookback (scheduled runs)
**Full loads:** Cloud Shell only — always TRUNCATE first

### 2. Nabis CSV Manual Uploads

- **Location:** `gs://pabst-mis-roshi-exports/nabis-manual-uploads/`
- **Load script:** `~/nabis-pipeline/load_nabis_csv.py`
- **Daily upload script:** `~/nabis-pipeline/upload_nabis.sh` (7-day rolling, deduplicates via SELECT DISTINCT, MERGE on `Line_Item_Id`)
- **Load command:**
```bash
bq load --source_format=CSV --skip_leading_rows=1 --allow_quoted_newlines \
  --allow_jagged_rows --max_bad_records=500 \
  --column_name_character_map=V2 --autodetect
```
- **Process:** Load each year into `nabis_staging_YYYY` → INSERT SELECT into `bronze_nabis_orders`
- **Bronze row count:** 352,639 rows (2022–2026, $121.7M revenue)

### 3. Roshi (Production Data)

- **Vendor contact:** Jason West (CTO, Roshi) — email sent for full historical export
- **GCS path:** `gs://pabst-mis-roshi-exports/`
- **Table:** `ProductionBatchPerformance_Raw` (deduplicated permanent table)
- **External tables contain duplicates** — always use `_Raw` for calculations
- **Join key:** `batchNumber` (Nabis) = `Batch_Number` (Roshi)
- **Status:** 459 batches in Roshi vs. 1,083 referenced in Nabis — 657 missing COGS pending Jason's full export

### 4. IES / QuickBooks (Intuit Enterprise Suite)

- **Status:** Pending Arden COA sign-off before go-live
- **Intuit app:** "PabstBrain" — Client ID: `ABiS5qgbguPoLrZIHvDzI616ZpNx30VyKoXFJCOZILiCzK4fVv`
- **Secrets in GCP:** `qb-client-id`, `qb-client-secret`, `qb-refresh-token`, `qb-realm-id`
- **Sandbox realm:** `9341456602115923`
- **Pipeline:** `~/qb-pipeline/nabis_to_qb.py`
- **Test:** Receipt posted successfully ($455.28, ST IDES MAUI MANGO, Batch PL-MT038)
- **CRITICAL:** QBO inventory start date must be set to 2022-01-01 or earlier before syncing historical POs

---

## REVENUE FORMULA (CONFIRMED)

```
Gross Revenue (List)    = units × standardPricePerUnit          ← stable baseline, no SKU table needed
Invoiced Gross          = SUM(lineItemSubtotal)                  ← excludes penny-out records
Penny-Out Promos        = records where pricePerUnit ≤ $0.01     ← ~16,409 records, ~$2M, shown as contra-revenue KPI
Total Discount          = SUM(DISTINCT orderDiscount per order) + SUM(DISTINCT creditMemo per order)
Net Revenue             = lineItemSubtotalAfterDiscount          ← use this field directly, avoids double-deduction
```

**CRITICAL EXCLUSION:** Never include intercompany transfers (LP District 11 manufacturing → Nabis warehouse) in revenue.
- Filter: `orderAction != 'TRANSFER'` AND `soldBy` does not match internal company names
- These are inventory movements, not sales. They will inflate revenue by millions if included.

---

## SILVER VIEW FILTER (CONFIRMED)

```sql
WHERE status IN ('DELIVERED', 'DELIVERED_WITH_EDITS')
  AND orderAction = 'DELIVERY_TO_RETAILER'
  AND soldBy IS NOT NULL
  AND LENGTH(soldBy) < 30
```

**Why this matters:** The old filter (`NOT IN ('CANCELLED','REJECTED')`) let SCHEDULED and IN_TRANSIT orders through, inflating revenue.

**Bronze field note:** The order number field is named `order` (backtick-escaped in SQL). Silver aliases it as `orderNumber`.

---

## P&L MODEL

```
Revenue         = lineItemSubtotalAfterDiscount (Nabis Silver)
COGS            = Cost_Per_Unit × units_sold    (Roshi — NOT Total_Cost, which is whole-batch cost)
Gross Profit    = Revenue − COGS
Net Profit      = Gross Profit − labor/overhead (IES — pending)
```

---

## CHART OF ACCOUNTS (COA) STATUS

- Master COA v1.3 sent to Arden (CFO). 39 new accounts added.
- Key accounts:
  - `4012/4013` — API clearing
  - `5011/5012` — Roshi COGS
  - `6115–6127` — Marketing split (Brand vs Field per Arden)
  - `6128–6130` — Gross-to-net by retailer
  - `6219–6223` — Tech/PabstBrain
- Credit memo treatment: `4014` (contra-revenue), `2006` (liability) — GAAP ASC 606
- Credit memos apply to current period invoices only (Arden policy)
- **Blocking:** Arden must sign off before IES go-live

---

## STREAMLIT DASHBOARD

- **Live URL:** `https://pabstbrain-l2goypx6dukf7nujajoc8y.streamlit.app`
- **GitHub repo:** `roywoo-voa/pabstbrain` (private)
- **File:** `sales.py`
- **BigQuery credentials:** Streamlit secrets `[gcp_service_account]`
- **Service account:** `streamlit-reader@amplified-name-490015-e0.iam.gserviceaccount.com`
- **KPI card order:** Gross (List) → Invoiced Total → Total Discount → Penny-Out Promos → Net Revenue → Orders → Active Accts → Accts <$1K → Units

---

## KEY PEOPLE

| Person | Role | Contact | Notes |
|--------|------|---------|-------|
| Arden | CFO | — | He/him. Security contact for PabstBrain infra. Awaiting COA sign-off. |
| Jason West | CTO, Roshi | — | Manufacturing/production data. Email sent for full historical export. |
| Cyrus Pirasteh | External (Anvil dashboard) | cyruspirasteh@gmail.com | Being wound down. Anvil data reliable from Feb 15, 2025+. |
| Matthew Stumpf | Nabis Partnerships Manager | — | Confirmed API field definitions. |
| Marissa Ramirez | AR Specialist | — | Will receive AR aging alerts. |
| Bryant | Sales Rep | — | — |

---

## RETAILER CREDIT POLICY (TO IMPLEMENT IN IES)

| Age | Action |
|-----|--------|
| Current / 1–30 days | Normal |
| 31–60 days | COD + 20% of new order |
| 61–90 days | Credit hold, no new orders |
| 90+ days | Collections + fridge retrieval |
| Exception | One-in-One-Out: deliver new, collect prior invoice on delivery |

---

## REMAINING WORK (STATUS)

| # | Item | Status |
|---|------|--------|
| 1 | Bronze Nabis load 2022–2026 | ✅ Done |
| 2 | Cloud Function chunked pipeline | ✅ Done |
| 3 | Cloud Scheduler running | ✅ Done |
| 4 | Silver view built | ✅ Done |
| 5 | Gold view built | ✅ Done |
| 6 | Sales dashboard built and deployed | ✅ Done |
| 7 | Silver formula — confirm penny-out treatment with Cyrus | ⏳ Pending |
| 8 | AR aging BigQuery view + automated alerts | ⏳ Not started |
| 9 | IES connection (full P&L) | ⏳ Blocking on Arden COA sign-off |
| 10 | Full Nabis → IES pipeline (retailers as customers, orders as receipts by soldBy) | ⏳ Not started |
| 11 | Roshi full historical export (657 missing COGS batches) | ⏳ Awaiting Jason |
| 12 | Email reports (PDF/CSV scheduled delivery) | ⏳ Not started |
| 13 | IES payment terms + credit policy | ⏳ Not started |
| 14 | Claude Q&A layer on Gold tables | Future phase |
| 15 | HubSpot CRM integration | Future phase |

---

## HARD-WON LESSONS (DO NOT REPEAT THESE MISTAKES)

### Nabis API
- **No server-side date filtering.** `startDate` param is completely non-functional. Must pull all pages, filter client-side.
- **No webhooks.** Poll only.
- **`updatedDate` not `deliveryDate`** — use updatedDate for delete/filter logic to catch post-delivery updates (payments, credit memos).
- **API returns camelCase; CSV exports use space-separated column names** (`Delivery Date`, `Sold By`).
- **`Sold By` in CSV = full name; in API = email address.** These are not the same field.
- **`orderDiscount` and `orderCreditAmount` repeat on every line item.** Must deduplicate by order number before summing or you'll overcount discounts by 10–20x.
- **`Discounted Line Item Total` ≈ `Line Item Subtotal`** — it is NOT true net. `lineItemSubtotalAfterDiscount` is the correct net field.

### Roshi
- **External tables have duplicates** — hourly full-history CSV uploads cause this. Always `SELECT DISTINCT`. Always use `_Raw` table for any calculation.
- **COGS = `Cost_Per_Unit × units`.** Never `Total_Cost` (that's whole-batch cost, not per-unit sold).

### BigQuery
- **External tables don't support DML (TRUNCATE).** Use `bq rm -f` + `bq mk` to reset.
- **`--column_name_character_map=V2`** required for CSVs with special characters in headers.
- **`CAST(col AS STRING)`** required before TRIM/NULLIF when autodetect types a column as DATE or FLOAT64.
- **`rows` is a reserved keyword** — use `total_records` or similar.
- **UNION ALL with LIMIT requires wrapping each subquery.**
- **Multiple wildcards in GCS URIs not supported** for external tables.
- **Re-authenticate with BOTH** `gcloud auth login` AND `gcloud auth application-default login` in new sessions — these are independent and both required.

### Cloud Shell
- **Sessions expire after ~20 min inactivity.** Always run `gcloud auth application-default login` in new terminals.
- **Never use heredoc to write Python in Cloud Shell.** Use Editor or upload files.
- **Run keepalive:** `while true; do echo k; sleep 600; done &`
- **Full loads from Cloud Shell only.** Cloud Functions = incremental (2 days).
- **Always TRUNCATE before full load.** Verify syntax before running.

### Architecture Decisions
- **Cloud Functions 2nd gen over Cloud Run** — simpler, no Docker, right tool for this use case.
- **Minimize infrastructure complexity** — Roy is an accountant, not a DevOps engineer. Every added service is a maintenance burden.
- **Medallion architecture is non-negotiable** — Bronze is always raw/immutable. Never transform in Bronze. Silver is clean. Gold is reporting-ready. Mixing these layers causes silent errors that are nearly impossible to debug later.

---

## TOOLS & INFRASTRUCTURE REFERENCE

| Category | Tool |
|----------|------|
| Cloud | GCP: Cloud Functions 2nd gen, Cloud Scheduler, BigQuery, GCS, Secret Manager, Cloud Shell |
| Data sources | Nabis API (v2), Nabis CSV exports, Roshi CSV → GCS, IES/QuickBooks (OAuth 2.0) |
| Dashboards | Streamlit (Community Cloud), Power BI (DirectQuery → BigQuery) |
| Accounting | Intuit Enterprise Suite (IES); Class tracking; Location tracking |
| Version control | GitHub (`roywoo-voa/pabstbrain`, private) |
| Future | HubSpot CRM, Fivetran (IES → BigQuery), AI Q&A layer on Gold tables |

---

## HOW TO WORK WITH ROY

1. **Strategic-first.** Every suggestion must consider full PabstBrain architecture, not just the immediate task.
2. **One step at a time.** No last-minute changes mid-instruction. Finish the step, confirm, then move.
3. **Plain language.** Roy is an accountant. Use analogies. Explain why, not just what.
4. **Iterative.** Run commands, paste output back for diagnosis. Treat failures as signals.
5. **Never make destructive changes** (schema, infra, external systems) without explicit approval.
6. **When something is wrong, say so directly.** Don't soften it. Roy needs to make real decisions.
7. **The Elon framework applies to every suggestion.** Before adding anything, ask: what can we delete first?
