# %% [markdown]
# # GB Rail Busyness Indicator — Production Pipeline
#
# **Purpose**: produces the three components of the railway busyness indicator agreed
# with the stakeholder:
#
# 1. Passenger journey counts by travel date (day / week / month)
# 2. The same counts broken down by destination region (ITL1)
# 3. Average journey length (passenger miles), including a provisional
#    commute-like / leisure-like split by distance
#
# **Audience**: this is intended to become a published ONS statistic (part of a
# weekly/monthly "faster indicators" release, general public audience) — not an
# internal one-off analysis. That has real consequences for how this file is written:
# every methodology choice is a named, documented, easily-changed parameter rather
# than a hardcoded assumption, because the definition of "commute vs. leisure" in
# particular is explicitly provisional and expected to be revised.
#
# **How to run**: top to bottom, either as a plain Python script
# (`python busyness_indicator_pipeline.py`) or as a notebook — the `# %%` markers
# make this file directly openable as a notebook in VS Code, or convertible with
# `jupytext --to notebook busyness_indicator_pipeline.py`.
#
# **What it produces**: five CSV files in `OUTPUT_DIR` (raw daily/weekly/monthly
# figures, plus indexed — base period = 100 — weekly/monthly figures), and a printed
# cost/performance/sanity-check summary at the end of the run.
#
# **Companion reading**: `busyness-indicator-documentation.md` covers the full
# methodology, every assumption, and every open question this pipeline currently
# depends on. `analysis-notes.md` and `data-dictionary.md` cover how we got here.
#
# **Change log** (update this as the pipeline evolves — see "designed to change"
# below):
# - v1 — initial build, three indicators, provisional commute threshold.

# %%
# ============================================================================
# 0. IMPORTS
# ============================================================================
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

# %% [markdown]
# ## 1. Configuration
#
# **Everything that might reasonably need to change lives here, and nowhere else.**
# The stakeholder was explicit that the commute/leisure distance threshold is a
# starting proposal, not a final answer, and that new requests are expected — the
# whole point of collecting every tunable value in one place at the top of the file
# is that changing the analysis later means editing a number here, not hunting
# through query logic further down.

# %%
# ============================================================================
# 1. CONFIGURATION
# ============================================================================

VIEW_FQN = "ons-ids-data-rfasta-prod.views.prices_lennon-staged_research"

# --- Commute vs. leisure classification -------------------------------------
# PROVISIONAL. The stakeholder asked us to propose a starting definition while
# they work on their own, on the understanding this must stay trivially
# adjustable. A journey with ticket_miles <= this threshold is treated as
# "commute-like"; anything longer is "leisure-like". 30 miles was chosen as a
# round, defensible starting point (roughly the outer edge of a typical UK
# metro-area commute) — NOT derived from a rigorous review of commuting
# literature, and should be revisited once the stakeholder's own work lands.
COMMUTE_DISTANCE_THRESHOLD_MILES: float = 30.0

# --- Date range ---------------------------------------------------------------
# None = process the full available history. The stakeholder confirmed "as far
# back as the data goes" — resolved at runtime from the data itself (see the
# sanity-check section below) rather than hardcoded, since the true earliest
# `collection_date` isn't something this file should need to know in advance.
DATE_RANGE_START: date | None = None
DATE_RANGE_END: date | None = None

# --- Indexing -----------------------------------------------------------------
# Base period for the indexed output (index = 100 in this period). None = use
# the first complete period in the processed range. Format: 'YYYY-MM' for
# monthly, ISO year-week (e.g. '2019-W27') for weekly — set by the indexing
# function itself if left as None.
INDEX_BASE_PERIOD: str | None = None

# --- Row-level filtering -------------------------------------------------------
# Fare product groups excluded from the passenger-journey count. This mirrors
# ONS's own published cleaning methodology for this exact dataset (APCP-T(22)02,
# Annex B): "N/A" covers non-journey products (car parking, seat reservations);
# "Other tickets" covers obscure non-consumer products. Neither represents a
# real passenger journey, so both are excluded from every indicator here.
EXCLUDED_FARE_PRODUCT_GROUPS = ["N/A", "Other Tickets"]

# --- Disclosure control ---------------------------------------------------------
# This pipeline does NOT perform statistical disclosure control (cell
# suppression/masking) — the stakeholder confirmed that happens in a separate,
# later step. What this pipeline DOES do is flag any output cell with a row
# count below this threshold, so whoever runs SDC downstream knows exactly
# where to look. Flagging, not masking.
LOW_COUNT_FLAG_THRESHOLD: int = 10

# --- Cost safety ----------------------------------------------------------------
# Refuse to actually run any single query whose dry-run estimate exceeds this
# many GB. This is a deliberate circuit breaker: if a future edit to this
# pipeline accidentally produces a much more expensive query (e.g. an
# unintended cross join, or a filter that stops pruning), this stops it before
# it runs, rather than after the bill arrives.
MAX_QUERY_GB: float = 100.0

# --- Cost estimation ------------------------------------------------------------
# BigQuery on-demand list price, used only to produce an ESTIMATED cost in the
# sanity-check output. This has never been confirmed against ONS IDS's actual
# billing model (it may be a shared slot reservation rather than per-byte
# billing — see analysis-notes.md, Phase 0) — treat every £/$ figure this
# pipeline prints as an estimate assuming on-demand pricing, not a real bill.
BIGQUERY_ON_DEMAND_USD_PER_TB: float = 6.25

# --- Output -----------------------------------------------------------------------
OUTPUT_DIR = Path("./output")
OUTPUT_DIR.mkdir(exist_ok=True)

# %% [markdown]
# ## 2. Cost, performance, and correctness tracking
#
# Every BigQuery call this pipeline makes goes through `run_query()` below,
# never a bare `client.query()`. That single choke point is what makes it
# possible to report, at the end of a run: how much data every step touched,
# how long it took, what it likely cost, and to refuse to run anything that
# blows past `MAX_QUERY_GB` without a human noticing.
#
# The pattern — dry-run first, then run for real — is the same discipline used
# throughout the exploratory phase of this project (see Principle 1 in
# `analysis-notes.md`), just wrapped into a reusable function instead of typed
# out by hand each time.

# %%
# ============================================================================
# 2. COST & PERFORMANCE TRACKING
# ============================================================================


@dataclass
class QueryRun:
    """A record of one query execution, kept for the end-of-run summary."""

    name: str
    rows_returned: int
    bytes_billed: int
    elapsed_seconds: float
    estimated_cost_usd: float


@dataclass
class CostTracker:
    """Accumulates every QueryRun made during this pipeline execution."""

    runs: list[QueryRun] = field(default_factory=list)

    def record(self, run: QueryRun) -> None:
        self.runs.append(run)

    def summary_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "query": r.name,
                    "rows_returned": r.rows_returned,
                    "gb_billed": round(r.bytes_billed / 1024**3, 3),
                    "seconds": round(r.elapsed_seconds, 1),
                    "est_cost_usd": round(r.estimated_cost_usd, 4),
                }
                for r in self.runs
            ]
        )

    def print_summary(self) -> None:
        df = self.summary_df()
        total_gb = df["gb_billed"].sum() if not df.empty else 0.0
        total_cost = df["est_cost_usd"].sum() if not df.empty else 0.0
        total_seconds = df["seconds"].sum() if not df.empty else 0.0
        print("\n" + "=" * 72)
        print("PIPELINE COST & PERFORMANCE SUMMARY")
        print("=" * 72)
        if not df.empty:
            print(df.to_string(index=False))
        print("-" * 72)
        print(
            f"Total: {total_gb:,.3f} GB billed | "
            f"~${total_cost:,.4f} estimated (on-demand pricing, unconfirmed "
            f"billing model — see config notes) | "
            f"{total_seconds:,.1f}s query time"
        )
        print("=" * 72 + "\n")


def bytes_to_estimated_usd(num_bytes: int) -> float:
    tb = num_bytes / 1024**4
    return tb * BIGQUERY_ON_DEMAND_USD_PER_TB


def run_query(
    client: bigquery.Client,
    tracker: CostTracker,
    sql: str,
    name: str,
    query_parameters: list | None = None,
) -> pd.DataFrame:
    """
    Dry-run a query first, refuse to proceed if it's implausibly expensive,
    then run it for real, time it, and record everything in `tracker`.

    This is the single choke point every BigQuery call in this pipeline goes
    through — see the section 2 docstring above for why.
    """
    base_config = bigquery.QueryJobConfig(query_parameters=query_parameters or [])

    dry_run_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters or [],
        dry_run=True,
        use_query_cache=False,
    )
    dry_run_job = client.query(sql, job_config=dry_run_config)
    estimated_gb = dry_run_job.total_bytes_processed / 1024**3
    print(f"[{name}] dry run: ~{estimated_gb:,.2f} GB estimated")

    if estimated_gb > MAX_QUERY_GB:
        raise RuntimeError(
            f"[{name}] refusing to run: estimated {estimated_gb:,.2f} GB exceeds "
            f"MAX_QUERY_GB={MAX_QUERY_GB} GB. If this is genuinely expected, raise "
            f"MAX_QUERY_GB in the config section deliberately — don't silently "
            f"bypass this."
        )

    start = time.time()
    job = client.query(sql, job_config=base_config)
    df = job.to_dataframe()
    elapsed = time.time() - start

    bytes_billed = job.total_bytes_billed or 0
    tracker.record(
        QueryRun(
            name=name,
            rows_returned=len(df),
            bytes_billed=bytes_billed,
            elapsed_seconds=elapsed,
            estimated_cost_usd=bytes_to_estimated_usd(bytes_billed),
        )
    )
    print(
        f"[{name}] done: {len(df):,} rows, "
        f"{bytes_billed / 1024**3:,.2f} GB billed, {elapsed:,.1f}s"
    )
    return df


client = bigquery.Client()
tracker = CostTracker()

# %% [markdown]
# ## 3. Sanity checks — establish what we're actually working with
#
# Before computing anything, confirm the basic shape of the data this run will
# process: how many rows, what date range, and how much of the raw table the
# exclusion filter removes. Stakeholders reviewing this pipeline want to see
# these numbers up front, not discover them by reading query results — and
# it's also the cheapest possible check that nothing has silently changed
# about the underlying data since this pipeline was last run (recall: this
# table is live and growing — see `analysis-notes.md`).

# %%
# ============================================================================
# 3. SANITY CHECKS
# ============================================================================

sanity_sql = f"""
SELECT
  COUNT(*) AS total_rows,
  MIN(DATE(collection_date)) AS earliest_travel_date,
  MAX(DATE(collection_date)) AS latest_travel_date,
  COUNTIF(pro_fpg_description IN UNNEST(@excluded_fpg)) AS rows_excluded_by_fpg_filter,
  -- Confirms the assumption this pipeline relies on: that passenger_journeys
  -- carries the same +/- sign as number_of_tickets for refund rows, so that
  -- SUM(passenger_journeys) nets refunds out the same way SUM(number_of_tickets)
  -- does. This was a reasonable but unverified assumption when this pipeline
  -- was first written — checked here on every run rather than assumed silently.
  COUNTIF(number_of_tickets = -1 AND passenger_journeys > 0) AS refund_sign_mismatch_count
FROM `{VIEW_FQN}`
"""

sanity_df = run_query(
    client,
    tracker,
    sanity_sql,
    name="sanity_checks",
    query_parameters=[
        bigquery.ArrayQueryParameter(
            "excluded_fpg", "STRING", EXCLUDED_FARE_PRODUCT_GROUPS
        ),
    ],
)
print(sanity_df.to_string(index=False))

_row = sanity_df.iloc[0]
if _row["refund_sign_mismatch_count"] > 0:
    print(
        f"\n⚠ WARNING: {_row['refund_sign_mismatch_count']:,} refund rows have a "
        f"POSITIVE passenger_journeys value. The assumption that SUM(passenger_journeys) "
        f"nets out refunds the same way SUM(number_of_tickets) does may not hold for "
        f"these rows — this pipeline's journey counts could be a small overcount. "
        f"Worth investigating before trusting the headline figures if this number is "
        f"large relative to total_rows."
    )
else:
    print("\n✓ Refund sign convention confirmed: passenger_journeys nets out correctly.")

# Resolve the actual date range to process, now that we know the data's real bounds.
_effective_start = DATE_RANGE_START or _row["earliest_travel_date"]
_effective_end = DATE_RANGE_END or _row["latest_travel_date"]
print(f"\nProcessing travel_date range: {_effective_start} to {_effective_end}")
print(
    f"Fare-product filter will exclude {_row['rows_excluded_by_fpg_filter']:,} of "
    f"{_row['total_rows']:,} rows "
    f"({100 * _row['rows_excluded_by_fpg_filter'] / _row['total_rows']:.1f}%)."
)

# %% [markdown]
# ## 4. Pull the base data — one query, daily × region grain
#
# Everything downstream (day/week/month rollups, raw and indexed views) is
# derived locally from a single BigQuery pull at the finest grain requested
# (daily, by destination region). This mirrors a discipline established
# throughout the exploratory phase of this project: minimize the number of
# full-table passes, not the number of things you learn from each one. Pulling
# three separate day/week/month queries would cost roughly three times as much
# for the same information, since week/month are just sums over day.
#
# `destination_region_code`/`destination_region_name` being NULL (16.5% of rows,
# per prior profiling) is bucketed here as an explicit "Unknown region" category
# — never silently dropped.

# %%
# ============================================================================
# 4. BASE QUERY — daily x region grain
# ============================================================================

base_sql = f"""
SELECT
  DATE(collection_date) AS travel_date,
  COALESCE(destination_region_code, 'UNKNOWN') AS destination_region_code,
  COALESCE(destination_region_name, 'Unknown region') AS destination_region_name,

  -- Primary journey-count measure. passenger_journeys is defined (confirmed
  -- against ONS's own published data dictionary) as the actual number of
  -- passenger journeys represented by the row -- not the number of tickets --
  -- so this, not number_of_tickets, is the right field for "count of
  -- passenger journeys". Refunds (number_of_tickets = -1) net out
  -- automatically via the sign convention checked in the sanity-check section.
  SUM(passenger_journeys) AS net_passenger_journeys,

  -- Secondary measure, kept alongside for cross-validation against
  -- net_passenger_journeys -- the two should move together; if they diverge
  -- sharply in a future run, that's worth investigating before trusting either.
  SUM(number_of_tickets) AS net_tickets,

  -- Numerator for the passenger-weighted average journey length.
  SUM(ticket_miles * passenger_journeys) AS total_passenger_miles,

  -- Unweighted average: the average length of a ticket PRODUCT, not of an
  -- actual passenger's journey. Reported alongside the weighted figure, both
  -- clearly labeled, per the agreed methodology (see documentation).
  AVG(ticket_miles) AS avg_journey_miles_unweighted,

  -- Provisional commute/leisure split by distance -- see
  -- COMMUTE_DISTANCE_THRESHOLD_MILES in the config section.
  SUM(IF(ticket_miles <= @commute_threshold, passenger_journeys, 0)) AS commute_like_journeys,
  SUM(IF(ticket_miles > @commute_threshold, passenger_journeys, 0)) AS leisure_like_journeys,

  -- Raw row count per cell -- used below to flag (not mask) low-count cells
  -- for downstream statistical disclosure control review.
  COUNT(*) AS row_count

FROM `{VIEW_FQN}`
WHERE pro_fpg_description NOT IN UNNEST(@excluded_fpg)
  AND DATE(collection_date) BETWEEN @start_date AND @end_date
GROUP BY travel_date, destination_region_code, destination_region_name
ORDER BY travel_date, destination_region_code
"""

base_df = run_query(
    client,
    tracker,
    base_sql,
    name="base_daily_by_region",
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "commute_threshold", "FLOAT64", COMMUTE_DISTANCE_THRESHOLD_MILES
        ),
        bigquery.ArrayQueryParameter(
            "excluded_fpg", "STRING", EXCLUDED_FARE_PRODUCT_GROUPS
        ),
        bigquery.ScalarQueryParameter("start_date", "DATE", _effective_start),
        bigquery.ScalarQueryParameter("end_date", "DATE", _effective_end),
    ],
)

# Weighted average computed locally rather than in SQL, since it's a simple
# division of two already-summed columns -- no reason to pay for BigQuery
# compute to do arithmetic pandas can do for free on data already in memory.
base_df["avg_journey_miles_weighted"] = (
    base_df["total_passenger_miles"] / base_df["net_passenger_journeys"]
)
base_df["low_count_flag"] = base_df["row_count"] < LOW_COUNT_FLAG_THRESHOLD

print(f"\nBase table: {len(base_df):,} rows (travel_date x region)")
print(
    f"{base_df['low_count_flag'].sum():,} of these are flagged as low-count "
    f"(row_count < {LOW_COUNT_FLAG_THRESHOLD}) for downstream disclosure-control "
    f"review -- not masked here, per the agreed scope of this pipeline."
)

# %% [markdown]
# ## 5. Roll up to week and month, all done locally
#
# ISO weeks (Monday-start) are used throughout, matching standard UK/European
# statistical convention rather than a Sunday-start week.

# %%
# ============================================================================
# 5. WEEKLY / MONTHLY ROLLUPS
# ============================================================================

_measure_cols = [
    "net_passenger_journeys",
    "net_tickets",
    "total_passenger_miles",
    "commute_like_journeys",
    "leisure_like_journeys",
    "row_count",
]


def roll_up(df: pd.DataFrame, period_col: str) -> pd.DataFrame:
    grouped = (
        df.groupby([period_col, "destination_region_code", "destination_region_name"])[
            _measure_cols
        ]
        .sum()
        .reset_index()
    )
    grouped["avg_journey_miles_weighted"] = (
        grouped["total_passenger_miles"] / grouped["net_passenger_journeys"]
    )
    grouped["low_count_flag"] = grouped["row_count"] < LOW_COUNT_FLAG_THRESHOLD
    return grouped


daily_df = base_df.copy()

weekly_df = base_df.copy()
weekly_df["iso_week"] = pd.to_datetime(weekly_df["travel_date"]).dt.strftime("%G-W%V")
weekly_df = roll_up(weekly_df, "iso_week")

monthly_df = base_df.copy()
monthly_df["month"] = pd.to_datetime(monthly_df["travel_date"]).dt.strftime("%Y-%m")
monthly_df = roll_up(monthly_df, "month")

print(f"Daily rows: {len(daily_df):,} | Weekly rows: {len(weekly_df):,} | Monthly rows: {len(monthly_df):,}")

# %% [markdown]
# ## 6. Index the weekly and monthly series (base period = 100)
#
# Follows the same convention ONS's own Prices Division paper on this exact
# dataset uses for its own rail fares index (e.g. "Jan 2019 = 100") — each
# region's series is indexed independently, against its own value in the base
# period, so regional comparisons are about relative *change*, not absolute
# journey volume.

# %%
# ============================================================================
# 6. INDEXING
# ============================================================================


def index_series(df: pd.DataFrame, period_col: str, base_period: str | None) -> pd.DataFrame:
    df = df.copy()
    resolved_base = base_period or sorted(df[period_col].unique())[0]

    base_values = (
        df[df[period_col] == resolved_base]
        .set_index("destination_region_code")["net_passenger_journeys"]
    )

    def _index(row):
        base = base_values.get(row["destination_region_code"])
        if not base:
            return None
        return 100.0 * row["net_passenger_journeys"] / base

    df["index_base_period"] = resolved_base
    df["journeys_index"] = df.apply(_index, axis=1)
    return df


weekly_indexed_df = index_series(weekly_df, "iso_week", INDEX_BASE_PERIOD)
monthly_indexed_df = index_series(monthly_df, "month", INDEX_BASE_PERIOD)

print(f"Weekly index base period: {weekly_indexed_df['index_base_period'].iloc[0]}")
print(f"Monthly index base period: {monthly_indexed_df['index_base_period'].iloc[0]}")

# %% [markdown]
# ## 7. Export
#
# Five files: raw figures at all three granularities, plus indexed figures at
# the two granularities that are actually meaningful to index (a daily index
# would be dominated by day-of-week noise rather than showing a genuine trend).

# %%
# ============================================================================
# 7. EXPORT
# ============================================================================

_exports = {
    "busyness_indicator_raw_daily.csv": daily_df,
    "busyness_indicator_raw_weekly.csv": weekly_df,
    "busyness_indicator_raw_monthly.csv": monthly_df,
    "busyness_indicator_indexed_weekly.csv": weekly_indexed_df,
    "busyness_indicator_indexed_monthly.csv": monthly_indexed_df,
}

for filename, df in _exports.items():
    out_path = OUTPUT_DIR / filename
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df):,} rows)")

# %% [markdown]
# ## 8. Final summary
#
# Everything a reviewer would want to see at a glance: how much data this run
# touched, how long it took, what it likely cost, and how many rows didn't
# make it into the output because of the fare-product filter or the
# unknown-region bucket.

# %%
# ============================================================================
# 8. FINAL SUMMARY
# ============================================================================

print(f"\nRun completed: {datetime.now().isoformat(timespec='seconds')}")
print(f"Travel date range processed: {_effective_start} to {_effective_end}")
print(f"Commute/leisure threshold used: {COMMUTE_DISTANCE_THRESHOLD_MILES} miles (provisional)")
tracker.print_summary()
