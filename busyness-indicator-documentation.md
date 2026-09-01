# GB Rail Busyness Indicator — Technical Documentation

**Status**: v1, built for methodology approval. Intended to become a recurring
published statistic once approved — not a one-off analysis.
**Audience for this document**: anyone who needs to understand, run, review, or modify
`busyness_indicator_pipeline.py` without having been part of the investigation that
led to it. If you want that full investigation, see `analysis-notes.md` and
`data-dictionary.md` in this same folder.
**Audience for the indicator itself**: the general public, as part of ONS's weekly and
monthly "faster indicators" headline and full releases.

---

## 1. What this produces

Three components of a single railway busyness indicator:

1. **Passenger journey counts by travel date** — daily, rolled up to weekly and
   monthly.
2. **The same counts broken down by destination region** (ITL1 — the post-Brexit
   successor naming for what was NUTS1).
3. **Average journey length** (passenger miles), reported two ways (see §3.3), plus a
   provisional split of journeys into "commute-like" and "leisure-like" by distance —
   intended to help identify shifts between commuting and leisure travel over time.

Output: five CSV files (raw daily/weekly/monthly, indexed weekly/monthly — see §3.5),
plus a printed run summary covering data volume, execution time, and estimated cost.

## 2. Where the data comes from, and what that implies

**Source**: `ons-ids-data-rfasta-prod.views.prices_lennon-staged_research`, a BigQuery
authorized view (ONS Integrated Data Service) over GB rail ticket sales/refund data
from LENNON, the rail industry's fares and settlement system. ~3.7 billion rows, 82
columns exposed (a narrower set than the full LENNON schema — some fields, including
any monetary/price values, are withheld from this view under statistical disclosure
control, confirmed directly by the data owner).

**This table is live and growing.** During the investigation that preceded this
pipeline, the row count increased several times within a single working session. Every
run of this pipeline is a snapshot as of when it's executed — there is currently no
confirmed ingestion schedule (see §6, question 1). Re-running this pipeline on
different days will not necessarily produce identical historical figures for dates
already processed, if late-arriving or corrected data lands against them.

## 3. Methodology, field by field

### 3.1 Travel date → `collection_date`

Confirmed directly by the data owner: `collection_date` in this view is the raw LENNON
field `travel_date`, renamed. Their own description: *"Date when the ticket was
collected. This is the closest date we have to the day the ticket was used."*

**This is an approximation, stated as such by the people who built it — not an exact
travel date.** Empirical testing (see `analysis-notes.md`) found it diverges from the
ticket's issue date specifically for advance-purchase ticket types (e-Ticket, M-ticket),
and essentially never falls before the issue date — both consistent with it tracking
something real about ticket use, not simply copying another field. The exact derivation
methodology for e-tickets/mobile tickets specifically has been asked of the data owner
but not yet answered (see §6, question 1) — any publication using this indicator should
carry the data owner's own caveat ("closest approximation") rather than imply
exactness.

### 3.2 Destination region → `destination_region_code` / `destination_region_name`

Already present in the source data — no postcode-to-region mapping was built or is
needed. Validated by cross-checking five well-known destinations (`LONDON BR` →
`E12000007`/London, `EDINBURGH` → `S99999999`/Scotland, etc.) with zero scatter.

**16.5% of rows have no region assigned.** These are bucketed as an explicit `UNKNOWN`
region code / `"Unknown region"` name in the pipeline's output — never silently
dropped from the totals. Anyone using this output for a regional breakdown needs to be
aware this bucket exists and decide how to treat it (include as its own row, exclude
from regional comparisons, etc.) — the pipeline does not make that decision on their
behalf.

### 3.3 Passenger journey count → `passenger_journeys`, not `number_of_tickets`

`passenger_journeys` is documented (confirmed against ONS's own published data
dictionary) as *"the number of passenger journeys represented by the transaction —
calculated by multiplying the number of people by the journey factor, or for season
tickets, the Season Ticket Journey Weightings."* That is a direct match for "count of
passenger journeys" as requested — `number_of_tickets` counts tickets, not journeys
(e.g. one return ticket = one ticket, two journeys via journey factor).

Refunds (`number_of_tickets = -1`) are expected to net out automatically via
`SUM(passenger_journeys)`, on the assumption that `passenger_journeys` carries the
same sign convention as `number_of_tickets` for refund rows. **This assumption is
checked on every pipeline run** (§4 of the pipeline script, "sanity checks") rather
than trusted blindly — if the check ever fails, the pipeline prints a warning and the
headline figures should not be trusted until investigated.

`number_of_tickets` is still reported alongside as a cross-validation measure — the
two should move together; a divergence between them in future data is worth
investigating before trusting either.

### 3.4 Average journey length → `ticket_miles`, reported two ways

- **Unweighted**: `AVG(ticket_miles)` — the average length of a ticket *product*.
  Every row counts equally regardless of how many passengers it represents.
- **Weighted**: `SUM(ticket_miles × passenger_journeys) / SUM(passenger_journeys)` —
  the average length actually experienced by a passenger journey. A group booking
  contributes proportionally more to this figure than a single-passenger ticket does.

Both are reported, clearly labeled, in every output file — this is a deliberate
choice rather than picking one, since they answer genuinely different questions and
collapsing them into one number would hide that difference.

### 3.5 Commute-like / leisure-like split, and indexing

Journeys are split by a single distance threshold —
`COMMUTE_DISTANCE_THRESHOLD_MILES` in the pipeline's configuration section, currently
**30 miles**. This is explicitly a **provisional proposal**, not a validated
definition: chosen as a round, defensible starting point, not derived from a formal
review of commuting patterns. The stakeholder has confirmed they may develop their own
definition in parallel, and that this pipeline's threshold must stay trivially
adjustable — it is a single named constant, used consistently, and nothing else in the
pipeline needs to change if it's revised.

Indexed output (base period = 100) follows the exact convention used in ONS's own
published rail fares index methodology for this dataset — each region's series is
indexed independently against its own value in the base period, so index values
represent relative change within a region, not comparable absolute volumes between
regions.

### 3.6 Scope decisions carried into this pipeline

- **TfL contactless/PAYG activity and Trainline-resold tickets are included, not
  filtered out.** These represent roughly 28% and 29% of the underlying table
  respectively. This was a deliberate decision to make the *scale* of a scoping choice
  visible rather than make the choice silently — worth flagging prominently in any
  published commentary, since a reader might reasonably assume "railway busyness"
  means National Rail specifically.
- **`pro_fpg_description IN ('N/A', 'Other Tickets')` rows are excluded**, matching
  ONS's own published cleaning methodology for this dataset (their Jan 2022 paper,
  Annex B) — these represent car parking, seat reservations, and other non-journey
  products, not real passenger travel.
- **Refunds are netted out**, not counted as journeys, since a refunded ticket
  represents a journey that didn't happen.

## 4. Disclosure control — what this pipeline does and does not do

This pipeline **flags** any output cell (a specific date/region combination) built
from fewer than `LOW_COUNT_FLAG_THRESHOLD` (currently 10) underlying rows, via a
`low_count_flag` column in every output file. **It does not suppress, round, or mask
anything.** Per the stakeholder's own confirmation, statistical disclosure control
(the actual masking of small/identifying cells, consistent with ONS's Code of
Practice) is handled in a separate, later step in the publication process. Anyone
consuming this pipeline's raw output directly, before that step, should not treat it
as disclosure-safe for public release on its own.

## 5. How to run it

1. Requires a JupyterLab/Python environment with `google-cloud-bigquery` and `pandas`
   installed, and read access to the source view.
2. Open `busyness_indicator_pipeline.py` directly as a notebook (the `# %%` markers
   make this work natively in VS Code), or convert it with
   `jupytext --to notebook busyness_indicator_pipeline.py`, or just run it as a plain
   script: `python busyness_indicator_pipeline.py`.
3. Adjust anything in the Configuration section (§1 of the script) before running if
   needed — that section is the only part of the file intended to be edited for
   routine changes (a new distance threshold, a narrower date range, a different
   low-count flag threshold).
4. Output lands in `./output/` as five CSV files; a cost/performance/sanity-check
   summary prints to the console at the end of the run.

**Cost expectation**: the pipeline makes two BigQuery queries total (one sanity-check
query, one main data pull) — every other transformation happens locally in pandas, at
no BigQuery cost. Actual GB billed and estimated cost are printed at the end of every
run; treat the dollar figure specifically as an estimate against standard on-demand
pricing, since ONS IDS's actual billing model (per-byte vs. a shared slot reservation)
has not been confirmed.

## 6. Open questions this pipeline currently depends on

Sent to the data team; not yet answered. None of these block running the pipeline —
each is noted here with what changes if the answer turns out to differ from the
current working assumption, so this document stays honest about what's confirmed
versus assumed.

1. **The exact `travel_date` derivation methodology, especially for e-tickets/mobile
   tickets.** *If it turns out to be materially different from "closest available
   proxy to actual use," the entire day/week/month axis of indicator 1 may need
   revisiting — this is the single most consequential open question for this
   pipeline.*
2. **Ingestion cadence and window** (how often the source table refreshes, and whether
   each load only contains new/previous-day data or also corrections to older dates).
   *Affects how often this pipeline should actually be re-run (daily vs. weekly), and
   whether historical output files need periodic re-generation to pick up late
   corrections rather than being treated as final once produced.*
3. **`refund_reason` code meanings.** *Not currently used by this pipeline at all —
   only relevant if a future request wants refund reasons broken out (e.g.
   distinguishing cancelled journeys from operational disruption refunds).*
4. **Why ~16% of refund rows lack a `refund_date_of_issue`.** *Not currently used by
   this pipeline — relevant only if a future version needs to date-attribute refunds
   themselves, rather than just netting them out of the sale-side count as now.*
5. **What `railcard_type` is actually meant to capture**, given it doesn't correlate
   with `ticket_status_code`/`discount_code` the way its name suggests. *Not currently
   used by this pipeline — would matter if a future request wants a railcard-specific
   breakdown.*
6. **What `transaction_number` actually represents**, given its cardinality is far too
   low for a unique transaction ID. *Not currently used by this pipeline at all.*

## 7. This pipeline is designed to change

Every methodology choice above is a named parameter, not a buried assumption inside a
query — that's deliberate, since both the stakeholder and the data team have already
signaled more requests and answers are coming. When a new request arrives:

- A new distance threshold, date range, or low-count flag value → edit the
  Configuration section only.
- A new breakdown dimension (e.g. by fare product group, by refund status) → extend
  the `GROUP BY` in the base query and the `_measure_cols` list — the rollup/indexing/
  export logic downstream is written to work off whatever measure columns exist,
  without needing its own changes.
- A confirmed answer to one of the open questions in §6 that changes a methodology
  decision → update both this document's relevant section and the corresponding
  comment in the pipeline script together, so they never drift out of sync with each
  other.

Update the change log at the top of `busyness_indicator_pipeline.py` with every
methodology-affecting change, not just bug fixes — a future reader (or reviewer)
should be able to tell from that log alone what changed and why, without diffing code.
