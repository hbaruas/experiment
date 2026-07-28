# VAT-MBS Workbook — Manual Test Cases

For human testers. No coding required — every test is done by opening the workbook, going to a named sheet/cell, and typing a short formula (or just reading a value) into a nearby blank cell. Test cases are grouped in two families:

- **Section 1** — every specific number quoted in `methadology.md`/the Decision Paper, checked against the workbook.
- **Section 2** — whether the sheets that were *built* in this workbook (growth rates, comparisons, etc. — as opposed to sheets *downloaded directly from CORD*) can be recreated by hand from their stated inputs.

## How to use this document

For each test: go to the sheet named, follow the steps (usually: click an empty cell off to the side, e.g. column `Z`, type the given formula, press Enter), and compare what you get to the **Expected Result**. Record your own result and Pass/Fail in the blank columns. If a test doesn't match, don't "fix" anything — just record the actual number and flag it; several tests in this pack are already known to mismatch and are marked as such, with the likely reason given.

A blank summary log is provided at the end to collect every result in one place.

## Assignment — split for 2 testers

The two sections are different *kinds* of check, so the split is by section rather than by splitting either section in half — each tester stays in one mode of working rather than switching back and forth:

| Tester | Covers | Test IDs | What it involves |
|---|---|---|---|
| **Tester 1** | Section 1 — numbers quoted in the paper | TC-1 – TC-9 | 9 short, independent checks: mostly `SUMIF`/`COUNTIF` on one column at a time. Quicker per-test, no dependency between tests. |
| **Tester 2** | Section 2 — can the built sheets be reproduced by hand | TC-10 – TC-19 | 10 tests that form one connected worked example (the same industry/year carried through the whole pipeline) — more manual arithmetic per test, but each step is given the exact numbers it needs, so no need to wait on a prior step. |
| **Both** | TC-20 (generalisation) | TC-20a (Tester 1), TC-20b (Tester 2) | Each tester picks one additional industry/year of their own choosing and repeats TC-12–16 for it — a quick, independent extra spot-check on top of their own section. |

This keeps the two workloads roughly even (9 quick tests vs. 10 more involved ones) and means neither tester needs to read the other section's setup to get started. If you'd rather split some other way (e.g. straight down the middle regardless of section), the two sections don't depend on each other, so TC-1–TC-9 and TC-10–TC-19 can equally be reassigned as two blocks of ~9–10 to any split you prefer.

---

## Section 1 — Every number quoted in the paper *(Tester 1)*

All formulas below use a bounded row range (`3:114` or `2:113`, depending on sheet), **not** a whole-column reference like `F:F`. This matters: `Rev Comparison Hybrid vs MBS` (and `VAT Matrix`) each have an extra **totals row sitting below the last industry** (row 115), which would silently get counted twice if you used a whole-column formula. Please use the ranges exactly as given.

### TC-1: Whole-economy quality falls in 2020 and 2021

- **Claim (intro to Results section):** *"VAT appears to experience a dramatic fall in quality in 2020 and again in 2021."*
- **Sheet:** `Fig 1`
- **Steps:** Read row 2, columns B–H (2017–2023).
- **Expected result:** 2020 and 2021 are the only two negative values, and 2021 is the most negative of all seven years.
- **Formula (optional, to double check Fig 1 itself against its source):** in `Rev Comparison Hybrid vs MBS`, empty cell: `=SUM(F3:F114)` → should equal Fig 1's 2021 figure.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|
| | | | | |

### TC-2: 2021 — total of all negative M values = −384.0, across 44 industries

- **Sheet:** `Rev Comparison Hybrid vs MBS`, column F (2021)
- **Steps:** In an empty cell, type:
  - `=SUMIF(F3:F114,"<0")`
  - `=COUNTIF(F3:F114,"<0")`
- **Expected result:** `-384.0` and `44`

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|
| | | | | |

### TC-3: 2021 — 8 industries below −10 sum to −239.7 (62.4% of the total negative)

- **Sheet:** `Rev Comparison Hybrid vs MBS`, column F (2021)
- **Steps:**
  - `=SUMIF(F3:F114,"<-10")` → expect `-239.7`
  - `=COUNTIF(F3:F114,"<-10")` → expect `8`
  - `=SUMIF(F3:F114,"<-10")/SUMIF(F3:F114,"<0")` → expect `62.4%`

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|
| | | | | |

### TC-4: Fig. 2 table, 2021 row — 62.4% / 18.2%

- **Sheet:** `Rev Comparison Hybrid vs MBS`, column F (2021)
- **Steps:** First percentage is TC-3's last formula. Second percentage:
  - `=COUNTIF(F3:F114,"<-10")/COUNTIF(F3:F114,"<0")` → expect `18.2%`

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|
| | | | | |

### TC-5: Fig. 2 table, 2022 row — 53.4% / 15.2%

- **Sheet:** `Rev Comparison Hybrid vs MBS`, column G (2022) — repeat TC-3/TC-4's four formulas on column G instead of F.
- **Expected result:** −227.0 total negative, 46 industries, 7 industries below −10 summing to −121.2 → **53.4%** / **15.2%**

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-6: Fig. 2 table, 2020 row — 57.7% / 10.3% ⚠️ *expected partial mismatch*

- **Sheet:** `Rev Comparison Hybrid vs MBS`, column E (2020) — same four formulas on column E.
- **Expected per paper:** 57.7% / 10.3%
- **What we actually found:** concentration % reproduces (57.7%), but the count % recomputes as **11.1%**, not 10.3% (4 industries below −10 out of 36 negative, not out of ~38.8).

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-7: Fig. 2 table, 2023 row — 37.7% / 14.6% ⚠️ *expected mismatch*

- **Sheet:** `Rev Comparison Hybrid vs MBS`, column H (2023) — same four formulas on column H.
- **Expected per paper:** 37.7% / 14.6%
- **What we actually found:** recomputes as **37.2% / 13.6%** — both figures slightly off.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-8: Fig. 3 — "VAT already used", `M > 0`: 54.7% (29 out of 53)

- **Sheet:** `VAT Matrix` (column B = "Used for VAT?", column C = "Anomalous?", column D = "Mean M")
- **Steps:**
  - `=COUNTIFS(B3:B114,1,C3:C114,0)` → expect `53`
  - `=COUNTIFS(B3:B114,1,C3:C114,0,D3:D114,">0")` → expect `29`
  - Divide the two → expect `54.7%`

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-9: Fig. 3 — "VAT not used", `M > 0`: 36.3% (4 out of 11) ⚠️ *expected mismatch*

- **Sheet:** `VAT Matrix`, same columns
- **Steps:**
  - `=COUNTIFS(B3:B114,0,C3:C114,0)` → paper expects `11`
  - `=COUNTIFS(B3:B114,0,C3:C114,0,D3:D114,">0")` → expect `4`
- **What we actually found:** the count comes to **10**, not 11 (so 4/10 = 40.0%, not 36.3%). Root cause: rows for `C241T243` (row 39) and `H491_2` (row 67) are both flagged `Used=0, Anomalous=0` (i.e. should count) but column D (`Mean M`) is blank for both — because (see Section 2, and the main QA report Finding 1) neither industry has any values at all in `Rev Comparison Hybrid vs MBS` to average. One of the two most likely had a value when the paper was written and has since gone blank.
- **Extra check:** in `VAT Matrix`, confirm rows 39 and 67 show `Used=0`, `Anomalous=0`, `Mean M` = blank.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

---

## Section 2 — Can the built (non-CORD) sheets be reproduced by hand? *(Tester 2)*

Per the note on sheet roles: sheets whose name starts with a number are **downloaded directly from CORD** — there's nothing to "reproduce" about those, they're an input snapshot. Everything else in the list below is *calculated* from other sheets, and every one of these calculations is simple enough to redo by hand with a calculator or a basic Excel formula.

**Worked example used throughout this section:** industry **`10100`** (Processing and preserving of meat — a detailed CORD/SIC code) which rolls up 1:1 into the aggregate industry code **`C101`** used in `SU Levels` and everything downstream of it (no merging with any other code, which makes it a clean, unambiguous example). Year **2019** (compared to 2018), sizeband 1 initially. All expected results below have been independently verified — they are correct as of this workbook; if your result differs, that's worth flagging, not assuming you made an error.

> **Tip:** one sheet name (`" A Hybrid Levels sizeband 2"`) has a leading space in its tab. If typing a cross-sheet formula by hand, click the tab rather than typing the sheet name to avoid a `#REF!` from a mistyped name.

### TC-10: CORD raw data → sizeband annual total (T(VAT) aggregation, sizeband 1)

- **Input sheet:** `4.401HYBRIDMR1.0` (raw CORD extract, sizeband 1, Hybrid), row 8 (industry `10100`), the 12 monthly columns for 2019: **`AL8:AW8`**
- **Steps:** `=SUM(AL8:AW8)`
- **Expected result:** `317.2670248672`
- **Check against:** sheet `A Hybrid Levels sizeband 1`, row 8, column E (`2019DEC`) — should equal the sum above exactly.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-11: Five sizebands → sizeband Total (T) — includes a deliberate gotcha

- **Goal:** reproduce `A Hybrid Level sizeband T`, row 8, column E (2019) for industry `10100`. Expected value: **`20478.563052340556`**.
- **Naive approach (will NOT match):** sum row 8, column E from `A Hybrid Levels sizeband 1` (317.267) + `" A Hybrid Levels sizeband 2"` (1208.893) + `A Hybrid Levels sizeband 3` (2804.779) + `A Hybrid Levels sizeband 4` (15574.797) + `A Hybrid Levels sizeband 5` (784.344) = **20690.080** — this is **211.5 higher than the target and is expected to not match.**
- **Correct approach:** per the paper, sizebands 4 and 5 always take the **MBS** value, not a VAT/Hybrid one. The formula in `A Hybrid Level sizeband T!E8` actually reads: `='A Hybrid Levels sizeband 1'!E8 + ' A Hybrid Levels sizeband 2'!E8 + 'A Hybrid Levels sizeband 3'!E8 + 'MBS LEVELS SB4'!E8 + 'A Hybrid Levels sizeband 5'!E8` — i.e. it substitutes **`MBS LEVELS SB4`** (value `15363.28`) in place of `A Hybrid Levels sizeband 4`. Redo the sum with that substitution:
  `317.2670248672 + 1208.89338263889 + 2804.779044834466 + 15363.279999999999 + 784.3436`
- **Expected result:** `20478.563052340556` (exact match)

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

*(This is worth doing carefully even though it's designed to match in the end — the point of including the "naive" version above is to confirm your team recognises the sizeband 4/5 substitution rule rather than mistaking it for a bug.)*

### TC-12: Growth rate g(VAT) — from Hybrid Total levels

- **Sheet:** `A Hybrid Level sizeband T SU in`, row 9, industry `10100`. Column D (2018) = `19806.09075907905`, column E (2019) = `20478.563052340556` (same figure as TC-11).
- **Steps:** `=(20478.563052340556-19806.09075907905)/19806.09075907905`
- **Expected result:** `0.03395280277372498`
- **Check against:** `Hybrid A T % Growth Rates`, row 9, column E → should match exactly.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-13: Growth rate g(MBS) — from MBS Total levels

- **Sheet:** `A MBS Levels sizeband T SU in`, row 9, industry `10100`. Column D (2018) = `19027.668199999996`, column E (2019) = `20750.6888`.
- **Steps:** `=(20750.6888-19027.668199999996)/19027.668199999996`
- **Expected result:** `0.09055342892725046`
- **Check against:** `MBS A T % Growth Rates`, row 9, column E → should match exactly.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-14: Growth rate g(SU) — from SU Levels (the benchmark)

- **Sheet:** `SU Levels`, row 10, industry `C101` (the aggregate code for `10100`). Column D (2018) = `19410`, column E (2019) = `20087`.
- **Steps:** `=(20087-19410)/19410`
- **Expected result:** `0.03487892838742916`
- **Check against:** `SU A % Growth Rates`, row 10, column E → should match exactly.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-15: D(VAT) and D(MBS) — deviation from the SU benchmark

- **Steps:**
  - `D(VAT) = 100 * ABS(g(SU) - g(VAT)) = 100*ABS(0.03487892838742916 - 0.03395280277372498)` → expect `0.09261256137041846`
  - `D(MBS) = 100 * ABS(g(SU) - g(MBS)) = 100*ABS(0.03487892838742916 - 0.09055342892725046)` → expect `5.56745005398213`
- **Check against:** `Comparison - Rev Hybrid-SU`, row 11 (`C101`), column D (2019) for D(VAT); `Comparison - Rev MBS-SU`, row 11, column D for D(MBS).

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-16: M — the paper's decision metric

- **Steps:** `M = D(MBS) - D(VAT) = 5.56745005398213 - 0.09261256137041846`
- **Expected result:** `5.474837492611711`
- **Check against:** `Rev Comparison Hybrid vs MBS`, row 11 (`C101`), column D (2019).

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-17: Mean M across all 7 years, and the Anomalous flag — feeding Fig. 3

- **Sheet:** `Rev Comparison Hybrid vs MBS`, row 11 (`C101`), columns B–H (2017–2023): `-4.14, -0.16, 5.47, 3.00, 0.13, -0.25, -2.42`
- **Steps:**
  - `=AVERAGE(B11:H11)` → expect `0.23280881533902514`
  - Check whether any of the 7 values is beyond ±10 → none are, so "Anomalous" should be `0`.
- **Check against:** `VAT Matrix`, row 11 (`C101`): column D (Mean M) should read `0.23280881533902514`; column C (Anomalous?) should read `0`; column B (Used for VAT?) reads `1` (this one is an external input from ONS's separately published VAT selection matrix, not something to recompute — nothing to check it against inside this workbook).

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-18: Anomalous flag — a case that is expected NOT to match ⚠️

- **Sheet:** `Rev Comparison Hybrid vs MBS`, industry `R90`, columns B–H (2017–2023): `5.75, 5.87, 0.12, 7.11, -3.18, 39.08, -3.85`
- **Steps:** the largest absolute value here is **39.08** in 2022 (column G) — nearly four times the paper's stated ±10 anomaly threshold.
- **Expected per the paper's stated rule:** `Anomalous? = 1` (should be excluded from Fig. 3).
- **What `VAT Matrix` actually shows for `R90`:** `Anomalous? = 0` — **does not match the stated rule.**
- Other rows showing the same pattern, if your team wants extra spot checks: `C1107`, `C204`, `C244_5`, `C25OTHER`, `C30OTHER`, `C3315`, `C33OTHER`, `G46`, `I55`, `I56`, `M74`, `N81` (all understated as non-anomalous), and `N77` (the one case going the other way — flagged anomalous despite no year breaching ±10).

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-19: Coverage gap — confirm a large industry has no `M` value at all ⚠️

- **Sheet:** `SU Levels`, rows 60–62 (`F41`, `F42`, `F43` — Construction) and row 65 (`G47` — Retail Trade). Confirm these rows have complete, non-zero turnover figures 2016–2023.
- **Sheet:** `Rev Comparison Hybrid vs MBS`, rows 61–63 and row 66 (same four industries). Columns B–H should be **completely blank** — not zero, not an error, simply empty; no formula was ever entered for these rows.
- **Expected result:** confirms the gap described in the main QA report (Finding 1) — 36 industries in total have this pattern; these four are given as a quick, high-profile spot check since Construction and Retail are large sectors.

| Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|

### TC-20 (generalisation — pick your own): spot-check any other industry/year *(both testers)*

To extend coverage beyond the one worked example above, pick any industry code and year of your choosing and repeat TC-12 through TC-16 for it:
1. Find the industry's row in `SU Levels` (this gives you the aggregate code, e.g. `C104`) — note this may correspond to a single CORD code (like `10100` above) or a merged group of several (e.g. `C102_3` = CORD codes `10200`+`10300` added together — check the label text in `Hybrid A T % Growth Rates`/`MBS A T % Growth Rates` column A to see which).
2. Compute `g(SU)`, `g(VAT)`, `g(MBS)` from the level sheets for your chosen year, by hand, as in TC-12–14.
3. Compute `D(VAT)`, `D(MBS)`, `M` as in TC-15–16.
4. Compare each to the corresponding sheet's stored value.

**TC-20a — Tester 1:** pick one industry/year not already covered elsewhere.
**TC-20b — Tester 2:** pick a different industry/year, ideally one that's a *merged* aggregate code (e.g. something like `C102_3`) rather than a clean 1:1 code, to additionally confirm the merge-summing logic.

| Test ID | Industry/year chosen | Actual result | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|---|---|
| TC-20a | | | | Tester 1 | | |
| TC-20b | | | | Tester 2 | | |

---

## Summary log (fill in as each test is completed)

| Test ID | Short description | Assigned to | Pass/Fail | Tester | Date | Notes |
|---|---|---|---|---|---|---|
| TC-1 | 2020/2021 quality fall (Fig 1) | Tester 1 | | | | |
| TC-2 | 2021 total negative M | Tester 1 | | | | |
| TC-3 | 2021 concentration in 8 industries | Tester 1 | | | | |
| TC-4 | Fig 2, 2021 row | Tester 1 | | | | |
| TC-5 | Fig 2, 2022 row | Tester 1 | | | | |
| TC-6 | Fig 2, 2020 row | Tester 1 | | | | |
| TC-7 | Fig 2, 2023 row | Tester 1 | | | | |
| TC-8 | Fig 3, VAT-used bucket | Tester 1 | | | | |
| TC-9 | Fig 3, VAT-not-used bucket | Tester 1 | | | | |
| TC-10 | CORD → sizeband annual | Tester 2 | | | | |
| TC-11 | 5 sizebands → Total | Tester 2 | | | | |
| TC-12 | g(VAT) | Tester 2 | | | | |
| TC-13 | g(MBS) | Tester 2 | | | | |
| TC-14 | g(SU) | Tester 2 | | | | |
| TC-15 | D(VAT) / D(MBS) | Tester 2 | | | | |
| TC-16 | M | Tester 2 | | | | |
| TC-17 | Mean M / Anomalous (matching case) | Tester 2 | | | | |
| TC-18 | Anomalous flag (mismatching case) | Tester 2 | | | | |
| TC-19 | Coverage gap (F41–43, G47) | Tester 2 | | | | |
| TC-20a | Own spot-check | Tester 1 | | | | |
| TC-20b | Own spot-check (merged code) | Tester 2 | | | | |
