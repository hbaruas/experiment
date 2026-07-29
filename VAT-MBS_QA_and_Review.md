# VAT-MBS Analysis — Methodology Review, Data Dictionary & QA (v3)

**Reviewed files:**
- `methadology.md` / `ESP-118 Decision Paper (Expanding VAT Use).docx` (identical draft content, two formats)
- `VAT-MBS Analysis_Staggered_Vintage_ALLVAT 17.06.26.xlsx` (52 sheets)

**Purpose and status of this document:** this is an independent QA review and commentary, prepared to support a decision on whether to proceed with the approach described in the paper. It is not a statement of what ONS should decide — findings are reported as findings, and the commentary in Parts 8–10 is offered as analysis for the reader to weigh, not as a conclusion in itself.

**Revision history:**
- **v2** incorporated a set of author's notes clarifying which sheets play which role in the paper, added explicit reproduction steps to every finding, tested every specific number quoted in the paper against the workbook, and added sections on strengths, weaknesses, and the never-implemented MIDAS model.
- **v3** (this version) responds to a further round of review comments: it adds a section addressing five specific methodological questions raised on reading the paper (Part 6), adds an industry-code lookup table (Part 11) so that industry codes referenced throughout can be read as plain-English names, and removes language that read as prioritising or judging ease of implementation, which is not something this review is in a position to assess. As requested, every finding was independently re-tested a second time before this version was finalised — see the note at the start of Part 5. That second pass confirmed all prior findings and, in the process, found and corrected an error in v2 itself: the total industry count was stated as 113 in several places; the correct figure, confirmed twice independently, is 112.

---

## Part 1 — What the paper is trying to do

### The question

ONS's 2023 OSR (Office for Statistics Regulation) review recommended investigating whether HMRC **VAT turnover data** could be used more widely to improve **monthly GDP (mGDP)** estimates. Today, VAT data is blended into some (not all) industry/sizeband cells of the **Monthly Business Survey (MBS)** — this blend is called **"Hybrid"** data. The paper's job: work out, on an objective and documented basis, which industries should have VAT blended in and which shouldn't, replacing what the paper says is currently an undocumented, ad hoc selection.

### The logic

1. **Assume a "true" value.** Annual GDP / Supply-Use balanced data (**SU**) is taken as the "true" turnover per industry/year, because it has a larger sample, more cleaning, and cross-validation against other measures in the Supply-Use balancing process.
2. **Define quality as closeness to that truth**, for source `S` ∈ {MBS, Hybrid}, industry `i`, year `t`:
   - Growth rate: `g(S,i,t) = (T(S,i,t) − T(S,i,t−1)) / T(S,i,t−1)`
   - Deviation from truth: `D(S,i,t) = |g(SU,i,t) − g(S,i,t)|` (expressed in percentage points)
   - Decision metric: `M(i,t) = D(MBS,i,t) − D(Hybrid,i,t)` — **`M > 0` means Hybrid/VAT is the more accurate source that year; `M < 0` means MBS is.**
3. **Recommend based on the mean of `M` across years**, per industry, after excluding industries flagged "anomalous" (`|M| > 10` in any year, per the paper's stated rule).
4. A **MIDAS regression** is mentioned in the paper as a secondary robustness check on MBS/VAT as general predictors of AGDP. **Confirmed: this was never actually implemented** — it doesn't appear anywhere in the workbook, and the analyst who inherited this confirms it was intended but not built. See Part 7.

### Data and known limitations (as stated in the paper)

- Uses **snapshots** of VAT/MBS data as first published for each reference period (not later-revised versions), excluding ad hoc MBS adjustments as a side effect.
- Deliberately **ignores the current VAT-use matrix** and computes Hybrid/VAT-based growth for *all* industries (to test expansion), except sizebands 4–5, which always take the MBS value (fully enumerated in MBS, so no sampling-error benefit from VAT — explicitly out of scope).
- The "VAT" data used throughout is actually **"Hybrid"** data: quarterly VAT splined to monthly, not raw monthly VAT.
- No sizeband-level recommendations, because SU/AGDP has no sizeband breakdown.
- Known unquantified biases: misreporting differences, corporate-structure/KAU allocation differences between VAT and MBS, manual-cleaning adjustments applied to VAT microdata not reflected in this snapshot.
- **The dataset only exists from 2017 to 2023 — 7 annual points (6 growth-rate observations per industry).** This is the binding constraint behind almost every methodological choice in the paper and every point of discussion in Part 9–10 below.

### Tooling

Plain **Microsoft Excel** — no VBA, no Power Query, no pivot tables. One native bar chart. Raw data sheets are extracts **downloaded directly from CORD** (ONS's internal MBS/VAT processing system) — each carries a metadata header (`Dataset:Processing: MBS VAT:Live, question_no: 40, size band: ..., Allocation: ..., Adjustment basis: R1.0`). Everything downstream is built with ordinary cross-sheet cell formulas — no lookups (`VLOOKUP`/`INDEX-MATCH`/named ranges) anywhere in the file.

### Status: both documents are unfinished drafts

"The recommendation is:" is blank; "Industry Misallocation" bias section is empty; "Conclus[ion]" cuts off mid-word; "Appendix 2: Demonstration that" is incomplete; Fig. 3's table only has the `M > 0` row filled in; the `.md` still carries unresolved Word-comment anchors (`[KC1.1]`, `[AJ6.2]`, `[CM8.1]`, etc.).

---

## Part 2 — Data dictionary (per the author's own notes on sheet roles)

The following mapping was supplied directly and is treated as authoritative for sheet *purpose*; where I found the empirical evidence pointed somewhere slightly different, it's flagged explicitly (see the note after the table).

| Sheets | Role | Symbol in paper |
|---|---|---|
| Any sheet whose name starts with a number (`1.40TMBSMR1.0` … `16.40SB_TMBSVATOUTMR1.0`) | Input sheets downloaded directly from **CORD** | raw inputs |
| `A Hybrid Levels sizeband 1-5`, `A Hybrid Level sizeband T`, `A Hybrid Level sizeband T SU in`, `M MBS Levels 2016+`, `MBS LEVELS SB4`, `A MBS Levels sizeband T`, `A MBS Levels sizeband T SU in` | Aggregation datasets — process CORD input to the required industry/periodicity level | `T(MBS)` and `T(VAT)` |
| `Hybrid A T % Growth Rates` | Growth rate of VAT (Hybrid), calculated from the corresponding aggregation dataset | `g(VAT)` |
| `MBS A T % Growth Rates` | Growth rate of MBS, calculated from the corresponding aggregation dataset | `g(MBS)` |
| `SU Levels` | Input dataset **supplied by the SU team** (external to this workbook, like CORD is) | `T(SU)` |
| `SU A % Growth Rates` | Growth rate of SU, calculated from `SU Levels` | `g(SU)` |
| `Comparison - Rev Hybrid-SU` | `D(VAT)` |
| `Comparison - Rev MBS-SU` | `D(MBS)` |
| `Rev Comparison Hybrid vs MBS` | `M` |
| `Fig 1`, `Fig 2` (+ `Fig 3`, see note) | Linked to the corresponding figures in the paper |
| Everything else (`VAT Matrix`, `Mean Metric Values`, `Industry Selection`, `Analysis with Selected Inds`, `Sign Counts`, `CP SU Weights`, `Weighted Comparison`, `Volatility Comparison`, `Hybrid-SU/MBS-SU Comparison with Sign`, `Cumulative Growths SU`, `Hybrid/MBS M data by industry`, `Hybrid/MBS M, SU industries`, `SU Levels for MIDAS`) | Analysis **not currently used** in the paper | — |

**A necessary clarification on `VAT Matrix`:** there is no sheet literally named "Fig 3" in the workbook, yet the paper's Fig. 3 (the industry-level `% of industries, VAT already used` / `VAT not used` table) clearly exists as a number somewhere. I tested this directly (Finding 2 below): `VAT Matrix`'s `Mean M` column is **exactly** the average of each industry's 7 yearly `M` values, and its `Used for VAT?` / `Anomalous?` columns are exactly what's needed to reproduce Fig. 3's first percentage (54.7%, 29/53) to the decimal. So — regardless of its "not currently used" administrative classification — **`VAT Matrix` functions as the direct source of Fig. 3**, and I've treated it that way for the reproduction and QA below. Everything else in the "not currently used" list I've left out of scope, per the notes.

---

## Part 3 — How the charts and tables were actually built (plain language)

**Fig. 1 (whole-economy bar chart):** for each year 2017–2023, add up the `M` number for every industry that has one. If the total is positive, VAT-blended data was — averaged across the whole economy — closer to the "true" SU figure that year than plain MBS was; negative means the reverse. Fig. 1 is just that yearly total, one bar per year.

**Fig. 2 (concentration table):** for a given year, look at every industry whose `M` is negative (i.e. MBS beat VAT that year) and add up how negative they all are — that's the "total negative" figure quoted in the text. Then find just the especially bad ones (`M < −10`) and add up how negative *those* are. The "Concentration" % is (sum of the especially-bad ones) ÷ (sum of all negative ones). It's answering: "is the bad year spread thinly across many industries, or is it really just a handful dragging the average down?" The second column is simpler: what fraction of all the "MBS beat VAT" industries were among that especially-bad handful.

**Fig. 3 / `VAT Matrix` (industry recommendation table):** for each industry — (1) average its 7 yearly `M` numbers into one number ("Mean M"); (2) check whether any single year was extreme (beyond ±10) and if so, drop the industry from this count as "anomalous" (this step turns out not to be applied fully consistently — see Finding 3); (3) split the remaining industries into two groups: those where VAT is already blended in today, and those where it isn't (this flag comes from ONS's separately published VAT industry selection matrix, not from anything computed in this workbook); (4) within each group, work out what % have a positive average — i.e. what % look like VAT is, on average, the better source for them. Fig. 3 is just those two percentages.

---

## Part 4 — Every number in the paper, tested against the workbook

Every specific figure quoted in `methadology.md`/`.docx` was independently recomputed from the workbook. Method and result for each:

| # | Claim in paper | Reproduction steps | Result |
|---|---|---|---|
| 1 | "VAT appears to experience a dramatic fall in quality in 2020 and again in 2021" | Open `Fig 1`. Read row 2 (`B2:H2` = 2017…2023 totals). | Matches: 2020 and 2021 are the only negative years, and 2021 (−274.2) is far the worst. |
| 2 | 2021: total of all negative `M` cells = **−384.0** across **44 industries** | Open `Rev Comparison Hybrid vs MBS`, column F (2021). Select all populated cells with a negative value, `=SUM()` them and `=COUNT()` them. | Exact match: −384.0, n=44. |
| 3 | 2021: 8 industries below −10 sum to **−239.7 (62.4%)** of the total negative | Same column F. Filter to values `< −10`; `=SUM()` and `=COUNT()`; divide by the −384.0 from row 2 above. | Exact match: −239.7, 8 industries, 62.4%. |
| 4 | Fig. 2 table, 2021: **62.4% / 18.2%** | As above, plus: count% = (8 industries below −10) ÷ (44 negative industries). | Exact match on both figures. |
| 5 | Fig. 2 table, 2022: **53.4% / 15.2%** | Repeat steps 2–4 on column G (2022). | Exact match on both figures. |
| 6 | Fig. 2 table, 2020: **57.7% / 10.3%** | Repeat on column E (2020). | Does not fully match: concentration % (57.7%) matches; count % recomputes as **11.1%**, not 10.3%. |
| 7 | Fig. 2 table, 2023: **37.7% / 14.6%** | Repeat on column H (2023). | Does not fully match: recomputes as **37.2% / 13.6%** — both figures slightly off. |
| 8 | Fig. 3: "VAT already used", `M > 0`: **54.7% (29 out of 53)** | See Finding 2/Finding 3 below for the exact recipe using `VAT Matrix`. | Exact match using the workbook's own (as-applied) `Used`/`Anomalous` flags. |
| 9 | Fig. 3: "VAT not used", `M > 0`: **36.3% (4 out of 11)** | Same recipe, `Used for VAT? = 0` rows. | Does not fully match: recomputes as **4 out of 10 (40.0%)** — one industry short of the paper's denominator. Root cause identified in Finding 1/4. |

**Rows 6, 7 and 9 are the only claims in the whole paper that do not reproduce exactly from the current workbook.** All are small, specific, and traceable (see Findings 1–4) — not evidence of a wrong formula, but evidence that the workbook has moved on slightly since these particular numbers were generated for the paper.

---

## Part 5 — QA findings, with reproduction steps

Ranked by severity. Each includes an explicit, step-by-step recipe so it can be checked independently in Excel.

**Note on independent re-verification:** every finding below, and every figure in Part 4, was recalculated a second time using a separate, independently written check, specifically to catch mistakes in this review's own analysis rather than only the workbook's. The second pass reproduced every number and every finding below unchanged, with one exception: it identified that the total industry count had been misstated as 113 in the previous version of this document (it is 112 — the header row of `SU Levels` was mistakenly included in that count). That has been corrected throughout. No other discrepancies were found between the two passes.

### Finding 1 (Critical): a third of GDP industries were never carried through `M`, `D(MBS)`, or `D(VAT)` — including two major sectors

**What's wrong:** 36 of the 112 GDP-industry rows in `SU Levels` have **no `M` value at all**, in any year, in `Rev Comparison Hybrid vs MBS` (`M`), `Comparison - Rev Hybrid-SU` (`D(VAT)`), or `Comparison - Rev MBS-SU` (`D(MBS)`) — not an error value, just empty cells; no formula was ever entered for these rows. 34 of the 36 have complete, non-zero SU turnover data available (only `B07` and `K653` are genuinely all-zero industries where leaving them out is defensible — see Finding 5). The missing industries include **all of Construction** (`F41`, `F42`, `F43` — SU turnover ~£110–190bn/year combined) and **all of Retail Trade** (`G47` — ~£150–190bn/year), plus large parts of Transport, Finance, Real Estate, Health/Social Work, and Education.

**Steps to reproduce:**
1. Open `SU Levels`. Confirm row 60 (`F41`), row 61 (`F42`), row 62 (`F43`), and row 65 (`G47`) each have complete, non-zero turnover figures for 2016–2023.
2. Open `Rev Comparison Hybrid vs MBS`. Go to row 61 (`F41`), row 62 (`F42`), row 63 (`F43`), row 66 (`G47`). Columns `B:H` (2017–2023) are entirely blank for all four rows.
3. Open `Comparison - Rev Hybrid-SU` and `Comparison - Rev MBS-SU` and confirm the same rows are blank there too — the gap is upstream, not just in the final `M` sheet.
4. To get the full list of affected industries: in `Rev Comparison Hybrid vs MBS`, for every row with a label in column A, check whether `B:H` are all empty. 36 rows are (listed in full: `A01,A02,A03,B05,B06,B07,B09,C12,C19,C241T243,D351,D352_3,E36,E38,E39,F41,F42,F43,G47,H491_2,H50,H51,K64,K651_2,K653,K66,L68BXL683,L68A,O84,P85,Q86,Q87,Q88,R92,S94,T97`).

**Why it matters:** the paper's whole-economy conclusion (Fig. 1) and industry-level conclusion (Fig. 3) both implicitly describe "the economy," but are actually built on roughly two-thirds of it, missing exactly the kind of large sectors (Construction, Retail) that would most affect a total.

### Finding 2 (Revised from v1 — no longer "broken," but not maintained live): `VAT Matrix`'s `Mean M` column is exactly reproducible — just not via a live formula

**What I found:** `VAT Matrix` columns B (`Used for VAT?`), C (`Anomalous?`), D (`Mean M`) contain plain hardcoded numbers, not formulas. In v1 of this review I flagged this as unverifiable. On closer testing:
- **`Mean M` (col D) is exactly `=AVERAGE()` of that industry's 7 yearly values in `Rev Comparison Hybrid vs MBS`, for all 76 industries that have values.** Zero mismatches.
- **`Used for VAT?` (col B) is an external input** — it should match ONS's separately published VAT industry selection matrix (the paper's Appendix 1 links to `vatselectionmatrixfeb2024.xlsx` on ons.gov.uk), not anything computed inside this file. It being hardcoded is correct by design, not a defect — I have not independently cross-checked it against that external published file.
- `Anomalous?` (col C) is discussed separately in Finding 3, below.

**Steps to reproduce (Mean M):**
1. Open `Rev Comparison Hybrid vs MBS`, row 9 (`B08`). Read `B9:H9` = `6.50, 4.76, -1.93, -1.63, -12.22, 2.67, -8.54`.
2. Average those 7 numbers: `= (6.50+4.76-1.93-1.63-12.22+2.67-8.54)/7 = -1.482`.
3. Open `VAT Matrix`, row 9 (`B08`), column D. Value is `-1.4821401963475058`. Matches to 10 decimal places.
4. Repeat for any other industry row — this holds for all 76 populated rows, with zero exceptions.

**Why it matters:** the number is genuinely correct and reproducible today, but because it's pasted rather than formula-linked, it will **silently go stale** the next time `Rev Comparison Hybrid vs MBS` is updated (e.g. a new vintage adds 2024) — nothing will recalculate it, and nothing will warn that it's out of date.

### Finding 3 (Critical, new): the `Anomalous?` flag does not implement the rule stated in the paper

**What's wrong:** the paper states the exclusion rule plainly: "Industries identified as anomalous (with positive or negative values of `M` above absolute value 10 in any year) are excluded." I tested this mechanically — `Anomalous? = 1` if `MAX(ABS(M over 2017:2023)) > 10`, else `0` — against `VAT Matrix`'s actual column C, for all 76 populated industries.

**Result: 62 of 76 match; 14 do not — always in the direction of being *less* exclusionary than the stated rule** (i.e. an industry has a year with `|M| > 10` yet is still marked `Anomalous? = 0`), with one exception (`N77`) going the other way. Examples: `R90` has an `M` of **39.1** in 2022 (nearly 4× the stated threshold) yet is marked not-anomalous; `C25OTHER` has two separate years beyond −20 (2020: −30.7, 2021: −26.8) and is still marked not-anomalous.

**Steps to reproduce:**
1. Open `Rev Comparison Hybrid vs MBS`, find row for `R90`. Read across `B:H` — 2022 (column G) = `39.08`.
2. Open `VAT Matrix`, find row for `R90`, column C (`Anomalous?`). Value is `0`.
3. Per the paper's stated rule (`|M|>10` in any year ⇒ anomalous), this should be `1`.
4. Repeat for `C1107`, `C204`, `C244_5`, `C25OTHER`, `C30OTHER`, `C3315`, `C33OTHER`, `G46`, `I55`, `I56`, `M74`, `N81` — same pattern (13 industries understated as non-anomalous), plus `N77` overstated as anomalous despite no year breaching ±10.

**I could not find a mechanical rule that reconciles all 76 rows** (it isn't based on the mean, and it isn't a fixed subset of years — the "missed" breaches occur in years scattered from 2018 through 2023). The most likely explanation is that the flag reflects additional manual review by the original analyst (e.g. judging some extreme years as real/expected and not exclusion-worthy) that was never written down.

**Why it matters — and I show the sensitivity below:** re-running Fig. 3 with the paper's stated rule applied mechanically, instead of the workbook's actual (partly manual) flags, changes the published percentages:

| Rule used | VAT already used | VAT not used |
|---|---|---|
| Paper's stated text | 54.7% (29/53) | 36.3% (4/11) |
| Workbook's actual `Anomalous?` flags (what I could reproduce) | 54.7% (29/53) — matches | 40.0% (4/10) — does not match |
| Stated rule applied mechanically (`|M|>10` in any year) | 53.5% (23/43) | 37.5% (3/8) |

None of the three fully agree with each other. The published number matches the *workbook's applied practice*, not the *rule as written* — meaning the paper's methodology section and its own results section are not, strictly, describing the same procedure.

### Finding 4: the "VAT not used" bucket in Fig. 3 is one industry short of the paper — traced to the same root cause as Finding 1

The paper says 11 industries qualify for the "VAT not used, non-anomalous" bucket; the workbook currently supports 10. **Steps to reproduce:** open `VAT Matrix`, rows for `C241T243` (row 39) and `H491_2` (row 67) — both are flagged `Used for VAT? = 0`, `Anomalous? = 0` (i.e. should be counted), but column D (`Mean M`) is blank for both, because (per Finding 1) neither industry has any `M` value in `Rev Comparison Hybrid vs MBS` to average. One of these two almost certainly had a value at the time the paper's numbers were generated and has since fallen into the same 36-industry gap; I can't determine which from the file alone.

### Finding 5 (Minor): unguarded divide-by-zero

**Steps to reproduce:** open `SU A % Growth Rates`, row 82 (`K653`), columns `C:I` (2018–2023). Every cell shows `#DIV/0!`. Open `SU Levels`, row 82, and confirm `K653`'s turnover is `0` in every year 2016–2023 — the growth-rate formula `=(cur-prev)/prev` divides by zero with no `IFERROR` guard (unlike, e.g., `Cumulative Growths SU`, which does use one). Affects a single industry with genuinely zero recorded turnover throughout, and is included here mainly as an indicator of inconsistent error-handling across the sheet set, rather than as something affecting the paper's reported numbers.

### Finding 6 (Minor): one raw CORD extract sheet is completely broken

**Steps to reproduce:** open `6.402MBSVATOUTMR1.0`. Every formula cell (e.g. `B9`) reads `=(#REF!-1)*100` and evaluates to `#REF!` — 1,305 cells, 100% of the sheet's formulas. Its structure also doesn't match its siblings (17 columns of annual `...DEC` dates, including a nonsensical `2024DEC`, vs. ~188 columns of monthly dates in the equivalent sizeband 1/3/4/T sheets), consistent with a mid-rebuild that was abandoned. The corresponding `7.402HYBRIDMR1.0` sheet is static pasted values rather than a formula link to this sheet, so it likely hasn't propagated into the headline numbers — but anyone inspecting sizeband-2 VAT data directly will hit 100% errors.

### Finding 7 (Structural risk, not currently realized): cross-sheet linkage is by row position only

**Steps to reproduce (illustrative case):** open `A Hybrid Level sizeband T SU in`, row 60, column A — label reads `46100+46200+46300+46400+46500+46710+46900 (46 Grouped)`. Open `Hybrid A T % Growth Rates`, row 60, column A — label reads `46100+46200+46300+46400+46500+**46600**+46710+46900 (46 Grouped)` (note the extra `46600`), despite `Hybrid A T % Growth Rates!C60` being a formula that reads directly from `'A Hybrid Level sizeband T SU in'!C60` — the same row. The values still matched when I independently recomputed them (see "What's actually correct," below), so this specific case is a stale label rather than a live numeric error — but it demonstrates the underlying risk directly: every cross-sheet link in this workbook is a fixed cell reference, not a lookup keyed on industry code, so a future inserted/deleted/reordered row (exactly what happens when adding a new vintage year) would silently compare the wrong industries with no error message.

### What's actually correct (don't over-conclude "the workbook is broken")

I independently rebuilt the entire `M(i,t)` calculation from raw annual levels (`SU Levels`, `A Hybrid Level sizeband T SU in`, `A MBS Levels sizeband T SU in`) in Python, bypassing the workbook's own growth-rate and comparison sheets, for **every one of the 532 currently-populated industry-year cells** in `Rev Comparison Hybrid vs MBS`. **All 532 reproduced exactly.** Fig. 1's whole-economy totals, and `VAT Matrix`'s `Mean M` column (Finding 2), also reproduce exactly. The problems found are about **coverage** (Finding 1), **an undocumented/manual exclusion step** (Finding 3), and **process fragility** (Finding 7) — not arithmetic errors in the parts that were actually built.

---

## Part 6 — Open questions from reading the paper

These are questions about the paper's own text and reasoning — distinct from Part 5, which tests whether the workbook's numbers are internally correct. These concern places where the paper's explanation appears incomplete or where I could not verify a stated claim from the material available. I raise them as questions for the author/team to answer, not as findings of error.

**1. The splining method the paper points to does not appear to be where the paper says it is.**

Paragraph 28 states: *"This 'hybrid' data was calculated using the splining methodology detailed above in the 'Background' section."* Reading the Background section, the only relevant sentence is: *"For some, but not all, industry/sizeband cells, the Monthly Business Survey (MBS) turnover estimates are each constrained to the quarterly sum of the VAT value for the corresponding cell."* This tells the reader that VAT data is used at quarterly frequency and that MBS is constrained to match the quarterly VAT total for certain cells. It does not describe how that quarterly total is then distributed across the three months within the quarter to produce the monthly "Hybrid" figures actually used in this analysis. I could not find this method described anywhere else in the paper. It is also not visible in the workbook: the raw "Hybrid" sheets (e.g. `4.401HYBRIDMR1.0`) contain static pasted monthly values rather than formulas, which means the splining calculation happens upstream of this workbook (within CORD) and is not something I can inspect or verify from these files. I think it is worth confirming with the author whether this method is documented elsewhere, since the paper currently cites a description that does not appear to be present.

**2. VAT data vintage is addressed, but VAT return timing/completeness at first use is not discussed separately.**

The "Data and Limitations" section explains that data snapshots are taken "at the time data for that reference period was first used in mGDP calculation," with later revisions excluded. This tells the reader which vintage is used. It does not address a related but distinct question: VAT returns for a given period are commonly still being filed for some time after that period ends, so a "first use" VAT figure may be less complete than the same period's VAT figure would be if measured later — independent of any inherent bias in VAT as a data source. The "Known Data Issues (Biases)" section lists misreporting, corporate-structure/KAU allocation, and manual-cleaning adjustments as known biases, but does not mention early-vintage incompleteness as a separate consideration. I think this is worth raising because, if part of VAT's measured underperformance in the metric reflects incomplete first-use data rather than a persistent quality difference, that would have a bearing on how the recommendation should be interpreted.

**3. Comparing annual growth rates may not capture monthly-level differences between the two candidate sources.**

The metric compares annual growth rates of MBS and Hybrid data against annual SU growth rates. I confirmed in Part 5 that no part of the calculation chain operates at monthly frequency — everything is summed to annual before any comparison is made. This means that any within-year timing differences, or monthly noise in either source that nets out over the year, would not be visible to `M`, even though the decision this analysis is meant to inform is specifically about monthly GDP estimates. Two data sources could show very different month-to-month volatility while producing similar annual growth, and this metric would not distinguish between them. I could not find this point discussed in the paper, and raise it as a question about whether an annual-only comparison is expected to fully capture what matters for the monthly use case, or whether this was a deliberate simplification given the constraints described in the paper.

**4. The stated benefit of absolute error over squared error is only partly evidenced.**

Paragraph 14 gives two reasons for using absolute deviations rather than squared ones: that "detailed analysis is easier with the unsquared errors," and that squared-error techniques such as regression or MIDAS require assumptions (particularly unbiasedness) that may not hold here. The second reason is specific and is discussed further elsewhere in the paper. I was not able to identify what the first reason refers to — I reviewed the calculations that exist in the workbook (sums, means, threshold-based counts) and none of them appear to depend on the error being unsquared rather than squared; a squared-error version of the same calculations would work in the same way. This may be a general observation rather than a reference to a specific analysis, but I was not able to confirm this either way and would ask the author to clarify what "detailed analysis" this refers to.

**5. No justification is given for the specific M = ±10 anomaly threshold.**

The paper states that industries are excluded as "anomalous" where `M` exceeds an absolute value of 10 in any year, but does not explain why 10 was chosen, or whether it was derived from the data (for example, as a percentile of the observed distribution, or a multiple of typical year-to-year variation) rather than set as a round-number convention. This is a separate point from Finding 3 in Part 5, which found that the threshold is not applied consistently even on its own terms — this question is about the threshold's justification, independent of whether it was applied consistently.

---

## Part 7 — The MIDAS model: mentioned, never built

The paper states: *"We do, however, run a MIDAS model to assess the general quality of MBS and VAT as predictors of AGDP."* This is written as something already done. **It is not present anywhere in the workbook, and it was never actually implemented** — confirmed directly.

**Why it's plausible this was skipped:** MIDAS (Mixed Data Sampling) regression is designed to combine a high-frequency regressor (monthly/quarterly VAT and MBS) with a low-frequency target (annual AGDP), which is exactly this problem's shape — so it isn't a bad idea in principle. But the dependent variable here — the "true" AGDP/SU annual figure being predicted — only has **7 annual observations (2017–2023)**. Even a MIDAS specification, which needs comparatively few parameters versus other regression approaches, typically wants considerably more than 6–7 non-overlapping annual outcomes to fit and validate a lag-weighting structure with any confidence; with this few, any fitted model is essentially unfalsifiable — it can be made to fit perfectly with no way to tell if it would predict the next year at all.

**What it would have added, had it been built:** a general, economy-wide (not per-industry) check on whether MBS or VAT-based data tends to track AGDP better, using the higher-frequency monthly data directly rather than pre-aggregating to annual — a useful sanity check on the main per-industry `M` metric, but explicitly secondary in the paper's own framing ("to assess the general quality," not to drive the industry-level recommendation).

**Recommendation:** either build a deliberately minimal specification (very few lag parameters, e.g. a low-order Almon polynomial or simple U-MIDAS) as originally intended and report it as a low-confidence, general robustness check — or, if even that is judged unreliable at n=7, remove the sentence claiming it was run and replace it with an explicit statement that it was considered and set aside due to sample size, so the paper doesn't claim an analysis that doesn't exist.

---

## Part 8 — Strengths of the current model/formula

- **Directly tied to the actual decision.** `M` answers the literal question being asked ("which source ends up closer to the truth, for this industry") rather than an abstract statistical fit criterion that would need translating into a decision afterwards.
- **No distributional assumptions required.** Using absolute deviations rather than squared ones (and a simple mean/threshold rather than a fitted model) means the method doesn't depend on unbiasedness, normality, or linearity — all of which would be very hard to justify or test with only 6 growth-rate observations per industry. This was a deliberate, sound choice, explicitly reasoned through in the paper.
- **Uses the best available benchmark.** SU/AGDP is cross-validated through the Supply-Use balancing process against other measures of GVA, which is a stronger claim to "truth" than either of the two candidate sources being evaluated.
- **Right level of granularity.** The actual VAT-use decision is made per industry, and the metric is computed per industry — no aggregation/disaggregation step is needed between the analysis and the decision it's meant to inform.
- **Transparent arithmetic, where built.** The formula chain that does exist (Groups D–F in Part 2) is simple enough to audit by hand, and — per "What's actually correct" above — it checks out completely.

## Part 9 — Weaknesses: why the current approach could fail, and how

- **Too little data to tell signal from noise.** With 6–7 yearly observations per industry, there's no way to know whether a positive or negative mean `M` reflects a real, persistent difference in data quality or just which way 3–4 coin flips happened to land. No confidence interval, standard error, or significance test is applied anywhere to the mean-`M` decision rule — a small industry could easily "flip" the recommendation from one vintage to the next for reasons unrelated to genuine accuracy.
- **The sample window is dominated by an acknowledged shock.** 4 of the 7 years (2020–2023) fall in or after the COVID-19 disruption, and the paper itself says VAT quality "fell dramatically" in exactly those years. A recommendation meant to hold going forward is being calibrated mostly on data from an abnormal period — patterns that held in 2020–2022 may simply not recur in ordinary years.
- **The anomaly rule is blunt and (per Finding 3) inconsistently applied.** A single bad year currently either exempts an industry entirely from the count (if flagged anomalous) or is averaged in at full weight alongside 5–6 good years (if not) — there's no middle ground, and which of those two things happens to a given industry isn't fully explained by the stated rule.
- **No adjustment for how naturally volatile an industry's turnover is.** Two industries could have identical *relative* accuracy of MBS vs. VAT, but the one with a choppier underlying SU series (small industries, industries prone to restructuring) will mechanically produce larger `D` values on both sides and can dominate whole-economy totals or trip the ±10 anomaly threshold — for reasons unrelated to genuine data quality.
- **Coverage gaps quietly shrink "the economy" to "most of the economy."** Finding 1's 36 missing industries mean both the aggregate (Fig. 1) and the industry-level split (Fig. 3) describe roughly two-thirds of GDP by industry count, and disproportionately exclude some very large sectors.
- **The spreadsheet adds a second, purely operational, layer of risk** on top of all of the above (Findings 2, 6, 7) — even a perfectly sound method returns wrong answers if the calculation silently drifts.

## Part 10 — Options to consider, given the data only runs 2017–2023

The paper's own reasoning for avoiding squared-error regression methods (OLS, MIDAS as a primary tool) — that there isn't enough data to trust the assumptions those methods need — appears well-founded and, in my assessment, is worth retaining rather than replaced with "a bigger model." The suggestions below don't require more calendar time; they aim to get more out of the ~112-industry, 7-year panel that already exists. These are offered as options for ONS to weigh, not as a ranked list of what must be done:

1. **Add uncertainty, don't just add a bigger model.** For each industry, run a simple non-parametric check (e.g. a sign test on the 6–7 years' worth of `M`, or a bootstrap resample across years) to say whether "VAT looks better here" is distinguishable from noise, rather than reporting a bare mean with an implicit >0/<0 cutoff.
2. **Replace the binary anomaly exclusion with a robust central-tendency measure.** A trimmed mean or median across the 6–7 years naturally down-weights one bad year without discarding the other 5–6 good ones — this also removes the undocumented manual judgment call in Finding 3.
3. **Report normal years and shock years separately.** Show the recommendation computed on 2017–2019 + 2022–2023 alongside the all-years version, so a reader can see how much the COVID years are driving each industry's answer, rather than folding a structural break silently into one average.
4. **Borrow strength across industries (partial pooling).** With ~112 industries but only 6–7 years each, a hierarchical/shrinkage estimate — pulling each industry's noisy small-sample mean `M` partway toward its section average — uses the cross-sectional sample that already exists to stabilise individual industry estimates, without needing a single extra year of data.
5. **Address the coverage gap (Finding 1).** None of the above changes anything for the 36 industries that currently have no `M` value at all — whatever statistical refinement is chosen, it has no effect until those industries are included. I note this without a view on how much work this would take to fix, which is not something I'm in a position to judge from outside the team that maintains the workbook.
6. **Consider moving the calculation off hand-linked Excel.** This would allow all 112 industries to be processed consistently (or produce a visible error for the ones that can't be), make it easier to re-run and compare the analysis against the previous vintage, and make it more straightforward to add MIDAS or a simple regression to the same pipeline as more years of data accumulate. I raise this as an option for ONS to weigh against its own resourcing and priorities, not as a recommendation that it is straightforward or low-cost to do.

---

## Part 11 — Industry code lookup

This table is provided so that the industry codes referenced throughout this review (and in Fig. 2 and Fig. 3 of the paper) can be read as plain-English industry descriptions, since the workbook itself contains no such lookup anywhere (I checked — see below).

**Important caveat on sourcing:** the descriptions below are compiled from general knowledge of the UK Standard Industrial Classification 2007 (SIC 2007) and the way ONS typically groups SIC classes into industries for National Accounts / Supply-Use purposes. They are **not** taken from an authoritative lookup file within this project, because no such file exists in the material I was given — I searched every sheet in the workbook for anything resembling an industry-name lookup and found none; only industry codes and, in a few CORD sheets, concatenated lists of the underlying detailed codes (e.g. `10200+10300`). Most codes below correspond closely to a standard, well-defined SIC 2007 division or group title and are marked **Standard**. A smaller number are ONS-specific merged or "residual" groupings (marked **Inferred**) where I have described the likely content logically (e.g. "everything in this division not already broken out elsewhere") rather than citing a known published title verbatim, because I could not verify the exact wording ONS uses for that specific grouping. Before this table is relied on for the go/no-go decision, I recommend it be checked against ONS's own published industry classification for this Supply-Use breakdown (for example, the classification underlying the VAT industry selection matrix referenced in the paper's Appendix 1, or the Blue Book / Supply-Use industry list), rather than taken as authoritative on my word alone.

| Code | Description | Confidence |
|---|---|---|
| A01 | Crop and animal production, hunting and related service activities | Standard |
| A02 | Forestry and logging | Standard |
| A03 | Fishing and aquaculture | Standard |
| B05 | Mining of coal and lignite | Standard |
| B06 | Extraction of crude petroleum and natural gas | Standard |
| B07 | Mining of metal ores | Standard |
| B08 | Other mining and quarrying | Standard |
| B09 | Mining support service activities | Standard |
| C101 | Processing and preserving of meat and production of meat products | Standard |
| C102_3 | Processing/preserving of fish, crustaceans and molluscs; processing/preserving of fruit and vegetables | Inferred (merged group) |
| C104 | Manufacture of vegetable and animal oils and fats | Standard |
| C105 | Manufacture of dairy products | Standard |
| C106 | Manufacture of grain mill products, starches and starch products | Standard |
| C107 | Manufacture of bakery and farinaceous products | Standard |
| C108 | Manufacture of other food products | Standard |
| C109 | Manufacture of prepared animal feeds | Standard |
| C1101T1106 | Manufacture of beverages, excluding soft drinks and mineral waters (spirits, wine, cider, malt liquors, malt) | Inferred (merged group) |
| C1107 | Manufacture of soft drinks; production of mineral waters and other bottled waters | Standard |
| C12 | Manufacture of tobacco products | Standard |
| C13 | Manufacture of textiles | Standard |
| C14 | Manufacture of wearing apparel | Standard |
| C15 | Manufacture of leather and related products | Standard |
| C16 | Manufacture of wood and of products of wood and cork, except furniture | Standard |
| C17 | Manufacture of paper and paper products | Standard |
| C18 | Printing and reproduction of recorded media | Standard |
| C19 | Manufacture of coke and refined petroleum products | Standard |
| C203 | Manufacture of paints, varnishes, printing ink and mastics | Standard |
| C204 | Manufacture of soap and detergents, cleaning and polishing preparations, perfumes and toilet preparations | Standard |
| C205 | Manufacture of other chemical products | Inferred (residual group) |
| C20A | A sub-division of chemicals manufacture (likely basic chemicals, fertilisers and plastics/synthetic rubber in primary form) | Inferred — low confidence |
| C20B | A sub-division of chemicals manufacture (exact scope unclear) | Inferred — low confidence |
| C20C | A sub-division of chemicals manufacture (possibly man-made fibres) | Inferred — low confidence |
| C21 | Manufacture of basic pharmaceutical products and pharmaceutical preparations | Standard |
| C22 | Manufacture of rubber and plastic products | Standard |
| C235_6 | Manufacture of cement, lime and plaster; manufacture of articles of concrete, cement and plaster | Inferred (merged group) |
| C23OTHER | Other non-metallic mineral products not covered above (e.g. glass, ceramics, bricks and tiles, stone) | Inferred (residual group) |
| C241T243 | Basic iron and steel and ferro-alloys; steel tubes/pipes; other first processing of iron and steel | Inferred (merged group) |
| C244_5 | Basic precious and other non-ferrous metals; casting of metals | Inferred (merged group) |
| C254 | Manufacture of weapons and ammunition | Standard |
| C25OTHER | Other fabricated metal products not covered above (structural metal products, tanks, forging/pressing, metal treatment/coating, cutlery/tools, etc.) | Inferred (residual group) |
| C26 | Manufacture of computer, electronic and optical products | Standard |
| C27 | Manufacture of electrical equipment | Standard |
| C28 | Manufacture of machinery and equipment not elsewhere classified | Standard |
| C29 | Manufacture of motor vehicles, trailers and semi-trailers | Standard |
| C301 | Building of ships and boats | Standard |
| C303 | Manufacture of air and spacecraft and related machinery | Standard |
| C30OTHER | Other transport equipment not covered above (railway rolling stock, military vehicles, etc.) | Inferred (residual group) |
| C31 | Manufacture of furniture | Standard |
| C32 | Other manufacturing | Standard |
| C3315 | Repair and maintenance of ships and boats | Standard |
| C3316 | Repair and maintenance of aircraft and spacecraft | Standard |
| C33OTHER | Other repair and installation of machinery and equipment not covered above | Inferred (residual group) |
| D351 | Electric power generation, transmission and distribution | Standard |
| D352_3 | Manufacture of gas and distribution of gaseous fuels through mains; steam and air conditioning supply | Inferred (merged group) |
| E36 | Water collection, treatment and supply | Standard |
| E37 | Sewerage | Standard |
| E38 | Waste collection, treatment and disposal activities; materials recovery | Standard |
| E39 | Remediation activities and other waste management services | Standard |
| F41 | Construction of buildings | Standard |
| F42 | Civil engineering | Standard |
| F43 | Specialised construction activities | Standard |
| G45 | Wholesale and retail trade and repair of motor vehicles and motorcycles | Standard |
| G46 | Wholesale trade, except of motor vehicles and motorcycles | Standard |
| G47 | Retail trade, except of motor vehicles and motorcycles | Standard |
| H491_2 | Passenger rail transport, interurban; freight rail transport | Inferred (merged group) |
| H493T495 | Other land transport (bus, taxi, road freight) and transport via pipeline | Inferred (residual/merged group) |
| H50 | Water transport | Standard |
| H51 | Air transport | Standard |
| H52 | Warehousing and support activities for transportation | Standard |
| H53 | Postal and courier activities | Standard |
| I55 | Accommodation | Standard |
| I56 | Food and beverage service activities | Standard |
| J58 | Publishing activities | Standard |
| J59 | Motion picture, video and television programme production, sound recording and music publishing | Standard |
| J60 | Programming and broadcasting activities | Standard |
| J61 | Telecommunications | Standard |
| J62 | Computer programming, consultancy and related activities | Standard |
| J63 | Information service activities | Standard |
| K64 | Financial service activities, except insurance and pension funding | Standard |
| K651_2 | Insurance; reinsurance | Inferred (merged group) |
| K653 | Pension funding | Standard |
| K66 | Activities auxiliary to financial services and insurance activities | Standard |
| L68BXL683 | Real estate activities with own or leased property, excluding imputed rent and excluding fee/contract-basis activities (L683) | Inferred — low confidence on exact scope |
| L68A | Imputed rental of owner-occupied dwellings (a National Accounts concept, not a standalone SIC class) | Inferred |
| L683 | Real estate activities on a fee or contract basis (e.g. estate agency, property management for others) | Standard |
| M691 | Legal activities | Standard |
| M692 | Accounting, bookkeeping and auditing activities; tax consultancy | Standard |
| M70 | Activities of head offices; management consultancy activities | Standard |
| M71 | Architectural and engineering activities; technical testing and analysis | Standard |
| M72 | Scientific research and development | Standard |
| M73 | Advertising and market research | Standard |
| M74 | Other professional, scientific and technical activities | Standard |
| M75 | Veterinary activities | Standard |
| N77 | Rental and leasing activities | Standard |
| N78 | Employment activities | Standard |
| N79 | Travel agency, tour operator and other reservation service and related activities | Standard |
| N80 | Security and investigation activities | Standard |
| N81 | Services to buildings and landscape activities | Standard |
| N82 | Office administrative, office support and other business support activities | Standard |
| O84 | Public administration and defence; compulsory social security | Standard |
| P85 | Education | Standard |
| Q86 | Human health activities | Standard |
| Q87 | Residential care activities | Standard |
| Q88 | Social work activities without accommodation | Standard |
| R90 | Creative, arts and entertainment activities | Standard |
| R91 | Libraries, archives, museums and other cultural activities | Standard |
| R92 | Gambling and betting activities | Standard |
| R93 | Sports activities and amusement and recreation activities | Standard |
| S94 | Activities of membership organisations | Standard |
| S95 | Repair of computers and personal and household goods | Standard |
| S96 | Other personal service activities | Standard |
| T97 | Activities of households as employers of domestic personnel | Standard |

For quick reference, the industries most frequently cited earlier in this review are: `R91` (libraries/archives/museums), `N82` (office administrative/business support), `C25OTHER` (fabricated metal products, residual), `C102_3` (fish/fruit/vegetable processing), `B08` (other mining and quarrying), `R90` (creative, arts and entertainment), `G47` (retail trade), `F41`/`F42`/`F43` (construction), and `C101` (meat processing, the worked example in the companion test-case document).
