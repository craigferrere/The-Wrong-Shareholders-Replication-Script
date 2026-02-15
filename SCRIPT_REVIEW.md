# Replication Script Review

## 1) Top-level pipeline outline (inputs → transformations → outputs)

1. **Merger universe extraction**
   - **Inputs:** WRDS SDC M&A details (`sdc.wrds_ma_details`) with filters for completed US public-public deals (2000–2025), 100% owned targets, and non-missing deal value.
   - **Transformations:** Date/numeric coercion and sample filtering.
   - **Output:** `mergers_analysis`.

2. **Target/acquirer CRSP identity mapping**
   - **Inputs:** `mergers_analysis`, CRSP `stocknames_v2`, `stocknames`, `ccm_lookup`.
   - **Transformations:** Multi-route CUSIP6/date matching (strict/loose) with ticker fallback for targets; keep best matches.
   - **Output:** `mergers_analysis` with `target_permno`/`acquirer_permno` and match counts.

3. **Final merger analysis sample construction**
   - **Inputs:** `mergers_analysis`.
   - **Transformations:** Event-quarter construction (`t_0`, `t_m1`, `t_end`), close-date eligibility (`dateeff > t_0`), top-N by deal value.
   - **Output:** `mergers`.

4. **Institutional holdings build (firm-quarter granular panel)**
   - **Inputs:** `mergers`, CRSP monthly files (`msf`, `msenames`), Thomson 13F (`tfn.s34type1`, `tfn.s34type3`).
   - **Transformations:**
     - Pull merger and sampled non-merger control PERMNOs.
     - Construct first-vintage filer-quarter panel.
     - Pull holdings in chunks by `fdate`; map CUSIP to PERMNO.
     - Join CRSP adjustment factors and compute split-adjusted shares.
   - **Output:** `institutional_holdings_granular.rds`.

5. **Security-level IO metrics**
   - **Inputs:** `institutional_holdings_granular`, CRSP quarter-end shares out.
   - **Transformations:** Roll up to permno-quarter (`IOR`, `IOC_HHI`, counts, missingness flags).
   - **Output:** `institutional_ownership_timeseries.rds` (`io_rollup`).

6. **Event-study panels**
   - **Inputs:** `mergers`, `io_rollup`.
   - **Transformations:**
     - Build `target_panel` and `acquirer_panel` with event time/buckets.
     - Build matched non-merger control panel from CRSP candidate universe via IO/size distance matching.
     - Create DiD panels (`matched_panel`, `full_panel`) with post indicators.
   - **Outputs:** `target_panel.rds`, `acquirer_panel.rds`, `control_panel.rds`, `matched_panel.rds`, `full_panel.rds`.

7. **Core empirical analyses**
   - **Event-study regressions:** IOR levels around announcement (all/cash/stock subsamples, trend-break specs).
   - **Flow decomposition:** entrant/exiter/stayer contributions to net ownership changes.
   - **Electorate replacement:** top-K Jaccard/cosine/HHI change.
   - **Arbitrageur concentration:** entry-intensity concentration and Gini.
   - **Velocity analysis:** timing of flows vs days-to-quarter-end with model selection/bootstrap.
   - **Stayer dynamics:** size-vs-trim behavior, stayer-only concentration/replacement.
   - **Insider ownership:** FactSet-like insider filings around announcement.
   - **Risk transformation:** rolling beta/correlation/idiosyncratic vol pre/post announcement.
   - **Cross-deal arbitrage:** institutions adding counterparty positions in stock deals.
   - **Outputs:** multiple `.rds` datasets and `.png` figures (velocity plots), plus printed model tables.

## 2) Global parameters that should be in a single config block

### Already in `cfg` (good)
- `top_n`, `sample_n`, `control_firms_n`, `winsorization`, `begin_date`, `end_date`.
- `vars`, `date_vars`, `numeric_vars`.

### Currently global/hard-coded and should be moved to config
- `cash_cutoff` (unused currently, but intended threshold).
- Data coverage windows: `begdate`, `enddate`.
- 13F extraction: `chunk_size`.
- Control matching:
  - `CONTROL_TARGET_RATIO`
  - `candidate_multiplier`
  - `min_market_cap`
  - `min_IO_threshold`
- Event windows / binning cutoffs:
  - event-time inclusion (e.g., `-4:4`), placebo/treatment windows (`-2,-1,0`)
  - period labels for risk windows (`-250:-60`, `-60:-1`, `0:60`, `60:250`)
  - velocity buckets (`1-30`, `31-60`, `61-90` days)
- Deal-type definitions:
  - cash/stock purity thresholds (75%, 90%).
- Modeling knobs:
  - spline dfs (`3,4,5`), GAM `k=5`, segmented breakpoints (`npsi=1,2`), bootstrap reps (`1000`), random seed (`42`).
- Data quality / trimming:
  - CRSP ME floor (`ABS(prc)*shrout > 50000` in one place and `$500M` in another context)
  - return outlier trim (`abs(ret) > 2`).
  - insider winsorization cap (`10%` per person).

## Parsing & logic errors found

1. **Hard runtime error: undefined object in velocity step**
   - `timing_data <- deal_flows %>% ...` references `deal_flows`, but the script creates `deals_panel` (not `deal_flows`). This will fail at Step 15.

2. **Hard runtime error near final output block**
   - `print_table(...)` is called, but there is no function definition or imported package function named `print_table` in the script.

3. **Potential runtime fragility: removing objects that may not exist**
   - `rm("m5", ...)` appears in a cleanup block in the acquirer mapping section before `m5` is created in that branch.
   - `rm("io_hhi", "crsp_m")` is called again later after those objects were already removed.

4. **Environment prerequisite not enforced in-script**
   - The script assumes an existing `wrds` DB connection (`# Precursor: wrds connection`) but does not create or validate it.

5. **Redundant duplicated computation block**
   - `risk_metrics` rolling metrics are computed twice in Step 19 (first block then repeated “Calculate rolling metrics” block), which is not fatal but increases maintenance risk.

## Does the script achieve the stated research objectives?

**Short answer:** Mostly yes on *coverage of analyses*, but not yet production-stable due to blocking errors.

- **Aligned with objective:** The script implements the full empirical arc in your introduction: ownership decline event study, flow decomposition (entry/exit/stayer), top-holder replacement (Jaccard/cosine), arbitrageur concentration, cash-vs-stock heterogeneity, timing/velocity, insider ownership, and risk-transformation diagnostics.
- **Not yet reliable end-to-end:** At least two hard object/function reference errors prevent a clean full run, so results are not reproducible as-is until fixed.
- **Recommendation:** Fix blocking errors first, then add a thin orchestration wrapper (or `{targets}` pipeline) and centralize all tunable constants into `cfg` for reproducibility and sensitivity sweeps.
