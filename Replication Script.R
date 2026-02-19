###########################################################################################################
# REPLICATION SCRIPT:
# "The Wrong Shareholders: Why Merger Abitragers Now Control M&A Approval"
#
# Precursor: wrds connection
#
###########################################################################################################


# CONFIG ==================================================================================================

cfg <- list(
	top_n		= 2500L,
	sample_n  	= 1000L,
	control_firms_n = 4000,
	winsorization 	= 1.20,
	begin_date	= as.Date("2000-01-01"),
	end_date	= as.Date("2025-01-01"),
	vars 		= c("master_deal_no", "dateann", "dateeff", "aup", "tup", "master_cusip",
			    "acusip", "tticker", "tip", "status", "tpublic", "apublic",
			    "pct_cash", "pct_stk", "pct_other", "pct_unknown",
			    "pctacq", "pctown",
			    "tnationcode", "anationcode", "deal_value"
			     ),
	date_vars	= c("dateann", "dateeff"),
	numeric_vars	= c("deal_value", "pct_cash", "pct_stk", "pct_other", "pct_unknown", "pctacq", "pctown")
)

attach_packages <- function(pkgs) {invisible(lapply(pkgs, function(p) {
	if (!requireNamespace(p, quietly = TRUE)) stop("Missing package: ", p, call. = FALSE)
	suppressPackageStartupMessages(library(p, character.only = TRUE))
 	}))
}

attach_packages(c("DBI", "RPostgres", "dplyr", "tidyr", "dbplyr", "lubridate", "conflicted", "tidyverse", "ggplot2", "fixest", "modelsummary", "purrr", "mgcv", "segmented", "stringr", "renv")
)

conflicts_prefer(dplyr::filter, dplyr::select, dplyr::mutate, dplyr::arrange, dplyr::summarise,
	dplyr::slice, dplyr::lag
)

qend <- function(x) lubridate::ceiling_date(as.Date(x), "quarter") - lubridate::days(1)

qend_prev <- function(x) {lubridate::floor_date(as.Date(x), "quarter") - lubridate::days(1)
}

qseq <- function(from, to) seq.Date(as.Date(from), as.Date(to), by = "quarter")

q_index <- function(qe) {
 	qe <- as.Date(qe)
 	lubridate::year(qe) * 4L + lubridate::quarter(qe)
}

#=================================================================================================
# STEP 1: Merger Universe Extraction
# create intermediate sample before top_n applied
# Input: WRDS SDC M&A details (`sdc.wrds_ma_details`) with filters for completed US public-public 
# deals (2000–2025), 100% owned targets, and non-missing deal value
# Transformations: Date/numeric coercion and sample filtering
# Output: mergers_analysis
#=================================================================================================

mergers_analysis <- tbl(wrds, in_schema("sdc", "wrds_ma_details")) %>%
 		select(all_of(cfg$vars)) %>%
 		filter(
   			dateann >= cfg$begin_date,
   			dateann <  cfg$end_date,
   			status == "Completed",
   			tpublic == "Public",
   			apublic == "Public",
   			tnationcode == "US",
   			anationcode == "US",
   			(aup != tup),
			!is.na(deal_value)
		) %>%
 			collect() %>%
 		mutate(
   			across(all_of(cfg$date_vars), as.Date),
   			across(all_of(cfg$numeric_vars), as.numeric)
 		) %>%
 		filter(
   			!is.na(pctown),
   			pctown == 100
 		)

#=================================================================================================
# STEP 2: Target/acquirer CRSP identity mapping
# Input: mergers_analysis, CRSP stocknames_v2, stocknames, ccm_lookup
# Transformation: multi-route CUSIP6/date matching (strict/loose) with ticker fallback for targets; keep best matches
# Output: mergers_analysis with target_permno, acquirer_permno and match counts
#=================================================================================================

# ---- acquirer permno ---------------------------------------------------------------------------
# ---- helper ------------------------------------------------------------------------------------
pick_best_by_date <- function(df, asof_col = "asof", start_col = "namedt", end_col = "nameenddt") {
 asof <- df[[asof_col]]
 st   <- df[[start_col]]
 en   <- df[[end_col]]
 overlap <- !is.na(st) & !is.na(en) & st <= asof & asof <= en

 # Distance: 0 if overlap, else days to nearest boundary
 dist <- if_else(overlap, 0,
                 pmin(abs(as.numeric(st - asof)),
                      abs(as.numeric(asof - en)),
                      na.rm = TRUE))

 df$._overlap <- overlap
 df$._dist <- dist
 df %>%
   group_by(master_deal_no) %>%
   arrange(desc(._overlap), ._dist) %>%
   slice_head(n = 1) %>%
   ungroup() %>%
   select(-._overlap, -._dist)
}

# ---- keys --------------------------------------------------------------------------------------
xk <- mergers_analysis %>%
 transmute(
   master_deal_no = as.character(master_deal_no),
   asof   = as.Date(dateann),
   cusip6 = substr(gsub("\\s+", "", acusip), 1, 6)
 ) %>%
 mutate(
   cusip6 = if_else(nchar(cusip6) == 6, cusip6, NA_character_),
   cusip6 = na_if(cusip6, "")
 ) %>%
 distinct(master_deal_no, asof, cusip6)

cus6 <- xk %>% filter(!is.na(cusip6)) %>% distinct(cusip6) %>% pull(cusip6)

# ---- pull CRSP sources (restricted) ------------------------------------------------------------
sn2_cols <- tbl(wrds, in_schema("crsp", "stocknames_v2")) %>% head(1) %>% collect() %>% names()
sn_cols  <- tbl(wrds, in_schema("crsp", "stocknames"))    %>% head(1) %>% collect() %>% names()
ccm_cols <- tbl(wrds, in_schema("crsp", "ccm_lookup"))    %>% head(1) %>% collect() %>% names()

# ---- stocknames_v2: pick the best available cusip field (prefer header cusip if present) -------
sn2_cusip_field <- dplyr::case_when(
 "hdrcusip"  %in% sn2_cols ~ "hdrcusip",
 "hdrcusip9" %in% sn2_cols ~ "hdrcusip9",
 "cusip"     %in% sn2_cols ~ "cusip",
 "cusip9"    %in% sn2_cols ~ "cusip9",
 TRUE                     ~ NA_character_
)

sn2 <- if (!is.na(sn2_cusip_field)) {
 tbl(wrds, in_schema("crsp", "stocknames_v2")) %>%
   transmute(
     permno    = permno,
     cusip6    = substr(.data[[sn2_cusip_field]], 1, 6),
     namedt    = as.Date(namedt),
     nameenddt = as.Date(nameenddt)
   ) %>%
   filter(cusip6 %in% cus6) %>%
   collect()
} else {
 tibble(permno = integer(), cusip6 = character(),
        namedt = as.Date(character()), nameenddt = as.Date(character()))
}

sn <- tbl(wrds, in_schema("crsp", "stocknames")) %>%
 transmute(
   permno    = permno,
   cusip6    = substr(ncusip, 1, 6),
   namedt    = as.Date(namedt),
   nameenddt = as.Date(nameenddt)
 ) %>%
 filter(cusip6 %in% cus6) %>%
 collect()

ccm_permno_col <- if ("permno" %in% ccm_cols) "permno" else if ("lpermno" %in% ccm_cols) "lpermno" else NA_character_
ccm <- if (!is.na(ccm_permno_col) && "cusip" %in% ccm_cols) {
 tbl(wrds, in_schema("crsp", "ccm_lookup")) %>%
   transmute(
     cusip6 = substr(cusip, 1, 6),
     permno = .data[[ccm_permno_col]],
     linkdt    = if ("linkdt" %in% ccm_cols) as.Date(linkdt) else as.Date(NA),
     linkenddt = if ("linkenddt" %in% ccm_cols) as.Date(coalesce(linkenddt, as.Date("9999-12-31"))) else as.Date(NA)
   ) %>%
   filter(cusip6 %in% cus6) %>%
   collect()
} else {
 tibble(cusip6 = character(), permno = integer(), linkdt = as.Date(character()), linkenddt = as.Date(character()))
}

# ---- Route 1: stocknames_v2 CUSIP6 with strict overlap -----------------------------------------
m1 <- xk %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn2 %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno), namedt <= asof, asof <= nameenddt) %>%
 select(master_deal_no, permno) %>%
 distinct()

matched <- m1 %>% distinct(master_deal_no)

# ---- Route 2: ccm_lookup (strict if link dates exist; else accept) -----------------------------
m2 <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(ccm, by = "cusip6", relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 filter(is.na(linkdt) | is.na(linkenddt) | (linkdt <= asof & asof <= linkenddt)) %>%
 select(master_deal_no, permno) %>%
 distinct()

matched <- bind_rows(matched, m2 %>% distinct(master_deal_no)) %>% distinct()

# ---- Route 3: stocknames (strict overlap) ------------------------------------------------------
m3 <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno), namedt <= asof, asof <= nameenddt) %>%
 select(master_deal_no, permno) %>%
 distinct()

matched <- bind_rows(matched, m3 %>% distinct(master_deal_no)) %>% distinct()

# ---- Route 4 (loose): pick closest CUSIP6 window from v2 then stocknames -----------------------
m4a <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn2 %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 pick_best_by_date()

matched <- bind_rows(matched, m4a %>% distinct(master_deal_no)) %>% distinct()

m4b <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 pick_best_by_date()

matched <- bind_rows(matched, m4b %>% distinct(master_deal_no)) %>% distinct()

# ---- combine + attach --------------------------------------------------------------------------
x_map <- bind_rows(m1, m2, m3, m4a, m4b) %>%
 distinct(master_deal_no, permno) %>%
 group_by(master_deal_no) %>%
 summarise(
   acquirer_permno   = paste(sort(unique(permno)), collapse = ";"),
   acquirer_n_permno = n_distinct(permno),
   .groups = "drop"
 )

mergers_analysis <- mergers_analysis %>%
 mutate(master_deal_no = as.character(master_deal_no)) %>%
 select(-any_of(c("acquirer_permno", "acquirer_n_permno"))) %>%
 left_join(
   x_map %>% mutate(master_deal_no = as.character(master_deal_no)),
   by = "master_deal_no"
 ) %>%
 mutate(
   acquirer_permno   = coalesce(acquirer_permno, ""),
   acquirer_n_permno = coalesce(acquirer_n_permno, 0L)
 )

# ---- clear intermediate objects ----------------------------------------------------------------
rm("ccm","m1","m2","m3","m4a","m4b", "cus6", "sn2_cols", "sn_cols", "sn","sn_t","sn2","x_map","xk", "matched", "ccm_cols", "sn2_cusip_field", "ccm_permno_col")

# ---- target permno -----------------------------------------------------------------------------
# ---- keys --------------------------------------------------------------------------------------
xk <- mergers_analysis %>%
 transmute(
   master_deal_no = as.character(master_deal_no),
   asof   = as.Date(dateann),
   cusip6 = substr(gsub("\\s+", "", master_cusip), 1, 6),
   ticker = toupper(trimws(tticker))
 ) %>%
 mutate(
   cusip6 = if_else(nchar(cusip6) == 6, cusip6, NA_character_),
   cusip6 = na_if(cusip6, ""),
   ticker = na_if(ticker, "")
 ) %>%
 distinct(master_deal_no, asof, cusip6, ticker)

cus6 <- xk %>% filter(!is.na(cusip6)) %>% distinct(cusip6) %>% pull(cusip6)

# ---- pull CRSP sources (restricted) ------------------------------------------------------------
sn2_cols <- tbl(wrds, in_schema("crsp", "stocknames_v2")) %>% head(1) %>% collect() %>% names()
sn_cols  <- tbl(wrds, in_schema("crsp", "stocknames"))    %>% head(1) %>% collect() %>% names()
ccm_cols <- tbl(wrds, in_schema("crsp", "ccm_lookup"))    %>% head(1) %>% collect() %>% names()

# stocknames_v2: pick the best available cusip field (prefer header cusip if present)
sn2_cusip_field <- dplyr::case_when(
 "hdrcusip"  %in% sn2_cols ~ "hdrcusip",
 "hdrcusip9" %in% sn2_cols ~ "hdrcusip9",
 "cusip"     %in% sn2_cols ~ "cusip",
 "cusip9"    %in% sn2_cols ~ "cusip9",
 TRUE                     ~ NA_character_
)

sn2 <- if (!is.na(sn2_cusip_field)) {
 tbl(wrds, in_schema("crsp", "stocknames_v2")) %>%
   transmute(
     permno    = permno,
     cusip6    = substr(.data[[sn2_cusip_field]], 1, 6),
     ticker    = if ("ticker" %in% sn2_cols) toupper(ticker) else NA_character_,
     namedt    = as.Date(namedt),
     nameenddt = as.Date(nameenddt)
   ) %>%
   filter(cusip6 %in% cus6) %>%
   collect()
} else {
 tibble(permno = integer(), cusip6 = character(), ticker = character(),
        namedt = as.Date(character()), nameenddt = as.Date(character()))
}

sn <- tbl(wrds, in_schema("crsp", "stocknames")) %>%
 transmute(
   permno    = permno,
   cusip6    = substr(ncusip, 1, 6),
   ticker    = toupper(ticker),
   namedt    = as.Date(namedt),
   nameenddt = as.Date(nameenddt)
 ) %>%
 filter(cusip6 %in% cus6) %>%
 collect()

ccm_permno_col <- if ("permno" %in% ccm_cols) "permno" else if ("lpermno" %in% ccm_cols) "lpermno" else NA_character_
ccm <- if (!is.na(ccm_permno_col) && "cusip" %in% ccm_cols) {
 # keep lean; date fields vary across WRDS installs
 tbl(wrds, in_schema("crsp", "ccm_lookup")) %>%
   transmute(
     cusip6 = substr(cusip, 1, 6),
     permno = .data[[ccm_permno_col]],
     linkdt    = if ("linkdt" %in% ccm_cols) as.Date(linkdt) else as.Date(NA),
     linkenddt = if ("linkenddt" %in% ccm_cols) as.Date(coalesce(linkenddt, as.Date("9999-12-31"))) else as.Date(NA)
   ) %>%
   filter(cusip6 %in% cus6) %>%
   collect()
} else {
 tibble(cusip6 = character(), permno = integer(), linkdt = as.Date(character()), linkenddt = as.Date(character()))
}

# ---- Route 1: stocknames_v2 CUSIP6 with strict overlap -----------------------------------------
m1 <- xk %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn2 %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno), namedt <= asof, asof <= nameenddt) %>%
 select(master_deal_no, permno) %>%
 distinct()

matched <- m1 %>% distinct(master_deal_no)

# ---- Route 2: ccm_lookup (strict if link dates exist; else accept) -----------------------------
m2 <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(ccm, by = "cusip6", relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 filter(is.na(linkdt) | is.na(linkenddt) | (linkdt <= asof & asof <= linkenddt)) %>%
 select(master_deal_no, permno) %>%
 distinct()

matched <- bind_rows(matched, m2 %>% distinct(master_deal_no)) %>% distinct()

# ---- Route 3: stocknames (strict overlap) ------------------------------------------------------
m3 <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno), namedt <= asof, asof <= nameenddt) %>%
 select(master_deal_no, permno) %>%
 distinct()

matched <- bind_rows(matched, m3 %>% distinct(master_deal_no)) %>% distinct()

# ---- Route 4 (loose): pick closest CUSIP6 window from v2 then stocknames -----------------------
m4a <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn2 %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 pick_best_by_date()

matched <- bind_rows(matched, m4a %>% distinct(master_deal_no)) %>% distinct()

m4b <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, asof, cusip6) %>%
 left_join(sn %>% select(permno, cusip6, namedt, nameenddt), by = "cusip6",
           relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 pick_best_by_date()

matched <- bind_rows(matched, m4b %>% distinct(master_deal_no)) %>% distinct()

# ---- Route 5: ticker fallback via stocknames (closest window) ----------------------------------
tickers <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(ticker)) %>%
 distinct(ticker) %>%
 pull(ticker)

sn_t <- if (length(tickers) > 0) {
 tbl(wrds, in_schema("crsp", "stocknames")) %>%
   filter(toupper(ticker) %in% tickers) %>%
   transmute(
     permno    = permno,
     ticker    = toupper(ticker),
     namedt    = as.Date(namedt),
     nameenddt = as.Date(nameenddt)
   ) %>%
   collect()
} else {
 tibble(permno = integer(), ticker = character(),
        namedt = as.Date(character()), nameenddt = as.Date(character()))
}

m5 <- xk %>%
 anti_join(matched, by = "master_deal_no") %>%
 filter(!is.na(ticker)) %>%
 select(master_deal_no, asof, ticker) %>%
 left_join(sn_t, by = "ticker", relationship = "many-to-many") %>%
 filter(!is.na(permno)) %>%
 pick_best_by_date()

# ---- combine + attach --------------------------------------------------------------------------
x_map <- bind_rows(m1, m2, m3, m4a, m4b, m5) %>%
 distinct(master_deal_no, permno) %>%
 group_by(master_deal_no) %>%
 summarise(
   target_permno   = paste(sort(unique(permno)), collapse = ";"),
   target_n_permno = n_distinct(permno),
   .groups = "drop"
 )

mergers_analysis <- mergers_analysis %>%
 mutate(master_deal_no = as.character(master_deal_no)) %>%
 select(-any_of(c("target_permno", "target_n_permno"))) %>%
 left_join(
   x_map %>% mutate(master_deal_no = as.character(master_deal_no)),
   by = "master_deal_no"
 ) %>%
 mutate(
   target_permno   = coalesce(target_permno, ""),
   target_n_permno = coalesce(target_n_permno, 0L)
 )

# ---- clear objects intermediate objects ---------------------------------------------------------
rm("ccm","m1","m2","m3","m4a","m4b","m5", "cus6", "sn2_cols", "sn_cols", "sn","sn_t","sn2","x_map","xk", "matched", "ccm_cols", "sn2_cusip_field", "ccm_permno_col")

#=================================================================================================
# STEP 3: Finalize Merger Sample for Institutional Ownership
# Input:  mergers_analysis
# Transformations: Event-quarter construction (t_0, t_m1, t_end), close-date eligibility (dateeff > t_0), top-N by deal value
# Output: mergers
#=================================================================================================

mergers <- mergers_analysis %>%
 filter(
   target_n_permno >= 1L,
   acquirer_n_permno >= 1L
 ) %>%
 mutate(
   t_0 = qend(dateann),
   t_m1 = qend_prev(t_0),
   t_end = if_else(
     as.Date(dateeff) == qend(dateeff),
     qend(dateeff),
     qend_prev(dateeff)
   ),
   days_to_t0 = as.integer(t_0 - as.Date(dateann))
 ) %>%
 filter(
   dateeff > t_0  # Exclude deals that closed before quarter-end measurement
 ) %>%
 	select(
  		master_deal_no,
   		dateann, dateeff, t_0, t_m1, t_end, days_to_t0,
   		deal_value,
   		pct_cash, pct_stk, pct_other, pct_unknown,
   		master_cusip, target_permno, target_n_permno,
		acusip, acquirer_permno, acquirer_n_permno
 	) %>%
 	arrange(desc(deal_value)) %>%
 	slice_head(n = cfg$top_n)

rm("mergers_analysis")

#=================================================================================================
# STEP 4: Build Granular Institutional Ownership Panel (with Aquirer Controls)
# Input:  mergers, CRSP monthly files (msf, msenames), Thomson 13F (tfn.s34type1, tfn.s34type3)
# Transformations: Pull merger and sampled non-merger control PERMNOs, construct first-vintage filer-quarter panel
# pull holdings in chunks by `fdate`; map CUSIP to PERMNO, join CRSP adjustment factors and compute split-adjusted shares 
# Output: institutional_ownership_granular
#=================================================================================================

# ---- parameters --------------------------------------------------------------------------------
begdate <- as.Date("1998-01-01")
enddate <- as.Date("2025-06-30")

# ---- Get merger firm PERMNOs -------------------------------------------------------------------
target_permnos <- mergers %>%
 transmute(permno = suppressWarnings(as.integer(sub(";.*$", "", target_permno)))) %>%
 filter(!is.na(permno)) %>%
 distinct(permno) %>%
 pull(permno)

acquirer_permnos <- mergers %>%
 transmute(permno = suppressWarnings(as.integer(sub(";.*$", "", acquirer_permno)))) %>%
 filter(!is.na(permno)) %>%
 distinct(permno) %>%
 pull(permno)

# ---- Combine for exclusion from controls -------------------------------------------------------
merger_permnos <- unique(c(target_permnos, acquirer_permnos))

# ---- Exclude BOTH from controls ----------------------------------------------------------------
exclude_clause <- paste0("AND a.permno NOT IN (", paste(merger_permnos, collapse = ","), ")")
control_query <- paste0(
 "SELECT a.permno ",
 "FROM crsp.msf a ",
 "INNER JOIN crsp.msenames b ",
   "ON a.permno = b.permno ",
   "AND a.date >= b.namedt ",
   "AND a.date <= b.nameendt ",
 "WHERE b.shrcd IN (10, 11) ",
 exclude_clause, " ",
 "AND a.date BETWEEN '", begdate, "' AND '", enddate, "' ",
 "AND ABS(a.prc) * a.shrout > 50000 ",
 "GROUP BY a.permno ",
 "HAVING COUNT(*) >= 40 ",
 "ORDER BY RANDOM() ",
 "LIMIT ", cfg$control_firms_n
)

control_permnos <- dbGetQuery(wrds, control_query) %>%
 	pull(permno)

# ---- Combine -----------------------------------------------------------------------------------
permnos_needed <- unique(c(merger_permnos, control_permnos))

# ---- create monthly crsp dataset ---------------------------------------------------------------
# ---- Query CRSP monthly stock file -------------------------------------------------------------
crsp_msf_query <- paste0("
SELECT a.permno, a.date, a.prc, a.ret, a.shrout, a.cfacpr, a.cfacshr
FROM crsp.msf a
WHERE a.permno IN (", paste(permnos_needed, collapse = ","), ")
AND a.date >= '", begdate, "'
AND a.date <= '", enddate, "'
")

crsp_msf <- dbGetQuery(wrds, crsp_msf_query)

# ---- merger tr-13f s34type1 and s34type3 -------------------------------------------------------
# ---- Get first vintage for each RDATE-MGRNO combination ----------------------------------------
s34type1_query <- paste0("
SELECT rdate, fdate, mgrno, mgrname
FROM tfn.s34type1
WHERE rdate >= '", begdate, "'
AND rdate <= '", enddate, "'
ORDER BY mgrno, rdate, fdate
")

s34type1 <- dbGetQuery(wrds, s34type1_query)

# ---- Keep first vintage (earliest fdate) for each manager-quarter ------------------------------
first_vint <- s34type1 %>%
group_by(mgrno, rdate) %>%
arrange(fdate) %>%
slice(1) %>%
ungroup()

# ---- Mark first and last reports ---------------------------------------------------------------
first_vint <- first_vint %>%
arrange(mgrno, rdate) %>%
group_by(mgrno) %>%
mutate(
First_Report = (row_number() == 1) |
(as.numeric(difftime(rdate, dplyr::lag(rdate), units = "days")) > 100),
First_Report = as.integer(First_Report)
) %>%
ungroup()

# ---- Mark last reports (reverse order) ---------------------------------------------------------
first_vint <- first_vint %>%
arrange(mgrno, desc(rdate)) %>%
group_by(mgrno) %>%
mutate(
Last_Report = (row_number() == 1) |
(as.numeric(difftime(rdate, dplyr::lag(rdate), units = "days")) > 100),
Last_Report = as.integer(Last_Report)
) %>%
ungroup() %>%
arrange(mgrno, rdate)

# ---- Add total number of filers per quarter ----------------------------------------------------
first_vint <- first_vint %>%
 group_by(rdate) %>%
 mutate(NumInst = n_distinct(mgrno)) %>%
 ungroup()

# --- extract holdings and adjust shares ---------------------------------------------------------

# ---- Get all needed fdates ---------------------------------------------------------------------
fdates_needed <- unique(first_vint$fdate)

# ---- Query s34type3 in chunks ------------------------------------------------------------------
chunk_size <- 25
fdate_chunks <- split(fdates_needed, ceiling(seq_along(fdates_needed) / chunk_size))

holdings_list <- list()

for (i in seq_along(fdate_chunks)) {
 cat(sprintf("  Processing chunk %d/%d...\r", i, length(fdate_chunks)))

 s34type3_query <- paste0("
SELECT fdate, mgrno, cusip, shares
FROM tfn.s34type3
WHERE fdate IN ('", paste(fdate_chunks[[i]], collapse = "','"), "')
AND shares > 0
")

 chunk_data <- dbGetQuery(wrds, s34type3_query)
 holdings_list[[i]] <- chunk_data
}

s34type3 <- bind_rows(holdings_list)
gc()

# ---- Merge with first_vint ---------------------------------------------------------------------
holdings_v1 <- first_vint %>%
 select(-mgrname) %>%
 inner_join(s34type3, by = c("fdate", "mgrno"))

# ---- map cusip to permno -----------------------------------------------------------------------
cusip_query <- paste0("
SELECT DISTINCT ncusip, permno
FROM crsp.msenames
WHERE permno IN (", paste(permnos_needed, collapse = ","), ")
AND ncusip IS NOT NULL
AND NOT (
COALESCE(nameendt, DATE '9999-12-31') < DATE '", begdate, "'
OR namedt > DATE '", enddate, "'
)
")

cusip_permno_map <- dbGetQuery(wrds, cusip_query)

# ---- Map to holdings ---------------------------------------------------------------------------
 holdings_v2 <- holdings_v1 %>%
   inner_join(cusip_permno_map, by = c("cusip" = "ncusip")) %>%
   filter(permno %in% permnos_needed)

# ---- build crsp_m with adjustment factors ------------------------------------------------------

# finish processing crsp_msf
crsp_mse_query <- paste0("
SELECT permno, ncusip, shrcd, namedt, nameendt
FROM crsp.msenames
WHERE permno IN (", paste(permnos_needed, collapse = ","), ")
  AND NOT (
    COALESCE(nameendt, DATE '9999-12-31') < DATE '", begdate, "'
    OR namedt > DATE '", enddate, "'
  )
")

crsp_mse <- dbGetQuery(wrds, crsp_mse_query)

# ---- Merge MSF with MSE to get shrcd, then filter to common stocks -----------------------------
crsp_m <- crsp_msf %>%
 left_join(
   crsp_mse %>%
     select(permno, ncusip, shrcd, namedt, nameendt),
   by = "permno",
   relationship = "many-to-many"
 ) %>%
 filter(
   date >= namedt,
   date <= if_else(is.na(nameendt), as.Date("9999-12-31"), nameendt),
   shrcd %in% c(10, 11)
 )

# ---- Adjust shares and align to quarter ends ---------------------------------------------------
crsp_m <- crsp_m %>%
 mutate(
   qdate = ceiling_date(as.Date(date), "quarter") - days(1),
   date = ceiling_date(as.Date(date), "month") - days(1),
   P = abs(prc) / cfacpr,
   TSO = shrout * cfacshr * 1000,
   TSO = if_else(TSO <= 0, NA_real_, TSO),
   ME = P * TSO / 1000000
 ) %>%
 select(permno, date, qdate, P, TSO, ME, cfacshr, ncusip)

# ---- Keep last monthly observation for each quarter --------------------------------------------
crsp_m <- crsp_m %>%
 group_by(permno, qdate) %>%
 arrange(desc(date)) %>%
 slice(1) %>%
 ungroup() %>%
 select(-date)

# ---- Adjust shares using CRSP factors ----------------------------------------------------------
holdings <- holdings_v2 %>%
 inner_join(
   crsp_m %>% select(permno, qdate, cfacshr),
   by = c("permno", "rdate" = "qdate")
 ) %>%
 mutate(shares_adj = shares * cfacshr) %>%
 select(rdate, mgrno, NumInst, Last_Report, permno, shares_adj)

gc()

# ---- Convert to data.table and aggregate (faster than dplyr) -----------------------------------
library(data.table)
holdings_dt <- as.data.table(holdings)

institutional_holdings_granular <- holdings_dt[,
 .(shares_adj = sum(shares_adj, na.rm = TRUE),
   n_cusips = .N),
 by = .(permno, rdate, mgrno, NumInst, Last_Report)
]

# ---- Convert back to tibble and save -----------------------------------------------------------
institutional_holdings_granular <- as_tibble(institutional_holdings_granular)

saveRDS(institutional_holdings_granular, "institutional_holdings_granular.rds")

# ---- clean up ----------------------------------------------------------------------------------
rm("acquirer_permnos", "holdings_v2", "holdings", "merger_permnos",
    "permnos_needed", "crsp_msf", "s34type1", "first_vint", "fdates_needed", "holdings_list",
    "cusip_permno_map", "crsp_mse")
gc()

#=================================================================================================
# STEP 5: Calculate Institutional Metrics at Security Level
# Input:  institutional_holdings_granular, CRSP quarter-end shares outstanding
# Transformations: Roll up to permno-quarter (IOR, IOC_HHI, counts, missingness flags)
# Output: institutional_ownership_timeseries (io_rollup)
#=================================================================================================

# ---- simple firm-quarter rollup ----------------------------------------------------------------
io_rollup <- institutional_holdings_granular %>%
 group_by(permno, rdate) %>%
 summarise(
   n_institutions = n_distinct(mgrno),
   total_shares_held = sum(shares_adj, na.rm = TRUE),
   .groups = "drop"
 )

# ---- add adjusted TSO from crsp_m --------------------------------------------------------------
io_rollup <- io_rollup %>%
 left_join(
   crsp_m %>% select(permno, qdate, TSO, P, ME),
   by = c("permno", "rdate" = "qdate")
 )

# ---- add IOR -----------------------------------------------------------------------------------
io_rollup <- io_rollup %>%
	mutate(
		IOR = total_shares_held / TSO,
		IOR_above_threshold = (IOR > cfg$winsorization)
)

# ---- add IOC_HHI -------------------------------------------------------------------------------
io_hhi <- institutional_holdings_granular %>%
 group_by(permno, rdate) %>%
 summarise(
   IO_SS = sum(shares_adj^2, na.rm = TRUE),
   IO_TOTAL = sum(shares_adj, na.rm = TRUE),
   .groups = "drop"
 ) %>%
 mutate(
   IOC_HHI = IO_SS / (IO_TOTAL^2)
 ) %>%
 select(permno, rdate, IOC_HHI)

# ---- merge -------------------------------------------------------------------------------------
io_rollup <- io_rollup %>%
 left_join(io_hhi, by = c("permno", "rdate"))

# ---- flag missing ------------------------------------------------------------------------------
io_rollup <- io_rollup %>%
 mutate(
   IO_MISSING = (n_institutions == 0 | is.na(IOR) | is.na(TSO))
 )

# ---- save timeseries panel ---------------------------------------------------------------------
saveRDS(io_rollup, "institutional_ownership_timeseries.rds")

# ---- memory cleanup ----------------------------------------------------------------------------
rm("io_hhi", "crsp_m")
gc()

#=================================================================================================
# STEP 6: Create Event Study Panels (Top sample_n Deals with IO Data)
# Input:  mergers, io_rollup
# Transformation: build target_panel and acquirer_panel with event time/buckets,
# build matched non-merger control panel from CRSP candidate universe via IO/size distance matching,
# create DiD panels (matched_panel, full_panel) with post indicators
# Output: target_panel, acquirer_panel, control_panel, matched_panel, full_panel
#=================================================================================================

# ---- TARGET PANEL ------------------------------------------------------------------------------
# ---- Identify deals with IO data available -----------------------------------------------------

deals_with_io <- mergers %>%
transmute(
  master_deal_no,
  target_permno = as.integer(sub(";.*$", "", target_permno)),
  acquirer_permno = as.integer(sub(";.*$", "", acquirer_permno)),
  dateann,
  dateeff,
  t_0,
  t_m1,
  t_end,
  deal_value,
  pct_cash, pct_stk, pct_other, pct_unknown
) %>%
filter(
  !is.na(target_permno),
  !is.na(acquirer_permno)
) %>%
# Check if target has IO data at t=-1 (critical for DiD baseline)
inner_join(
  io_rollup %>%
    filter(!is.na(IOR)) %>%
    distinct(permno, rdate),
  by = c("target_permno" = "permno", "t_m1" = "rdate")
) %>%
# Take top sample_n by deal value
arrange(desc(deal_value)) %>%
slice_head(n = cfg$sample_n)

# ---- Create target panel: merge deals with full IO timeseries ----------------------------------
target_panel <- deals_with_io %>%
inner_join(
  io_rollup,
  by = c("target_permno" = "permno")
) %>%
mutate(
  firm_type = "target"
)

#---- overlay event time grid --------------------------------------------------------------------
target_panel <- target_panel %>%
mutate(
  event_time = q_index(rdate) - q_index(t_0),

  event_bucket = case_when(
    event_time >= -4 & event_time <= -2 ~ "t_-4_to_-2",
    event_time == -1 ~ "t_-1_baseline",
    event_time == 0  ~ "t_0",
    event_time >= 1 & event_time <= 2 ~ "t_1_to_2",
    event_time >= 3 & event_time <= 4 ~ "t_3_to_4",
    event_time < -4  ~ "pre_window",
    event_time > 4   ~ "post_window",
    TRUE ~ NA_character_
  )
)

target_panel <- target_panel %>%
filter(rdate <= t_end)  # Only keep quarters up to (but not after) completion

saveRDS(target_panel, "target_panel.rds")

# ---- memory cleanup ----------------------------------------------------------------------------
gc()

# ---- ACQUIRER PANEL ----------------------------------------------------------------------------
acquirer_panel <- deals_with_io %>%
inner_join(
  io_rollup,
  by = c("acquirer_permno" = "permno")
) %>%
mutate(
  firm_type = "acquirer"
)

# ---- Overlay event time grid (matched to target's deal announcement) --------------------------
acquirer_panel <- acquirer_panel %>%
mutate(
  event_time = q_index(rdate) - q_index(t_0),

  event_bucket = case_when(
   event_time >= -4 & event_time <= -2 ~ "t_-4_to_-2",
   event_time == -1 ~ "t_-1_baseline",
   event_time == 0  ~ "t_0",
    event_time >= 1 & event_time <= 2 ~ "t_1_to_2",
    event_time >= 3 & event_time <= 4 ~ "t_3_to_4",
    event_time < -4  ~ "pre_window",
    event_time > 4   ~ "post_window",
    TRUE ~ NA_character_
  )
) %>%
filter(rdate <= t_end)  # Truncate at merger close (same as targets)

saveRDS(acquirer_panel, "acquirer_panel.rds")

# ---- MATCH CONTROLS ----------------------------------------------------------------------------
	   
# ---- configuration -----------------------------------------------------------------------------
CONTROL_TARGET_RATIO <- 2
n_controls_target <- n_distinct(target_panel$target_permno) * CONTROL_TARGET_RATIO
min_market_cap <- 500
min_IO_threshold <- 0.60

# ---- Step A: Pull Larger Candidate Pool with Tight Restrictions --------------------------------
candidate_multiplier <- 5
n_candidates <- n_controls_target * candidate_multiplier

# ---- query with higher ME threshold ------------------------------------------------------------
control_query_improved <- paste0(
"SELECT a.permno, ",
"       AVG(ABS(a.prc) * a.shrout) / 1000 as avg_me_millions ",
"FROM crsp.msf a ",
"INNER JOIN crsp.msenames b ",
  "ON a.permno = b.permno ",
  "AND a.date >= b.namedt ",
  "AND a.date <= b.nameendt ",
"WHERE b.shrcd IN (10, 11) ",
exclude_clause, " ",
"AND a.date BETWEEN '", begdate, "' AND '", enddate, "' ",
"AND ABS(a.prc) * a.shrout > ", min_market_cap * 1000, " ",  # $500M threshold
"GROUP BY a.permno ",
"HAVING COUNT(*) >= 40 ",
"  AND AVG(ABS(a.prc) * a.shrout) / 1000 > ", min_market_cap, " ",  # Avg ME > $500M
"ORDER BY RANDOM() ",
"LIMIT ", n_candidates
)

candidate_permnos <- dbGetQuery(wrds, control_query_improved) %>%
pull(permno)

# ---- Step B: Filter Candidates by IO Level -----------------------------------------------------
candidate_chars <- io_rollup %>%
filter(permno %in% candidate_permnos) %>%
group_by(permno) %>%
summarise(
  median_IOR = median(IOR, na.rm = TRUE),
  median_ME = median(ME, na.rm = TRUE),
  p25_IOR = quantile(IOR, 0.25, na.rm = TRUE),
  n_quarters = n(),
  .groups = "drop"
) %>%
filter(
  !is.na(median_IOR),
  !is.na(median_ME),
  n_quarters >= 20,
  median_IOR >= min_IO_threshold,  # NEW: IO threshold
  median_ME >= min_market_cap      # Verify ME filter
)

# ---- Step C: Match Using Mahalanobis Distance --------------------------------------------------
# ---- Get target characteristics ----------------------------------------------------------------
target_chars <- target_panel %>%
filter(event_time == -1) %>%
summarise(
  median_IOR = median(IOR, na.rm = TRUE),
  mean_IOR = mean(IOR, na.rm = TRUE),
  median_ME = median(ME, na.rm = TRUE),
  mean_ME = mean(ME, na.rm = TRUE),
  sd_ME = sd(ME, na.rm = TRUE)
)

# ---- Standardize -------------------------------------------------------------------------------
candidate_chars <- candidate_chars %>%
mutate(
  IOR_std = (median_IOR - target_chars$mean_IOR) / sd(median_IOR, na.rm = TRUE),
  ME_std = (log(median_ME) - log(target_chars$mean_ME)) /
           sd(log(median_ME), na.rm = TRUE)
)

# ---- Calculate distance (weight IO more heavily since it's the key concern) --------------------
candidate_chars <- candidate_chars %>%
mutate(
  distance = sqrt(2 * IOR_std^2 + ME_std^2)  # Weight IO 2x
)

# ---- Select best matches -----------------------------------------------------------------------
matched_controls <- candidate_chars %>%
arrange(distance) %>%
head(n_controls_target)

control_permnos_matched <- matched_controls$permno

# ---- Step D: balance Check ---------------------------------------------------------------------
balance_table <- data.frame(
Group = c("Targets", "Controls (Matched)", "Difference"),
N = c(
  n_distinct(target_panel$target_permno),
  length(control_permnos_matched),
  NA
),
Median_IO = c(
  target_chars$median_IOR * 100,
  median(matched_controls$median_IOR, na.rm = TRUE) * 100,
  (target_chars$median_IOR - median(matched_controls$median_IOR, na.rm = TRUE)) * 100
),
Median_ME_B = c(
  target_chars$median_ME / 1000,
  median(matched_controls$median_ME, na.rm = TRUE) / 1000,
  (target_chars$median_ME - median(matched_controls$median_ME, na.rm = TRUE)) / 1000
)
)

print(balance_table, digits = 2)

# T-tests
t_test_IO <- t.test(
target_panel$IOR[target_panel$event_time == -1],
matched_controls$median_IOR
)

t_test_ME <- t.test(
log(target_panel$ME[target_panel$event_time == -1]),
log(matched_controls$median_ME)
)

# ---- Standardized mean differences (SMD < 0.1 is good) -----------------------------------------
smd_IO <- (target_chars$mean_IOR - mean(matched_controls$median_IOR)) /
        sd(target_panel$IOR[target_panel$event_time == -1])
smd_ME <- (log(target_chars$mean_ME) - mean(log(matched_controls$median_ME))) /
        sd(log(target_panel$ME[target_panel$event_time == -1]))

# ---- step E: Overlap Plot ----------------------------------------------------------------------

overlap_data <- bind_rows(
target_panel %>%
  filter(event_time == -1) %>%
  transmute(IOR = IOR * 100, ME_log = log(ME), group = "Target"),
matched_controls %>%
  transmute(IOR = median_IOR * 100, ME_log = log(median_ME), group = "Control")
)

p_overlap <- ggplot(overlap_data, aes(x = IOR, fill = group)) +
geom_density(alpha = 0.5) +
scale_fill_manual(values = c("Target" = "blue", "Control" = "darkgreen")) +
labs(
  title = "Institutional Ownership Distribution",
  subtitle = "Overlap between targets and matched controls",
  x = "Institutional Ownership (%)",
  y = "Density",
  fill = "Group"
) +
theme_minimal(base_size = 12) +
theme(plot.title = element_text(face = "bold"))

print(p_overlap)

# ---- Return matched control permnos ------------------------------------------------------------
control_permnos <- control_permnos_matched

# ---- CREATE CONTROL PANEL (NON-MERGER FIRMS) ---------------------------------------------------
# ---- Get date range from target panel ----------------------------------------------------------
date_range <- target_panel %>%
summarise(
  min_date = min(rdate, na.rm = TRUE),
  max_date = max(rdate, na.rm = TRUE)
)

# ---- Create control panel: all quarters for control firms --------------------------------------
control_panel <- io_rollup %>%
filter(
  permno %in% control_permnos,
  rdate >= date_range$min_date,
  rdate <= date_range$max_date
) %>%
mutate(
  firm_type = "control",
  event_time = NA_integer_,
  event_bucket = NA_character_,
  master_deal_no = NA_character_,
  target_permno = NA_integer_,
  acquirer_permno = NA_integer_,
  dateann = as.Date(NA),
  dateeff = as.Date(NA),
  t_0 = as.Date(NA),
  t_m1 = as.Date(NA),
  t_end = as.Date(NA),
  deal_value = NA_real_,
  pct_cash = NA_real_,
  pct_stk = NA_real_,
  pct_other = NA_real_,
  pct_unknown = NA_real_
)

saveRDS(control_panel, "control_panel.rds")

# ---- clean up ----------------------------------------------------------------------------------
rm("date_range", "candidate_permnos", "candidate_chars", "target_chars", "matched_controls",
    "control_permnos_matched", "overlap_data")

#=================================================================================================
# STEP 7: Combine All Panels
# Input: target_panel, acquirer_panel, control_panel
# Output: full_panel, matched_panel
#=================================================================================================

# ---- Panel 1: Targets + Acquirers (matched DiD) ------------------------------------------------
matched_panel <- bind_rows(
target_panel %>% mutate(treated = 1),
acquirer_panel %>% mutate(treated = 0)
) %>%
arrange(master_deal_no, firm_type, rdate)

# ---- Panel 2: Targets + Random Controls (traditional DiD) --------------------------------------
full_panel <- bind_rows(
target_panel %>% mutate(treated = 1),
control_panel %>% mutate(treated = 0)
) %>%
arrange(permno, rdate)

# ---- Create post indicator ---------------------------------------------------------------------
matched_panel <- matched_panel %>%
mutate(post = as.integer(event_time >= 0))

full_panel <- full_panel %>%
mutate(post = as.integer(!is.na(event_time) & event_time >= 0))

saveRDS(matched_panel, "matched_panel.rds")
saveRDS(full_panel, "full_panel.rds")

#=================================================================================================
# STEP 8: EVENT STUDY: Institutional Ownership Levels (Targets Only)
# Event-study regressions: IOR levels around announcement (all/cash/stock subsamples, trend-break specs)
#=================================================================================================

# ---- Prepare data ------------------------------------------------------------------------------
event_study_data <- target_panel %>%
 filter(event_time >= -4, event_time <= 4) %>%
 transmute(
   permno = target_permno,
   rdate,
   event_time,
   IOR = IOR * 100,
   ME
 ) %>%
 mutate(
   pre_43 = as.integer(event_time >= -4 & event_time <= -3),
   pre_2 = as.integer(event_time == -2),
   post_0 = as.integer(event_time == 0),
   post_12 = as.integer(event_time >= 1 & event_time <= 2),
   post_34 = as.integer(event_time >= 3 & event_time <= 4),
   et = as.integer(event_time),
   post = as.integer(et >= 0),
   log_ME = log(ME),
   year = lubridate::year(rdate)
 ) %>%
 filter(!is.na(IOR))

# ---- (1) Simple post ---------------------------------------------------------------------------
es1 <- feols(IOR ~ post | permno + rdate, data = event_study_data, cluster = ~permno)

# ---- (2) Binned --------------------------------------------------------------------------------
es2 <- feols(IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno + rdate,
            data = event_study_data, cluster = ~permno)

# ---- (3) Binned with firm×year trends ----------------------------------------------------------
es3 <- feols(IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno^year + rdate,
            data = event_study_data, cluster = ~permno)

# ---- (4) Linear trend break --------------------------------------------------------------------
es4 <- feols(IOR ~ et + post + et:post | rdate,
            data = event_study_data, cluster = ~permno)

# ---- Print table -------------------------------------------------------------------------------
modelsummary(
 list("(1) Post" = es1, "(2) Binned" = es2, "(3) + Trends" = es3, "(4) Linear" = es4),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c(
   "post" = "Post-announcement",
   "pre_43" = "Pre: t=-4 to -3",
   "pre_2" = "Pre: t=-2",
   "post_0" = "Post: t=0",
   "post_12" = "Post: t=1-2",
   "post_34" = "Post: t=3-4",
   "et" = "Trend (per qtr)",
   "et:post" = "Post × Trend (break)"
 ),
 gof_map = c("nobs", "r.squared", "adj.r.squared"),
 notes = c(
   "Dependent variable: Institutional ownership ratio (%), 0-100 scale.",
   "Reference period: t=-1 (quarter before announcement).",
   "Cols (1)-(3): Firm and quarter FE. Col (3): + Firm×year trends. Col (4): Quarter FE only.",
   "Standard errors clustered at firm level."
 ),
 output = "markdown"
)

# ---- appendix: Create quarter-to-quarter changes -------------------------------------------------
test_data <- target_panel %>%
 filter(event_time >= -4, event_time <= 4) %>%
 arrange(target_permno, rdate) %>%
 group_by(target_permno) %>%
 mutate(
   dIOR_qtq = (IOR - lag(IOR)) * 100,
   pre_43 = as.integer(event_time >= -4 & event_time <= -3),
   pre_2 = as.integer(event_time == -2),
   post_0 = as.integer(event_time == 0),
   post_12 = as.integer(event_time >= 1 & event_time <= 2),
   post_34 = as.integer(event_time >= 3 & event_time <= 4)
 ) %>%
 ungroup() %>%
 filter(!is.na(dIOR_qtq))

test_model <- feols(
 dIOR_qtq ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | target_permno + rdate,
 data = test_data,
 cluster = ~target_permno
)

summary(test_model)

# ---- Cash Deals Only ---------------------------------------------------------------------------

# ---- Prepare data with payment filters ---------------------------------------------------------
event_study_all <- target_panel %>%
 filter(event_time >= -4, event_time <= 4) %>%
 transmute(
   permno = target_permno,
   rdate, event_time,
   IOR = IOR * 100,
   ME,
   pct_cash
 ) %>%
 mutate(
   pre_43 = as.integer(event_time >= -4 & event_time <= -3),
   pre_2 = as.integer(event_time == -2),
   post_0 = as.integer(event_time == 0),
   post_12 = as.integer(event_time >= 1 & event_time <= 2),
   post_34 = as.integer(event_time >= 3 & event_time <= 4),
   et = as.integer(event_time),
   post = as.integer(et >= 0),
   year = lubridate::year(rdate)
 ) %>%
 filter(!is.na(IOR))

# ---- Filter to cash deals ----------------------------------------------------------------------
event_study_cash75 <- event_study_all %>% filter(!is.na(pct_cash), pct_cash >= 75)
event_study_cash90 <- event_study_all %>% filter(!is.na(pct_cash), pct_cash >= 90)

# ---- Run models for each sample ----------------------------------------------------------------

# ---- All deals ---------------------------------------------------------------------------------
all_deals <- feols(
 IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno + rdate,
 data = event_study_all, cluster = ~permno
)

# ---- Cash ≥75% ---------------------------------------------------------------------------------
cash75 <- feols(
 IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno + rdate,
 data = event_study_cash75, cluster = ~permno
)

# ---- Cash ≥90% ---------------------------------------------------------------------------------
cash90 <- feols(
 IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno + rdate,
 data = event_study_cash90, cluster = ~permno
)

# ---- Print comparison table --------------------------------------------------------------------
modelsummary(
 list("(1) All Deals" = all_deals,
      "(2) Cash ≥75%" = cash75,
      "(3) Cash ≥90%" = cash90),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c(
   "pre_43" = "Pre: t=-4 to -3",
   "pre_2" = "Pre: t=-2",
   "post_0" = "Post: t=0",
   "post_12" = "Post: t=1-2",
   "post_34" = "Post: t=3-4"
 ),
 gof_map = c("nobs", "r.squared"),
 notes = c(
   "Dependent variable: Institutional ownership ratio (%), 0-100 scale.",
   "Reference period: t=-1. Firm and quarter FE. SEs clustered at firm level.",
   "Column (1): All deals. Columns (2)-(3): Cash deals only (≥75% or ≥90% cash consideration)."
 ),
 output = "markdown"
)

# ---- Cash vs Non-Cash Difference ---------------------------------------------------------------

# ---- Create pooled sample with cash indicator --------------------------------------------------
pooled_data <- event_study_all %>%
 filter(!is.na(pct_cash)) %>%
 mutate(
   is_cash75 = as.integer(pct_cash >= 75),
   is_cash90 = as.integer(pct_cash >= 90),

   # Interactions with post periods
   cash75_post_0 = is_cash75 * post_0,
   cash75_post_12 = is_cash75 * post_12,
   cash75_post_34 = is_cash75 * post_34
 )

# ---- Test model: Cash interaction --------------------------------------------------------------
test_cash <- feols(
 IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 +
       cash75_post_0 + cash75_post_12 + cash75_post_34 |
       permno + rdate,
 data = pooled_data,
 cluster = ~permno
)

# ---- Extract results ---------------------------------------------------------------------------
diff_t0 <- coef(test_cash)["cash75_post_0"]
se_t0 <- se(test_cash)["cash75_post_0"]
t_stat_t0 <- diff_t0 / se_t0
p_val_t0 <- 2 * pt(-abs(t_stat_t0), df = test_cash$nobs)

diff_t12 <- coef(test_cash)["cash75_post_12"]
se_t12 <- se(test_cash)["cash75_post_12"]

diff_t34 <- coef(test_cash)["cash75_post_34"]
se_t34 <- se(test_cash)["cash75_post_34"]

# ---- Print full comparison ---------------------------------------------------------------------
summary(test_cash)

# ---- Recreate event_study_all with pct_stk -----------------------------------------------------
event_study_all <- target_panel %>%
 filter(event_time >= -4, event_time <= 4) %>%
 transmute(
   permno = target_permno,
   rdate, event_time,
   IOR = IOR * 100,
   ME,
   pct_cash,
   pct_stk
 ) %>%
 mutate(
   pre_43 = as.integer(event_time >= -4 & event_time <= -3),
   pre_2 = as.integer(event_time == -2),
   post_0 = as.integer(event_time == 0),
   post_12 = as.integer(event_time >= 1 & event_time <= 2),
   post_34 = as.integer(event_time >= 3 & event_time <= 4),
   et = as.integer(event_time),
   post = as.integer(et >= 0),
   year = lubridate::year(rdate)
 ) %>%
 filter(!is.na(IOR))

# ---- run stock deal filters --------------------------------------------------------------------
event_study_stock75 <- event_study_all %>% filter(!is.na(pct_stk), pct_stk >= 75)
event_study_stock90 <- event_study_all %>% filter(!is.na(pct_stk), pct_stk >= 90)

# ---- Estimate models ---------------------------------------------------------------------------
stock75 <- feols(IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno + rdate,
                data = event_study_stock75, cluster = ~permno)

stock90 <- feols(IOR ~ pre_43 + pre_2 + post_0 + post_12 + post_34 | permno + rdate,
                data = event_study_stock90, cluster = ~permno)

# ---- Print -------------------------------------------------------------------------------------
modelsummary(
 list("(1) All Deals" = all_deals, "(2) Stock ≥75%" = stock75, "(3) Stock ≥90%" = stock90),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c("pre_43" = "Pre: t=-4 to -3", "pre_2" = "Pre: t=-2",
                 "post_0" = "Post: t=0", "post_12" = "Post: t=1-2", "post_34" = "Post: t=3-4"),
 gof_map = c("nobs", "r.squared"),
 output = "markdown"
)

#---- structural break analysis ------------------------------------------------------------------
panel_analysis <- matched_panel %>%
  filter(event_time >= -4, event_time <= 4) %>%
  mutate(
    firm_permno = if_else(firm_type == "target", target_permno, acquirer_permno),
    treated = as.integer(firm_type == "target"),
    et = as.integer(event_time),
    
    # simple specification
    post = as.integer(et >= 0),
    
    # For trend break, use et and et×post interaction
    et_post_interact = et * post,
    
    # Baseline
    IOR_baseline = IOR[event_time == -1][1],
    dIOR = (IOR - IOR_baseline) * 100
  ) %>%
  group_by(master_deal_no, firm_permno) %>%
  mutate(IOR_baseline = dplyr::first(na.omit(IOR_baseline))) %>%
  ungroup() %>%
  filter(!is.na(IOR_baseline), !is.na(dIOR))

# ---- target firms ------------------------------------------------------------------------------
panel_tgt <- panel_analysis %>% filter(treated == 1)

# (1A) Level shift only (baseline)
tgt_1a <- feols(
  dIOR ~ post | firm_permno + rdate,
  data = panel_tgt,
  cluster = ~master_deal_no
)

# ---- (1B) trend break without collinearity -----------------------------------------------------
# This estimates: α + β₁·t + β₂·Post + β₃·(t×Post)
tgt_1b <- feols(
  dIOR ~ et + post + et:post | rdate,  # Remove firm FE to avoid collinearity
  data = panel_tgt,
  cluster = ~master_deal_no
)

# ---- (1C) With firm FE but only post interaction -----------------------------------------------
tgt_1c <- feols(
  dIOR ~ post + et:post | firm_permno + rdate,
  data = panel_tgt,
  cluster = ~master_deal_no
)

# ---- (1D) Binned (most stable with firm FE) ----------------------------------------------------
panel_tgt <- panel_tgt %>%
  mutate(
    t_m4 = as.integer(et == -4),
    t_m3 = as.integer(et == -3),
    t_m2 = as.integer(et == -2),
    # t_m1 = baseline (omitted)
    t_0 = as.integer(et == 0),
    t_1 = as.integer(et == 1),
    t_2 = as.integer(et == 2),
    t_3 = as.integer(et == 3),
    t_4 = as.integer(et == 4)
  )

tgt_1d <- feols(
  dIOR ~ i(et, ref = -1) | firm_permno + rdate,
  data = panel_tgt,
  cluster = ~master_deal_no
)

# ---- (1E) i() syntax (equivalent to 1d, cleaner) -----------------------------------------------
tgt_1e <- feols(
  dIOR ~ i(et, ref = -1) | firm_permno + rdate,
  data = panel_tgt,
  cluster = ~master_deal_no
)

# ---- Print results -----------------------------------------------------------------------------
modelsummary(
  list(
    "(1A) Level" = tgt_1a,
    "(1B) Trend\nBreak" = tgt_1b,
    "(1C) Post×Trend\n(with FE)" = tgt_1c,
    "(1D) Saturated" = tgt_1d
  ),
  stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
  coef_rename = c(
    "post" = "Post (level shift)",
    "et" = "Trend (per qtr)",
    "et:post" = "Post × Trend (break)",
    "t_m4" = "t = -4",
    "t_m3" = "t = -3",
    "t_m2" = "t = -2",
    "t_0" = "t = 0",
    "t_1" = "t = 1",
    "t_2" = "t = 2",
    "t_3" = "t = 3",
    "t_4" = "t = 4"
  ),
  gof_map = c("nobs", "r.squared"),
  output = "markdown"
)

# ---- Test if break is significant --------------------------------------------------------------
trend_break_t <- coef(tgt_1b)["et:post"] / se(tgt_1b)["et:post"]
trend_break_p <- 2 * (1 - pt(abs(trend_break_t), df = tgt_1b$nobs - length(coef(tgt_1b))))

#=================================================================================================
# STEP 12: Institutional Flow Decomposition (Entrants, Exiters, Stayers)
# Input:  institutional_holdings_granular, target_panel
# Output: Flow decomposition analysis
#=================================================================================================

# ---- Prepare deal-level data -------------------------------------------------------------------

deals_flow <- target_panel %>%
 filter(event_time %in% c(-1, 0)) %>%
 group_by(master_deal_no, target_permno) %>%
 summarise(
   t_m1 = rdate[event_time == -1][1],
   t_0 = rdate[event_time == 0][1],
   TSO_tm1 = TSO[event_time == -1][1],
   IOR_tm1 = IOR[event_time == -1][1],
   IOR_t0 = IOR[event_time == 0][1],
   deal_value = deal_value[1],
   pct_cash = pct_cash[1],
   pct_stk = pct_stk[1],
   .groups = "drop"
 ) %>%
 filter(!is.na(t_m1), !is.na(t_0), !is.na(TSO_tm1)) %>%
 mutate(
   target_permno = as.integer(target_permno),
   master_deal_no = as.character(master_deal_no)
 )

# ---- Get holdings ------------------------------------------------------------------------------

holdings_tm1 <- institutional_holdings_granular %>%
 inner_join(
   deals_flow %>% select(master_deal_no, target_permno, t_m1),
   by = c("permno" = "target_permno", "rdate" = "t_m1")
 ) %>%
 filter(shares_adj > 0) %>%
 transmute(master_deal_no, permno, mgrno, shares_tm1 = shares_adj)

holdings_t0 <- institutional_holdings_granular %>%
 inner_join(
   deals_flow %>% select(master_deal_no, target_permno, t_0),
   by = c("permno" = "target_permno", "rdate" = "t_0")
 ) %>%
 filter(shares_adj > 0) %>%
 transmute(master_deal_no, permno, mgrno, shares_t0 = shares_adj)

# ---- Classify (institution-level) -------------------------------------------------------------

flow_panel <- full_join(
 holdings_tm1,
 holdings_t0,
 by = c("master_deal_no", "permno", "mgrno")
) %>%
 mutate(
   shares_tm1 = coalesce(shares_tm1, 0),
   shares_t0  = coalesce(shares_t0, 0),

   Entrant = as.integer(shares_tm1 == 0 & shares_t0 > 0),
   Exiter  = as.integer(shares_tm1 > 0 & shares_t0 == 0),
   Stayer  = as.integer(shares_tm1 > 0 & shares_t0 > 0),

   shares_change = shares_t0 - shares_tm1
 ) %>%
 left_join(deals_flow, by = c("master_deal_no", "permno" = "target_permno"))

# ---- Aggregate to deal×firm (deal-level) -------------------------------------------------------

deals_panel <- flow_panel %>%
 group_by(master_deal_no, permno) %>%
 summarise(
   n_entrants = sum(Entrant, na.rm = TRUE),
   n_exiters  = sum(Exiter,  na.rm = TRUE),
   n_stayers  = sum(Stayer,  na.rm = TRUE),

   shares_entry         = sum(shares_t0  * Entrant, na.rm = TRUE),
   shares_exit          = sum(shares_tm1 * Exiter,  na.rm = TRUE),
   shares_stayer_change = sum(shares_change * Stayer, na.rm = TRUE),

   t_m1      = dplyr::first(t_m1),
   t_0       = dplyr::first(t_0),
   TSO_tm1   = dplyr::first(TSO_tm1),
   IOR_tm1   = dplyr::first(IOR_tm1),
   IOR_t0    = dplyr::first(IOR_t0),
   deal_value = dplyr::first(deal_value),
   pct_cash   = dplyr::first(pct_cash),
   pct_stk    = dplyr::first(pct_stk),

   .groups = "drop"
 ) %>%
 filter(!is.na(TSO_tm1), TSO_tm1 > 0) %>%
 mutate(
   entry_pct = (shares_entry / TSO_tm1) * 100,
   exit_pct  = (shares_exit  / TSO_tm1) * 100,
   stayer_change_pct = (shares_stayer_change / TSO_tm1) * 100,

   net_change_pct = entry_pct - exit_pct + stayer_change_pct,
   actual_IOR_change = (IOR_t0 - IOR_tm1) * 100,

   deal_type = case_when(
     !is.na(pct_cash) & pct_cash >= 90 ~ "Cash",
     !is.na(pct_stk)  & pct_stk  >= 90 ~ "Stock",
     TRUE ~ "Mixed"
   )
 )

# ---- Results -----------------------------------------------------------------------------------

cat("Overall Statistics:\n")
cat(sprintf("  Mean exit flow:       %.2f%% of TSO\n", mean(deals_panel$exit_pct)))
cat(sprintf("  Mean entry flow:      %.2f%% of TSO\n", mean(deals_panel$entry_pct)))
cat(sprintf("  Mean stayer change:   %.2f%% of TSO\n", mean(deals_panel$stayer_change_pct)))
cat(sprintf("  Net change (decomp):  %.2f pp\n", mean(deals_panel$net_change_pct)))
cat(sprintf("  Actual IOR change:    %.2f pp (verification)\n\n", mean(deals_panel$actual_IOR_change)))

# ---- Tests -------------------------------------------------------------------------------------
t_exit <- t.test(deals_panel$exit_pct)
t_entry <- t.test(deals_panel$entry_pct)
t_net <- t.test(deals_panel$net_change_pct)

cat("Statistical Tests:\n")
cat(sprintf("  Exit > 0:  t=%.2f, p=%.4f %s\n", t_exit$statistic, t_exit$p.value,
           ifelse(t_exit$p.value < 0.01, "***", "")))
cat(sprintf("  Entry > 0: t=%.2f, p=%.4f %s\n", t_entry$statistic, t_entry$p.value,
           ifelse(t_entry$p.value < 0.01, "***", "")))
cat(sprintf("  Net ≠ 0:   t=%.2f, p=%.4f %s\n\n", t_net$statistic, t_net$p.value,
           ifelse(t_net$p.value < 0.01, "***", "")))

# ---- Cash vs Stock -----------------------------------------------------------------------------
by_type <- deals_panel %>%
 filter(deal_type %in% c("Cash", "Stock")) %>%
 group_by(deal_type) %>%
 summarise(across(c(entry_pct, exit_pct, net_change_pct),
                  list(mean = mean, sd = sd, n = ~n())))

print(by_type, width = Inf)

# ---- clean up ----------------------------------------------------------------------------------
rm("deals_flow", "holdings_tm1", "holdings_t0")

#=================================================================================================
# STEP 13: Electorate Replacement Analysis (Top Holders)
# Input:  institutional_holdings_granular, target_panel
# Output: Jaccard & Cosine replacement for top-K holders
#=================================================================================================

# ---- Helper functions --------------------------------------------------------------------------
jaccard_similarity <- function(set_a, set_b) {
 if (length(set_a) == 0 || length(set_b) == 0) return(NA_real_)
 intersection <- length(intersect(set_a, set_b))
 union <- length(union(set_a, set_b))
 if (union == 0) return(NA_real_)
 intersection / union
}

cosine_similarity <- function(weights_a, weights_b, ids_a, ids_b) {
 all_ids <- union(ids_a, ids_b)
 if (length(all_ids) == 0) return(NA_real_)

 vec_a <- sapply(all_ids, function(id) {
   idx <- which(ids_a == id)
   if (length(idx) > 0) weights_a[idx[1]] else 0
 })

 vec_b <- sapply(all_ids, function(id) {
   idx <- which(ids_b == id)
   if (length(idx) > 0) weights_b[idx[1]] else 0
 })

 dot_product <- sum(vec_a * vec_b)
 norm_a <- sqrt(sum(vec_a^2))
 norm_b <- sqrt(sum(vec_b^2))

 if (norm_a == 0 || norm_b == 0) return(NA_real_)
 dot_product / (norm_a * norm_b)
}

compute_hhi <- function(shares) {
 if (length(shares) == 0 || sum(shares) == 0) return(NA_real_)
 share_fracs <- shares / sum(shares)
 sum(share_fracs^2) * 10000
}

# ---- Prepare data ------------------------------------------------------------------------------
quarters_needed <- target_panel %>%
 filter(event_time %in% c(-2, -1, 0)) %>%
 transmute(
   master_deal_no = as.character(master_deal_no),
   target_permno = as.integer(target_permno),
   event_time, rdate, deal_value, pct_cash, pct_stk
 ) %>%
 distinct()

holdings_multi <- institutional_holdings_granular %>%
 inner_join(quarters_needed, by = c("permno" = "target_permno", "rdate")) %>%
 filter(shares_adj > 0) %>%
 transmute(master_deal_no, permno, event_time, mgrno, shares = shares_adj,
           deal_value, pct_cash, pct_stk)

# ---- Function to get top-K metrics -------------------------------------------------------------
calculate_replacement <- function(holdings_df, periods, k) {

 # Get top-K at each period
 topk_all <- holdings_df %>%
   filter(event_time %in% periods) %>%
   group_by(master_deal_no, permno, event_time) %>%
   arrange(desc(shares)) %>%
   slice_head(n = k) %>%
   mutate(
     total_topk_shares = sum(shares),
     share_weight = shares / total_topk_shares
   ) %>%
   ungroup()

 # Split by period
 period_a <- periods[1]
 period_b <- periods[2]

 topk_a <- topk_all %>% filter(event_time == period_a)
 topk_b <- topk_all %>% filter(event_time == period_b)

 # Calculate for each deal
 deals_unique <- topk_a %>% distinct(master_deal_no, permno, deal_value, pct_cash, pct_stk)

 deals_unique %>%
   mutate(
     jaccard_repl = map2_dbl(master_deal_no, permno, ~{
       mgrs_a <- topk_a %>% filter(master_deal_no == .x, permno == .y) %>% pull(mgrno)
       mgrs_b <- topk_b %>% filter(master_deal_no == .x, permno == .y) %>% pull(mgrno)
       if (length(mgrs_a) == 0 | length(mgrs_b) == 0) return(NA_real_)
       1 - jaccard_similarity(mgrs_a, mgrs_b)
     }),

     cosine_repl = map2_dbl(master_deal_no, permno, ~{
       data_a <- topk_a %>% filter(master_deal_no == .x, permno == .y)
       data_b <- topk_b %>% filter(master_deal_no == .x, permno == .y)
       if (nrow(data_a) == 0 | nrow(data_b) == 0) return(NA_real_)
       1 - cosine_similarity(data_a$share_weight, data_b$share_weight,
                            data_a$mgrno, data_b$mgrno)
     }),

     hhi_a = map2_dbl(master_deal_no, permno, ~{
       shares <- topk_a %>% filter(master_deal_no == .x, permno == .y) %>% pull(shares)
       compute_hhi(shares)
     }),

     hhi_b = map2_dbl(master_deal_no, permno, ~{
       shares <- topk_b %>% filter(master_deal_no == .x, permno == .y) %>% pull(shares)
       compute_hhi(shares)
     }),

     delta_hhi = hhi_b - hhi_a,

     deal_type = case_when(
       !is.na(pct_cash) & pct_cash >= 90 ~ "Cash",
       !is.na(pct_stk) & pct_stk >= 90 ~ "Stock",
       TRUE ~ "Mixed"
     )
   )
}

# ---- Panel A: Treatment (t=-1 to t=0) ----------------------------------------------------------
panel_a <- map_dfr(c(5, 10, 20), ~{
 res <- calculate_replacement(holdings_multi, c(-1, 0), .x)
 res %>%
   summarise(
     K = .x,
     Mean_Jaccard = mean(jaccard_repl, na.rm = TRUE) * 100,
     Mean_Cosine = mean(cosine_repl, na.rm = TRUE) * 100,
     Mean_ΔHHI = mean(delta_hhi, na.rm = TRUE)
   )
})

# ---- Panel B: Placebo (t=-2 to t=-1) -----------------------------------------------------------
panel_b <- map_dfr(c(5, 10, 20), ~{
 res <- calculate_replacement(holdings_multi, c(-2, -1), .x)
 res %>%
   summarise(
     K = .x,
     Mean_Jaccard = mean(jaccard_repl, na.rm = TRUE) * 100,
     Mean_Cosine = mean(cosine_repl, na.rm = TRUE) * 100,
     Mean_ΔHHI = mean(delta_hhi, na.rm = TRUE)
   )
})

# ---- Combined table ----------------------------------------------------------------------------
combined <- bind_rows(
 panel_a %>% mutate(Period = "Treatment (t=-1→0)"),
 panel_b %>% mutate(Period = "Placebo (t=-2→-1)")
) %>%
 select(Period, K, Mean_Jaccard, Mean_Cosine, Mean_ΔHHI)

print(combined, digits = 2)

# ---- DiD test ----------------------------------------------------------------------------------
did_test <- tibble(
 K = c(5, 10, 20),
 DiD_Jaccard = panel_a$Mean_Jaccard - panel_b$Mean_Jaccard,
 DiD_Cosine = panel_a$Mean_Cosine - panel_b$Mean_Cosine
)

print(did_test, digits = 2)

# ---- clean up ---------------------------------------------------------------------------------
rm("quarters_needed")
gc()

#=================================================================================================
# STEP 14: Arbitrageur Validation - Entry Pattern Analysis
# Input:  flow_panel, institutional_holdings_granular, target_panel, mergers
# Output: Institution-level entry intensity metrics
#=================================================================================================

# ---- Get entrants with their positions ---------------------------------------------------------
entrants_with_volume <- flow_panel %>%
 filter(Entrant == 1) %>%
 mutate(
   deal_type = case_when(
     !is.na(pct_cash) & pct_cash >= 75 ~ "Cash",
     !is.na(pct_stk)  & pct_stk  >= 75 ~ "Stock",
     TRUE ~ "Mixed"
   )
 ) %>%
 select(mgrno, master_deal_no, permno, shares_t0, deal_type)

# ---- Institution-level metrics -------------------------------------------------------------------
institution_metrics <- entrants_with_volume %>%
 group_by(mgrno) %>%
 summarise(
   n_entries = n(),
   total_shares_entered = sum(shares_t0, na.rm = TRUE),
   avg_position_size = mean(shares_t0, na.rm = TRUE),
   n_cash = sum(deal_type == "Cash", na.rm = TRUE),
   pct_cash_entries = n_cash / n_entries,
   .groups = "drop"
 )

# ---- Add breadth ---------------------------------------------------------------------------------
inst_breadth <- institutional_holdings_granular %>%
 group_by(mgrno) %>%
 summarise(n_firms_held = n_distinct(permno), .groups = "drop")

institution_metrics <- institution_metrics %>%
 left_join(inst_breadth, by = "mgrno") %>%
 filter(n_entries >= 3)  # Min 3 entries

# ---- Distribution ---------------------------------------------------------------------------------
entry_dist <- institution_metrics %>%
 mutate(
   entry_bucket = case_when(
     n_entries <= 5 ~ "3-5",
     n_entries <= 10 ~ "6-10",
     n_entries <= 20 ~ "11-20",
     n_entries <= 50 ~ "21-50",
     TRUE ~ "51+"
   ),
   entry_bucket = factor(entry_bucket, levels = c("3-5", "6-10", "11-20", "21-50", "51+"))
 ) %>%
 group_by(entry_bucket) %>%
 summarise(
   N_institutions = n(),
   Pct_institutions = n() / nrow(institution_metrics) * 100,
   Total_entries = sum(n_entries),
   Total_volume = sum(total_shares_entered),
   .groups = "drop"
 ) %>%
 mutate(
   Pct_entries = Total_entries / sum(Total_entries) * 100,
   Pct_volume = Total_volume / sum(Total_volume) * 100
 )

print(entry_dist, digits = 2)

# Top percentiles
institution_metrics_sorted <- institution_metrics %>%
 arrange(desc(n_entries)) %>%
 mutate(
   rank_pct = row_number() / n(),
   cum_entries = cumsum(n_entries) / sum(n_entries),
   cum_volume = cumsum(total_shares_entered) / sum(total_shares_entered)
 )

top_10 <- institution_metrics_sorted %>% filter(rank_pct <= 0.10) %>% tail(1)
top_5 <- institution_metrics_sorted %>% filter(rank_pct <= 0.05) %>% tail(1)
top_1 <- institution_metrics_sorted %>% filter(rank_pct <= 0.01) %>% tail(1)

cat(sprintf("Top 10%% of entrants account for:\n"))
cat(sprintf("  %.1f%% of entry events\n", top_10$cum_entries * 100))
cat(sprintf("  %.1f%% of entry volume (shares)\n\n", top_10$cum_volume * 100))

cat(sprintf("Top 5%% of entrants account for:\n"))
cat(sprintf("  %.1f%% of entry events\n", top_5$cum_entries * 100))
cat(sprintf("  %.1f%% of entry volume (shares)\n\n", top_5$cum_volume * 100))

cat(sprintf("Top 1%% of entrants account for:\n"))
cat(sprintf("  %.1f%% of entry events\n", top_1$cum_entries * 100))
cat(sprintf("  %.1f%% of entry volume (shares)\n\n", top_1$cum_volume * 100))

# Classify
institution_metrics <- institution_metrics %>%
 arrange(desc(n_entries)) %>%
 mutate(
   rank = row_number(),
   rank_pct = rank / n(),
   arb_class = case_when(
     rank_pct <= 0.01 ~ "Top 1% (Elite Arbs)",
     rank_pct <= 0.05 ~ "Top 5% (Professional Arbs)",
     rank_pct <= 0.10 ~ "Top 10% (Active Arbs)",
     rank_pct <= 0.25 ~ "Top 25% (Regular Arbs)",
     TRUE ~ "Occasional Entrants"
   )
)

# Comparison
final_comp <- institution_metrics %>%
 group_by(arb_class) %>%
 summarise(
   N = n(),
   Mean_Entries = mean(n_entries),
   Pct_Cash = mean(pct_cash_entries, na.rm = TRUE) * 100,
   Pct_Entry_Events = sum(n_entries) / sum(institution_metrics$n_entries) * 100,
   Pct_Entry_Volume = sum(total_shares_entered) / sum(institution_metrics$total_shares_entered) * 100,
   .groups = "drop"
 )

print(final_comp, digits = 2)

saveRDS(institution_metrics, "institution_arb_classification_final.rds")

# ---- Calculate Gini coefficient for entry concentration ----------------------------------------
calculate_gini <- function(x) {
 x <- x[!is.na(x) & x > 0]
 x <- sort(x)
 n <- length(x)
 if (n == 0) return(NA_real_)

 # Gini = (2 * sum(i * x_i) / (n * sum(x))) - (n+1)/n
 gini <- (2 * sum((1:n) * x) / (n * sum(x))) - (n + 1) / n
 return(gini)
}

# ---- Gini for entry counts and volume -----------------------------------------------------------
gini_entries <- calculate_gini(institution_metrics$n_entries)
gini_volume <- calculate_gini(institution_metrics$total_shares_entered)

cat("\n\nCONCENTRATION (GINI COEFFICIENTS):\n")
cat(strrep("=", 60), "\n")
cat(sprintf("Entry events (# deals):  %.3f\n", gini_entries))
cat(sprintf("Entry volume (shares):   %.3f\n\n", gini_volume))

# ---- clean up ----------------------------------------------------------------------------------
rm("entrants_with_volume", "inst_breadth")

#==================================================================================================
# STEP 15 (END-TO-END, CORRECTED): Velocity Analysis — Institutional Trading Speed (Cash Deals)
# Requires: mgcv, gratia, dplyr, tidyr, purrr, ggplot2
#
# Inputs expected in env:
#   deals_panel: master_deal_no, permno, t_0, deal_type, exit_pct, entry_pct, net_change_pct
#   deals_with_io: master_deal_no, target_permno, dateann
#
# Outputs:
#   figures_step15/*.pdf and *.png
#   figures_step15/velocity_results_step15.rds
#
# Notes:
#   - No hard n_deals cutoffs: we keep all bins and downweight via precision weights + shrinkage.
#   - Derivatives/SE are computed via gratia::derivatives(data=...) and standardized to dfit/dse.
#==================================================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(ggplot2)
  library(mgcv)
  library(splines)
  library(gratia)
  library(conflicted)
})

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::first)

#-----------------------------#
# 0) CONFIG
#-----------------------------#
cfg <- list(
  deal_type_pattern = "Cash",
  min_days = 1,
  max_days = 90,
  week_width = 7,

  figures_dir = "figures_step15",
  export_dpi = 450,
  theme_base = 13,

  grid_days = 1:90,
  grid_week_mid = 1:90,

  # Completion rules
  completion_L = 14,        # derivative CI includes 0 for L consecutive days
  completion_frac = 0.95,   # level reaches 95% of day-90 value

  # Model settings
  gam_k_weekly = 6,
  gam_k_deal = 10,

  run = list(
    weekly_wls = TRUE,
    weekly_gam = TRUE,
    deal_level_gam = TRUE,
    isotonic = TRUE,
    scam_monotone = FALSE
  ),

  # Plot method sets
  show_methods_all = c("Weekly GAM (REML)", "Weekly WLS (best)", "Deal-level GAM (REML)", "Isotonic (monotone)"),
  paper_methods = c("Weekly GAM (REML)", "Deal-level GAM (REML)", "Isotonic (monotone)")
)

dir.create(cfg$figures_dir, showWarnings = FALSE, recursive = TRUE)

#-----------------------------#
# 1) DATA PREP
#-----------------------------#
timing_data_raw <- deals_panel %>%
  left_join(
    deals_with_io %>% select(master_deal_no, target_permno, dateann),
    by = c("master_deal_no", "permno" = "target_permno")
  ) %>%
  filter(!is.na(dateann), !is.na(t_0)) %>%
  mutate(days_to_qtr_end = as.numeric(t_0 - dateann)) %>%
  filter(days_to_qtr_end >= cfg$min_days, days_to_qtr_end <= cfg$max_days) %>%
  filter(grepl(cfg$deal_type_pattern, deal_type, ignore.case = TRUE)) %>%
  mutate(
    week = floor((days_to_qtr_end - 1) / cfg$week_width) + 1,
    week_mid = (week - 0.5) * cfg$week_width
  )

stopifnot(nrow(timing_data_raw) > 0)

timing_long <- timing_data_raw %>%
  select(master_deal_no, permno, deal_type, dateann, t_0, days_to_qtr_end, week, week_mid,
         exit_pct, entry_pct, net_change_pct) %>%
  pivot_longer(
    cols = c(exit_pct, entry_pct, net_change_pct),
    names_to = "component_raw",
    values_to = "value"
  ) %>%
  mutate(
    component = recode(component_raw,
                       exit_pct = "Exit",
                       entry_pct = "Entry",
                       net_change_pct = "Net"),
    component = factor(component, levels = c("Exit", "Entry", "Net"))
  )

#-----------------------------#
# 2) WEEKLY AGGREGATION (NO CUT-OFFS)
#-----------------------------#
stabilized_invvar <- function(se, floor_quantile = 0.10) {
  ok <- is.finite(se) & se > 0
  if (!any(ok)) return(rep(1, length(se)))
  se_floor <- as.numeric(stats::quantile(se[ok], probs = floor_quantile, na.rm = TRUE))
  se_adj <- pmax(se, se_floor)
  1 / (se_adj^2)
}

weekly_agg <- timing_long %>%
  group_by(component, week, week_mid) %>%
  summarise(
    n = sum(!is.na(value)),
    mean = mean(value, na.rm = TRUE),
    sd = sd(value, na.rm = TRUE),
    se = sd / sqrt(pmax(n, 1)),
    median = median(value, na.rm = TRUE),
    q25 = quantile(value, 0.25, na.rm = TRUE, names = FALSE),
    q75 = quantile(value, 0.75, na.rm = TRUE, names = FALSE),
    .groups = "drop"
  ) %>%
  mutate(
    w_invvar = stabilized_invvar(se),
    w_n = pmax(n, 1)
  )

support_week <- timing_data_raw %>%
  group_by(week, week_mid) %>%
  summarise(n_obs = n(), .groups = "drop")

#-----------------------------#
# 3) MODELS
#-----------------------------#
fit_weekly_wls <- function(df, y = "mean", x = "week_mid", w = "w_invvar") {
  d <- df %>% filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]]))
  wvec <- d[[w]]

  models <- list(
    linear = lm(reformulate(x, y), data = d, weights = wvec),
    quad   = lm(reformulate(sprintf("poly(%s,2)", x), y), data = d, weights = wvec),
    ns3    = lm(reformulate(sprintf("splines::ns(%s, df=3)", x), y), data = d, weights = wvec),
    ns4    = lm(reformulate(sprintf("splines::ns(%s, df=4)", x), y), data = d, weights = wvec)
  )

  comp <- tibble(model = names(models)) %>%
    mutate(
      aic = map_dbl(model, ~AIC(models[[.x]])),
      r2  = map_dbl(model, ~summary(models[[.x]])$r.squared)
    ) %>%
    arrange(aic) %>%
    mutate(delta_aic = aic - min(aic))

  best <- comp$model[1]
  list(models = models, comp = comp, best = best, best_model = models[[best]])
}

fit_weekly_gam <- function(df, y = "mean", x = "week_mid", w = "w_invvar", k = 6) {
  d <- df %>% filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]]))
  wvec <- d[[w]]
  gam(
    reformulate(sprintf("s(%s, bs='ts', k=%d)", x, k), y),
    data = d, weights = wvec, method = "REML"
  )
}

fit_deal_level_gam <- function(df_long, component_name, k = 10) {
  d <- df_long %>%
    filter(component == component_name) %>%
    filter(is.finite(value), is.finite(days_to_qtr_end))
  gam(
    value ~ s(days_to_qtr_end, bs = "ts", k = k),
    data = d, method = "REML"
  )
}

fit_isotonic_weekly <- function(df, y = "mean", x = "week_mid", w = "w_n", increasing = TRUE) {
  d <- df %>%
    filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]])) %>%
    arrange(.data[[x]])

  cap <- 25
  reps <- pmin(cap, pmax(1, round(d[[w]] / median(d[[w]], na.rm = TRUE))))
  x_rep <- rep(d[[x]], reps)
  y_rep <- rep(d[[y]], reps)

  iso <- stats::isoreg(x_rep, if (increasing) y_rep else -y_rep)
  pred_fun <- stats::approxfun(iso$x, iso$yf, rule = 2)

  list(
    data = d,
    iso = iso,
    pred_fun = function(xnew) if (increasing) pred_fun(xnew) else -pred_fun(xnew)
  )
}

fit_scam_monotone <- function(df, y = "mean", x = "week_mid", w = "w_invvar", k = 6) {
  if (!requireNamespace("scam", quietly = TRUE)) return(NULL)
  d <- df %>% filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]]))
  wvec <- d[[w]]
  scam::scam(
    reformulate(sprintf("s(%s, bs='mpi', k=%d)", x, k), y),
    data = d, weights = wvec
  )
}

components <- levels(weekly_agg$component)

fits <- map(components, function(comp) {
  dfw <- weekly_agg %>% filter(component == comp)
  out <- list()

  if (cfg$run$weekly_wls) out$weekly_wls <- fit_weekly_wls(dfw)
  if (cfg$run$weekly_gam) out$weekly_gam <- fit_weekly_gam(dfw, k = cfg$gam_k_weekly)
  if (cfg$run$deal_level_gam) out$deal_level_gam <- fit_deal_level_gam(timing_long, comp, k = cfg$gam_k_deal)

  if (cfg$run$isotonic && comp %in% c("Exit","Entry")) out$isotonic <- fit_isotonic_weekly(dfw, increasing = TRUE)
  if (cfg$run$scam_monotone && comp %in% c("Exit","Entry")) out$scam <- fit_scam_monotone(dfw, k = cfg$gam_k_weekly)

  out
})
names(fits) <- components

#-----------------------------#
# 4) PREDICTIONS (LEVELS)
#-----------------------------#
predict_lm <- function(model, xnew, xname) {
  nd <- tibble(!!xname := xnew)
  p <- predict(model, newdata = nd, se.fit = TRUE)
  tibble(x = xnew, fit = as.numeric(p$fit), se = as.numeric(p$se.fit))
}

predict_gam <- function(model, xnew, xname) {
  nd <- tibble(!!xname := xnew)
  p <- predict(model, newdata = nd, se.fit = TRUE)
  tibble(x = xnew, fit = as.numeric(p$fit), se = as.numeric(p$se.fit))
}

grid_week <- cfg$grid_week_mid
grid_days <- cfg$grid_days

preds_df <- map_dfr(components, function(comp) {
  out <- list()

  if (!is.null(fits[[comp]]$weekly_wls)) {
    out$wls <- predict_lm(fits[[comp]]$weekly_wls$best_model, grid_week, "week_mid") %>%
      mutate(method = "Weekly WLS (best)")
  }
  if (!is.null(fits[[comp]]$weekly_gam)) {
    out$wg <- predict_gam(fits[[comp]]$weekly_gam, grid_week, "week_mid") %>%
      mutate(method = "Weekly GAM (REML)")
  }
  if (!is.null(fits[[comp]]$deal_level_gam)) {
    out$dg <- predict_gam(fits[[comp]]$deal_level_gam, grid_days, "days_to_qtr_end") %>%
      mutate(method = "Deal-level GAM (REML)")
  }
  if (!is.null(fits[[comp]]$isotonic)) {
    f <- fits[[comp]]$isotonic$pred_fun
    out$iso <- tibble(x = grid_week, fit = f(grid_week), se = NA_real_, method = "Isotonic (monotone)")
  }
  if (!is.null(fits[[comp]]$scam)) {
    out$scam <- predict_gam(fits[[comp]]$scam, grid_week, "week_mid") %>%
      mutate(method = "SCAM monotone")
  }

  bind_rows(out) %>% mutate(component = as.character(comp))
})

weekly_points <- weekly_agg %>%
  mutate(component = as.character(component)) %>%
  transmute(component, method = "Weekly means", x = week_mid, fit = mean, n = n, se = se, q25 = q25, q75 = q75)

#-----------------------------#
# 5) DERIVATIVES (VELOCITY) USING GRATIA — UPDATED API + ROBUST RENAMING
#-----------------------------#
# gratia::derivatives() now prefers `data=`. We standardize output to:
#   x, dfit, dse, component, method

standardize_deriv <- function(d, xvec, method_label, component_label) {
  # gratia derivatives commonly returns: derivative, se, lower, upper (names can vary)
  # We'll map defensively.
  dfit <- d[["derivative"]]
  if (is.null(dfit)) dfit <- d[[".derivative"]]  # fallback (rare)
  dse <- d[["se"]]
  if (is.null(dse)) dse <- d[[".se"]]           # fallback (rare)

  # If still missing, return empty tibble (prevents downstream failure)
  if (is.null(dfit) || is.null(dse)) {
    return(tibble())
  }

  tibble(
    x = xvec,
    dfit = as.numeric(dfit),
    dse  = as.numeric(dse),
    method = method_label,
    component = component_label
  )
}

get_deriv_safe <- function(model, term, data_grid, xcol, method_label, component_label) {
  if (is.null(model)) return(tibble())
  # catch errors (e.g., term name mismatch)
  d <- try(gratia::derivatives(model, term = term, data = data_grid), silent = TRUE)
  if (inherits(d, "try-error")) return(tibble())
  standardize_deriv(d, xvec = data_grid[[xcol]], method_label, component_label)
}

grid_w <- tibble(week_mid = grid_week)
grid_d <- tibble(days_to_qtr_end = grid_days)

derivs_df <- bind_rows(
  map_dfr(components, function(comp) {
    get_deriv_safe(
      model = fits[[comp]]$weekly_gam,
      term = "s(week_mid)",
      data_grid = grid_w,
      xcol = "week_mid",
      method_label = "Weekly GAM (REML)",
      component_label = as.character(comp)
    )
  }),
  map_dfr(components, function(comp) {
    get_deriv_safe(
      model = fits[[comp]]$deal_level_gam,
      term = "s(days_to_qtr_end)",
      data_grid = grid_d,
      xcol = "days_to_qtr_end",
      method_label = "Deal-level GAM (REML)",
      component_label = as.character(comp)
    )
  }),
  map_dfr(c("Exit","Entry"), function(comp) {
    get_deriv_safe(
      model = fits[[comp]]$scam,
      term = "s(week_mid)",
      data_grid = grid_w,
      xcol = "week_mid",
      method_label = "SCAM monotone",
      component_label = comp
    )
  })
)

#-----------------------------#
# 6) COMPLETION RULES
#-----------------------------#
completion_day_deriv <- function(x, dfit, dse, L = 14) {
  n <- length(x)
  if (n < L) return(NA_real_)
  inside <- (dfit - 1.96*dse <= 0) & (dfit + 1.96*dse >= 0)
  inside[is.na(inside)] <- FALSE
  r <- rle(inside)
  ends <- cumsum(r$lengths)
  starts <- ends - r$lengths + 1
  idx <- which(r$values & r$lengths >= L)
  if (length(idx) == 0) return(NA_real_)
  x[starts[idx[1]]]
}

completion_day_level <- function(x, fit, frac = 0.95) {
  if (length(x) == 0) return(NA_real_)
  target <- frac * fit[which.max(x)]
  idx <- which(fit >= target)
  if (length(idx) == 0) return(NA_real_)
  x[min(idx)]
}

completion_tbl_deriv <- derivs_df %>%
  filter(component %in% c("Exit","Entry")) %>%
  group_by(component, method) %>%
  summarise(completion_day = completion_day_deriv(x, dfit, dse, L = cfg$completion_L),
            .groups = "drop") %>%
  arrange(component, completion_day)

completion_tbl_level <- preds_df %>%
  filter(component %in% c("Exit","Entry")) %>%
  group_by(component, method) %>%
  summarise(completion_day = completion_day_level(x, fit, frac = cfg$completion_frac),
            .groups = "drop") %>%
  arrange(component, completion_day)

#-----------------------------#
# 7) THEMES + SAVE HELPERS
#-----------------------------#
theme_pub <- theme_minimal(base_size = cfg$theme_base) +
  theme(
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold"),
    strip.text = element_text(face = "bold"),
    legend.title = element_blank()
  )

save_plot <- function(name, p, w, h) {
  ggsave(file.path(cfg$figures_dir, paste0(name, ".pdf")), p, width = w, height = h)
  ggsave(file.path(cfg$figures_dir, paste0(name, ".png")), p, width = w, height = h, dpi = cfg$export_dpi)
}

#-----------------------------#
# 8) VISUALS (LOTS OF ANGLES)
#-----------------------------#

# 8.1 Support by week
p_support <- ggplot(support_week, aes(x = week_mid, y = n_obs)) +
  geom_col() +
  labs(title = "Support Across Announcement→Quarter-End Window (Cash Deals)",
       x = "Days from announcement to quarter-end (weekly bins)",
       y = "Deal-observations per bin") +
  theme_pub
save_plot("01_support_by_week", p_support, 8.2, 3.2)

# 8.2 Weekly means + IQR (distribution view)
p_weekly_iqr <- weekly_points %>%
  ggplot(aes(x = x, y = fit)) +
  geom_linerange(aes(ymin = q25, ymax = q75), alpha = 0.4) +
  geom_point(aes(size = n), alpha = 0.7) +
  facet_wrap(~component, scales = "free_y", ncol = 1) +
  scale_size_continuous(range = c(1.2, 6.5)) +
  labs(title = "Weekly Means with Within-Bin IQR (No Bins Dropped)",
       x = "Days", y = "Weekly mean (IQR bars; point size ∝ n)") +
  theme_pub +
  theme(legend.position = "bottom")
save_plot("02_weekly_means_iqr", p_weekly_iqr, 8.2, 7.8)

# 8.3 Method overlays for all components
p_methods_all <- ggplot() +
  geom_ribbon(
    data = preds_df %>% filter(method %in% cfg$show_methods_all, is.finite(se)),
    aes(x = x, ymin = fit - 1.96*se, ymax = fit + 1.96*se, group = method),
    alpha = 0.10
  ) +
  geom_line(
    data = preds_df %>% filter(method %in% cfg$show_methods_all),
    aes(x = x, y = fit, linetype = method),
    linewidth = 1.05
  ) +
  geom_point(
    data = weekly_points,
    aes(x = x, y = fit, size = n),
    alpha = 0.55
  ) +
  facet_wrap(~component, scales = "free_y", ncol = 1) +
  scale_size_continuous(range = c(1.2, 6.5)) +
  labs(title = "Cumulative Flow Curves: Multiple Estimators (No Cutoffs)",
       x = "Days", y = "Cumulative flow",
       caption = "Points = weekly means. Ribbons = 95% CI where available.") +
  theme_pub +
  theme(legend.position = "bottom")
save_plot("03_methods_overlay_all_components", p_methods_all, 8.6, 9.5)

# 8.4 Two clocks (paper-ready): Entry vs Exit + 95% completion lines (level-based)
two_df <- preds_df %>%
  filter(component %in% c("Exit","Entry"),
         method %in% cfg$paper_methods)

two_pts <- weekly_points %>% filter(component %in% c("Exit","Entry"))

lines_level <- completion_tbl_level %>%
  filter(component %in% c("Exit","Entry"), method %in% cfg$paper_methods) %>%
  filter(is.finite(completion_day))

p_two_clocks <- ggplot() +
  geom_ribbon(
    data = two_df %>% filter(is.finite(se)),
    aes(x = x, ymin = fit - 1.96*se, ymax = fit + 1.96*se, group = method),
    alpha = 0.10
  ) +
  geom_line(
    data = two_df,
    aes(x = x, y = fit, linetype = method),
    linewidth = 1.15
  ) +
  geom_point(
    data = two_pts,
    aes(x = x, y = fit, size = n),
    alpha = 0.55
  ) +
  geom_vline(
    data = lines_level,
    aes(xintercept = completion_day),
    linetype = "dashed", linewidth = 0.5
  ) +
  facet_wrap(~component, scales = "free_y", ncol = 1) +
  scale_size_continuous(range = c(1.2, 6.5)) +
  labs(
    title = "Two Clocks: Entry Completes Before Exit",
    subtitle = sprintf("Dashed line = day reaching %.0f%% of day-90 level (method-specific)",
                       100 * cfg$completion_frac),
    x = "Days", y = "Cumulative flow (% of TSO)"
  ) +
  theme_pub +
  theme(legend.position = "bottom")
save_plot("04_two_clocks_paper", p_two_clocks, 8.2, 7.6)

# 8.5 Velocity (derivatives) with correct SE via gratia
# If derivs_df is empty, plot will be empty but won’t error.
p_deriv <- derivs_df %>%
  filter(method %in% c("Weekly GAM (REML)", "Deal-level GAM (REML)", "SCAM monotone")) %>%
  ggplot(aes(x = x, y = dfit, linetype = method)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_ribbon(
    aes(ymin = dfit - 1.96*dse, ymax = dfit + 1.96*dse, group = method),
    alpha = 0.10
  ) +
  geom_line(linewidth = 1.05) +
  facet_wrap(~component, scales = "free_y", ncol = 1) +
  labs(
    title = "Velocity (Derivative of Smooth) with Correct Uncertainty",
    subtitle = sprintf("Derivative SE from gratia::derivatives(data=...); completion uses %d consecutive days CI includes 0",
                       cfg$completion_L),
    x = "Days", y = "Daily velocity (approx.)"
  ) +
  theme_pub +
  theme(legend.position = "bottom")
save_plot("05_velocity_derivatives_gratia", p_deriv, 8.6, 9.0)

# 8.6 Completion diagnostic (derivative-based) for Exit/Entry only
lines_deriv <- completion_tbl_deriv %>%
  filter(method %in% c("Weekly GAM (REML)", "Deal-level GAM (REML)", "SCAM monotone")) %>%
  filter(is.finite(completion_day))

p_deriv_exit_entry <- derivs_df %>%
  filter(component %in% c("Exit","Entry"),
         method %in% c("Weekly GAM (REML)", "Deal-level GAM (REML)", "SCAM monotone")) %>%
  ggplot(aes(x = x, y = dfit, linetype = method)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_ribbon(aes(ymin = dfit - 1.96*dse, ymax = dfit + 1.96*dse, group = method),
              alpha = 0.10) +
  geom_line(linewidth = 1.05) +
  geom_vline(data = lines_deriv, aes(xintercept = completion_day),
             linetype = "dashed", linewidth = 0.5) +
  facet_wrap(~component, scales = "free_y", ncol = 1) +
  labs(
    title = "Derivative-Based Completion Days (Exit/Entry)",
    subtitle = sprintf("Dashed line = first day CI includes 0 for %d consecutive days", cfg$completion_L),
    x = "Days", y = "Velocity"
  ) +
  theme_pub +
  theme(legend.position = "bottom")
save_plot("06_velocity_completion_derivative", p_deriv_exit_entry, 8.2, 7.2)

# 8.7 ECDF by phase (distribution check)
timing_data <- timing_data_raw %>%
  mutate(
    phase = case_when(
      days_to_qtr_end <= 30 ~ "Early (1–30)",
      days_to_qtr_end <= 60 ~ "Middle (31–60)",
      TRUE ~ "Late (61–90)"
    ),
    phase = factor(phase, levels = c("Early (1–30)", "Middle (31–60)", "Late (61–90)"))
  )

ecdf_long <- timing_data %>%
  select(days_to_qtr_end, phase, exit_pct, entry_pct, net_change_pct) %>%
  pivot_longer(cols = c(exit_pct, entry_pct, net_change_pct),
               names_to = "component_raw", values_to = "value") %>%
  mutate(component = recode(component_raw,
                            exit_pct="Exit", entry_pct="Entry", net_change_pct="Net"))

p_ecdf <- ecdf_long %>%
  filter(component %in% c("Exit","Entry")) %>%
  ggplot(aes(x = value, color = phase)) +
  stat_ecdf(geom = "step", linewidth = 1.0) +
  facet_wrap(~component, scales = "free_x", ncol = 1) +
  labs(
    title = "Distribution of Deal-Level Flows by Phase (ECDF)",
    subtitle = "Checks whether the story is broad-based or tail-driven",
    x = "Flow (% of TSO)", y = "ECDF"
  ) +
  theme_pub +
  theme(legend.position = "bottom")
save_plot("07_ecdf_by_phase_exit_entry", p_ecdf, 8.2, 7.2)

# 8.8 Phase decomposition + difference tests (clean)
coarse_decomp <- timing_data %>%
  group_by(phase) %>%
  summarise(
    N = n(),
    Mean_Days = mean(days_to_qtr_end),
    Exit_Mean = mean(exit_pct, na.rm=TRUE),
    Entry_Mean = mean(entry_pct, na.rm=TRUE),
    Stayer_Mean = mean(net_change_pct, na.rm=TRUE) - mean(entry_pct, na.rm=TRUE) + mean(exit_pct, na.rm=TRUE),
    Net_Mean = mean(net_change_pct, na.rm=TRUE),
    Gross_Turnover = abs(Exit_Mean) + abs(Entry_Mean) + abs(Stayer_Mean),
    .groups = "drop"
  )

early <- timing_data %>% filter(phase == "Early (1–30)")
middle <- timing_data %>% filter(phase == "Middle (31–60)")
late <- timing_data %>% filter(phase == "Late (61–90)")

tests <- tibble(
  contrast = c("Middle - Early", "Late - Middle"),
  exit_diff = c(mean(middle$exit_pct, na.rm=TRUE) - mean(early$exit_pct, na.rm=TRUE),
                mean(late$exit_pct, na.rm=TRUE) - mean(middle$exit_pct, na.rm=TRUE)),
  exit_p = c(t.test(middle$exit_pct, early$exit_pct)$p.value,
             t.test(late$exit_pct, middle$exit_pct)$p.value),
  net_diff = c(mean(middle$net_change_pct, na.rm=TRUE) - mean(early$net_change_pct, na.rm=TRUE),
               mean(late$net_change_pct, na.rm=TRUE) - mean(middle$net_change_pct, na.rm=TRUE)),
  net_p = c(t.test(middle$net_change_pct, early$net_change_pct)$p.value,
            t.test(late$net_change_pct, middle$net_change_pct)$p.value)
)

#-----------------------------#
# 9) SAVE RESULTS
#-----------------------------#
velocity_results_step15 <- list(
  cfg = cfg,
  timing_data = timing_data,
  timing_long = timing_long,
  weekly_agg = weekly_agg,
  support_week = support_week,
  fits = fits,
  preds = preds_df,
  derivs = derivs_df,
  completion_tbl_deriv = completion_tbl_deriv,
  completion_tbl_level = completion_tbl_level,
  coarse_decomp = coarse_decomp,
  tests = tests,
  figures = list.files(cfg$figures_dir, full.names = TRUE)
)

saveRDS(velocity_results_step15, file.path(cfg$figures_dir, "velocity_results_step15.rds"))

cat("\nSaved: ", file.path(cfg$figures_dir, "velocity_results_step15.rds"), "\n\n")

cat("Completion days (LEVEL-based; includes isotonic):\n")
print(completion_tbl_level)

cat("\nCompletion days (DERIV-based; GAM only):\n")
print(completion_tbl_deriv)

cat("\nCoarse decomposition:\n")
print(coarse_decomp)

cat("\nDifference tests:\n")
print(tests)
	   

# PHASE 1: Early (Days 1-30, N=119)
#  Exit:   13.1%  ← Baseline institutional departure
#  Entry:  19.5%  ← Initial arb entry
#  Stayer: -8.3%  ← Moderate trimming
#  Net:    -1.9pp ← Small net effect
#  Gross:  40.9%  ← Substantial churn

# PHASE 2: Middle (Days 31-60, N=118)  [PEAK ACTIVITY]
#  Exit:   18.5%  ← +5.4pp jump (p<0.001) ***
#  Entry:  23.0%  ← Rising (arbs continue entering)
#  Stayer: -8.7%  ← Stable trimming
#  Net:    -4.2pp ← 2.2x worse than early (p=0.085) *
#  Gross:  50.2%  ← Peak turnover

# PHASE 3: Late (Days 61-90, N=73)  [STABILIZATION]
#  Exit:   18.2%  ← Plateau (no diff from middle, p=0.89)
#  Entry:  24.3%  ← Still rising (late arbs)
#  Stayer: -9.5%  ← Continuing trimming
#  Net:    -3.4pp ← Similar to middle (p=0.56)
#  Gross:  52.0%  ← Sustained high turnover

# Table: Institutional Trading Phases Around Merger Announcements (Cash Deals)

# Phase                N      Exit    Entry   Stayer   Net      Gross
#                    Deals    (%)     (%)     Trim(%)  (pp)   Turnover
# ─────────────────────────────────────────────────────────────────────
# Early (1-30d)      119     13.1    19.5    -8.3     -1.9     40.9%
# Middle (31-60d)    118     18.5    23.0    -8.7     -4.2     50.2%
# Late (61-90d)       73     18.2    24.3    -9.5     -3.4     52.0%
# ─────────────────────────────────────────────────────────────────────
# Difference tests (Middle vs Early):
#  Exit: +5.4pp (p<0.001)***
#  Net:  -2.2pp (p=0.085)*
  
# Difference tests (Late vs Middle):  
#  Exit: -0.2pp (p=0.886) [no difference]
#  Net:  +0.8pp (p=0.559) [no difference]

# Note: All flows as % of baseline TSO. Net = Entry - Exit + Stayer.
# Gross turnover = |Exit| + |Entry| + |Stayer|. Statistical tests use 
# deal-level data. *** p<0.01, * p<0.10.


#==================================================================================================
# STEP 15: VELOCITY ANALYSIS - INSTITUTIONAL TRADING SPEED (COMPREHENSIVE)
#
# Philosophy: Combine Version 1's methodological rigor with Version 2's narrative clarity
# 
# Key features:
#   - NO hard cutoffs (use precision weighting instead)
#   - Multiple model families with formal selection
#   - Bootstrap validation across all models
#   - Derivative-based velocity analysis
#   - Phase decomposition with formal ANOVA
#   - Dual completion metrics (level + velocity)
#   - Publication-ready figures and tables
#
# Inputs:  deals_panel, deals_with_io
# Outputs: figures_step15/, velocity_results_step15.rds, summary tables
#==================================================================================================

suppressPackageStartupMessages({
    library(dplyr)
    library(tidyr)
    library(purrr)
    library(ggplot2)
    library(mgcv)
    library(splines)
    library(gratia)
    library(segmented)
    library(conflicted)
})

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::select)
conflicts_prefer(dplyr::first)

#==================================================================================================
# SECTION 1: CONFIGURATION
#==================================================================================================

cfg <- list(
    # Sample
    deal_type_pattern = "Cash",
    min_days = 1,
    max_days = 90,
    week_width = 7,
    
    # Phases
    phase_breaks = c(30, 60),  # Early: 1-30, Middle: 31-60, Late: 61-90
    
    # Model fitting
    gam_k_weekly = 6,
    gam_k_deal = 10,
    bootstrap_n = 1000,
    bootstrap_seed = 42,
    
    # Completion criteria
    completion_L = 14,          # derivative: CI includes 0 for L consecutive days
    completion_frac = 0.95,     # level: reaches 95% of day-90 value
    
    # Weighting
    weight_floor_quantile = 0.10,
    
    # Output
    figures_dir = "figures_step15",
    export_dpi = 450,
    theme_base = 14,
    
    # Prediction grid
    grid_days = 1:90,
    grid_horizons = c(15, 30, 45, 60, 75, 90)
)

dir.create(cfg$figures_dir, showWarnings = FALSE, recursive = TRUE)

#==================================================================================================
# SECTION 2: DATA PREPARATION
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("STEP 15: VELOCITY ANALYSIS\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 2.1 Construct timing data
timing_data_raw <- deals_panel %>%
    left_join(
        deals_with_io %>% select(master_deal_no, target_permno, dateann),
        by = c("master_deal_no", "permno" = "target_permno")
    ) %>%
    filter(!is.na(dateann), !is.na(t_0)) %>%
    mutate(days_to_qtr_end = as.numeric(t_0 - dateann)) %>%
    filter(days_to_qtr_end >= cfg$min_days, days_to_qtr_end <= cfg$max_days) %>%
    filter(grepl(cfg$deal_type_pattern, deal_type, ignore.case = TRUE))

stopifnot(nrow(timing_data_raw) > 0)

cat(sprintf("Sample: %d deal-quarters (%s deals)\n", 
            nrow(timing_data_raw), 
            cfg$deal_type_pattern))
cat(sprintf("  Days range: %d to %d\n", 
            min(timing_data_raw$days_to_qtr_end), 
            max(timing_data_raw$days_to_qtr_end)))
cat(sprintf("  Mean days: %.1f (SD=%.1f)\n\n", 
            mean(timing_data_raw$days_to_qtr_end), 
            sd(timing_data_raw$days_to_qtr_end)))

# 2.2 Phase assignment
timing_data <- timing_data_raw %>%
    mutate(
        phase = case_when(
            days_to_qtr_end <= cfg$phase_breaks[1] ~ "Early",
            days_to_qtr_end <= cfg$phase_breaks[2] ~ "Middle",
            TRUE ~ "Late"
        ),
        phase = factor(phase, levels = c("Early", "Middle", "Late")),
        week = floor((days_to_qtr_end - 1) / cfg$week_width) + 1,
        week_mid = (week - 0.5) * cfg$week_width
    )

# 2.3 Long format for component analysis
timing_long <- timing_data %>%
    select(master_deal_no, permno, deal_type, dateann, t_0, 
           days_to_qtr_end, week, week_mid, phase,
           exit_pct, entry_pct, net_change_pct) %>%
    pivot_longer(
        cols = c(exit_pct, entry_pct, net_change_pct),
        names_to = "component_raw",
        values_to = "value"
    ) %>%
    mutate(
        component = recode(component_raw,
                           exit_pct = "Exit",
                           entry_pct = "Entry",
                           net_change_pct = "Net"),
        component = factor(component, levels = c("Exit", "Entry", "Net"))
    )

#==================================================================================================
# SECTION 3: PHASE ANALYSIS (Version 2 approach, improved)
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("PHASE ANALYSIS\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 3.1 Phase decomposition
phase_decomp <- timing_data %>%
    group_by(phase) %>%
    summarise(
        N = n(),
        Days_Mean = mean(days_to_qtr_end),
        Days_Range = sprintf("%d-%d", min(days_to_qtr_end), max(days_to_qtr_end)),
        
        Exit_Mean = mean(exit_pct, na.rm = TRUE),
        Exit_SE = sd(exit_pct, na.rm = TRUE) / sqrt(n()),
        
        Entry_Mean = mean(entry_pct, na.rm = TRUE),
        Entry_SE = sd(entry_pct, na.rm = TRUE) / sqrt(n()),
        
        Net_Mean = mean(net_change_pct, na.rm = TRUE),
        Net_SE = sd(net_change_pct, na.rm = TRUE) / sqrt(n()),
        
        Stayer_Mean = Net_Mean - Entry_Mean + Exit_Mean,
        
        Gross_Turnover = abs(Exit_Mean) + abs(Entry_Mean) + abs(Stayer_Mean),
        .groups = "drop"
    )

cat("Phase Decomposition:\n")
print(phase_decomp, digits = 2, width = Inf)
cat("\n")

# 3.2 Formal ANOVA tests (better than pairwise t-tests)
anova_results <- tibble(
    Component = c("Exit", "Entry", "Net"),
    F_stat = NA_real_,
    p_value = NA_real_,
    interpretation = NA_character_
)

for (i in 1:3) {
    comp <- c("exit_pct", "entry_pct", "net_change_pct")[i]
    formula_str <- paste(comp, "~ phase")
    
    model <- lm(as.formula(formula_str), data = timing_data)
    anova_res <- anova(model)
    
    anova_results$F_stat[i] <- anova_res$`F value`[1]
    anova_results$p_value[i] <- anova_res$`Pr(>F)`[1]
    anova_results$interpretation[i] <- ifelse(
        anova_res$`Pr(>F)`[1] < 0.001, "***",
        ifelse(anova_res$`Pr(>F)`[1] < 0.01, "**",
               ifelse(anova_res$`Pr(>F)`[1] < 0.05, "*", "n.s."))
    )
}

cat("ANOVA: Phase Effect on Trading Components\n")
print(anova_results, digits = 3)
cat("\n")

# 3.3 Pairwise comparisons (conditional on significant ANOVA)
pairwise_tests <- map_dfr(c("Exit", "Entry", "Net"), function(comp_name) {
    var_name <- case_when(
        comp_name == "Exit" ~ "exit_pct",
        comp_name == "Entry" ~ "entry_pct",
        comp_name == "Net" ~ "net_change_pct"
    )
    
    early <- timing_data %>% filter(phase == "Early") %>% pull(!!sym(var_name))
    middle <- timing_data %>% filter(phase == "Middle") %>% pull(!!sym(var_name))
    late <- timing_data %>% filter(phase == "Late") %>% pull(!!sym(var_name))
    
    t1 <- t.test(middle, early)
    t2 <- t.test(late, middle)
    
    tibble(
        Component = comp_name,
        Contrast = c("Middle - Early", "Late - Middle"),
        Diff = c(mean(middle, na.rm=TRUE) - mean(early, na.rm=TRUE),
                 mean(late, na.rm=TRUE) - mean(middle, na.rm=TRUE)),
        t_stat = c(t1$statistic, t2$statistic),
        p_value = c(t1$p.value, t2$p.value),
        sig = c(
            ifelse(t1$p.value < 0.001, "***",
                   ifelse(t1$p.value < 0.01, "**",
                          ifelse(t1$p.value < 0.05, "*",
                                 ifelse(t1$p.value < 0.10, "†", "")))),
            ifelse(t2$p.value < 0.001, "***",
                   ifelse(t2$p.value < 0.01, "**",
                          ifelse(t2$p.value < 0.05, "*",
                                 ifelse(t2$p.value < 0.10, "†", ""))))
        )
    )
})

cat("Pairwise Comparisons:\n")
print(pairwise_tests, digits = 3)
cat("\n")

#==================================================================================================
# SECTION 4: WEEKLY AGGREGATION (Version 1 approach - NO CUTOFFS)
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("WEEKLY AGGREGATION\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 4.1 Stabilized inverse-variance weights
stabilized_invvar <- function(se, floor_quantile = cfg$weight_floor_quantile) {
    ok <- is.finite(se) & se > 0
    if (!any(ok)) return(rep(1, length(se)))
    
    se_floor <- as.numeric(quantile(se[ok], probs = floor_quantile, na.rm = TRUE))
    se_adj <- pmax(se, se_floor)
    1 / (se_adj^2)
}

# 4.2 Weekly aggregation (no filtering)
weekly_agg <- timing_long %>%
    group_by(component, week, week_mid) %>%
    summarise(
        n = sum(!is.na(value)),
        mean = mean(value, na.rm = TRUE),
        sd = sd(value, na.rm = TRUE),
        se = sd / sqrt(pmax(n, 1)),
        median = median(value, na.rm = TRUE),
        q25 = quantile(value, 0.25, na.rm = TRUE, names = FALSE),
        q75 = quantile(value, 0.75, na.rm = TRUE, names = FALSE),
        .groups = "drop"
    ) %>%
    mutate(
        w_invvar = stabilized_invvar(se),
        w_n = pmax(n, 1)
    )

cat(sprintf("Weekly bins: %d (across %d components)\n", 
            nrow(weekly_agg) / 3, 3))
cat(sprintf("  Mean deals/week-bin: %.1f (range: %d to %d)\n",
            mean(weekly_agg$n),
            min(weekly_agg$n),
            max(weekly_agg$n)))
cat(sprintf("  Bins with n<5: %d (%.1f%%) - KEPT with downweighting\n\n",
            sum(weekly_agg$n < 5),
            100 * mean(weekly_agg$n < 5)))

# 4.3 Support summary
support_week <- timing_data %>%
    group_by(week, week_mid) %>%
    summarise(n_obs = n(), .groups = "drop")

#==================================================================================================
# SECTION 5: MODEL FITTING (Comprehensive)
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("MODEL FITTING\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 5.1 Weekly WLS models
# 5.1 Weekly WLS models
fit_weekly_wls <- function(df, y = "mean", x = "week_mid", w = "w_invvar") {
  d <- df %>% filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]]))
  wvec <- d[[w]]
  
  models <- list(
    linear = lm(reformulate(x, y), data = d, weights = wvec),
    quad   = lm(reformulate(sprintf("poly(%s, 2)", x), y), data = d, weights = wvec),
    cubic  = lm(reformulate(sprintf("poly(%s, 3)", x), y), data = d, weights = wvec),
    ns3    = lm(reformulate(sprintf("splines::ns(%s, df=3)", x), y), data = d, weights = wvec),
    ns4    = lm(reformulate(sprintf("splines::ns(%s, df=4)", x), y), data = d, weights = wvec),
    ns5    = lm(reformulate(sprintf("splines::ns(%s, df=5)", x), y), data = d, weights = wvec)
  )
  
  # Segmented (with error handling)
  models$seg1 <- tryCatch({
    segmented(models$linear, seg.Z = as.formula(paste0("~", x)), npsi = 1,
             control = seg.control(display = FALSE, it.max = 100))
  }, error = function(e) NULL)
  
  models$seg2 <- tryCatch({
    segmented(models$linear, seg.Z = as.formula(paste0("~", x)), npsi = 2,
             control = seg.control(display = FALSE, it.max = 100))
  }, error = function(e) NULL)
  
  # Comparison - FIX: compute valid first, then use it
  valid_models <- sapply(models, function(m) !is.null(m))
  
  comp <- tibble(
    model = names(models),
    valid = valid_models
  ) %>%
    filter(valid) %>%  # Only keep valid models
    mutate(
      aic = map_dbl(model, ~AIC(models[[.x]])),
      df = map_int(model, ~{
        m <- models[[.x]]
        if (inherits(m, "segmented")) length(coef(m)) else length(coef(m))
      }),
      r2 = map_dbl(model, ~{
        m <- models[[.x]]
        if (inherits(m, "segmented")) {
          summary(m)$r.squared
        } else {
          summary(m)$r.squared
        }
      })
    ) %>%
    arrange(aic) %>%
    mutate(delta_aic = aic - min(aic))
  
  best <- comp$model[1]
  list(models = models, comp = comp, best = best, best_model = models[[best]])
}

# 5.2 Weekly GAM
fit_weekly_gam <- function(df, y = "mean", x = "week_mid", w = "w_invvar", k = 6) {
    d <- df %>% filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]]))
    wvec <- d[[w]]
    
    tryCatch(
        gam(reformulate(sprintf("s(%s, bs='tp', k=%d)", x, k), y),
            data = d, weights = wvec, method = "REML"),
        error = function(e) NULL
    )
}

# 5.3 Deal-level GAM
fit_deal_level_gam <- function(df_long, component_name, k = 10) {
    d <- df_long %>%
        filter(component == component_name) %>%
        filter(is.finite(value), is.finite(days_to_qtr_end))
    
    tryCatch(
        gam(value ~ s(days_to_qtr_end, bs = "tp", k = k),
            data = d, method = "REML"),
        error = function(e) NULL
    )
}

# 5.4 Isotonic (for Exit/Entry only)
fit_isotonic_weekly <- function(df, y = "mean", x = "week_mid", w = "w_n", increasing = TRUE) {
    d <- df %>%
        filter(is.finite(.data[[y]]), is.finite(.data[[x]]), is.finite(.data[[w]])) %>%
        arrange(.data[[x]])
    
    # Weighted isotonic via replication (capped to avoid memory issues)
    cap <- 25
    reps <- pmin(cap, pmax(1, round(d[[w]] / median(d[[w]], na.rm = TRUE))))
    x_rep <- rep(d[[x]], reps)
    y_rep <- rep(d[[y]], reps)
    
    iso <- stats::isoreg(x_rep, if (increasing) y_rep else -y_rep)
    pred_fun <- stats::approxfun(iso$x, iso$yf, rule = 2)
    
    list(
        data = d,
        iso = iso,
        pred_fun = function(xnew) if (increasing) pred_fun(xnew) else -pred_fun(xnew)
    )
}

# 5.5 Fit all components
components <- levels(weekly_agg$component)

fits <- map(components, function(comp) {
    cat(sprintf("Fitting models for: %s\n", comp))
    
    dfw <- weekly_agg %>% filter(component == comp)
    
    out <- list()
    out$weekly_wls <- fit_weekly_wls(dfw)
    out$weekly_gam <- fit_weekly_gam(dfw, k = cfg$gam_k_weekly)
    out$deal_level_gam <- fit_deal_level_gam(timing_long, comp, k = cfg$gam_k_deal)
    
    if (comp %in% c("Exit", "Entry")) {
        out$isotonic <- fit_isotonic_weekly(dfw, increasing = TRUE)
    }
    
    # Print comparison
    cat(sprintf("  Weekly WLS best: %s (AIC=%.1f, R²=%.3f)\n",
                out$weekly_wls$best,
                min(out$weekly_wls$comp$aic),
                out$weekly_wls$comp$r2[1]))
    
    if (!is.null(out$weekly_gam)) {
        cat(sprintf("  Weekly GAM:      REML (AIC=%.1f, R²=%.3f)\n",
                    AIC(out$weekly_gam),
                    summary(out$weekly_gam)$r.sq))
    }
    
    if (!is.null(out$deal_level_gam)) {
        cat(sprintf("  Deal-level GAM:  REML (AIC=%.1f, R²=%.3f)\n",
                    AIC(out$deal_level_gam),
                    summary(out$deal_level_gam)$r.sq))
    }
    
    cat("\n")
    out
})
names(fits) <- components

# 5.6 Consolidate model comparison table
model_comparison <- map_dfr(components, function(comp) {
    bind_rows(
        fits[[comp]]$weekly_wls$comp %>% 
            mutate(component = comp, family = "Weekly WLS"),
        
        if (!is.null(fits[[comp]]$weekly_gam)) {
            tibble(
                component = comp,
                family = "Weekly GAM",
                model = "gam_tp",
                valid = TRUE,
                aic = AIC(fits[[comp]]$weekly_gam),
                df = sum(fits[[comp]]$weekly_gam$edf),
                r2 = summary(fits[[comp]]$weekly_gam)$r.sq,
                delta_aic = NA_real_
            )
        },
        
        if (!is.null(fits[[comp]]$deal_level_gam)) {
            tibble(
                component = comp,
                family = "Deal-level GAM",
                model = "gam_deal",
                valid = TRUE,
                aic = AIC(fits[[comp]]$deal_level_gam),
                df = sum(fits[[comp]]$deal_level_gam$edf),
                r2 = summary(fits[[comp]]$deal_level_gam)$r.sq,
                delta_aic = NA_real_
            )
        }
    )
}) %>%
    group_by(component) %>%
    mutate(delta_aic = aic - min(aic, na.rm = TRUE)) %>%
    ungroup() %>%
    arrange(component, delta_aic)

cat("CONSOLIDATED MODEL COMPARISON:\n")
print(model_comparison %>% select(component, family, model, aic, delta_aic, r2, df), 
      digits = 2, width = Inf)
cat("\n")

#==================================================================================================
# SECTION 6: BOOTSTRAP VALIDATION (Version 2 approach, expanded)
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("BOOTSTRAP MODEL SELECTION\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 6.1 Bootstrap function (test ALL weekly WLS models)
boot_model_selection <- function(data, component_name, indices) {
    d <- data %>% filter(component == component_name)
    d_boot <- d[indices, ]
    
    # Only keep valid rows
    d_boot <- d_boot %>% 
        filter(is.finite(mean), is.finite(week_mid), is.finite(w_invvar))
    
    if (nrow(d_boot) < 5) return(NA_character_)
    
    models <- tryCatch({
        list(
            linear = lm(mean ~ week_mid, data = d_boot, weights = w_invvar),
            quad = lm(mean ~ poly(week_mid, 2), data = d_boot, weights = w_invvar),
            cubic = lm(mean ~ poly(week_mid, 3), data = d_boot, weights = w_invvar),
            ns3 = lm(mean ~ splines::ns(week_mid, df=3), data = d_boot, weights = w_invvar),
            ns4 = lm(mean ~ splines::ns(week_mid, df=4), data = d_boot, weights = w_invvar),
            ns5 = lm(mean ~ splines::ns(week_mid, df=5), data = d_boot, weights = w_invvar)
        )
    }, error = function(e) NULL)
    
    if (is.null(models)) return(NA_character_)
    
    aics <- sapply(models, function(m) {
        tryCatch(AIC(m), error = function(e) NA_real_)
    })
    
    if (all(is.na(aics))) return(NA_character_)
    
    names(which.min(aics))
}

# 6.2 Run bootstrap for Exit and Entry (most important)
set.seed(cfg$bootstrap_seed)

boot_results <- map(c("Exit", "Entry"), function(comp) {
    cat(sprintf("Bootstrapping %s (%d draws)...\n", comp, cfg$bootstrap_n))
    
    results <- replicate(cfg$bootstrap_n, {
        indices <- sample(nrow(weekly_agg %>% filter(component == comp)), replace = TRUE)
        boot_model_selection(weekly_agg, comp, indices)
    })
    
    valid <- results[!is.na(results)]
    
    tbl <- table(valid)
    df <- tibble(
        component = comp,
        model = names(tbl),
        frequency = as.integer(tbl),
        share = frequency / length(valid)
    ) %>%
        arrange(desc(frequency))
    
    cat(sprintf("  Non-linear selected: %.1f%% of draws\n\n", 
                100 * (1 - df$share[df$model == "linear"])))
    
    df
})

boot_summary <- bind_rows(boot_results)

cat("BOOTSTRAP RESULTS:\n")
print(boot_summary, digits = 3)
cat("\n")

#==================================================================================================
# SECTION 7: PREDICTIONS AND COMPLETION METRICS
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("PREDICTIONS & COMPLETION\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 7.1 Helper functions
predict_lm <- function(model, xnew, xname) {
    nd <- tibble(!!xname := xnew)
    p <- predict(model, newdata = nd, se.fit = TRUE)
    tibble(x = xnew, fit = as.numeric(p$fit), se = as.numeric(p$se.fit))
}

predict_gam <- function(model, xnew, xname) {
    nd <- tibble(!!xname := xnew)
    p <- predict(model, newdata = nd, se.fit = TRUE)
    tibble(x = xnew, fit = as.numeric(p$fit), se = as.numeric(p$se.fit))
}

# 7.2 Generate predictions on fine grid
grid_week <- cfg$grid_days  # Use daily grid for all
grid_horizons <- cfg$grid_horizons

preds_list <- map(components, function(comp) {
    out <- list()
    
    # Weekly WLS best
    if (!is.null(fits[[comp]]$weekly_wls$best_model)) {
        out$wls <- predict_lm(
            fits[[comp]]$weekly_wls$best_model, 
            grid_week, 
            "week_mid"
        ) %>% mutate(method = "Weekly WLS (best)")
    }
    
    # Weekly GAM
    if (!is.null(fits[[comp]]$weekly_gam)) {
        out$wgam <- predict_gam(
            fits[[comp]]$weekly_gam, 
            grid_week, 
            "week_mid"
        ) %>% mutate(method = "Weekly GAM (REML)")
    }
    
    # Deal-level GAM
    if (!is.null(fits[[comp]]$deal_level_gam)) {
        out$dgam <- predict_gam(
            fits[[comp]]$deal_level_gam, 
            grid_week, 
            "days_to_qtr_end"
        ) %>% mutate(method = "Deal-level GAM (REML)")
    }
    
    # Isotonic
    if (!is.null(fits[[comp]]$isotonic)) {
        f <- fits[[comp]]$isotonic$pred_fun
        out$iso <- tibble(
            x = grid_week, 
            fit = f(grid_week), 
            se = NA_real_, 
            method = "Isotonic (monotone)"
        )
    }
    
    bind_rows(out) %>% mutate(component = comp)
})

preds_df <- bind_rows(preds_list)

# 7.3 Predictions at key horizons
horizon_preds <- map_dfr(grid_horizons, function(h) {
    map_dfr(c("Exit", "Entry", "Net"), function(comp) {
        # Use deal-level GAM as primary (best for paper)
        model <- fits[[comp]]$deal_level_gam
        if (is.null(model)) model <- fits[[comp]]$weekly_gam
        if (is.null(model)) model <- fits[[comp]]$weekly_wls$best_model
        
        p <- predict(model, 
                     newdata = tibble(days_to_qtr_end = h, week_mid = h), 
                     se.fit = TRUE)
        
        tibble(
            component = comp,
            days = h,
            fit = as.numeric(p$fit),
            se = as.numeric(p$se.fit),
            ci_low = fit - 1.96*se,
            ci_high = fit + 1.96*se
        )
    })
})

cat("Predictions at Key Horizons (Deal-level GAM):\n")
print(horizon_preds %>% select(component, days, fit, se, ci_low, ci_high), 
      digits = 2)
cat("\n")

# 7.4 Level-based completion (continued)
completion_day_level <- function(x, fit, frac = 0.95) {
    if (length(x) == 0) return(NA_real_)
    target <- frac * fit[which.max(x)]
    idx <- which(fit >= target)
    if (length(idx) == 0) return(NA_real_)
    x[min(idx)]
}

completion_tbl_level <- preds_df %>%
    filter(component %in% c("Exit", "Entry")) %>%
    group_by(component, method) %>%
    summarise(
        completion_day = completion_day_level(x, fit, frac = cfg$completion_frac),
        .groups = "drop"
    ) %>%
    arrange(component, completion_day)

cat("Level-based Completion (95% of day-90 value):\n")
print(completion_tbl_level, digits = 1)
cat("\n")

#==================================================================================================
# SECTION 8: DERIVATIVES (VELOCITY)
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("VELOCITY ANALYSIS (DERIVATIVES)\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 8.1 Helper function for gratia derivatives with robust error handling
standardize_deriv <- function(d, xvec, method_label, component_label) {
    if (is.null(d) || nrow(d) == 0) return(tibble())
    
    # gratia returns: .derivative, .se, .lower_ci, .upper_ci (with dots)
    dfit <- d[[".derivative"]]
    if (is.null(dfit)) dfit <- d[["derivative"]]  # fallback
    
    dse <- d[[".se"]]
    if (is.null(dse)) dse <- d[["se"]]  # fallback
    
    if (is.null(dfit) || is.null(dse)) {
        warning(sprintf("Could not extract derivatives for %s - %s", 
                        component_label, method_label))
        return(tibble())
    }
    
    tibble(
        x = xvec,
        dfit = as.numeric(dfit),
        dse = as.numeric(dse),
        method = method_label,
        component = component_label
    )
}

get_deriv_safe <- function(model, term, data_grid, xcol, method_label, component_label) {
    if (is.null(model)) return(tibble())
    
    d <- tryCatch(
        gratia::derivatives(model, term = term, data = data_grid, unconditional = FALSE),
        error = function(e) {
            warning(sprintf("Derivative failed for %s - %s: %s", 
                            component_label, method_label, e$message))
            NULL
        }
    )
    
    if (is.null(d)) return(tibble())
    
    standardize_deriv(d, xvec = data_grid[[xcol]], method_label, component_label)
}

# 8.2 Compute derivatives for GAM models
grid_w <- tibble(week_mid = grid_week)
grid_d <- tibble(days_to_qtr_end = grid_week)

derivs_list <- list()

# Weekly GAM derivatives
for (comp in components) {
    if (!is.null(fits[[comp]]$weekly_gam)) {
        derivs_list[[paste0(comp, "_wgam")]] <- get_deriv_safe(
            model = fits[[comp]]$weekly_gam,
            term = "s(week_mid)",
            data_grid = grid_w,
            xcol = "week_mid",
            method_label = "Weekly GAM (REML)",
            component_label = comp
        )
    }
}

# Deal-level GAM derivatives
for (comp in components) {
    if (!is.null(fits[[comp]]$deal_level_gam)) {
        derivs_list[[paste0(comp, "_dgam")]] <- get_deriv_safe(
            model = fits[[comp]]$deal_level_gam,
            term = "s(days_to_qtr_end)",
            data_grid = grid_d,
            xcol = "days_to_qtr_end",
            method_label = "Deal-level GAM (REML)",
            component_label = comp
        )
    }
}

derivs_df <- bind_rows(derivs_list)

cat(sprintf("Computed derivatives: %d rows across %d component-method combinations\n\n",
            nrow(derivs_df),
            length(unique(paste(derivs_df$component, derivs_df$method)))))

# 8.3 Derivative-based completion
completion_day_deriv <- function(x, dfit, dse, L = 14) {
    n <- length(x)
    if (n < L) return(NA_real_)
    
    # CI includes 0
    inside <- (dfit - 1.96*dse <= 0) & (dfit + 1.96*dse >= 0)
    inside[is.na(inside)] <- FALSE
    
    # Find first run of L consecutive days
    r <- rle(inside)
    ends <- cumsum(r$lengths)
    starts <- ends - r$lengths + 1
    
    idx <- which(r$values & r$lengths >= L)
    if (length(idx) == 0) return(NA_real_)
    
    x[starts[idx[1]]]
}

completion_tbl_deriv <- derivs_df %>%
    filter(component %in% c("Exit", "Entry")) %>%
    group_by(component, method) %>%
    summarise(
        completion_day = completion_day_deriv(x, dfit, dse, L = cfg$completion_L),
        .groups = "drop"
    ) %>%
    arrange(component, completion_day)

cat(sprintf("Derivative-based Completion (%d consecutive days CI includes 0):\n", 
            cfg$completion_L))
print(completion_tbl_deriv, digits = 1)
cat("\n")

#==================================================================================================
# SECTION 9: VISUALIZATION
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("CREATING FIGURES\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# 9.1 Theme
theme_pub <- theme_minimal(base_size = cfg$theme_base) +
    theme(
        panel.grid.minor = element_blank(),
        plot.title = element_text(face = "bold", size = cfg$theme_base + 2),
        plot.subtitle = element_text(size = cfg$theme_base - 1),
        strip.text = element_text(face = "bold"),
        legend.title = element_blank(),
        legend.position = "bottom"
    )

save_plot <- function(name, p, w, h) {
    ggsave(file.path(cfg$figures_dir, paste0(name, ".pdf")), 
           p, width = w, height = h)
    ggsave(file.path(cfg$figures_dir, paste0(name, ".png")), 
           p, width = w, height = h, dpi = cfg$export_dpi)
    cat(sprintf("  Saved: %s\n", name))
}

# 9.2 Weekly points dataframe
weekly_points <- weekly_agg %>%
    mutate(component = as.character(component)) %>%
    transmute(
        component, 
        method = "Weekly means", 
        x = week_mid, 
        fit = mean, 
        n = n, 
        se = se, 
        q25 = q25, 
        q75 = q75
    )

# FIGURE 1: Support by week
cat("Creating Figure 1: Support...\n")
p1_support <- ggplot(support_week, aes(x = week_mid, y = n_obs)) +
    geom_col(fill = "steelblue", alpha = 0.7) +
    labs(
        title = "Sample Support Across Days-to-Quarter-End Window",
        subtitle = sprintf("%s deals, no bins dropped", cfg$deal_type_pattern),
        x = "Days from announcement to quarter-end",
        y = "Deal-observations per week"
    ) +
    theme_pub

save_plot("01_support_by_week", p1_support, 10, 4)

# FIGURE 2: Phase decomposition with error bars
cat("Creating Figure 2: Phase decomposition...\n")

phase_long <- phase_decomp %>%
    select(phase, Exit_Mean, Exit_SE, Entry_Mean, Entry_SE, Net_Mean, Net_SE) %>%
    pivot_longer(
        cols = -phase,
        names_to = c("component", ".value"),
        names_pattern = "(.+)_(Mean|SE)"
    ) %>%
    mutate(
        component = factor(component, levels = c("Exit", "Entry", "Net")),
        ci_low = Mean - 1.96*SE,
        ci_high = Mean + 1.96*SE
    )

p2_phases <- ggplot(phase_long, aes(x = phase, y = Mean, fill = component)) +
    geom_col(position = "dodge", alpha = 0.8) +
    geom_errorbar(
        aes(ymin = ci_low, ymax = ci_high),
        position = position_dodge(0.9),
        width = 0.25
    ) +
    scale_fill_manual(values = c("Exit" = "#d73027", 
                                 "Entry" = "#1a9850", 
                                 "Net" = "#4575b4")) +
    labs(
        title = "Three-Phase Decomposition of Trading Activity",
        subtitle = sprintf("Early: 1-%d days | Middle: %d-%d days | Late: %d-90 days",
                           cfg$phase_breaks[1], 
                           cfg$phase_breaks[1]+1, 
                           cfg$phase_breaks[2],
                           cfg$phase_breaks[2]+1),
        x = "Phase",
        y = "Flow (% of TSO for Exit/Entry, pp for Net)",
        fill = "Component"
    ) +
    theme_pub

save_plot("02_phase_decomposition", p2_phases, 10, 6)

# FIGURE 3: Comprehensive method comparison (all components)
cat("Creating Figure 3: Method comparison...\n")

methods_to_show <- c("Weekly WLS (best)", "Weekly GAM (REML)", 
                     "Deal-level GAM (REML)", "Isotonic (monotone)")

p3_methods <- ggplot() +
    geom_ribbon(
        data = preds_df %>% filter(method %in% methods_to_show, is.finite(se)),
        aes(x = x, ymin = fit - 1.96*se, ymax = fit + 1.96*se, 
            group = method, fill = method),
        alpha = 0.15
    ) +
    geom_line(
        data = preds_df %>% filter(method %in% methods_to_show),
        aes(x = x, y = fit, color = method, linetype = method),
        linewidth = 1.0
    ) +
    geom_point(
        data = weekly_points,
        aes(x = x, y = fit, size = n),
        alpha = 0.5, color = "black"
    ) +
    facet_wrap(~component, scales = "free_y", ncol = 1) +
    scale_size_continuous(range = c(1, 6), name = "Deals/week") +
    scale_color_brewer(palette = "Set1") +
    scale_fill_brewer(palette = "Set1") +
    labs(
        title = "Cumulative Flow Curves: Method Comparison",
        subtitle = "Shaded regions = 95% CI | Points = weekly means (no cutoffs applied)",
        x = "Days from announcement to quarter-end",
        y = "Cumulative flow (% of TSO for Exit/Entry, pp for Net)",
        color = "Method",
        linetype = "Method",
        fill = "Method"
    ) +
    theme_pub +
    guides(color = guide_legend(nrow = 2),
           linetype = guide_legend(nrow = 2),
           fill = guide_legend(nrow = 2))

save_plot("03_method_comparison_all", p3_methods, 10, 11)

# FIGURE 4: Paper-ready two clocks (Exit vs Entry)
cat("Creating Figure 4: Two clocks (paper version)...\n")

paper_methods <- c("Weekly GAM (REML)", "Deal-level GAM (REML)")

two_df <- preds_df %>%
    filter(component %in% c("Exit", "Entry"),
           method %in% paper_methods)

two_pts <- weekly_points %>% 
    filter(component %in% c("Exit", "Entry"))

# Add completion lines (level-based)
comp_lines_level <- completion_tbl_level %>%
    filter(component %in% c("Exit", "Entry"), 
           method %in% paper_methods,
           is.finite(completion_day))

p4_two_clocks <- ggplot() +
    geom_ribbon(
        data = two_df %>% filter(is.finite(se)),
        aes(x = x, ymin = fit - 1.96*se, ymax = fit + 1.96*se, 
            group = method),
        alpha = 0.12, fill = "gray40"
    ) +
    geom_line(
        data = two_df,
        aes(x = x, y = fit, linetype = method),
        linewidth = 1.3
    ) +
    geom_point(
        data = two_pts,
        aes(x = x, y = fit, size = n),
        alpha = 0.6, color = "darkred"
    ) +
    geom_vline(
        data = comp_lines_level,
        aes(xintercept = completion_day, linetype = method),
        color = "blue", alpha = 0.5
    ) +
    facet_wrap(~component, scales = "free_y", ncol = 1) +
    scale_size_continuous(range = c(1.5, 7), name = "Deals/week") +
    labs(
        title = "Two Clocks: Entry Completes Before Exit",
        subtitle = sprintf("Vertical lines = day reaching %.0f%% of day-90 level (method-specific)",
                           100 * cfg$completion_frac),
        x = "Days from announcement to quarter-end",
        y = "Cumulative flow (% of TSO)",
        linetype = "Method"
    ) +
    theme_pub

save_plot("04_two_clocks_paper", p4_two_clocks, 10, 8)

# FIGURE 5: Velocity (derivatives) with uncertainty
cat("Creating Figure 5: Velocity...\n")

if (nrow(derivs_df) > 0) {
    deriv_methods <- unique(derivs_df$method)
    
    p5_velocity <- derivs_df %>%
        filter(method %in% deriv_methods) %>%
        ggplot(aes(x = x, y = dfit, color = method, fill = method)) +
        geom_hline(yintercept = 0, linewidth = 0.4, color = "gray30") +
        geom_ribbon(
            aes(ymin = dfit - 1.96*dse, ymax = dfit + 1.96*dse, group = method),
            alpha = 0.15, color = NA
        ) +
        geom_line(linewidth = 1.1) +
        facet_wrap(~component, scales = "free_y", ncol = 1) +
        scale_color_brewer(palette = "Dark2") +
        scale_fill_brewer(palette = "Dark2") +
        labs(
            title = "Instantaneous Velocity of Institutional Trading",
            subtitle = "Derivative of smooth with 95% CI from gratia::derivatives()",
            x = "Days from announcement",
            y = "Daily velocity (% of TSO per day)",
            color = "Method",
            fill = "Method"
        ) +
        theme_pub
    
    save_plot("05_velocity_derivatives", p5_velocity, 10, 10)
} else {
    cat("  Skipping velocity plot (no derivatives computed)\n")
}

# FIGURE 6: Completion diagnostic (derivative-based, Exit/Entry only)
cat("Creating Figure 6: Completion diagnostic...\n")

if (nrow(derivs_df %>% filter(component %in% c("Exit", "Entry"))) > 0) {
    comp_lines_deriv <- completion_tbl_deriv %>%
        filter(is.finite(completion_day))
    
    p6_completion <- derivs_df %>%
        filter(component %in% c("Exit", "Entry")) %>%
        ggplot(aes(x = x, y = dfit, color = method)) +
        geom_hline(yintercept = 0, linewidth = 0.5, linetype = "dashed") +
        geom_ribbon(
            aes(ymin = dfit - 1.96*dse, ymax = dfit + 1.96*dse, 
                group = method, fill = method),
            alpha = 0.12, color = NA
        ) +
        geom_line(linewidth = 1.2) +
        geom_vline(
            data = comp_lines_deriv,
            aes(xintercept = completion_day, color = method),
            linetype = "dotted", linewidth = 0.8
        ) +
        facet_wrap(~component, scales = "free_y", ncol = 1) +
        scale_color_brewer(palette = "Set1") +
        scale_fill_brewer(palette = "Set1") +
        labs(
            title = "Velocity-Based Completion Diagnostic",
            subtitle = sprintf("Vertical lines = first day CI includes 0 for %d consecutive days",
                               cfg$completion_L),
            x = "Days from announcement",
            y = "Velocity (derivative)",
            color = "Method",
            fill = "Method"
        ) +
        theme_pub
    
    save_plot("06_velocity_completion", p6_completion, 10, 8)
} else {
    cat("  Skipping completion diagnostic (insufficient data)\n")
}

# FIGURE 7: ECDF by phase (distribution check)
cat("Creating Figure 7: ECDF by phase...\n")

ecdf_long <- timing_data %>%
    select(days_to_qtr_end, phase, exit_pct, entry_pct, net_change_pct) %>%
    pivot_longer(
        cols = c(exit_pct, entry_pct, net_change_pct),
        names_to = "component_raw",
        values_to = "value"
    ) %>%
    mutate(
        component = recode(component_raw,
                           exit_pct = "Exit",
                           entry_pct = "Entry", 
                           net_change_pct = "Net")
    )

p7_ecdf <- ecdf_long %>%
    filter(component %in% c("Exit", "Entry")) %>%
    ggplot(aes(x = value, color = phase)) +
    stat_ecdf(geom = "step", linewidth = 1.1) +
    facet_wrap(~component, scales = "free_x", ncol = 1) +
    scale_color_manual(values = c("Early" = "#fee08b", 
                                  "Middle" = "#f46d43", 
                                  "Late" = "#a50026")) +
    labs(
        title = "Distribution of Deal-Level Flows by Phase",
        subtitle = "Tests whether acceleration is broad-based or driven by outliers",
        x = "Flow (% of TSO)",
        y = "Cumulative probability",
        color = "Phase"
    ) +
    theme_pub

save_plot("07_ecdf_by_phase", p7_ecdf, 10, 8)

# FIGURE 8: Bootstrap stability
cat("Creating Figure 8: Bootstrap results...\n")

if (nrow(boot_summary) > 0) {
    p8_boot <- ggplot(boot_summary, aes(x = reorder(model, -frequency), 
                                        y = share, fill = component)) +
        geom_col(position = "dodge", alpha = 0.8) +
        geom_text(
            aes(label = sprintf("%.1f%%", 100*share)),
            position = position_dodge(0.9),
            vjust = -0.5,
            size = 3.5
        ) +
        scale_fill_manual(values = c("Exit" = "#d73027", "Entry" = "#1a9850")) +
        scale_y_continuous(labels = scales::percent_format()) +
        labs(
            title = "Bootstrap Model Selection Stability",
            subtitle = sprintf("%d bootstrap resamples | Non-linear strongly preferred",
                               cfg$bootstrap_n),
            x = "Model",
            y = "Selection frequency",
            fill = "Component"
        ) +
        theme_pub +
        theme(axis.text.x = element_text(angle = 45, hjust = 1))
    
    save_plot("08_bootstrap_stability", p8_boot, 10, 6)
}

cat("\n")

#==================================================================================================
# SECTION 10: SUMMARY TABLES FOR PAPER
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("SUMMARY TABLES\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

# Table 1: Phase decomposition (cleaned for paper)
table1_phase <- phase_decomp %>%
    mutate(
        Phase = sprintf("%s (n=%d)", phase, N),
        Exit = sprintf("%.1f (%.1f)", Exit_Mean, Exit_SE),
        Entry = sprintf("%.1f (%.1f)", Entry_Mean, Entry_SE),
        Net = sprintf("%.1f (%.1f)", Net_Mean, Net_SE),
        Gross = sprintf("%.1f", Gross_Turnover)
    ) %>%
    select(Phase, Days_Range, Exit, Entry, Net, Gross)

cat("TABLE 1: Phase Decomposition\n")
print(table1_phase, width = Inf)
cat("\n")

# Table 2: Model comparison (best models only)
table2_models <- model_comparison %>%
  group_by(component) %>%
  slice_min(aic, n = 3) %>%
  ungroup() %>%
  mutate(
    Component = component,
    Family = family,
    Model = model,
    AIC = sprintf("%.1f", aic),
    Delta_AIC = sprintf("%.1f", delta_aic),
    R2 = sprintf("%.3f", r2)
  ) %>%
  select(Component, Family, Model, AIC, Delta_AIC, R2)

cat("TABLE 2: Top 3 Models per Component\n")
print(table2_models, width = Inf)
cat("\n")

# Table 3: Key horizon predictions
table3_horizons <- horizon_preds %>%
    mutate(
        Component = component,
        Days = days,
        Estimate = sprintf("%.1f", fit),
        SE = sprintf("%.1f", se),
        `95% CI` = sprintf("[%.1f, %.1f]", ci_low, ci_high)
    ) %>%
    select(Component, Days, Estimate, SE, `95% CI`)

cat("TABLE 3: Predictions at Key Horizons (Deal-level GAM)\n")
print(table3_horizons, width = Inf)
cat("\n")

# Table 4: Completion days
table4_completion <- bind_rows(
    completion_tbl_level %>% 
        mutate(metric = "Level (95%)"),
    completion_tbl_deriv %>% 
        mutate(metric = "Velocity (zero)")
) %>%
    mutate(
        Component = component,
        Method = method,
        Metric = metric,
        `Completion Day` = sprintf("%.0f", completion_day)
    ) %>%
    select(Component, Method, Metric, `Completion Day`) %>%
    arrange(Component, Method, Metric)

cat("TABLE 4: Completion Days (Two Metrics)\n")
print(table4_completion, width = Inf)
cat("\n")

#==================================================================================================
# SECTION 11: SAVE RESULTS
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("SAVING RESULTS\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

velocity_results_step15 <- list(
    # Config
    cfg = cfg,
    
    # Data
    timing_data = timing_data,
    timing_long = timing_long,
    weekly_agg = weekly_agg,
    support_week = support_week,
    
    # Phase analysis
    phase_decomp = phase_decomp,
    anova_results = anova_results,
    pairwise_tests = pairwise_tests,
    
    # Models
    fits = fits,
    model_comparison = model_comparison,
    
    # Bootstrap
    boot_summary = boot_summary,
    
    # Predictions
    preds_df = preds_df,
    horizon_preds = horizon_preds,
    
    # Derivatives
    derivs_df = derivs_df,
    
    # Completion
    completion_tbl_level = completion_tbl_level,
    completion_tbl_deriv = completion_tbl_deriv,
    
    # Tables for paper
    tables = list(
        phase = table1_phase,
        models = table2_models,
        horizons = table3_horizons,
        completion = table4_completion
    ),
    
    # Figure list
    figures = list.files(cfg$figures_dir, full.names = TRUE)
)

saveRDS(velocity_results_step15, 
        file.path(cfg$figures_dir, "velocity_results_step15.rds"))

cat(sprintf("Saved: %s\n", 
            file.path(cfg$figures_dir, "velocity_results_step15.rds")))

cat("\n")

#==================================================================================================
# SECTION 12: EXECUTIVE SUMMARY
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("EXECUTIVE SUMMARY\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

cat("SAMPLE:\n")
cat(sprintf("  %d %s deal-quarters\n", nrow(timing_data), cfg$deal_type_pattern))
cat(sprintf("  %d weekly bins (no cutoffs applied)\n", 
            nrow(weekly_agg) / length(components)))
cat("\n")

cat("PHASE EFFECTS:\n")
for (i in 1:nrow(anova_results)) {
    cat(sprintf("  %s: F=%.2f, p=%.4f %s\n",
                anova_results$Component[i],
                anova_results$F_stat[i],
                anova_results$p_value[i],
                anova_results$interpretation[i]))
}
cat("\n")

cat("KEY FINDINGS (Exit, most important):\n")
exit_early <- phase_decomp$Exit_Mean[phase_decomp$phase == "Early"]
exit_middle <- phase_decomp$Exit_Mean[phase_decomp$phase == "Middle"]
exit_late <- phase_decomp$Exit_Mean[phase_decomp$phase == "Late"]

cat(sprintf("  Early:  %.1f%% of TSO\n", exit_early))
cat(sprintf("  Middle: %.1f%% (+%.1f pp, %s)\n", 
            exit_middle, 
            exit_middle - exit_early,
            pairwise_tests$sig[pairwise_tests$Component == "Exit" & 
                                   pairwise_tests$Contrast == "Middle - Early"]))
cat(sprintf("  Late:   %.1f%% (+%.1f pp, %s)\n",
            exit_late,
            exit_late - exit_middle,
            pairwise_tests$sig[pairwise_tests$Component == "Exit" & 
                                   pairwise_tests$Contrast == "Late - Middle"]))
cat("\n")

cat("BEST MODELS:\n")
for (comp in components) {
    best <- model_comparison %>%
        filter(component == comp) %>%
        slice_min(aic, n = 1)
    
    cat(sprintf("  %s: %s %s (AIC=%.1f, R²=%.3f)\n",
                comp, best$family, best$model, best$aic, best$r2))
}
cat("\n")

cat("BOOTSTRAP VALIDATION:\n")
for (comp in c("Exit", "Entry")) {
    nonlin_pct <- 100 * (1 - boot_summary$share[
        boot_summary$component == comp & boot_summary$model == "linear"
    ])
    if (length(nonlin_pct) == 0) nonlin_pct <- 100  # linear never selected
    
    cat(sprintf("  %s: %.1f%% of draws select non-linear\n", comp, nonlin_pct))
}
cat("\n")

cat("COMPLETION (Deal-level GAM):\n")
exit_comp <- completion_tbl_deriv %>%
    filter(component == "Exit", method == "Deal-level GAM (REML)") %>%
    pull(completion_day)

entry_comp <- completion_tbl_deriv %>%
    filter(component == "Entry", method == "Deal-level GAM (REML)") %>%
    pull(completion_day)

if (length(exit_comp) > 0) {
    cat(sprintf("  Exit velocity crosses zero: day %.0f\n", exit_comp))
}
if (length(entry_comp) > 0) {
    cat(sprintf("  Entry velocity crosses zero: day %.0f\n", entry_comp))
}

exit_comp_level <- completion_tbl_level %>%
    filter(component == "Exit", method == "Deal-level GAM (REML)") %>%
    pull(completion_day)

if (length(exit_comp_level) > 0) {
    cat(sprintf("  Exit reaches 95%% of day-90: day %.0f\n", exit_comp_level))
}
cat("\n")

cat("FIGURES CREATED:\n")
figs <- list.files(cfg$figures_dir, pattern = "\\.(pdf|png)$")
for (f in sort(unique(sub("\\.(pdf|png)$", "", figs)))) {
    cat(sprintf("  %s (.pdf + .png)\n", f))
}
cat("\n")

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("STEP 15 COMPLETE\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

#==================================================================================================
# STEP 15B: OPTIMAL SEGMENTATION ANALYSIS
# Goal: Find data-driven breakpoints and test alternative phase definitions
#==================================================================================================

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("OPTIMAL SEGMENTATION ANALYSIS\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

#--------------------------------------------------------------------------------------------------
# 1. EXTRACT BREAKPOINTS FROM SEGMENTED MODELS
#--------------------------------------------------------------------------------------------------

cat("1. EXTRACTING SEGMENTED REGRESSION BREAKPOINTS\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

# Function to safely extract breakpoints
extract_breakpoints <- function(seg_model) {
  if (is.null(seg_model)) return(NULL)
  if (!inherits(seg_model, "segmented")) return(NULL)
  
  bp <- tryCatch(
    summary(seg_model)$psi,
    error = function(e) NULL
  )
  
  if (is.null(bp)) return(NULL)
  
  # psi is a matrix with columns: Est., St.Err, etc.
  if (is.matrix(bp)) {
    data.frame(
      breakpoint = bp[, "Est."],
      se = bp[, "St.Err"],
      row.names = NULL
    )
  } else {
    NULL
  }
}

# Extract for Entry (seg1 won)
entry_bp <- extract_breakpoints(fits[["Entry"]]$weekly_wls$models$seg1)
cat("ENTRY (seg1 - 1 breakpoint):\n")
if (!is.null(entry_bp)) {
  print(entry_bp, digits = 1)
  cat(sprintf("\n  → Entry regime changes at day %.0f (±%.1f)\n\n", 
              entry_bp$breakpoint[1], 1.96*entry_bp$se[1]))
} else {
  cat("  No breakpoints extracted (model may have failed)\n\n")
}

# Extract for Exit (try seg2 - 2 breakpoints)
exit_bp <- extract_breakpoints(fits[["Exit"]]$weekly_wls$models$seg2)
cat("EXIT (seg2 - 2 breakpoints):\n")
if (!is.null(exit_bp)) {
  print(exit_bp, digits = 1)
  cat(sprintf("\n  → Exit regime 1→2 at day %.0f (±%.1f)\n", 
              exit_bp$breakpoint[1], 1.96*exit_bp$se[1]))
  if (nrow(exit_bp) > 1) {
    cat(sprintf("  → Exit regime 2→3 at day %.0f (±%.1f)\n\n", 
                exit_bp$breakpoint[2], 1.96*exit_bp$se[2]))
  }
} else {
  cat("  No breakpoints extracted (model may have failed)\n\n")
}

#--------------------------------------------------------------------------------------------------
# 2. VELOCITY-BASED BREAKPOINTS (using derivative peaks)
#--------------------------------------------------------------------------------------------------

cat("\n2. VELOCITY-BASED BREAKPOINTS (derivative analysis)\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

velocity_breakpoints <- derivs_df %>%
  filter(!is.na(dfit), !is.na(dse)) %>%
  group_by(component, method) %>%
  summarise(
    peak_velocity_day = x[which.max(abs(dfit))],
    peak_velocity_value = max(abs(dfit), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(component %in% c("Exit", "Entry"))

cat("Peak Velocity Days (where acceleration is maximum):\n")
print(velocity_breakpoints, digits = 1)
cat("\n")

#--------------------------------------------------------------------------------------------------
# 3. ALTERNATIVE MANUAL SEGMENTATIONS
#--------------------------------------------------------------------------------------------------

cat("\n3. TESTING ALTERNATIVE MANUAL SEGMENTATIONS\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

# Define multiple segmentation schemes
segmentation_schemes <- list(
  original = c(30, 60),
  
  # Hypothesis 1: Very early takeoff (days 1-14)
  early_takeoff = c(14, 45),
  
  # Hypothesis 2: Sharp middle peak (zoom into acceleration)
  sharp_middle = c(20, 50),
  
  # Hypothesis 3: Data-driven from Entry seg1 (use extracted breakpoint)
  data_driven_entry = if (!is.null(entry_bp)) c(round(entry_bp$breakpoint[1]), 60) else c(30, 60),
  
  # Hypothesis 4: Data-driven from Exit seg2 (use both breakpoints)
  data_driven_exit = if (!is.null(exit_bp) && nrow(exit_bp) == 2) {
    round(exit_bp$breakpoint)
  } else {
    c(25, 55)
  },
  
  # Hypothesis 5: Velocity-based (use peak velocity days)
  velocity_based = if (nrow(velocity_breakpoints) >= 2) {
    c(round(mean(velocity_breakpoints$peak_velocity_day[1:2])), 60)
  } else {
    c(30, 60)
  },
  
  # Hypothesis 6: Finer early resolution
  fine_early = c(10, 30, 50, 70)  # 4 phases
)

# Function to compute phase stats for any breakpoint set
compute_phase_stats <- function(data, breaks, scheme_name) {
  
  # Create phase labels
  if (length(breaks) == 2) {
    phase_labels <- c(
      sprintf("Phase 1 (1-%d)", breaks[1]),
      sprintf("Phase 2 (%d-%d)", breaks[1]+1, breaks[2]),
      sprintf("Phase 3 (%d-90)", breaks[2]+1)
    )
    
    data_phased <- data %>%
      mutate(
        phase_new = case_when(
          days_to_qtr_end <= breaks[1] ~ phase_labels[1],
          days_to_qtr_end <= breaks[2] ~ phase_labels[2],
          TRUE ~ phase_labels[3]
        ),
        phase_new = factor(phase_new, levels = phase_labels)
      )
    
  } else if (length(breaks) == 4) {
    # 4-phase version
    phase_labels <- c(
      sprintf("Phase 1 (1-%d)", breaks[1]),
      sprintf("Phase 2 (%d-%d)", breaks[1]+1, breaks[2]),
      sprintf("Phase 3 (%d-%d)", breaks[2]+1, breaks[3]),
      sprintf("Phase 4 (%d-%d)", breaks[3]+1, breaks[4]),
      sprintf("Phase 5 (%d-90)", breaks[4]+1)
    )
    
    data_phased <- data %>%
      mutate(
        phase_new = case_when(
          days_to_qtr_end <= breaks[1] ~ phase_labels[1],
          days_to_qtr_end <= breaks[2] ~ phase_labels[2],
          days_to_qtr_end <= breaks[3] ~ phase_labels[3],
          days_to_qtr_end <= breaks[4] ~ phase_labels[4],
          TRUE ~ phase_labels[5]
        ),
        phase_new = factor(phase_new, levels = phase_labels)
      )
  } else {
    return(NULL)
  }
  
  # Compute phase means
  stats <- data_phased %>%
    group_by(phase_new) %>%
    summarise(
      n = n(),
      exit_mean = mean(exit_pct, na.rm = TRUE),
      exit_se = sd(exit_pct, na.rm = TRUE) / sqrt(n()),
      entry_mean = mean(entry_pct, na.rm = TRUE),
      entry_se = sd(entry_pct, na.rm = TRUE) / sqrt(n()),
      net_mean = mean(net_change_pct, na.rm = TRUE),
      net_se = sd(net_change_pct, na.rm = TRUE) / sqrt(n()),
      .groups = "drop"
    ) %>%
    mutate(scheme = scheme_name)
  
  # ANOVA F-stat for exit
  anova_exit <- anova(lm(exit_pct ~ phase_new, data = data_phased))
  f_stat <- anova_exit$`F value`[1]
  p_val <- anova_exit$`Pr(>F)`[1]
  
  list(
    stats = stats,
    f_stat = f_stat,
    p_value = p_val,
    breaks = breaks
  )
}

# Test all schemes
segmentation_results <- map(names(segmentation_schemes), function(scheme_name) {
  breaks <- segmentation_schemes[[scheme_name]]
  cat(sprintf("Testing: %s (breaks at: %s)\n", 
              scheme_name, 
              paste(breaks, collapse = ", ")))
  
  result <- compute_phase_stats(timing_data, breaks, scheme_name)
  
  if (!is.null(result)) {
    cat(sprintf("  F-stat (Exit): %.2f, p = %.4f\n", result$f_stat, result$p_value))
  }
  
  result
})
names(segmentation_results) <- names(segmentation_schemes)

cat("\n")

#--------------------------------------------------------------------------------------------------
# 4. COMPARE SEGMENTATION SCHEMES
#--------------------------------------------------------------------------------------------------

cat("\n4. SEGMENTATION COMPARISON (which breakpoints best explain variation?)\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

# Extract F-stats and p-values
comparison_table <- map_dfr(names(segmentation_results), function(scheme_name) {
  result <- segmentation_results[[scheme_name]]
  if (is.null(result)) return(NULL)
  
  tibble(
    scheme = scheme_name,
    breakpoints = paste(result$breaks, collapse = ", "),
    n_phases = length(result$breaks) + 1,
    F_stat_exit = result$f_stat,
    p_value_exit = result$p_value,
    exit_range = sprintf("%.1f to %.1f", 
                        min(result$stats$exit_mean), 
                        max(result$stats$exit_mean)),
    max_jump = max(diff(result$stats$exit_mean))
  )
}) %>%
  arrange(desc(F_stat_exit))

cat("Comparison Table (ranked by F-statistic for Exit):\n")
print(comparison_table, digits = 3, width = Inf)
cat("\n")

# Best scheme
best_scheme <- comparison_table$scheme[1]
cat(sprintf("*** BEST SEGMENTATION: %s (F=%.2f, p=%.4f) ***\n\n",
            best_scheme,
            comparison_table$F_stat_exit[1],
            comparison_table$p_value_exit[1]))

#--------------------------------------------------------------------------------------------------
# 5. DETAILED VIEW OF BEST SEGMENTATION
#--------------------------------------------------------------------------------------------------

cat("\n5. DETAILED BREAKDOWN: BEST SEGMENTATION\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

best_result <- segmentation_results[[best_scheme]]

cat(sprintf("Scheme: %s\n", best_scheme))
cat(sprintf("Breakpoints: %s\n", paste(best_result$breaks, collapse = ", ")))
cat(sprintf("F-statistic: %.2f (p = %.4f)\n\n", best_result$f_stat, best_result$p_value))

cat("Phase Statistics:\n")
best_stats <- best_result$stats %>%
  mutate(
    Exit = sprintf("%.1f (±%.1f)", exit_mean, 1.96*exit_se),
    Entry = sprintf("%.1f (±%.1f)", entry_mean, 1.96*entry_se),
    Net = sprintf("%.1f (±%.1f)", net_mean, 1.96*net_se)
  ) %>%
  select(phase_new, n, Exit, Entry, Net)

print(best_stats, width = Inf)
cat("\n")

# Pairwise comparisons for best scheme
cat("Pairwise Exit Comparisons (consecutive phases):\n")
phases <- levels(best_result$stats$phase_new)
for (i in 1:(length(phases)-1)) {
  phase1_data <- timing_data %>% 
    filter(days_to_qtr_end %in% 
           (if (i == 1) 1 else best_result$breaks[i-1]+1):best_result$breaks[i])
  phase2_data <- timing_data %>%
    filter(days_to_qtr_end %in% 
           (best_result$breaks[i]+1):(if (i == length(phases)-1) 90 else best_result$breaks[i+1]))
  
  if (nrow(phase1_data) > 0 && nrow(phase2_data) > 0) {
    t_test <- t.test(phase2_data$exit_pct, phase1_data$exit_pct)
    
    cat(sprintf("  %s → %s: Δ = %.2f pp (t=%.2f, p=%.4f) %s\n",
                phases[i],
                phases[i+1],
                mean(phase2_data$exit_pct, na.rm=TRUE) - mean(phase1_data$exit_pct, na.rm=TRUE),
                t_test$statistic,
                t_test$p.value,
                ifelse(t_test$p.value < 0.001, "***",
                       ifelse(t_test$p.value < 0.01, "**",
                              ifelse(t_test$p.value < 0.05, "*", "")))))
  }
}

cat("\n")

#--------------------------------------------------------------------------------------------------
# 6. VISUAL: COMPARE TOP 3 SEGMENTATIONS
#--------------------------------------------------------------------------------------------------

cat("\n6. CREATING COMPARISON FIGURE\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

# Get top 3 schemes
top3_schemes <- comparison_table$scheme[1:min(3, nrow(comparison_table))]

# Prepare data for plotting
plot_data <- map_dfr(top3_schemes, function(scheme_name) {
  segmentation_results[[scheme_name]]$stats %>%
    mutate(scheme = scheme_name)
})

# Create faceted plot
p_segmentation_compare <- ggplot(plot_data, 
                                 aes(x = phase_new, y = exit_mean, fill = scheme)) +
  geom_col(position = "dodge", alpha = 0.8) +
  geom_errorbar(
    aes(ymin = exit_mean - 1.96*exit_se, ymax = exit_mean + 1.96*exit_se),
    position = position_dodge(0.9),
    width = 0.25
  ) +
  facet_wrap(~scheme, ncol = 1, scales = "free_x") +
  scale_fill_brewer(palette = "Set2") +
  labs(
    title = "Exit Flow by Segmentation Scheme",
    subtitle = "Comparing alternative phase definitions (ranked by F-statistic)",
    x = "Phase",
    y = "Exit flow (% of TSO)",
    caption = sprintf("Best: %s (F=%.1f, p<%.3f)", 
                     best_scheme, 
                     comparison_table$F_stat_exit[1],
                     comparison_table$p_value_exit[1])
  ) +
  theme_pub +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 10),
    legend.position = "none"
  )

save_plot("09_segmentation_comparison", p_segmentation_compare, 10, 9)

#--------------------------------------------------------------------------------------------------
# 7. EARLY WINDOW ZOOM (Days 1-14 detailed)
#--------------------------------------------------------------------------------------------------

cat("\n7. EARLY WINDOW ANALYSIS (Days 1-14 in detail)\n")
cat("-" %>% rep(80) %>% paste(collapse=""), "\n\n")

early_window <- timing_data %>%
  filter(days_to_qtr_end <= 14) %>%
  mutate(
    day_bin = case_when(
      days_to_qtr_end <= 3 ~ "Days 1-3",
      days_to_qtr_end <= 7 ~ "Days 4-7",
      days_to_qtr_end <= 10 ~ "Days 8-10",
      TRUE ~ "Days 11-14"
    ),
    day_bin = factor(day_bin, levels = c("Days 1-3", "Days 4-7", "Days 8-10", "Days 11-14"))
  ) %>%
  group_by(day_bin) %>%
  summarise(
    n = n(),
    exit = mean(exit_pct, na.rm = TRUE),
    exit_se = sd(exit_pct, na.rm = TRUE) / sqrt(n()),
    entry = mean(entry_pct, na.rm = TRUE),
    entry_se = sd(entry_pct, na.rm = TRUE) / sqrt(n()),
    .groups = "drop"
  )

cat("Early Takeoff Analysis (first 14 days, by sub-period):\n")
print(early_window, digits = 2)
cat("\n")

# Test if days 1-3 differ from 4-7
if (nrow(timing_data %>% filter(days_to_qtr_end <= 3)) >= 5 &&
    nrow(timing_data %>% filter(days_to_qtr_end > 3, days_to_qtr_end <= 7)) >= 5) {
  
  very_early <- timing_data %>% filter(days_to_qtr_end <= 3)
  early <- timing_data %>% filter(days_to_qtr_end > 3, days_to_qtr_end <= 7)
  
  t_test_early <- t.test(early$exit_pct, very_early$exit_pct)
  
  cat(sprintf("Days 4-7 vs Days 1-3: Δ = %.2f pp (p=%.3f) %s\n",
              mean(early$exit_pct, na.rm=TRUE) - mean(very_early$exit_pct, na.rm=TRUE),
              t_test_early$p.value,
              ifelse(t_test_early$p.value < 0.05, "*", "")))
  
  cat("\nInterpretation: ")
  if (t_test_early$p.value < 0.05) {
    cat("Significant acceleration in first week!\n")
  } else {
    cat("No significant difference in first week (gradual start).\n")
  }
}

cat("\n")

#--------------------------------------------------------------------------------------------------
# 8. SAVE RESULTS
#--------------------------------------------------------------------------------------------------

segmentation_analysis <- list(
  entry_breakpoint = entry_bp,
  exit_breakpoints = exit_bp,
  velocity_breakpoints = velocity_breakpoints,
  comparison_table = comparison_table,
  best_scheme = best_scheme,
  best_result = best_result,
  early_window = early_window,
  all_results = segmentation_results
)

saveRDS(segmentation_analysis, 
        file.path(cfg$figures_dir, "segmentation_analysis.rds"))

cat("=" %>% rep(80) %>% paste(collapse=""), "\n")
cat("SEGMENTATION ANALYSIS COMPLETE\n")
cat("=" %>% rep(80) %>% paste(collapse=""), "\n\n")

#=================================================================================================
# STEP 16: STAYER ANALYSIS: Position Changes by Continuing Holders
# Input: flow_panel, timing_data, target_panel
# Output: Stayer behavior patterns by measurement window
#=================================================================================================

# Extract stayers
stayers <- flow_panel %>%
  filter(Stayer == 1) %>%
  mutate(
    shares_change = shares_t0 - shares_tm1,
    pct_change = (shares_change / shares_tm1) * 100
  )

cat(sprintf("Stayer observations: %s\n", format(nrow(stayers), big.mark = ",")))
cat(sprintf("  Increased: %d (%.1f%%)\n", sum(stayers$shares_change > 0),
            100 * mean(stayers$shares_change > 0)))
cat(sprintf("  Decreased: %d (%.1f%%)\n", sum(stayers$shares_change < 0),
            100 * mean(stayers$shares_change < 0)))

# Get TSO baseline
tso_data <- target_panel %>%
  filter(event_time == -1) %>%
  select(master_deal_no, target_permno, TSO = TSO)

# Deal-level stayer behavior
deal_stayer <- stayers %>%
  group_by(master_deal_no, permno) %>%
  summarise(
    n_stayers = n(),
    n_decrease = sum(shares_change < 0),
    total_change = sum(shares_change),
    .groups = "drop"
  ) %>%
  left_join(
    tso_data,
    by = c("master_deal_no", "permno" = "target_permno")
  ) %>%
  filter(!is.na(TSO), TSO > 0) %>%
  mutate(stayer_pct_tso = (total_change / TSO) * 100)

# Add timing
deal_stayer_timing <- deal_stayer %>%
  left_join(
    timing_data %>% select(master_deal_no, permno, days_to_qtr_end, deal_type),
    by = c("master_deal_no", "permno")
  ) %>%
  filter(!is.na(days_to_qtr_end))

# Weekly aggregation
stayer_weekly <- deal_stayer_timing %>%
  filter(deal_type == "Cash") %>%
  mutate(week = floor(days_to_qtr_end / 7)) %>%
  group_by(week) %>%
  summarise(
    N = n(),
    Days_Mid = mean(days_to_qtr_end),
    Stayer_Change = mean(stayer_pct_tso),
    .groups = "drop"
  )

print(stayer_weekly, digits = 1)

# Institution types
stayer_types <- stayers %>%
  group_by(mgrno) %>%
  summarise(
    n = n(),
    n_increase = sum(shares_change > 0),
    n_decrease = sum(shares_change < 0),
    .groups = "drop"
  ) %>%
  mutate(
    type = case_when(
      n_increase / n > 0.66 ~ "Increaser",
      n_decrease / n > 0.66 ~ "Decreaser",
      TRUE ~ "Mixed"
    )
  ) %>%
  count(type) %>%
  mutate(pct = n / sum(n) * 100)

print(stayer_types, digits = 1)

#=================================================================================================
# STEP 17: STAYER-ONLY ELECTORATE ANALYSIS
# Question: Among continuing holders, does the power structure shift?
#=================================================================================================

# ---- Get stayer holdings -----------------------------------------------------------------------
stayer_holdings_tm1 <- flow_panel %>%
  filter(Stayer == 1, shares_tm1 > 0) %>%
  select(master_deal_no, permno, mgrno, shares_tm1)

stayer_holdings_t0 <- flow_panel %>%
  filter(Stayer == 1, shares_t0 > 0) %>%
  select(master_deal_no, permno, mgrno, shares_t0)

# Calculate all metrics for top-K stayers
top_k_stayers <- function(k) {
  
  topk_tm1 <- stayer_holdings_tm1 %>%
    group_by(master_deal_no, permno) %>%
    arrange(desc(shares_tm1)) %>%
    slice_head(n = k) %>%
    mutate(weight = shares_tm1 / sum(shares_tm1)) %>%
    ungroup()
  
  topk_t0 <- stayer_holdings_t0 %>%
    group_by(master_deal_no, permno) %>%
    arrange(desc(shares_t0)) %>%
    slice_head(n = k) %>%
    mutate(weight = shares_t0 / sum(shares_t0)) %>%
    ungroup()
  
  deals <- topk_tm1 %>% distinct(master_deal_no, permno)
  
  deals %>%
    mutate(
      # Jaccard
      jaccard = map2_dbl(master_deal_no, permno, ~{
        mgrs_tm1 <- topk_tm1 %>% filter(master_deal_no == .x, permno == .y) %>% pull(mgrno)
        mgrs_t0 <- topk_t0 %>% filter(master_deal_no == .x, permno == .y) %>% pull(mgrno)
        if (length(mgrs_tm1) == 0 | length(mgrs_t0) == 0) return(NA_real_)
        length(intersect(mgrs_tm1, mgrs_t0)) / length(union(mgrs_tm1, mgrs_t0))
      }),
      
      # Cosine
      cosine = map2_dbl(master_deal_no, permno, ~{
        d_tm1 <- topk_tm1 %>% filter(master_deal_no == .x, permno == .y)
        d_t0 <- topk_t0 %>% filter(master_deal_no == .x, permno == .y)
        if (nrow(d_tm1) == 0 | nrow(d_t0) == 0) return(NA_real_)
        
        all_mgrs <- union(d_tm1$mgrno, d_t0$mgrno)
        v1 <- sapply(all_mgrs, function(m) {
          idx <- which(d_tm1$mgrno == m)
          if (length(idx) > 0) d_tm1$weight[idx[1]] else 0
        })
        v2 <- sapply(all_mgrs, function(m) {
          idx <- which(d_t0$mgrno == m)
          if (length(idx) > 0) d_t0$weight[idx[1]] else 0
        })
        sum(v1 * v2) / (sqrt(sum(v1^2)) * sqrt(sum(v2^2)))
      }),
      
      # HHI
      hhi_tm1 = map2_dbl(master_deal_no, permno, ~{
        w <- topk_tm1 %>% filter(master_deal_no == .x, permno == .y) %>% pull(weight)
        if (length(w) == 0) return(NA_real_)
        sum(w^2) * 10000
      }),
      
      hhi_t0 = map2_dbl(master_deal_no, permno, ~{
        w <- topk_t0 %>% filter(master_deal_no == .x, permno == .y) %>% pull(weight)
        if (length(w) == 0) return(NA_real_)
        sum(w^2) * 10000
      }),
      
      delta_hhi = hhi_t0 - hhi_tm1,
      jaccard_repl = (1 - jaccard) * 100,
      cosine_repl = (1 - cosine) * 100
    ) %>%
    summarise(
      K = k,
      Jaccard = mean(jaccard_repl, na.rm = TRUE),
      Cosine = mean(cosine_repl, na.rm = TRUE),
      Delta_HHI = mean(delta_hhi, na.rm = TRUE)
    )
}

# Calculate for K=5,10,20
results_stayers <- map_dfr(c(5, 10, 20), top_k_stayers)

# Comparison table
comparison <- tibble(
  K = c(5, 10, 20),
  All_Holders_Jaccard = c(45.6, 50.9, 53.9),
  Stayers_Jaccard = results_stayers$Jaccard,
  All_Holders_Cosine = c(24.3, 23.0, 22.1),
  Stayers_Cosine = results_stayers$Cosine,
  All_Holders_HHI = c(18.7, 2.2, -23.6),
  Stayers_HHI = results_stayers$Delta_HHI
)

print(comparison, digits = 1)

# ---- Position Size vs Trim Analysis ------------------------------------------------------------

# ---- stayer_dynamics to stayers ----------------------------------------------------------------
  stayer_dynamics <- stayers %>%
    left_join(
      target_panel %>% filter(event_time == -1) %>%
        select(target_permno, TSO_baseline = TSO),
      by = c("permno" = "target_permno")
    ) %>%
    filter(!is.na(TSO_baseline), TSO_baseline > 0) %>%
    mutate(
      initial_pct_of_tso = (shares_tm1 / TSO_baseline) * 100,
      pct_trim = -100 * (shares_change / shares_tm1),
      size_category = case_when(
        initial_pct_of_tso >= 5 ~ "Large (≥5% TSO)",
        initial_pct_of_tso >= 1 ~ "Medium (1-5% TSO)",
        initial_pct_of_tso >= 0.1 ~ "Small (0.1-1% TSO)",
        TRUE ~ "Tiny (<0.1% TSO)"
      )
    )

# Regression
size_pred_model <- lm(abs(pct_trim) ~ log(initial_pct_of_tso + 0.01),
                      data = stayer_dynamics %>% filter(shares_change != 0))

cat("Regression: |% trim| ~ log(position size)\n")
print(summary(size_pred_model)$coefficients, digits = 3)

# ---- interpretation ------------------------------------------------------------------------------
if (coef(size_pred_model)[2] < 0 &
    abs(coef(size_pred_model)[2] / summary(size_pred_model)$coefficients[2, "Std. Error"]) > 1.96) {
  cat("\n✓ CONFIRMED: Larger positions → SMALLER % trims\n")
  cat("  → Large holders are STICKIER (trim less)\n")
  cat("  → Medium holders trim most aggressively\n")
  cat("  → This is why large holders maintain top-10 rank\n\n")
} else {
  cat("\n→ No strong relationship between size and trim %\n\n")
}

cat("But DOLLAR volume reverses this:\n")
cat("  • Medium holders: 6.4% of stayers, 40.4% of $ trims\n")
cat("  • Large holders: 1.5% of stayers, 23.8% of $ trims\n")
cat("  → Medium holders DOMINATE capital flows\n\n")

#=================================================================================================
# STEP 18: Insider Ownership at Announcement
# Input:  deals_with_io, target_panel, wrds connection
# Output: insider_ownership_clean.rds
#=================================================================================================

# ---- Get CUSIP6 from CRSP for target PERMNOs ---------------------------------------------------
target_permnos <- unique(deals_with_io$target_permno)

cusip_query <- sprintf("
 SELECT DISTINCT permno, ncusip, namedt, nameendt
 FROM crsp.msenames
 WHERE permno IN (%s) AND ncusip IS NOT NULL
", paste(target_permnos, collapse = ","))

permno_cusip <- dbGetQuery(wrds, cusip_query) %>%
 as_tibble() %>%
 mutate(
   cusip6 = substr(ncusip, 1, 6),
   namedt = as.Date(namedt),
   nameendt = as.Date(coalesce(nameendt, as.Date("9999-12-31")))
 )

# ---- Map to announcement dates -----------------------------------------------------------------
target_cusip_map <- deals_with_io %>%
 transmute(
   master_deal_no = as.character(master_deal_no),
   target_permno = as.integer(target_permno),
   dateann = as.Date(dateann),
   t_0 = as.Date(t_0)
 ) %>%
 left_join(permno_cusip, by = c("target_permno" = "permno"),
           relationship = "many-to-many") %>%
 filter(namedt <= dateann, dateann <= nameendt) %>%
 group_by(master_deal_no) %>%
 slice_head(n = 1) %>%
 ungroup() %>%
 mutate(
   window_start = dateann - (15 * 365),
   window_end = t_0
 ) %>%
 filter(!is.na(cusip6)) %>%
 select(master_deal_no, target_permno, cusip6, window_start, window_end)

# ---- Build SQL for FactSet ---------------------------------------------------------------------
keys_sql <- target_cusip_map %>%
 mutate(
   row_sql = sprintf("('%s', '%s', '%s', '%s')",
                    master_deal_no, cusip6, window_start, window_end)
 ) %>%
 pull(row_sql) %>%
 paste(collapse = ",\n")

sql <- sprintf("
 WITH deal_windows AS (
   SELECT * FROM (VALUES %s) AS t(master_deal_no, cusip6, window_start, window_end)
 ),
 holdings AS (
   SELECT
     w.master_deal_no, t.personid, t.trandate, t.ownership,
     COALESCE(t.sharesheld_adj, t.sharesheld, 0) AS shares,
     ROW_NUMBER() OVER (
       PARTITION BY w.master_deal_no, t.personid, t.ownership
       ORDER BY t.trandate DESC
     ) AS rn
   FROM deal_windows w
   JOIN tfn.table1 t ON t.cusip6 = w.cusip6
   WHERE t.trandate >= w.window_start::date
     AND t.trandate <= w.window_end::date
     AND t.formtype IN ('3', '4', '5', '3/A', '4/A', '5/A')
     AND t.ownership IN ('D', 'I')
     AND (t.sectitle ILIKE '%%COM%%' OR t.sectitle ILIKE '%%COMMON%%')
 )
 SELECT * FROM holdings WHERE rn = 1;
", keys_sql)

insider_raw <- dbGetQuery(wrds, sql) %>% as_tibble()

# Get TSO for normalization
target_tso <- target_panel %>%
 filter(event_time == 0) %>%
 select(master_deal_no, target_permno, TSO_t0 = TSO)

# Normalize PER-PERSON before aggregating
insider_normalized <- insider_raw %>%
 group_by(master_deal_no, personid) %>%
 summarise(total_shares = sum(shares), .groups = "drop") %>%
 left_join(target_cusip_map %>% select(master_deal_no, target_permno), by = "master_deal_no") %>%
 left_join(target_tso, by = c("master_deal_no", "target_permno")) %>%
 filter(!is.na(TSO_t0), TSO_t0 > 0) %>%
 mutate(
   person_pct = (total_shares / TSO_t0) * 100,
   person_pct_wins = pmin(person_pct, 10)  # Winsorize per-person at 10%
 )

# ---- Aggregate to deal level -------------------------------------------------------------------
insider_ownership_clean <- insider_normalized %>%
 group_by(master_deal_no) %>%
 summarise(
   n_insiders = n(),
   insider_pct_raw = sum(person_pct, na.rm = TRUE),
   insider_pct = pmin(sum(person_pct_wins, na.rm = TRUE), 100),
   max_individual = max(person_pct, na.rm = TRUE),
   .groups = "drop"
 )

# ---- Summary -----------------------------------------------------------------------------------
cat("\nInsider Ownership:\n")
cat(sprintf("  Deals: %d\n", nrow(insider_ownership_clean)))
cat(sprintf("  Mean: %.1f%%\n", mean(insider_ownership_clean$insider_pct)))
cat(sprintf("  Median: %.1f%%\n", median(insider_ownership_clean$insider_pct)))
cat(sprintf("  P25-P75: %.1f%% - %.1f%%\n",
           quantile(insider_ownership_clean$insider_pct, 0.25),
           quantile(insider_ownership_clean$insider_pct, 0.75)))

saveRDS(insider_ownership_clean, "insider_ownership_clean.rds")

#=================================================================================================
# STEP 19 (Corrected): Risk Transformation Analysis
# Inputs:  deals_with_io, wrds connection
# Outputs: Target risk transformation table (Just Before vs Just After) + RDS results
# Notes:
#   - Computes metrics directly within event windows (no rolling contamination).
#   - Idiosyncratic volatility = SD(residuals) from market model on excess returns.
#   - Market correlation uses excess returns (ret - rf) vs mktrf (already excess).
#=================================================================================================

library(dplyr)
library(tidyr)

# ----------------------------
# 19.0 Deal sample for risk
# ----------------------------
deals_for_risk <- deals_with_io %>%
  transmute(
    master_deal_no  = as.character(master_deal_no),
    target_permno   = as.integer(target_permno),
    acquirer_permno = as.integer(acquirer_permno),
    dateann         = as.Date(dateann),
    dateeff         = as.Date(dateeff),
    start_date      = dateann - 250,
    end_date        = pmin(dateann + 250, dateeff),
    deal_type       = case_when(
      !is.na(pct_cash) & pct_cash >= 90 ~ "All Cash",
      !is.na(pct_stk)  & pct_stk  >= 90 ~ "All Stock",
      TRUE ~ "Mixed"
    ),
    deal_value
  ) %>%
  filter(!is.na(target_permno), !is.na(acquirer_permno), !is.na(dateann)) %>%
  distinct(master_deal_no, .keep_all = TRUE)

cat(sprintf("STEP 19 sample: %d deals\n\n", nrow(deals_for_risk)))

all_permnos <- unique(c(deals_for_risk$target_permno, deals_for_risk$acquirer_permno))
min_date <- min(deals_for_risk$start_date, na.rm = TRUE)
max_date <- max(deals_for_risk$end_date, na.rm = TRUE)

# ----------------------------
# 19.1 Pull CRSP daily returns
# ----------------------------
crsp_query <- sprintf("
SELECT permno, date, ret
FROM crsp.dsf
WHERE permno IN (%s)
  AND date BETWEEN '%s' AND '%s'
  AND ret IS NOT NULL
", paste(all_permnos, collapse = ","), min_date, max_date)

crsp_daily <- dbGetQuery(wrds, crsp_query) %>%
  mutate(
    date = as.Date(date),
    ret  = as.numeric(ret),
    # Drop clearly bad daily return codes/outliers; adjust threshold if needed
    ret  = if_else(is.na(ret) | abs(ret) > 2, NA_real_, ret)
  )

# ----------------------------
# 19.2 Pull FF daily (mktrf, rf)
# ----------------------------
ff_query <- sprintf("
SELECT date, mktrf, rf
FROM ff.factors_daily
WHERE date BETWEEN '%s' AND '%s'
", min_date, max_date)

ff_raw <- dbGetQuery(wrds, ff_query) %>%
  mutate(
    date  = as.Date(date),
    mktrf = as.numeric(mktrf),
    rf    = as.numeric(rf)
  )

# ---- Factor scaling sanity check ----
# On WRDS, ff.factors_daily is typically in percent units (e.g., 0.12 = 0.12%).
# We detect and scale accordingly.
mktrf_med_abs <- median(abs(ff_raw$mktrf), na.rm = TRUE)
rf_med_abs    <- median(abs(ff_raw$rf), na.rm = TRUE)

scale_is_percent <- (mktrf_med_abs > 0.005 | rf_med_abs > 0.002) # heuristic
scale_factor <- if (scale_is_percent) 100 else 1

ff_daily <- ff_raw %>%
  mutate(
    mktrf = mktrf / scale_factor,
    rf    = rf    / scale_factor
  )

cat(sprintf("FF scaling detected as %s; dividing by %d.\n\n",
            ifelse(scale_is_percent, "PERCENT units", "DECIMAL units"), scale_factor))

# ----------------------------
# 19.3 Build deal-day panel with both target and acquirer returns
# ----------------------------
# Target return rows
target_daily <- crsp_daily %>%
  inner_join(
    deals_for_risk %>%
      select(master_deal_no, target_permno, acquirer_permno, dateann, dateeff,
             start_date, end_date, deal_type, deal_value),
    by = c("permno" = "target_permno")
  ) %>%
  filter(date >= start_date, date <= end_date) %>%
  rename(target_ret = ret) %>%
  select(master_deal_no, date, target_ret, acquirer_permno,
         dateann, dateeff, deal_type, deal_value)

# Acquirer return rows keyed to deal
acquirer_daily <- crsp_daily %>%
  inner_join(
    deals_for_risk %>%
      select(master_deal_no, acquirer_permno) %>%
      distinct(),
    by = c("permno" = "acquirer_permno")
  ) %>%
  rename(acquirer_ret = ret) %>%
  select(master_deal_no, date, acquirer_ret)

# Combine + add FF
deal_daily <- target_daily %>%
  left_join(acquirer_daily, by = c("master_deal_no", "date")) %>%
  inner_join(ff_daily, by = "date") %>%
  mutate(
    days_rel   = as.integer(date - dateann),
    targ_excess = target_ret - rf
  )

# ----------------------------
# 19.4 Helper: compute window metrics within a deal
# ----------------------------
compute_window_metrics <- function(df) {
  # Requires columns: target_ret, targ_excess, mktrf, acquirer_ret
  ok_mkt <- !is.na(df$targ_excess) & !is.na(df$mktrf)
  ok_cor <- !is.na(df$targ_excess) & !is.na(df$mktrf)
  ok_acq <- !is.na(df$target_ret)  & !is.na(df$acquirer_ret)

  n_mkt <- sum(ok_mkt)
  n_acq <- sum(ok_acq)

  mkt_beta <- if (n_mkt >= 30) {
    cov(df$targ_excess[ok_mkt], df$mktrf[ok_mkt]) / var(df$mktrf[ok_mkt])
  } else NA_real_

  mkt_corr <- if (sum(ok_cor) >= 30) {
    cor(df$targ_excess[ok_cor], df$mktrf[ok_cor])
  } else NA_real_

  acq_corr <- if (n_acq >= 30) {
    cor(df$target_ret[ok_acq], df$acquirer_ret[ok_acq])
  } else NA_real_

  # Idiosyncratic vol: SD of residuals from market model on excess returns
  idio_vol <- if (n_mkt >= 30) {
    fit <- lm(df$targ_excess[ok_mkt] ~ df$mktrf[ok_mkt])
    sd(resid(fit), na.rm = TRUE)
  } else NA_real_

  tibble(
    n_mkt = n_mkt,
    n_acq = n_acq,
    mkt_beta = mkt_beta,
    mkt_corr = mkt_corr,
    acq_corr = acq_corr,
    idio_vol = idio_vol
  )
}

# ----------------------------
# 19.5 Compute Just Before / Just After metrics per deal
# ----------------------------
risk_by_window <- deal_daily %>%
  mutate(
    window = case_when(
      days_rel >= -60 & days_rel <= -1  ~ "Just Before (-60 to -1)",
      days_rel >= 0   & days_rel <= 60  ~ "Just After (0 to 60)",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(window)) %>%
  group_by(master_deal_no, deal_type, window) %>%
  group_modify(~ compute_window_metrics(.x)) %>%
  ungroup() %>%
  # Require enough data in each window
  filter(n_mkt >= 30, n_acq >= 30)

# ----------------------------
# 19.6 Build table with before/after + deltas and t-tests (within type)
# ----------------------------
risk_wide <- risk_by_window %>%
  select(master_deal_no, deal_type, window, mkt_corr, mkt_beta, acq_corr, idio_vol) %>%
  pivot_wider(names_from = window, values_from = c(mkt_corr, mkt_beta, acq_corr, idio_vol))

# Deal-level deltas
risk_deltas <- risk_wide %>%
  mutate(
    d_mkt_corr = `mkt_corr_Just After (0 to 60)` - `mkt_corr_Just Before (-60 to -1)`,
    d_mkt_beta = `mkt_beta_Just After (0 to 60)` - `mkt_beta_Just Before (-60 to -1)`,
    d_acq_corr = `acq_corr_Just After (0 to 60)` - `acq_corr_Just Before (-60 to -1)`,
    d_idio_vol = `idio_vol_Just After (0 to 60)` - `idio_vol_Just Before (-60 to -1)`
  )

# Summary by deal type (levels + change)
risk_summary <- risk_deltas %>%
  group_by(deal_type) %>%
  summarise(
    N = n(),

    mkt_corr_before = mean(`mkt_corr_Just Before (-60 to -1)`, na.rm = TRUE),
    mkt_corr_after  = mean(`mkt_corr_Just After (0 to 60)`, na.rm = TRUE),
    mkt_corr_delta  = mean(d_mkt_corr, na.rm = TRUE),

    mkt_beta_before = mean(`mkt_beta_Just Before (-60 to -1)`, na.rm = TRUE),
    mkt_beta_after  = mean(`mkt_beta_Just After (0 to 60)`, na.rm = TRUE),
    mkt_beta_delta  = mean(d_mkt_beta, na.rm = TRUE),

    acq_corr_before = mean(`acq_corr_Just Before (-60 to -1)`, na.rm = TRUE),
    acq_corr_after  = mean(`acq_corr_Just After (0 to 60)`, na.rm = TRUE),
    acq_corr_delta  = mean(d_acq_corr, na.rm = TRUE),

    idio_before     = mean(`idio_vol_Just Before (-60 to -1)`, na.rm = TRUE),
    idio_after      = mean(`idio_vol_Just After (0 to 60)`, na.rm = TRUE),
    idio_delta      = mean(d_idio_vol, na.rm = TRUE),

    # within-type t-tests of delta != 0 (match your note)
    p_mkt_corr = tryCatch(t.test(d_mkt_corr)$p.value, error = function(e) NA_real_),
    p_mkt_beta = tryCatch(t.test(d_mkt_beta)$p.value, error = function(e) NA_real_),
    p_acq_corr = tryCatch(t.test(d_acq_corr)$p.value, error = function(e) NA_real_),
    p_idio_vol = tryCatch(t.test(d_idio_vol)$p.value, error = function(e) NA_real_),

    .groups = "drop"
  )

print(risk_summary, digits = 3)

# Optional: cash vs stock delta differences (for your note)
cash_vs_stock <- risk_deltas %>%
  filter(deal_type %in% c("All Cash", "All Stock")) %>%
  group_by(deal_type) %>%
  summarise(
    N = n(),
    d_mkt_corr = mean(d_mkt_corr, na.rm = TRUE),
    d_mkt_beta = mean(d_mkt_beta, na.rm = TRUE),
    d_acq_corr = mean(d_acq_corr, na.rm = TRUE),
    d_idio_vol = mean(d_idio_vol, na.rm = TRUE),
    .groups = "drop"
  )

cat("\nCash vs Stock (mean deltas):\n")
print(cash_vs_stock, digits = 3)

cat("\nCash-Stock delta tests:\n")
tmp <- risk_deltas %>% filter(deal_type %in% c("All Cash","All Stock"))
for (v in c("d_mkt_corr","d_mkt_beta","d_acq_corr","d_idio_vol")) {
  tt <- tryCatch(t.test(tmp[[v]] ~ tmp$deal_type), error = function(e) NULL)
  if (!is.null(tt)) {
    cat(sprintf("  %s: diff=%.4f, p=%.4g\n",
                v, diff(rev(tt$estimate)), tt$p.value))
  }
}

# ----------------------------
# 19.7 Save results
# ----------------------------
saveRDS(list(
  deals_for_risk = deals_for_risk,
  deal_daily     = deal_daily,
  risk_by_window = risk_by_window,
  risk_deltas    = risk_deltas,
  risk_summary   = risk_summary
), "risk_transformation_results.rds")

cat("\nSaved: risk_transformation_results.rds\n")

#===========================================================================================
# STEP 20: Cross-Deal Arbitrage - Target/Acquirer Cross-Holdings in Stock Deals
# Input:  institutional_holdings_granular, target_panel, mergers
# Output: Analysis of institutions taking positions in counterparty
#===========================================================================================

# ---- Identify Deal Pairs (Target-Acquirer) -----------------------------------------------------

deal_pairs <- mergers %>%
 transmute(
   master_deal_no = as.character(master_deal_no),
   target_permno = as.integer(sub(";.*$", "", target_permno)),
   acquirer_permno = as.integer(sub(";.*$", "", acquirer_permno)),
   t_m1 = qend_prev(qend(dateann)),
   t_0 = qend(dateann),
   deal_value,
   pct_cash,
   pct_stk,
   deal_type = case_when(
     pct_cash >= 90 ~ "Cash",
     pct_stk >= 90 ~ "Stock",
     TRUE ~ "Mixed"
   )
 ) %>%
 filter(!is.na(target_permno), !is.na(acquirer_permno)) %>%
 # Focus on pure cash vs pure stock for clean comparison
 filter(deal_type %in% c("Cash", "Stock"))

cat(sprintf("Deal pairs: %d (Cash: %d, Stock: %d)\n",
           nrow(deal_pairs),
           sum(deal_pairs$deal_type == "Cash"),
           sum(deal_pairs$deal_type == "Stock")))

# ---- Get Holdings at t=-1 and t=0 -------------------------------------------------------------

# Process target holdings at t=-1
target_holdings_tm1 <- institutional_holdings_granular %>%
 inner_join(
   deal_pairs %>% select(master_deal_no, permno = target_permno, t_m1, deal_type),
   by = c("permno", "rdate" = "t_m1")
 ) %>%
 filter(shares_adj > 0) %>%
 distinct(master_deal_no, mgrno, deal_type) %>%
 mutate(holds_target_tm1 = TRUE)

# Process acquirer holdings at t=-1
acquirer_holdings_tm1 <- institutional_holdings_granular %>%
 inner_join(
   deal_pairs %>% select(master_deal_no, permno = acquirer_permno, t_m1, deal_type),
   by = c("permno", "rdate" = "t_m1")
 ) %>%
 filter(shares_adj > 0) %>%
 distinct(master_deal_no, mgrno, deal_type) %>%
 mutate(holds_acquirer_tm1 = TRUE)

# Combine t=-1 holdings
holdings_tm1 <- full_join(target_holdings_tm1, acquirer_holdings_tm1,
                        by = c("master_deal_no", "mgrno", "deal_type")) %>%
 mutate(
   holds_target_tm1 = coalesce(holds_target_tm1, FALSE),
   holds_acquirer_tm1 = coalesce(holds_acquirer_tm1, FALSE)
 )

cat(sprintf("Holdings at t=-1: %d manager-deal pairs\n", nrow(holdings_tm1)))

# Process target holdings at t=0
target_holdings_t0 <- institutional_holdings_granular %>%
 inner_join(
   deal_pairs %>% select(master_deal_no, permno = target_permno, t_0, deal_type),
   by = c("permno", "rdate" = "t_0")
 ) %>%
 filter(shares_adj > 0) %>%
 distinct(master_deal_no, mgrno, deal_type) %>%
 mutate(holds_target_t0 = TRUE)

# Process acquirer holdings at t=0
acquirer_holdings_t0 <- institutional_holdings_granular %>%
 inner_join(
   deal_pairs %>% select(master_deal_no, permno = acquirer_permno, t_0, deal_type),
   by = c("permno", "rdate" = "t_0")
 ) %>%
 filter(shares_adj > 0) %>%
 distinct(master_deal_no, mgrno, deal_type) %>%
 mutate(holds_acquirer_t0 = TRUE)

# Combine t=0 holdings
holdings_t0 <- full_join(target_holdings_t0, acquirer_holdings_t0,
                       by = c("master_deal_no", "mgrno", "deal_type")) %>%
 mutate(
   holds_target_t0 = coalesce(holds_target_t0, FALSE),
   holds_acquirer_t0 = coalesce(holds_acquirer_t0, FALSE)
 )

# ---- Identify Cross-Deal Arbitrage Patterns ---------------------------------------------------

cross_holdings <- full_join(holdings_tm1, holdings_t0,
                           by = c("master_deal_no", "mgrno", "deal_type")) %>%
 mutate(
   # Fill NAs (no holding = FALSE)
   holds_target_tm1 = coalesce(holds_target_tm1, FALSE),
   holds_acquirer_tm1 = coalesce(holds_acquirer_tm1, FALSE),
   holds_target_t0 = coalesce(holds_target_t0, FALSE),
   holds_acquirer_t0 = coalesce(holds_acquirer_t0, FALSE),

   # Classify patterns at t=-1
   target_only_tm1 = holds_target_tm1 & !holds_acquirer_tm1,
   acquirer_only_tm1 = !holds_target_tm1 & holds_acquirer_tm1,
   both_tm1 = holds_target_tm1 & holds_acquirer_tm1,
   neither_tm1 = !holds_target_tm1 & !holds_acquirer_tm1,

   # Cross-deal arbitrage indicators
   # 1. Target holder adds acquirer position
   target_adds_acquirer = target_only_tm1 & holds_acquirer_t0,

   # 2. Acquirer holder adds target position
   acquirer_adds_target = acquirer_only_tm1 & holds_target_t0,

   # 3. Either cross-holding established
   cross_arb = target_adds_acquirer | acquirer_adds_target
 )

cat(sprintf("Cross-holdings panel: %d manager-deal pairs\n", nrow(cross_holdings)))

# ---- Aggregate Statistics ----------------------------------------------------------------------

cat("\n\nCROSS-HOLDING PATTERNS:\n")
cat("=" %>% rep(60) %>% paste(collapse = ""), "\n\n")

cross_stats <- cross_holdings %>%
 group_by(deal_type) %>%
 summarise(
   n_mgr_deal_pairs = n(),

   # Baseline patterns at t=-1
   n_target_only = sum(target_only_tm1),
   n_acquirer_only = sum(acquirer_only_tm1),
   n_both = sum(both_tm1),
   n_neither = sum(neither_tm1),

   # Cross-arbitrage activity
   n_target_adds_acq = sum(target_adds_acquirer, na.rm = TRUE),
   n_acq_adds_target = sum(acquirer_adds_target, na.rm = TRUE),
   n_cross_arb = sum(cross_arb, na.rm = TRUE),

   # Rates (conditional on being single-side holder)
   rate_target_adds_acq = n_target_adds_acq / n_target_only * 100,
   rate_acq_adds_target = n_acq_adds_target / n_acquirer_only * 100,

   .groups = "drop"
 )

print(cross_stats)

#=================================================================================================
# Step 21: CASE STUDIES TABLE: Kahan & Rock (Top-20 overlap + gross flows)
# Inputs: target_panel, institutional_holdings_granular
# Notes:
#   - Overlap metrics (Jaccard, Cosine) are computed on Top-20 holders.
#   - Cosine uses pctTSO vectors over the union of Top-20 identities (as table note states).
#   - Entrants = sum pctTSO at end among mgrno absent at start.
#   - Leavers  = sum pctTSO at start among mgrno absent at end.
#=================================================================================================

library(dplyr)
library(tidyr)
library(purrr)
library(knitr)
library(kableExtra)

# ---------- helpers (overlap, not replacement) --------------------------------------------------
jaccard_overlap <- function(ids_a, ids_b) {
  ids_a <- unique(ids_a); ids_b <- unique(ids_b)
  if (length(ids_a) == 0 || length(ids_b) == 0) return(NA_real_)
  inter <- length(intersect(ids_a, ids_b))
  uni   <- length(union(ids_a, ids_b))
  if (uni == 0) NA_real_ else inter / uni
}

cosine_overlap_pct <- function(df_a, df_b, id_col = "mgrno", w_col = "pct_tso") {
  # cosine similarity of pctTSO vectors over union of ids
  ids <- union(df_a[[id_col]], df_b[[id_col]])
  if (length(ids) == 0) return(NA_real_)

  a <- df_a %>% select(id = all_of(id_col), w = all_of(w_col)) %>% distinct(id, .keep_all = TRUE)
  b <- df_b %>% select(id = all_of(id_col), w = all_of(w_col)) %>% distinct(id, .keep_all = TRUE)

  u <- tibble(id = ids) %>%
    left_join(a, by = "id") %>% rename(w_a = w) %>%
    left_join(b, by = "id") %>% rename(w_b = w) %>%
    mutate(
      w_a = replace_na(w_a, 0),
      w_b = replace_na(w_b, 0)
    )

  num <- sum(u$w_a * u$w_b)
  den <- sqrt(sum(u$w_a^2)) * sqrt(sum(u$w_b^2))
  if (den == 0) NA_real_ else num / den
}

gross_flows_pct <- function(df_a, df_b, id_col = "mgrno", w_col = "pct_tso") {
  # entrants: at end, absent at start (end weights)
  # leavers:  at start, absent at end (start weights)
  a <- df_a %>% select(id = all_of(id_col), w0 = all_of(w_col)) %>% distinct(id, .keep_all = TRUE)
  b <- df_b %>% select(id = all_of(id_col), w1 = all_of(w_col)) %>% distinct(id, .keep_all = TRUE)

  ids_a <- a$id; ids_b <- b$id
  entrants_ids <- setdiff(ids_b, ids_a)
  leavers_ids  <- setdiff(ids_a, ids_b)

  entrants <- b %>% filter(id %in% entrants_ids) %>% summarise(x = sum(w1, na.rm = TRUE)) %>% pull(x)
  leavers  <- a %>% filter(id %in% leavers_ids)  %>% summarise(x = sum(w0, na.rm = TRUE)) %>% pull(x)

  tibble(Entrants = entrants, Leavers = leavers)
}

# ---------- deal list ---------------------------------------------------------------------------
case_deals <- tibble::tribble(
  ~master_deal_no, ~deal_label,
  "2974484020", "Tesla--SolarCity (SCTY)",
  "4147606020", "Chevron--Hess (HES)"
)

# ---------- windows for panels ------------------------------------------------------------------
panels <- tibble::tribble(
  ~panel, ~t0, ~t1,
  "Panel A. $t=-1 \\rightarrow t=0$", -1L, 0L,
  "Panel B. $t=0 \\rightarrow t=1$",  0L, 1L,
  "Panel C. $t=-1 \\rightarrow t=1$", -1L, 1L
)

K <- 20L

# ---------- prepare quarter grid + join holdings ------------------------------------------------
# Target-side only; we need rdate & TSO for pctTSO
quarters_needed <- target_panel %>%
  filter(firm_type == "target", event_time %in% c(-1, 0, 1)) %>%
  transmute(
    master_deal_no = as.character(master_deal_no),
    target_permno  = as.integer(target_permno),
    event_time     = as.integer(event_time),
    rdate,
    TSO
  ) %>%
  semi_join(case_deals, by = "master_deal_no") %>%
  distinct()

# Deal-quarter-institution holdings with pctTSO
holdings_cs <- institutional_holdings_granular %>%
  inner_join(quarters_needed, by = c("permno" = "target_permno", "rdate")) %>%
  filter(shares_adj > 0) %>%
  transmute(
    master_deal_no,
    permno,
    event_time,
    mgrno,
    shares   = shares_adj,
    TSO,
    pct_tso  = 100 * shares_adj / TSO
  )

# ---------- function: compute metrics for one deal and one (t0,t1) -------------------------------
compute_case_metrics <- function(deal_id, t0, t1, k = 20L) {
  # top-k by shares at each endpoint
  top0 <- holdings_cs %>%
    filter(master_deal_no == deal_id, event_time == t0) %>%
    group_by(master_deal_no, permno, event_time) %>%
    arrange(desc(shares)) %>%
    slice_head(n = k) %>%
    ungroup()

  top1 <- holdings_cs %>%
    filter(master_deal_no == deal_id, event_time == t1) %>%
    group_by(master_deal_no, permno, event_time) %>%
    arrange(desc(shares)) %>%
    slice_head(n = k) %>%
    ungroup()

  if (nrow(top0) == 0 || nrow(top1) == 0) {
    return(tibble(Jaccard = NA_real_, Cosine = NA_real_, Entrants = NA_real_, Leavers = NA_real_))
  }

  jac <- jaccard_overlap(top0$mgrno, top1$mgrno)
  cos <- cosine_overlap_pct(top0, top1, id_col = "mgrno", w_col = "pct_tso")
  flw <- gross_flows_pct(top0, top1, id_col = "mgrno", w_col = "pct_tso")

  tibble(Jaccard = jac, Cosine = cos) %>% bind_cols(flw)
}

# ---------- build table frame -------------------------------------------------------------------
case_studies <- crossing(case_deals, panels) %>%
  mutate(metrics = pmap(list(master_deal_no, t0, t1), ~compute_case_metrics(..1, ..2, ..3, k = K))) %>%
  unnest(metrics) %>%
  mutate(
    Jaccard = round(Jaccard, 3),
    Cosine  = round(Cosine, 3),
    Entrants = round(Entrants, 2),
    Leavers  = round(Leavers, 2)
  ) %>%
  select(panel, deal_label, Jaccard, Cosine, Entrants, Leavers)

print(case_studies, n = Inf)

#=================================================================================================
# STEP 21: Combined Output
#=================================================================================================

attrition <- target_panel %>%
 group_by(event_time) %>%
 summarise(
   `N Deals` = n_distinct(master_deal_no),
   `Mean IOR (%)` = mean(IOR * 100, na.rm = TRUE),
   `N Missing` = sum(is.na(IOR)),
   .groups = "drop"
 ) %>%
 arrange(event_time) %>%
 filter(event_time >= -4, event_time <= 8)

print_table(
 attrition,
 "TABLE 7: Sample Composition by Event Time",
 "Number of deals with observed institutional ownership at each quarter relative to announcement. Sample attrition occurs as deals complete (median = 5 months) and post-merger integration eliminates target CUSIPs."
)

print(balance_table, digits = 2)
print(p_overlap)

modelsummary(
 list("(1) Post" = es1, "(2) Binned" = es2, "(3) + Trends" = es3, "(4) Linear" = es4),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c(
   "post" = "Post-announcement",
   "pre_43" = "Pre: t=-4 to -3",
   "pre_2" = "Pre: t=-2",
   "post_0" = "Post: t=0",
   "post_12" = "Post: t=1-2",
   "post_34" = "Post: t=3-4",
   "et" = "Trend (per qtr)",
   "et:post" = "Post × Trend (break)"
 ),
 gof_map = c("nobs", "r.squared", "adj.r.squared"),
 notes = c(
   "Dependent variable: Institutional ownership ratio (%), 0-100 scale.",
   "Reference period: t=-1 (quarter before announcement).",
   "Cols (1)-(3): Firm and quarter FE. Col (3): + Firm×year trends. Col (4): Quarter FE only.",
   "Standard errors clustered at firm level."
 ),
 output = "markdown"
)

summary(test_model)

modelsummary(
 list("(1) All Deals" = all_deals,
      "(2) Cash ≥75%" = cash75,
      "(3) Cash ≥90%" = cash90),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c(
   "pre_43" = "Pre: t=-4 to -3",
   "pre_2" = "Pre: t=-2",
   "post_0" = "Post: t=0",
   "post_12" = "Post: t=1-2",
   "post_34" = "Post: t=3-4"
 ),
 gof_map = c("nobs", "r.squared"),
 notes = c(
   "Dependent variable: Institutional ownership ratio (%), 0-100 scale.",
   "Reference period: t=-1. Firm and quarter FE. SEs clustered at firm level.",
   "Column (1): All deals. Columns (2)-(3): Cash deals only (≥75% or ≥90% cash consideration)."
 ),
 output = "markdown"
)

summary(test_cash)

modelsummary(
 list("(1) All Deals" = all_deals, "(2) Stock ≥75%" = stock75, "(3) Stock ≥90%" = stock90),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c("pre_43" = "Pre: t=-4 to -3", "pre_2" = "Pre: t=-2",
                 "post_0" = "Post: t=0", "post_12" = "Post: t=1-2", "post_34" = "Post: t=3-4"),
 gof_map = c("nobs", "r.squared"),
 output = "markdown"
)

modelsummary(
 list(
   "(1A) Level" = tgt_1a,
   "(1B) Trend\nBreak" = tgt_1b,
   "(1C) Post×Trend\n(with FE)" = tgt_1c,
   "(1D) Saturated" = tgt_1d
 ),
 stars = c('***' = 0.01, '**' = 0.05, '*' = 0.1),
 coef_rename = c(
   "post" = "Post (level shift)",
   "et" = "Trend (per qtr)",
   "et:post" = "Post × Trend (break)",
   "t_m4" = "t = -4",
   "t_m3" = "t = -3",
   "t_m2" = "t = -2",
   "t_0" = "t = 0",
   "t_1" = "t = 1",
   "t_2" = "t = 2",
   "t_3" = "t = 3",
   "t_4" = "t = 4"
 ),
 gof_map = c("nobs", "r.squared"),
 output = "markdown"
)

cat("Overall Statistics:\n")
cat(sprintf("  Mean exit flow:       %.2f%% of TSO\n", mean(deals_panel$exit_pct)))
cat(sprintf("  Mean entry flow:      %.2f%% of TSO\n", mean(deals_panel$entry_pct)))
cat(sprintf("  Mean stayer change:   %.2f%% of TSO\n", mean(deals_panel$stayer_change_pct)))
cat(sprintf("  Net change (decomp):  %.2f pp\n", mean(deals_panel$net_change_pct)))
cat(sprintf("  Actual IOR change:    %.2f pp (verification)\n\n", mean(deals_panel$actual_IOR_change)))

cat("Statistical Tests:\n")
cat(sprintf("  Exit > 0:  t=%.2f, p=%.4f %s\n", t_exit$statistic, t_exit$p.value,
           ifelse(t_exit$p.value < 0.01, "***", "")))
cat(sprintf("  Entry > 0: t=%.2f, p=%.4f %s\n", t_entry$statistic, t_entry$p.value,
           ifelse(t_entry$p.value < 0.01, "***", "")))
cat(sprintf("  Net ≠ 0:   t=%.2f, p=%.4f %s\n\n", t_net$statistic, t_net$p.value,
           ifelse(t_net$p.value < 0.01, "***", "")))

print(by_type, width = Inf)

print(combined, digits = 2)

print(did_test, digits = 2)

print(entry_dist, digits = 2)

cat(sprintf("Top 10%% of entrants account for:\n"))
cat(sprintf("  %.1f%% of entry events\n", top_10$cum_entries * 100))
cat(sprintf("  %.1f%% of entry volume (shares)\n\n", top_10$cum_volume * 100))

cat(sprintf("Top 5%% of entrants account for:\n"))
cat(sprintf("  %.1f%% of entry events\n", top_5$cum_entries * 100))
cat(sprintf("  %.1f%% of entry volume (shares)\n\n", top_5$cum_volume * 100))

cat(sprintf("Top 1%% of entrants account for:\n"))
cat(sprintf("  %.1f%% of entry events\n", top_1$cum_entries * 100))
cat(sprintf("  %.1f%% of entry volume (shares)\n\n", top_1$cum_volume * 100))

print(final_comp, digits = 2)

cat("\n\nCONCENTRATION (GINI COEFFICIENTS):\n")
cat(strrep("=", 60), "\n")
cat(sprintf("Entry events (# deals):  %.3f\n", gini_entries))
cat(sprintf("Entry volume (shares):   %.3f\n\n", gini_volume))

print(weekly_flows_cash, digits = 1, width = Inf)

print(coarse_decomp, digits = 1, width = Inf)

print(cash_coarse, digits = 1, width = Inf)

cat("Middle vs Early:\n")
cat(sprintf("  Exit difference: %.1f pp (p=%.3f) %s\n",
           mean(middle_cash$exit_pct) - mean(early_cash$exit_pct),
           t_exit$p.value,
           ifelse(t_exit$p.value < 0.05, "**", "")))
cat(sprintf("  Net difference:  %.1f pp (p=%.3f) %s\n\n",
           mean(middle_cash$net_change_pct) - mean(early_cash$net_change_pct),
           t_net$p.value,
           ifelse(t_net$p.value < 0.05, "**", "")))

# Test if late differs from middle
t_exit_late <- t.test(late_cash$exit_pct, middle_cash$exit_pct)
t_net_late <- t.test(late_cash$net_change_pct, middle_cash$net_change_pct)

cat("Late vs Middle:\n")
cat(sprintf("  Exit difference: %.1f pp (p=%.3f) %s\n",
           mean(late_cash$exit_pct) - mean(middle_cash$exit_pct),
           t_exit_late$p.value,
           ifelse(t_exit_late$p.value < 0.05, "**", "")))
cat(sprintf("  Net difference:  %.1f pp (p=%.3f) %s\n\n",
           mean(late_cash$net_change_pct) - mean(middle_cash$net_change_pct),
           t_net_late$p.value,
           ifelse(t_net_late$p.value < 0.05, "**", "")))

cat(sprintf("Weekly data: %d week-buckets\n", nrow(weekly_agg)))
cat(sprintf("  Total deals: %d\n", sum(weekly_agg$n_deals)))
cat(sprintf("  Mean deals/week: %.1f\n\n", mean(weekly_agg$n_deals)))

cat("\n\nEXIT MODEL COMPARISON:\n")
print(exit_models$comparison, digits = 2)

cat("\n\nENTRY MODEL COMPARISON:\n")
print(entry_models$comparison, digits = 2)

cat("\n\nNET MODEL COMPARISON:\n")
print(net_models$comparison, digits = 2)

print(boot_df, digits = 3)

pct_nonlinear <- 100 * (1 - boot_df$Share[boot_df$Model == "linear"])
cat(sprintf("\nNon-linear selected: %.1f%% of draws\n\n", pct_nonlinear))

print(predictions_df, digits = 2)

print(p_exit)

print(p_components)

print(summary_table, digits = 2)

cat("Key findings:\n")
cat(sprintf("  • Exit plateaus at %.1f%% (R²=%.2f)\n",
           mean(predictions_df$Exit_Est), exit_models$comparison$R2[1]))
cat(sprintf("  • Entry rises from %.1f%% to %.1f%%\n",
           predictions_df$Entry_Est[1], predictions_df$Entry_Est[5]))
cat(sprintf("  • Peak net decline at week 6: %.1f pp\n",
           predictions_df$Net_Est[predictions_df$Days == 45]))
cat(sprintf("  • Bootstrap validates non-linearity: %.0f%%\n\n", pct_nonlinear))


cat(sprintf("Stayer observations: %s\n", format(nrow(stayers), big.mark = ",")))
cat(sprintf("  Increased: %d (%.1f%%)\n", sum(stayers$shares_change > 0),
           100 * mean(stayers$shares_change > 0)))
cat(sprintf("  Decreased: %d (%.1f%%)\n", sum(stayers$shares_change < 0),
           100 * mean(stayers$shares_change < 0)))

print(stayer_weekly, digits = 1)

print(stayer_types, digits = 1)

print(comparison, digits = 1)

print(summary(size_pred_model)$coefficients, digits = 3)

cat("\nInsider Ownership:\n")
cat(sprintf("  Deals: %d\n", nrow(insider_ownership_clean)))
cat(sprintf("  Mean: %.1f%%\n", mean(insider_ownership_clean$insider_pct)))
cat(sprintf("  Median: %.1f%%\n", median(insider_ownership_clean$insider_pct)))
cat(sprintf("  P25-P75: %.1f%% - %.1f%%\n",
           quantile(insider_ownership_clean$insider_pct, 0.25),
           quantile(insider_ownership_clean$insider_pct, 0.75)))

cat(sprintf("Sample: %d deals\n\n", nrow(deals_for_risk)))

print(transformation_table, digits = 3)

cat("\n\nCash Deal Changes (Just Before → Just After):\n")
cat(sprintf("  Market correlation: %.3f (t=%.2f, p=%.4f)\n",
           mean(cash_change$delta_mkt, na.rm = TRUE),
           t.test(cash_change$delta_mkt)$statistic,
           t.test(cash_change$delta_mkt)$p.value))
cat(sprintf("  Acquirer correlation: %.3f (t=%.2f, p=%.4f)\n\n",
           mean(cash_change$delta_acq, na.rm = TRUE),
           t.test(cash_change$delta_acq)$statistic,
           t.test(cash_change$delta_acq)$p.value))

print(cross_stats, width = Inf)
