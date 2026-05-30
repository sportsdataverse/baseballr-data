# DIAGNOSTIC (throwaway): probe baseballr::ncaa_schedule_info() output for a
# handful of teams across a control season (2025) vs the in-progress season
# (2026), through the proxy, to find why 2026 errors in bulk. Prints result
# structure (nrow/ncol/names/has-year) and any captured error verbatim.
# Run in CI by .github/workflows/diag_ncaa_baseball.yml (has the PROXY_* secrets).
lib_path <- Sys.getenv("R_LIBS")
if (!nzchar(lib_path)) lib_path <- NULL
suppressPackageStartupMessages({
  library(cli, lib.loc = lib_path)
  library(dplyr, lib.loc = lib_path)
  library(httr, lib.loc = lib_path)
})
source("R/utils.R")

proxies_df <- get_proxy_ips()
cli::cli_alert_info("Proxy pool size: {nrow(proxies_df)} ; columns: {paste(names(proxies_df), collapse=', ')}")

# A few well-known D1 team_ids (Texas, LSU, Vanderbilt, Florida, Arkansas) plus
# the roxygen example team. These are franchise team_ids; ncaa_schedule_info maps
# year -> season_id internally.
teams <- c(736, 365, 736, 235, 8)
teams <- unique(c(736, 235, 365, 8, 30))

probe <- function(team_id, year) {
  out <- tryCatch({
    px <- select_proxy(proxies = proxies_df)
    df <- baseballr::ncaa_schedule_info(team_id = team_id, year = year, proxy = px)
    sprintf("team %s yr %s -> OK nrow=%s ncol=%s has_year=%s names=[%s]",
            team_id, year, nrow(df), ncol(df),
            "year" %in% names(df), paste(utils::head(names(df), 12), collapse=","))
  }, error = function(e) {
    sprintf("team %s yr %s -> ERROR: %s", team_id, year, conditionMessage(e))
  }, warning = function(w) {
    sprintf("team %s yr %s -> WARN: %s", team_id, year, conditionMessage(w))
  })
  cli::cli_alert(out)
  Sys.sleep(3)
}

cli::cli_h2("Control season 2025 (complete)")
for (t in teams) probe(t, 2025)
cli::cli_h2("In-progress season 2026")
for (t in teams) probe(t, 2026)

# Also show what season_id the lookup maps each year to, and confirm the URL form.
sid <- baseballr::load_ncaa_baseball_season_ids()
cli::cli_h2("season_id lookup (2024-2026)")
print(sid[sid$season %in% c(2024, 2025, 2026), ])

# DECISIVE PROBE: fetch the raw schedule page THROUGH the proxy and print the
# HTTP status + a body snippet. "Access Denied" => proxy IPs are blocked by NCAA
# (proxy problem). Real schedule HTML => baseballr's parser is broken on the
# current page structure (baseballr problem).
cli::cli_h2("Raw proxied fetch of schedule pages")
raw_fetch <- function(team_id, year) {
  id <- sid$id[sid$season == year][1]
  url <- paste0("https://stats.ncaa.org/team/", team_id, "/", id)
  px <- select_proxy(proxies = proxies_df)
  out <- tryCatch({
    resp <- httr::GET(
      url,
      httr::use_proxy(url = sub("^http://", "", strsplit(px$url, ":")[[1]][1]),
                      port = as.integer(strsplit(sub("^http://", "", px$url), ":")[[1]][2]),
                      username = px$username, password = px$password),
      httr::user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"),
      httr::timeout(30)
    )
    body <- httr::content(resp, as = "text", encoding = "UTF-8")
    sprintf("team %s yr %s url=%s -> HTTP %s, %s bytes; has<fieldset>=%s has<table>=%s; head: %s",
            team_id, year, url, httr::status_code(resp), nchar(body),
            grepl("fieldset", body, ignore.case = TRUE),
            grepl("<table", body, ignore.case = TRUE),
            gsub("\\s+", " ", substr(body, 1, 240)))
  }, error = function(e) sprintf("team %s yr %s url=%s -> FETCH ERROR: %s", team_id, year, url, conditionMessage(e)))
  cli::cli_alert(out)
  Sys.sleep(3)
}
raw_fetch(736, 2025)
raw_fetch(8, 2026)
raw_fetch(235, 2026)
