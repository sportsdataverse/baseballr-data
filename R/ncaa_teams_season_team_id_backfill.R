# Backfill season_team_id onto the historical NCAA baseball team lookup.
#
# stats.ncaa.org migrated team identity from the legacy /team/{team_id}/{season_id}
# pair to a single /teams/{season_team_id} resource. The team history page
# (/teams/history/MBA/{team_id}) that lists a franchise's seasons is gated behind
# Akamai's bm-verify challenge, but the *ungated* inst_team_list endpoint now
# returns /teams/{season_team_id} links for ANY academic_year -- one request per
# (year, division) yields every team's season_team_id. We join those onto the
# existing lookup by (year, division, team_name); the lookup already carries the
# stable franchise team_id, season_id, and conference.
#
# Config (env vars):
#   NCAA_BACKFILL_YEARS  comma-separated years to backfill (default 2010:2024 --
#                        the rows that currently lack season_team_id).
#   NCAA_PROXY_CSV       optional path to a proxy CSV (cols: ip, port_http, login,
#                        password). When set, proxies are rotated per request;
#                        otherwise getOption("baseballr.proxy") / env proxies apply.
library(dplyr)
library(readr)
library(rvest)
library(baseballr)

LU_PATH <- "ncaa/teams_info/ncaa_team_lookup.csv"
log <- function(...) cat(format(Sys.time(), "%H:%M:%S"), "|", ..., "\n")

years <- {
  env <- Sys.getenv("NCAA_BACKFILL_YEARS", "")
  if (nzchar(env)) as.integer(strsplit(env, ",")[[1]]) else 2010:2024
}

# optional proxy pool for rotation -------------------------------------------
proxies <- NULL
px_csv <- Sys.getenv("NCAA_PROXY_CSV", "")
if (nzchar(px_csv) && file.exists(px_csv)) {
  pc <- read.csv(px_csv, stringsAsFactors = FALSE)
  proxies <- lapply(seq_len(nrow(pc)), function(i) list(
    url = paste0("http://", pc$ip[i], ":", pc$port_http[i]),
    username = pc$login[i], password = pc$password[i]))
  log("loaded", length(proxies), "proxies for rotation")
}
pick_proxy <- function(i) {
  if (is.null(proxies)) {
    getOption("baseballr.proxy")
  } else {
    proxies[[((i - 1) %% length(proxies)) + 1]]
  }
}

# fetch one (year, division): team_name -> season_team_id -------------------
fetch_div <- function(year, division, i) {
  url <- paste0("https://stats.ncaa.org/team/inst_team_list?academic_year=", year,
                "&conf_id=-1&division=", division, "&sport_code=MBA")
  resp <- tryCatch(
    baseballr:::request_with_proxy(url = url, proxy = pick_proxy(i)),
    error = function(e) { log("  ", year, "D", division, "->", conditionMessage(e)); NULL })
  if (is.null(resp)) { log("  ", year, "D", division, "-> request error"); return(NULL) }
  body <- httr2::resp_body_string(resp)
  if (baseballr:::.ncaa_is_interstitial(body)) {
    log("  ", year, "D", division, "-> bot-challenge, skipped"); return(NULL)
  }
  links <- read_html(body) |> html_elements("table") |> html_elements("a")
  href <- html_attr(links, "href"); name <- trimws(html_text(links))
  keep <- grepl("^/teams/\\d+$", href)
  if (!any(keep)) { log("  ", year, "D", division, "-> 0 team links"); return(NULL) }
  out <- data.frame(
    year = year, division = division, team_name = name[keep],
    season_team_id = as.integer(sub("^/teams/", "", href[keep])),
    stringsAsFactors = FALSE) |>
    dplyr::distinct(.data$year, .data$division, .data$team_name, .keep_all = TRUE)
  log("  ", year, "D", division, "->", nrow(out), "teams")
  out
}

grid <- expand.grid(year = years, division = 1:3)
log("backfilling", nrow(grid), "(year, division) cells for years",
    paste(range(years), collapse = "-"))
maps <- vector("list", nrow(grid))
for (i in seq_len(nrow(grid))) {
  maps[[i]] <- fetch_div(grid$year[i], grid$division[i], i)
}
backfill <- dplyr::bind_rows(maps)
log("collected", nrow(backfill), "team-season ids")

# join onto the lookup, fill season_team_id only where currently missing ------
lu <- read_csv(LU_PATH, show_col_types = FALSE)
# ensure the column exists (older lookups predate season_team_id)
if (!"season_team_id" %in% names(lu)) lu$season_team_id <- NA_integer_
lu <- lu |>
  dplyr::left_join(
    backfill |> dplyr::select("year", "division", "team_name",
                              bf_stid = "season_team_id"),
    by = c("year", "division", "team_name")) |>
  dplyr::mutate(
    season_team_id = dplyr::coalesce(as.integer(.data$season_team_id),
                                     .data$bf_stid)) |>
  dplyr::select(-"bf_stid")

# reconcile season_id against the authoritative season-id table by year (fixes
# legacy contamination, e.g. some 2019 rows carrying 2020's id); years not in
# the table (pre-2012) keep their existing value.
season_lu <- load_ncaa_baseball_season_ids() |>
  dplyr::transmute(year = as.integer(.data$season), auth_season_id = as.integer(.data$id))
lu <- lu |>
  dplyr::left_join(season_lu, by = "year") |>
  dplyr::mutate(season_id = dplyr::coalesce(.data$auth_season_id,
                                            as.integer(.data$season_id))) |>
  dplyr::select(-"auth_season_id") |>
  dplyr::arrange(.data$division, .data$team_name, -.data$year) |>
  baseballr:::make_baseballr_data(
    "NCAA Baseball Teams Information from baseballr data repository", Sys.time())

filled <- sum(!is.na(lu$season_team_id))
log("season_team_id populated:", filled, "/", nrow(lu),
    sprintf("(%.1f%%)", 100 * filled / nrow(lu)))

write_csv(lu, LU_PATH)
saveRDS(lu, "ncaa/teams_info/ncaa_team_lookup.rds")
arrow::write_parquet(lu, "ncaa/teams_info/ncaa_team_lookup.parquet")
log("DONE")
