# Roster-based gap-fill for season_team_id rows the inst_team_list backfill missed
# (2019 D-I, which returns no team table, plus rows whose team_name drifted from
# current site naming so the name-join missed them).
#
# The ungated roster page /team/{team_id}/roster/{season_id} links to
# /teams/{season_team_id}/season_to_date_stats, so we resolve season_team_id
# per (team_id, season_id) via baseballr:::.ncaa_resolve_season_team_id(). This
# is one request per team-season (~512), so it is slow; results are checkpointed
# to .gapfill_checkpoint.csv after every row and the run resumes from there.
#
# Config (env vars):
#   NCAA_PROXY_CSV   optional proxy CSV (cols ip, port_http, login, password) to
#                    rotate per request; otherwise getOption("baseballr.proxy").
library(dplyr)
library(readr)
library(baseballr)

LU   <- "ncaa/teams_info/ncaa_team_lookup.csv"
CKPT <- ".gapfill_checkpoint.csv"
log  <- function(...) cat(format(Sys.time(), "%H:%M:%S"), "|", ..., "\n")

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
  if (is.null(proxies)) getOption("baseballr.proxy")
  else proxies[[((i - 1) %% length(proxies)) + 1]]
}

lu <- read_csv(LU, show_col_types = FALSE)
targets <- lu |>
  dplyr::filter(is.na(.data$season_team_id),
                !is.na(.data$team_id), !is.na(.data$season_id)) |>
  dplyr::distinct(.data$team_id, .data$season_id)

done <- if (file.exists(CKPT)) read_csv(CKPT, show_col_types = FALSE) else
  data.frame(team_id = integer(), season_id = integer(), season_team_id = integer())
todo <- dplyr::anti_join(targets, done, by = c("team_id", "season_id"))
log(nrow(todo), "to resolve;", nrow(done), "already checkpointed")

for (i in seq_len(nrow(todo))) {
  tid <- todo$team_id[i]; sid <- todo$season_id[i]
  stid <- tryCatch(
    baseballr:::.ncaa_resolve_season_team_id(tid, sid, proxy = pick_proxy(i)),
    error = function(e) NA_character_)
  write_csv(data.frame(team_id = tid, season_id = sid,
                       season_team_id = suppressWarnings(as.integer(stid))),
            CKPT, append = file.exists(CKPT))
  if (i %% 25 == 0 || i == nrow(todo)) log("  ", i, "/", nrow(todo), "done")
}

# merge checkpoint into the lookup, fill only where still missing -------------
resolved <- read_csv(CKPT, show_col_types = FALSE) |>
  dplyr::filter(!is.na(.data$season_team_id)) |>
  dplyr::distinct(.data$team_id, .data$season_id, .keep_all = TRUE)
lu <- lu |>
  dplyr::left_join(resolved |>
    dplyr::select("team_id", "season_id", gf_stid = "season_team_id"),
    by = c("team_id", "season_id")) |>
  dplyr::mutate(season_team_id = dplyr::coalesce(.data$season_team_id, .data$gf_stid)) |>
  dplyr::select(-"gf_stid") |>
  dplyr::arrange(.data$division, .data$team_name, -.data$year) |>
  baseballr:::make_baseballr_data(
    "NCAA Baseball Teams Information from baseballr data repository", Sys.time())

filled <- sum(!is.na(lu$season_team_id))
log("season_team_id populated:", filled, "/", nrow(lu),
    sprintf("(%.1f%%)", 100 * filled / nrow(lu)))
write_csv(lu, LU)
saveRDS(lu, "ncaa/teams_info/ncaa_team_lookup.rds")
arrow::write_parquet(lu, "ncaa/teams_info/ncaa_team_lookup.parquet")
log("DONE")
