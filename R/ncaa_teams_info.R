library(dplyr)
library(purrr)
library(baseballr)

# Years/divisions to (re)scrape. Override `years` to backfill more than the
# current season, e.g. Sys.setenv(NCAA_TEAMS_YEARS = "2025,2026").
years <- {
  env <- Sys.getenv("NCAA_TEAMS_YEARS", "")
  if (nzchar(env)) as.integer(strsplit(env, ",")[[1]]) else
    baseballr::most_recent_ncaa_baseball_season()
}

ncaa_team_lu <- readr::read_csv("ncaa/teams_info/ncaa_team_lookup.csv", show_col_types = FALSE)

years_vec <- expand.grid(year = years, division = 1:3)

ncaa_teams <- purrr::map2(years_vec$year, years_vec$division, function(x, y){
  df <- data.frame()
  df <- baseballr::ncaa_teams(x, y)
  return(df)
}) %>%
  baseballr:::rbindlist_with_attrs()

ncaa_teams <- ncaa_teams %>%
  dplyr::mutate(
    team_id        = as.integer(.data$team_id),
    conference_id  = as.integer(.data$conference_id),
    season_id      = as.integer(.data$season_id),
    season_team_id = as.integer(.data$season_team_id)
  )

# stats.ncaa.org now serves /teams/{season_team_id} links, so freshly scraped
# rows arrive with season_team_id populated but the stable franchise team_id and
# the season_id NA. Backfill both from sources that don't require scraping the
# Akamai-gated season-team pages:
#   * team_id   -- carried forward by name+division from the most recent season
#                  in the existing lookup (franchise ids are stable year to year).
#   * season_id -- looked up from the season-id table by year.
franchise_lu <- ncaa_team_lu %>%
  dplyr::filter(!is.na(.data$team_id)) %>%
  dplyr::arrange(dplyr::desc(.data$year)) %>%
  dplyr::distinct(.data$team_name, .data$division, .keep_all = TRUE) %>%
  dplyr::select("team_name", "division", franchise_team_id = "team_id")

season_lu <- baseballr::load_ncaa_baseball_season_ids() %>%
  dplyr::transmute(year = as.integer(.data$season), lu_season_id = as.integer(.data$id))

ncaa_teams <- ncaa_teams %>%
  dplyr::left_join(franchise_lu, by = c("team_name", "division")) %>%
  dplyr::left_join(season_lu, by = "year") %>%
  dplyr::mutate(
    team_id   = dplyr::coalesce(.data$team_id, .data$franchise_team_id),
    season_id = dplyr::coalesce(.data$season_id, .data$lu_season_id)
  ) %>%
  dplyr::select(-"franchise_team_id", -"lu_season_id")

ncaa_team_lookup <- ncaa_team_lu %>%
  dplyr::bind_rows(ncaa_teams) %>%
  # de-duplicate when re-scraping an existing season; prefer the freshly scraped
  # row (which carries season_team_id) by keeping the last occurrence.
  dplyr::group_by(.data$team_name, .data$division, .data$year) %>%
  dplyr::slice_tail(n = 1) %>%
  dplyr::ungroup() %>%
  dplyr::arrange(.data$division, .data$team_name, -.data$year) %>%
  baseballr:::make_baseballr_data("NCAA Baseball Teams Information from baseballr data repository", Sys.time())

readr::write_csv(ncaa_team_lookup, "ncaa/teams_info/ncaa_team_lookup.csv")
saveRDS(ncaa_team_lookup, "ncaa/teams_info/ncaa_team_lookup.rds")
arrow::write_parquet(ncaa_team_lookup, "ncaa/teams_info/ncaa_team_lookup.parquet")
