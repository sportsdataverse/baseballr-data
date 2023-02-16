library(dplyr)
library(purrr)
library(baseballr)
## Write Existing List of Season IDs to csv
ncaa_season_id_lu <- baseballr::ncaa_season_id_lu
readr::write_csv(ncaa_season_id_lu, "ncaa/ncaa_season_id_lu.csv")

## Update for following season by checking stats.ncaa.org/team/pages
## 

ncaa_season_id_lu <- readr::read_csv("ncaa/ncaa_season_id_lu.csv")

usethis::use_data(ncaa_season_id_lu, internal = FALSE, overwrite = TRUE)


ncaa_team_lu <- baseballr::ncaa_team_lu

ncaa_team_most_recent <- ncaa_team_lu %>% dplyr::filter(.data$year == 2022)

ncaa_team_most_recent <- ncaa_team_most_recent %>% 
  dplyr::mutate(year = 2023)


ncaa_team_lu <- dplyr::bind_rows(ncaa_team_lu, ncaa_team_most_recent)

ncaa_team_lu <- ncaa_team_lu %>% 
  dplyr::arrange(.data$division, .data$school)

readr::write_csv(ncaa_team_lu, "ncaa/ncaa_team_lu.csv")

usethis::use_data(ncaa_team_lu, internal = FALSE, overwrite = TRUE)

ncaa_team_lu <- readr::read_csv("ncaa/ncaa_team_lu.csv", show_col_types = FALSE)

library(dplyr)
library(purrr)
library(baseballr)

ncaa_team_lu <- readr::read_csv("ncaa/ncaa_team_lookup.csv", show_col_types = FALSE)

years_vec <- expand.grid(year = baseballr::most_recent_ncaa_baseball_season(), division = 1:3)

ncaa_team_lookup <- data.frame()

ncaa_teams <- purrr::map2(years_vec$year, years_vec$division, function(x, y){
  df <- data.frame()
  df <- baseballr::ncaa_teams(x, y)
  return(df)
}) %>% 
  baseballr:::rbindlist_with_attrs() 

ncaa_teams <- ncaa_teams %>% 
  dplyr::mutate(
    team_id = as.integer(.data$team_id),
    conference_id = as.integer(.data$conference_id),
    season_id = as.integer(.data$season_id)
  )

ncaa_team_lookup <- ncaa_team_lu %>% 
  dplyr::bind_rows(ncaa_teams)

ncaa_team_lookup <- ncaa_team_lookup %>% 
  dplyr::arrange(.data$division, .data$team_name, -.data$year)

readr::write_csv(ncaa_team_lookup, "ncaa/ncaa_team_lookup.csv")