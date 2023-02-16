
## Update for following season by checking stats.ncaa.org/team/pages
## Can probably just be updated in the csv manually
ncaa_season_id_lu <- readr::read_csv("ncaa/ncaa_season_id_lu.csv")

ncaa_season_id_lu <- ncaa_season_id_lu %>% 
  baseballr:::make_baseballr_data("NCAA Baseball Season IDs from baseballr data repository", Sys.time())
  
readr::write_csv(ncaa_season_id_lu, "ncaa/ncaa_season_id_lu.csv")

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
  dplyr::arrange(.data$division, .data$team_name, -.data$year) %>%
  baseballr:::make_baseballr_data("NCAA Baseball Teams Information from baseballr data repository", Sys.time())

readr::write_csv(ncaa_team_lookup, "ncaa/ncaa_team_lookup.csv")