
library(dplyr)
library(purrr)
library(baseballr)
## Update for following season by checking stats.ncaa.org/team/pages
## Can probably just be updated in the csv manually
ncaa_season_id_lu <- readr::read_csv("ncaa/seasons_info/ncaa_season_id_lu.csv", show_col_types = FALSE)

ncaa_season_id_lu <- ncaa_season_id_lu %>% 
  baseballr:::make_baseballr_data("NCAA Baseball Season IDs from baseballr data repository", Sys.time())

readr::write_csv(ncaa_season_id_lu, "ncaa/seasons_info/ncaa_season_id_lu.csv")
saveRDS(ncaa_season_id_lu, "ncaa/seasons_info/ncaa_season_id_lu.rds")
arrow::write_parquet(ncaa_season_id_lu, "ncaa/seasons_info/ncaa_season_id_lu.parquet")