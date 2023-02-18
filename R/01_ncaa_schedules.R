lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman', lib = Sys.getenv("R_LIBS"), repo = 'http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc = lib_path)))

option_list = list(
  make_option(c("-s", "--start_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(), type = 'integer', help = "Start year of the seasons to process"),
  make_option(c("-e", "--end_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(), type = 'integer', help = "End year of the seasons to process")
)
opt = parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e
years_vec <- 2018:2015
# y <- 2018
# ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
#   dplyr::filter(.data$year %in% years_vec) %>%
#   dplyr::slice(588:593)

ncaa_baseball_schedules_scrape <- function(y){
  ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>% 
    dplyr::filter(.data$year == y) 
  progressr::with_progress({
    p <- progressr::progressor(along = ncaa_teams_lookup$team_id)
    ncaa_teams_schedule <- purrr::map(ncaa_teams_lookup$team_id, function(x){
      df <- baseballr::ncaa_schedule_info(teamid = x, year = y)
      p(sprintf("x=%s", as.integer(x)))
      Sys.sleep(4)
      return(df)
    }) %>% 
      baseballr:::rbindlist_with_attrs()
  })
  
  ifelse(!dir.exists(file.path("ncaa/schedules")), dir.create(file.path("ncaa/schedules")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/csv")), dir.create(file.path("ncaa/schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/rds")), dir.create(file.path("ncaa/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/parquet")), dir.create(file.path("ncaa/schedules/parquet")), FALSE)
  final_sched <- dplyr::distinct(ncaa_teams_schedule) %>% dplyr::arrange(desc(.data$date))
  final_sched <- final_sched %>%
    baseballr:::make_baseballr_data("NCAA Schedule Information from baseballr data repository", Sys.time())
  readr::write_csv(ncaa_teams_schedule, glue::glue("ncaa/schedules/csv/ncaa_baseball_schedule_{y}.csv"))
  saveRDS(ncaa_teams_schedule, glue::glue("ncaa/schedules/rds/ncaa_baseball_schedule_{y}.rds"))
  arrow::write_parquet(ncaa_teams_schedule, glue::glue("ncaa/schedules/parquet/ncaa_baseball_schedule_{y}.parquet"))
}

all_games <- purrr::map(years_vec, function(y){
  ncaa_baseball_schedules_scrape(y)
})
