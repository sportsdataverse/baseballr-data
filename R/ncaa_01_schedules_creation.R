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
  make_option(c("-e", "--end_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(), type = 'integer', help = "End year of the seasons to process"),
  make_option(c("-r", "--rescrape"), action="store", default=FALSE, type='logical', help="Rescrape the raw JSON files from web api")
  
)
opt = parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e
rescrape <- opt$r
# y <- 2023
# ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
#   dplyr::filter(.data$year %in% years_vec) %>%
#   dplyr::slice(533:540)

ncaa_baseball_schedules_scrape <- function(y){
  cli::cli_process_start("Starting NCAA Baseball schedule parse for {y}!")
  ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
    dplyr::filter(.data$year == y)
  
  ifelse(!dir.exists(file.path("ncaa/team_schedules")), dir.create(file.path("ncaa/team_schedules")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/csv")), dir.create(file.path("ncaa/team_schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/json")), dir.create(file.path("ncaa/team_schedules/json")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/parquet")), dir.create(file.path("ncaa/team_schedules/parquet")), FALSE)
  if (rescrape == TRUE) {
  progressr::with_progress({
    p <- progressr::progressor(along = ncaa_teams_lookup$team_id)
    ncaa_teams_schedule <- purrr::map(ncaa_teams_lookup$team_id, function(x){
      df <- baseballr::ncaa_schedule_info(teamid = x, year = y)
      readr::write_csv(df, glue::glue("ncaa/team_schedules/csv/{y}_{x}.csv"))
      jsonlite::write_json(df,glue::glue("ncaa/team_schedules/json/{y}_{x}.json"), pretty = 2)
      arrow::write_parquet(df, glue::glue("ncaa/team_schedules/parquet/{y}_{x}.parquet"))
      p(sprintf("x=%s", as.integer(x)))
      Sys.sleep(1)
      return(df)
    }) %>%
      baseballr:::rbindlist_with_attrs()
  }, enable = TRUE)
  }
  
  team_schedules_files <- list.files("ncaa/team_schedules/csv/")
  team_schedules_files_year <- stringr::str_extract(team_schedules_files, glue::glue("{y}_\\d+.csv"))
  team_schedules_files_year <- team_schedules_files_year[!is.na(team_schedules_files_year)]
  ncaa_teams_schedule <- purrr::map(team_schedules_files_year, function(x){
    df <- data.table::fread(glue::glue("ncaa/team_schedules/csv/{x}"))
    return(df)
  }) %>%
    baseballr:::rbindlist_with_attrs()
  ifelse(!dir.exists(file.path("ncaa/schedules")), dir.create(file.path("ncaa/schedules")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/csv")), dir.create(file.path("ncaa/schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/rds")), dir.create(file.path("ncaa/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/parquet")), dir.create(file.path("ncaa/schedules/parquet")), FALSE)
  final_sched <- dplyr::distinct(ncaa_teams_schedule) %>%
    dplyr::arrange(.data$date)
  final_sched <- final_sched %>%
    baseballr:::make_baseballr_data("NCAA Schedule Information from baseballr data repository", Sys.time())
  readr::write_csv(final_sched, glue::glue("ncaa/schedules/csv/ncaa_baseball_schedule_{y}.csv"))
  saveRDS(final_sched, glue::glue("ncaa/schedules/rds/ncaa_baseball_schedule_{y}.rds"))
  arrow::write_parquet(final_sched, glue::glue("ncaa/schedules/parquet/ncaa_baseball_schedule_{y}.parquet"))
  
  cli::cli_process_done(msg_done = "Finished NCAA Baseball schedule parse for {y}!")
}

all_games <- purrr::map(years_vec, function(y){
  ncaa_baseball_schedules_scrape(y)
})
