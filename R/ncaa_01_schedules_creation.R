lib_path <- Sys.getenv("R_LIBS")
# Fall back to the default library paths when R_LIBS is unset. The external
# scheduler exports R_LIBS to a pre-warmed library, but CI installs deps via
# r-lib/actions setup-r-dependencies into the default .libPaths(); lib.loc =
# NULL (and lib = NULL) make R search/install there instead of a literal "".
if (!nzchar(lib_path)) lib_path <- NULL
if (!requireNamespace("pacman", quietly = TRUE)){
  install.packages("pacman", lib = lib_path, repo = "http://cran.us.r-project.org")
}
suppressPackageStartupMessages(suppressMessages(library(cli, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(rvest, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(httr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(tictoc, lib.loc = lib_path)))

source("R/utils.R")

option_list <- list(
  make_option(c("-s", "--start_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(),
    type = "integer", help = "Start year of the seasons to process"),
  make_option(c("-e", "--end_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(),
    type = "integer", help = "End year of the seasons to process"),
  make_option(c("-r", "--rescrape"), action = "store", default = FALSE,
    type = "logical", help = "Rescrape the raw JSON files from web api")
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e
rescrape <- opt$r
# y <- 2023
# rvest::html_text(xml2::read_html("http://checkip.amazonaws.com/"))
proxies_df <- get_proxy_ips()
# Fail fast if the proxy pool is blocked by NCAA, instead of silently scraping
# nothing for ~30 min and crashing at schedule assembly.
preflight_proxy_check(proxies_df)

ncaa_baseball_schedules_scrape <- function(y) {
  cli::cli_process_start("Starting NCAA Baseball schedule parse for {y}! (Rescrape: {tolower(rescrape)})")
  ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
    dplyr::filter(.data$year == y)

  ifelse(!dir.exists(file.path("ncaa/team_schedules")), dir.create(file.path("ncaa/team_schedules")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/csv")), dir.create(file.path("ncaa/team_schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/json")), dir.create(file.path("ncaa/team_schedules/json")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/parquet")), dir.create(file.path("ncaa/team_schedules/parquet")), FALSE)
  if (rescrape == TRUE) {
    ncaa_teams_schedule <- purrr::map(ncaa_teams_lookup$team_id, function(x) {
      df <- data.frame()
      tryCatch(
        expr = {
          proxy <- select_proxy(proxies = proxies_df)
          df <- baseballr::ncaa_schedule_info(team_id = x, year = y, proxy = proxy)
          data.table::fwrite(df, glue::glue("ncaa/team_schedules/csv/{y}_{x}.csv"))
          jsonlite::write_json(df, glue::glue("ncaa/team_schedules/json/{y}_{x}.json"), pretty = 2)
          arrow::write_parquet(df, glue::glue("ncaa/team_schedules/parquet/{y}_{x}.parquet"))
          Sys.sleep(5)
        },
        error = function(e) {
          message(glue::glue("{Sys.time()}: Invalid arguments provided for team_id: {x}, year: {y}, proxy: {proxy}"))
        },
        finally = {
        }
      )
      return(df)
    }) %>%
      baseballr:::rbindlist_with_attrs()
  }

  team_schedules_files <- list.files("ncaa/team_schedules/csv/")
  team_schedules_files_year <- stringr::str_extract(team_schedules_files, glue::glue("{y}_\\d+.csv"))
  team_schedules_files_year <- team_schedules_files_year[!is.na(team_schedules_files_year)]

  ncaa_teams_schedule <- purrr::map(team_schedules_files_year, function(x) {
    df <- data.table::fread(glue::glue("ncaa/team_schedules/csv/{x}"))
    return(df)
  }) %>%
    baseballr:::rbindlist_with_attrs()
  ifelse(!dir.exists(file.path("ncaa/schedules")), dir.create(file.path("ncaa/schedules")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/csv")), dir.create(file.path("ncaa/schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/rds")), dir.create(file.path("ncaa/schedules/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/schedules/parquet")), dir.create(file.path("ncaa/schedules/parquet")), FALSE)
  ncaa_teams_schedule <- ncaa_teams_schedule %>%
    dplyr::filter(!is.na(.data$year)) %>%
    dplyr::select(-dplyr::any_of(c("Date", "Opponent", "Result", "opponent_slug")))
  final_sched <- dplyr::distinct(ncaa_teams_schedule) %>%
    dplyr::arrange(.data$date)
  final_sched <- final_sched %>%
    baseballr:::make_baseballr_data("NCAA Schedule Information from baseballr data repository", Sys.time())
  data.table::fwrite(final_sched, glue::glue("ncaa/schedules/csv/ncaa_baseball_schedule_{y}.csv"))
  saveRDS(final_sched, glue::glue("ncaa/schedules/rds/ncaa_baseball_schedule_{y}.rds"))
  arrow::write_parquet(final_sched, glue::glue("ncaa/schedules/parquet/ncaa_baseball_schedule_{y}.parquet"))


  sportsdataversedata::sportsdataverse_save(
    data_frame = final_sched,
    file_name =  glue::glue("ncaa_baseball_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "ncaa_baseball_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  rm(ncaa_teams_lookup)
  rm(team_schedules_files)
  rm(team_schedules_files_year)
  rm(final_sched)
  rm(ncaa_teams_schedule)
  empty <- gc()
  cli::cli_process_done(msg_done = "Finished NCAA Baseball schedule parse for {y}! (Rescrape: {tolower(rescrape)})")
}

all_games <- purrr::map(years_vec, function(y) {
  ncaa_baseball_schedules_scrape(y)
})
