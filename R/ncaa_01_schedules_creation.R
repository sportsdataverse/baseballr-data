lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace("pacman", quietly = TRUE)){
  install.packages("pacman", lib = Sys.getenv("R_LIBS"), repo = "http://cran.us.r-project.org")
}
suppressPackageStartupMessages(suppressMessages(library(cli, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(furrr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(future, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(rvest, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(httr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(tictoc, lib.loc = lib_path)))

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
proxies <- data.table::fread("../proxylist.csv")
select_proxy <- function(proxies) {
  proxy <- sample(proxies$ip, 1)          # pick a random proxy from the list above
  proxy_selected <- proxies %>%
    dplyr::filter(.data$ip == proxy)
  my_proxy <- httr::use_proxy(url = proxy_selected$ip,
                              port = proxy_selected$port,
                              username = proxy_selected$login,
                              password = proxy_selected$password)
  return(my_proxy)
}
ncaa_baseball_schedules_scrape <- function(y) {
  cli::cli_process_start("Starting NCAA Baseball schedule parse for {y}! (Rescrape: {tolower(rescrape)})")
  ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
    dplyr::filter(.data$year == y)

  ifelse(!dir.exists(file.path("ncaa/team_schedules")), dir.create(file.path("ncaa/team_schedules")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/csv")), dir.create(file.path("ncaa/team_schedules/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/json")), dir.create(file.path("ncaa/team_schedules/json")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/team_schedules/parquet")), dir.create(file.path("ncaa/team_schedules/parquet")), FALSE)
  if (rescrape == TRUE) {
    future::plan("multisession")
    ncaa_teams_schedule <- furrr::future_map(ncaa_teams_lookup$team_id, function(x) {
      df <- data.frame()
      tryCatch(
        expr = {
          proxy <- select_proxy(proxies)
          df <- baseballr::ncaa_schedule_info(team_id = x, year = y, proxy = proxy)
          data.table::fwrite(df, glue::glue("ncaa/team_schedules/csv/{y}_{x}.csv"))
          jsonlite::write_json(df, glue::glue("ncaa/team_schedules/json/{y}_{x}.json"), pretty = 2)
          arrow::write_parquet(df, glue::glue("ncaa/team_schedules/parquet/{y}_{x}.parquet"))
        },
        error = function(e) {
          message(glue::glue("{Sys.time()}: Invalid arguments provided for team_id: {x}, year: {y}, proxy: {proxy}"))
        },
        finally = {
        }
      )
      return(df)
    },
    .options = furrr::furrr_options(seed = TRUE)) %>%
      baseballr:::rbindlist_with_attrs()
  }

  team_schedules_files <- list.files("ncaa/team_schedules/csv/")
  team_schedules_files_year <- stringr::str_extract(team_schedules_files, glue::glue("{y}_\\d+.csv"))
  team_schedules_files_year <- team_schedules_files_year[!is.na(team_schedules_files_year)]

  future::plan("multisession")
  ncaa_teams_schedule <- furrr::future_map(team_schedules_files_year, function(x) {
    df <- data.table::fread(glue::glue("ncaa/team_schedules/csv/{x}"))
    return(df)
  },
  .options = furrr::furrr_options(seed = TRUE)) %>%
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
