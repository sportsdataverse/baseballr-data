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

# this should give you the same IP as step 1 (ignore the "\n")
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
# years_vec <- 2022
# rescrape <- FALSE
# y <- 2022
# ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
#   dplyr::filter(.data$year %in% years_vec) %>%
#   dplyr::slice(533:540)
# a very common library for webscraping
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
ncaa_baseball_pbp_scrape <- function(y) {
  cli::cli_process_start("Starting NCAA Baseball pbp parse for {y}! (Rescrape: {tolower(rescrape)})")
  sched <- data.table::fread(paste0("ncaa/schedules/csv/ncaa_baseball_schedule_", y, ".csv"))
  ifelse(!dir.exists(file.path("ncaa/game_pbp")), dir.create(file.path("ncaa/game_pbp")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/game_pbp/parquet")), dir.create(file.path("ncaa/game_pbp/parquet")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/game_pbp/rds")), dir.create(file.path("ncaa/game_pbp/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/game_pbp/json")), dir.create(file.path("ncaa/game_pbp/json")), FALSE)
  pbp_dir <- as.integer(stringr::str_extract(list.files("ncaa/game_pbp/parquet/"), "\\d+"))
  pbp_links <- sched %>%
    dplyr::filter(!is.na(.data$game_info_url), stringr::str_detect(.data$game_pbp_url, "play_by_play")) %>%
    dplyr::select("game_info_url", "game_pbp_url") %>%
    dplyr::mutate(
      game_pbp_id = as.integer(stringr::str_extract(.data$game_pbp_url, "\\d+"))) %>%
    dplyr::distinct()

  pbp_dir <- data.frame(game_pbp_id = pbp_dir)
  if (rescrape == FALSE) {
    pbp_links <- pbp_links %>%
      dplyr::filter(!(.data$game_pbp_id %in% pbp_dir$game_pbp_id))
  }

  if (nrow(pbp_links) > 0) {
    future::plan("multisession")
    pbp_g <- furrr::future_map(pbp_links$game_pbp_url, function(x) {
      df <- data.frame()
      tryCatch(
        expr = {

          proxy <- select_proxy(proxies)
          df <- baseballr::ncaa_pbp(
            game_pbp_url = x,
            proxy = proxy
          )
          game_pbp_id <- as.integer(stringr::str_extract(x, "\\d+"))
          arrow::write_parquet(df, glue::glue("ncaa/game_pbp/parquet/{game_pbp_id}.parquet"))
          saveRDS(df, glue::glue("ncaa/game_pbp/rds/{game_pbp_id}.rds"))
          jsonlite::write_json(df, glue::glue("ncaa/game_pbp/json/{game_pbp_id}.json"), pretty = 2)
        },
        error = function(e) {
          message(glue::glue("{Sys.time()}: Invalid arguments provided for game_pbp_url: {x}, proxy: {proxy}"))
        },
        finally = {
        }
      )
      return(df)
    }) %>%
      baseballr:::rbindlist_with_attrs()
  }

  pbp_games_dir <- as.integer(stringr::str_extract(list.files("ncaa/game_pbp/parquet/"), "\\d+"))
  pbp_links <- sched %>%
    dplyr::filter(!is.na(.data$game_info_url)) %>%
    dplyr::select("game_info_url", "game_pbp_url") %>%
    dplyr::mutate(
      game_pbp_id = as.integer(stringr::str_extract(.data$game_pbp_url, "\\d+"))) %>%
    dplyr::distinct()

  pbp_games_dir <- data.frame(game_pbp_id = pbp_games_dir)

  game_pbp_files_year <- pbp_links %>%
    dplyr::filter((.data$game_pbp_id %in% pbp_games_dir$game_pbp_id)) %>%
    dplyr::select("game_pbp_id")

  game_pbp_files_year <- game_pbp_files_year$game_pbp_id[!is.na(game_pbp_files_year$game_pbp_id)]

  future::plan("multisession")
  ncaa_game_pbps <- furrr::future_map(game_pbp_files_year, function(x) {
    df <- arrow::read_parquet(glue::glue("ncaa/game_pbp/parquet/{x}.parquet"))
    return(df)
  }) %>%
    baseballr:::rbindlist_with_attrs()

  ncaa_game_pbps <- ncaa_game_pbps %>%
    dplyr::arrange(desc(.data$game_date))

  ncaa_game_pbps <- ncaa_game_pbps %>%
    baseballr:::make_baseballr_data("NCAA Play-by-Play Information from baseballr data repository", Sys.time())

  sportsdataversedata::sportsdataverse_save(
    data_frame = ncaa_game_pbps,
    file_name =  glue::glue("ncaa_baseball_pbp_{y}"),
    sportsdataverse_type = "play by play data",
    release_tag = "ncaa_baseball_pbp",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
  rm(ncaa_game_pbps)
  rm(pbp_g)
  rm(pbp_dir)
  rm(pbp_games_dir)
  rm(pbp_links)
  rm(sched)
  rm(game_pbp_files_year)
  empty <- gc()
  cli::cli_process_done(msg_done = "Finished NCAA Baseball pbp parse for {y}! (Rescrape: {tolower(rescrape)})")
}

all_games <- purrr::map(years_vec, function(y) {
  ncaa_baseball_pbp_scrape(y)
})
