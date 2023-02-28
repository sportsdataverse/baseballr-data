lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman', lib = Sys.getenv("R_LIBS"), repo = 'http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(cli, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc = lib_path)))

library(rvest)                                           # a very common library for webscraping
html_text( read_html('http://checkip.amazonaws.com/') ) 
proxies <- data.table::fread("../../proxylist.csv")
proxy <- sample(proxies$ip, 1)          # pick a random proxy from the list above
proxy_selected <- proxies %>% 
  dplyr::filter(.data$ip == proxy)
my_proxy <- httr::use_proxy(url = proxy_selected$ip,
                port = proxy_selected$port,
                username = proxy_selected$login,
                password = proxy_selected$password)
content <- httr::RETRY("GET", url = 'http://checkip.amazonaws.com/', my_proxy) %>% 
  httr::content(as = "text", encoding = "UTF-8")


payload <- content %>% 
  httr::content(as = "text", encoding = "UTF-8") %>% 
  xml2::read_html() 
Sys.setenv(https_proxy = glue::glue("{proxy_selected$ip}:{proxy_selected$port}"))
Sys.setenv(https_proxy_user = glue::glue("{proxy_selected$login}:{proxy_selected$password}"))
Sys.setenv(http_proxy = glue::glue("{proxy_selected$ip}:{proxy_selected$port}"))
Sys.setenv(http_proxy_user = glue::glue("{proxy_selected$login}:{proxy_selected$password}"))
Sys.getenv(c("http_proxy", "http_proxy_user", "https_proxy", "https_proxy_user"))
library(rvest)                                           # a very common library for webscraping
html_text( read_html('http://checkip.amazonaws.com/') )  # this should give you the same IP as step 1 (ignore the "\n")
option_list = list(
  make_option(c("-s", "--start_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(), type = 'integer', help = "Start year of the seasons to process"),
  make_option(c("-e", "--end_year"), action = "store", default = baseballr:::most_recent_ncaa_baseball_season(), type = 'integer', help = "End year of the seasons to process"),
  make_option(c("-r", "--rescrape"), action = "store", default = FALSE, type = 'logical', help = "Rescrape the raw JSON files from web api")
)
opt = parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e
rescrape <- opt$r
# years_vec <- 2022
# y <- 2022
# rescrape <- FALSE
# ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
#   dplyr::filter(.data$year %in% years_vec) %>%
#   dplyr::slice(533:540)

ncaa_baseball_pbp_scrape <- function(y){
  cli::cli_process_start("Starting NCAA Baseball pbp parse for {y}! (Rescrape: {tolower(rescrape)})")
  sched <- data.table::fread(paste0('ncaa/schedules/csv/ncaa_baseball_schedule_',y,'.csv'))
  ifelse(!dir.exists(file.path("ncaa/html")), dir.create(file.path("ncaa/html")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/html/pbp")), dir.create(file.path("ncaa/html/pbp")), FALSE)
  pbp_dir <- as.integer(stringr::str_extract(list.files("ncaa/html/pbp/"), "\\d+"))
  pbp_links <- sched %>%
    dplyr::filter(!is.na(.data$game_info_url)) %>%
    dplyr::select("game_info_url","game_pbp_url") %>%
    dplyr::mutate(
      game_pbp_id = as.integer(stringr::str_extract(.data$game_pbp_url, "\\d+")))

  if (rescrape == FALSE) {
    pbp_links <- pbp_links %>% 
      dplyr::filter(!(.data$game_pbp_id %in% pbp_dir))
  }

  ifelse(!dir.exists(file.path("ncaa/game_pbp")), dir.create(file.path("ncaa/game_pbp")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/game_pbp/csv")), dir.create(file.path("ncaa/game_pbp/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/game_pbp/rds")), dir.create(file.path("ncaa/game_pbp/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/game_pbp/parquet")), dir.create(file.path("ncaa/game_pbp/parquet")), FALSE)

  
  pbp_g <- purrr::map(pbp_links$game_pbp_url, function(x){
    game_pbp_id <- as.integer(stringr::str_extract(x, "\\d+"))
    df <- baseballr::ncaa_baseball_pbp(game_pbp_url = x, raw_html_to_disk = TRUE, raw_html_path = "ncaa/html/pbp/")
    df$game_pbp_url <- x
    df$game_pbp_id <- as.integer(stringr::str_extract(x, "\\d+"))
    readr::write_csv(df, glue::glue("ncaa/game_pbp/csv/{game_pbp_id}.csv"))
    saveRDS(df, glue::glue("ncaa/game_pbp/rds/{game_pbp_id}.rds"))
    arrow::write_parquet(df, glue::glue("ncaa/game_pbp/parquet/{game_pbp_id}.parquet"))
    jsonlite::write_json(df,glue::glue("ncaa/game_pbp/json/{game_pbp_id}.json"), pretty = 2)
    Sys.sleep(2)
    return(df)}, .progress = TRUE) %>%
    baseballr:::rbindlist_with_attrs()
  game_pbp_files <- list.files("ncaa/game_pbp/csv/")
  game_pbp_files_year <- stringr::str_extract(game_pbp_files, glue::glue("\\d+.csv"))
  game_pbp_files_year <- game_pbp_files_year[!is.na(game_pbp_files_year)]
  ncaa_game_pbps <- purrr::map(game_pbp_files_year, function(x){
    df <- data.table::fread(glue::glue("ncaa/game_pbp/csv/{x}"))
    return(df)
  }) %>%
    baseballr:::rbindlist_with_attrs()

  ifelse(!dir.exists(file.path("ncaa/pbp")), dir.create(file.path("ncaa/pbp")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/pbp/csv")), dir.create(file.path("ncaa/pbp/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/pbp/rds")), dir.create(file.path("ncaa/pbp/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/pbp/parquet")), dir.create(file.path("ncaa/pbp/parquet")), FALSE)
  ncaa_game_pbps <- ncaa_game_pbps %>% dplyr::arrange(desc(.data$date))
  ncaa_game_pbps <- ncaa_game_pbps %>%
    baseballr:::make_baseballr_data("NCAA Play-by-Play Information from baseballr data repository", Sys.time())
  readr::write_csv(ncaa_game_pbps, glue::glue("ncaa/pbp/csv/ncaa_baseball_pbp_{y}.csv"))
  saveRDS(ncaa_game_pbps, glue::glue("ncaa/pbp/rds/ncaa_baseball_pbp_{y}.rds"))
  arrow::write_parquet(ncaa_game_pbps, glue::glue("ncaa/pbp/parquet/ncaa_baseball_pbp_{y}.parquet"))
  cli::cli_process_done(msg_done = "Finished NCAA Baseball pbp parse for {y}! (Rescrape: {tolower(rescrape)})")
}

all_games <- purrr::map(years_vec, function(y){
  ncaa_baseball_pbp_scrape(y)
})
