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
years_vec <- 2022
y <- 2022
# ncaa_teams_lookup <- baseballr::load_ncaa_baseball_teams() %>%
#   dplyr::filter(.data$year %in% years_vec) %>%
#   dplyr::slice(533:540)

ncaa_baseball_pbp_scrape <- function(y){
  sched <- data.table::fread(paste0('ncaa/schedules/csv/ncaa_baseball_schedule_',y,'.csv'))
  pbp_links <- sched %>% 
    dplyr::filter(!is.na(.data$game_info_url)) %>% 
    dplyr::select("game_info_url")
  
  pbp_g <- purrr::map(pbp_links$game_info_url, function(x){
    df <- baseballr::ncaa_baseball_pbp(game_info_url = x)
    df$game_info_url <- x
    df$contest_id <- as.integer(stringr::str_extract(x, "\\d+"))
    Sys.sleep(4)
    return(df)}) %>% 
    baseballr:::rbindlist_with_attrs()
  
  ifelse(!dir.exists(file.path("ncaa/pbp")), dir.create(file.path("ncaa/pbp")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/pbp/csv")), dir.create(file.path("ncaa/pbp/csv")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/pbp/rds")), dir.create(file.path("ncaa/pbp/rds")), FALSE)
  ifelse(!dir.exists(file.path("ncaa/pbp/parquet")), dir.create(file.path("ncaa/pbp/parquet")), FALSE)
  pbp_g <- pbp_g %>% dplyr::arrange(desc(.data$date))
  pbp_g <- pbp_g %>%
    baseballr:::make_baseballr_data("NCAA Play-by-Play Information from baseballr data repository", Sys.time())
  readr::write_csv(pbp_g, glue::glue("ncaa/pbp/csv/ncaa_baseball_pbp_{y}.csv"))
  saveRDS(pbp_g, glue::glue("ncaa/pbp/rds/ncaa_baseball_pbp_{y}.rds"))
  arrow::write_parquet(pbp_g, glue::glue("ncaa/pbp/parquet/ncaa_baseball_pbp_{y}.parquet"))
}

all_games <- purrr::map(years_vec, function(y){
  ncaa_baseball_pbp_scrape(y)
})
