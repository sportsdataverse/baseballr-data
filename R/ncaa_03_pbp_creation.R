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

ncaa_baseball_pbp_compilation <- function(y){
  cli::cli_process_start("Starting NCAA Baseball pbp compilation for {y}!")

  game_pbp_files <- list.files("ncaa/game_pbp/rds/")
  game_pbp_files_year <- stringr::str_extract(game_pbp_files, glue::glue("\\d+.rds"))
  game_pbp_files_year <- game_pbp_files_year[!is.na(game_pbp_files_year)]

  tictoc::tic()
  future::plan("multisession")
  ncaa_game_pbps <- furrr::future_map(game_pbp_files_year, function(x){
    df <- readRDS(glue::glue("ncaa/game_pbp/rds/{x}"))
    return(df)
  }) %>%
    baseballr:::rbindlist_with_attrs()
  tictoc::toc()

  ncaa_game_pbps <- ncaa_game_pbps %>% dplyr::arrange(desc(.data$game_date))
  ncaa_game_pbps <- ncaa_game_pbps %>%
    baseballr:::make_baseballr_data("NCAA Play-by-Play Information from baseballr data repository", Sys.time())
  sportsdataversedata::sportsdataverse_save(
      data_frame = ncaa_game_pbps,
      file_name =  glue::glue("ncaa_baseball_pbp_{y}"),
      sportsdataverse_type = "play by play data",
      release_tag = "ncaa_baseball_pbp",
      file_types = c("rds","csv","parquet"),
      .token = Sys.getenv("GITHUB_PAT")
  )
  rm(ncaa_game_pbps)
  rm(game_pbp_files)
  rm(game_pbp_files_year)
  empty <- gc()
  cli::cli_process_done(msg_done = "Finished NCAA Baseball pbp compilation for {y}!")
}

all_games <- purrr::map(years_vec, function(y) {
    ncaa_baseball_pbp_compilation(y)
})

rm(all_games)
empty <- gc()