# Backfill: push the historical per-year artifacts already on disk in this repo
# up to their sportsdataverse/sportsdataverse-data release tags. Run ONCE after
# 0000_create_baseballr_releases_init.R to populate the releases with every
# season currently compiled locally; thereafter the recurring creation scripts
# keep the current season fresh.
#
# Reads the compiled per-year files the creation scripts produce:
#   ncaa/schedules/rds/ncaa_baseball_schedule_{year}.rds  -> tag ncaa_baseball_schedules
#   ncaa/pbp/rds/ncaa_baseball_pbp_{year}.rds             -> tag ncaa_baseball_pbp
# and uploads rds/csv/parquet copies of each via sportsdataverse_save()
# (overwrite = TRUE, so re-runs clobber prior assets of the same name).
#
# Requires: GITHUB_PAT/SDV_GH_TOKEN with write access to sportsdataverse-data,
# and the sportsdataversedata + arrow + data.table packages.
suppressPackageStartupMessages({
  library(purrr)
  library(glue)
})

# upload one (dir, tag, type, loader) family of per-year rds files ------------
push_family <- function(dir, release_tag, sportsdataverse_type, pkg_function) {
  files <- list.files(dir, pattern = "_(\\d{4})\\.rds$", full.names = TRUE)
  if (length(files) == 0) {
    message(sprintf("%s: no files in %s, skipping", Sys.time(), dir))
    return(invisible())
  }
  # retry uploads on transient GitHub/network failures
  save_insistently <- purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = purrr::rate_backoff(pause_base = 1, pause_min = 2, max_times = 10),
    quiet = FALSE)

  purrr::walk(sort(files), function(f) {
    file_name <- tools::file_path_sans_ext(basename(f))     # e.g. ncaa_baseball_pbp_2024
    df <- readRDS(f)
    message(sprintf("%s: uploading %s (%d rows) -> %s", Sys.time(),
                    file_name, nrow(df), release_tag))
    save_insistently(
      data_frame          = df,
      file_name           = file_name,
      sportsdataverse_type = sportsdataverse_type,
      release_tag         = release_tag,
      pkg_function        = pkg_function,
      file_types          = c("rds", "csv", "parquet"),
      .token              = Sys.getenv("GITHUB_PAT")
    )
  })
}

push_family(
  dir = "ncaa/schedules/rds",
  release_tag = "ncaa_baseball_schedules",
  sportsdataverse_type = "schedule data",
  pkg_function = "baseballr::load_ncaa_baseball_schedule()")

push_family(
  dir = "ncaa/pbp/rds",
  release_tag = "ncaa_baseball_pbp",
  sportsdataverse_type = "play-by-play data",
  pkg_function = "baseballr::load_ncaa_baseball_pbp()")

message(sprintf("%s: backfill complete", Sys.time()))
