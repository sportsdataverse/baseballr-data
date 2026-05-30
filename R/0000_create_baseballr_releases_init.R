# One-time bootstrap: create the empty GitHub release skeletons on
# sportsdataverse/sportsdataverse-data that the baseballr `load_*()` functions
# read from. Run ONCE per tag before the first data upload -- the recurring
# creation scripts (ncaa_01_schedules_creation.R, ncaa_02_pbp_creation.R) call
# sportsdataversedata::sportsdataverse_save() to push assets to these tags, and
# sportsdataverse_save() does NOT create the release, so the tag must exist.
#
# pb_release_create() is effectively idempotent: re-running it for an existing
# tag is a no-op (the release already exists), so this is safe to re-run.
#
# Requires: GITHUB_PAT (or SDV_GH_TOKEN) with write access to
# sportsdataverse/sportsdataverse-data, and the piggyback package.
#
# Tags MUST match the release tags the baseballr loaders fetch from:
#   ncaa_baseball_pbp        <- baseballr::load_ncaa_baseball_pbp()
#   ncaa_baseball_schedules  <- baseballr::load_ncaa_baseball_schedule()
# (Note the loader uses the plural `ncaa_baseball_schedules` tag with singular
#  `ncaa_baseball_schedule_{year}` asset names.)

releases <- list(
  list(tag = "ncaa_baseball_pbp",
       body = "NCAA College Baseball Play-by-Play Data (from stats.ncaa.org), loaded by baseballr::load_ncaa_baseball_pbp()"),
  list(tag = "ncaa_baseball_schedules",
       body = "NCAA College Baseball Schedule Data (from stats.ncaa.org), loaded by baseballr::load_ncaa_baseball_schedule()")
)

for (r in releases) {
  message(sprintf("%s: ensuring release tag '%s' exists", Sys.time(), r$tag))
  piggyback::pb_release_create(
    repo  = "sportsdataverse/sportsdataverse-data",
    tag   = r$tag,
    name  = r$tag,
    body  = r$body,
    .token = Sys.getenv("GITHUB_PAT")
  )
}
