lib_path <- Sys.getenv("R_LIBS")
# Fall back to the default library paths when R_LIBS is unset (CI installs
# deps into the default .libPaths(); lib.loc = NULL searches there).
if (!nzchar(lib_path)) lib_path <- NULL

suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(httr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
get_proxy_ips <- function(
    api_key = Sys.getenv("PROXY_KEY"),
    user_package = Sys.getenv("PROXY_PKG"),
    proxy_endpoint = Sys.getenv("PROXY_ENDPOINT")){
  res <- httr::RETRY(
    "GET",
    glue::glue("{proxy_endpoint}/{user_package}.json"),
    httr::add_headers(Authorization = paste(api_key))) %>%
    httr::content(as = "text", encoding = "UTF-8")
  
  resp <- res %>%
    jsonlite::fromJSON() %>%
    purrr::pluck("data")
  
  login <- resp$login
  password <- resp$password
  ips <- resp$ippacks
  
  ips$login <- login
  ips$password <- password
  proxies <- ips %>%
    dplyr::select("ip","port_http","login", "password")
  return(proxies)
}

# Per-process proxy rotation state. Each furrr worker is a separate R process
# that re-sources this file, so every worker keeps its own independent rotation
# (shuffled differently), which is exactly what we want for IP diversity.
.proxy_rotation <- new.env(parent = emptyenv())

# select_proxy hands out proxies in ROUND-ROBIN order: the pool is shuffled
# once, then every call returns the next IP in that order, only reshuffling and
# repeating after the whole pool has been used. This guarantees no IP is reused
# until all others have been tried -- minimising per-IP request rate, which is
# what helps against rate-based blocking (true per-request rotation). Compare
# the previous sample(..., 1) which drew with replacement and could hammer the
# same IP repeatedly.
select_proxy <- function(proxies = get_proxy_ips()) {
  n <- nrow(proxies)
  if (is.null(n) || n == 0) stop("select_proxy: empty proxy pool")

  st <- .proxy_rotation
  # (Re)initialise when first used, when the pool size changes (e.g. a refreshed
  # get_proxy_ips() pool), or after the current shuffle has been exhausted.
  if (is.null(st$order) || !identical(st$n, n) || st$i > n) {
    st$n <- n
    st$order <- sample.int(n)   # shuffle the pool once
    st$i <- 1L
  }

  idx <- st$order[st$i]
  st$i <- st$i + 1L
  if (st$i > n) {               # exhausted the pool -> reshuffle and wrap
    st$order <- sample.int(n)
    st$i <- 1L
  }

  row <- proxies[idx, , drop = FALSE]
  # baseballr's request_with_proxy() (development_branch) routes NCAA requests
  # through httr2::req_proxy() and expects a NAMED LIST, not an httr::use_proxy()
  # object: list(url = "http://HOST:PORT", username = ..., password = ...). It
  # does do.call(httr2::req_proxy, c(list(req), proxy)), so the list names must
  # match req_proxy()'s args (url/username/password).
  list(
    url      = paste0("http://", row$ip, ":", row$port_http),
    username = row$login,
    password = row$password
  )
}
