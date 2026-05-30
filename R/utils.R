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

# Proxy rotation state. The scrape loops run sequentially (purrr::map) in a
# single R process, so this is one continuous round-robin over the whole pool
# for the entire run -- no IP is reused until every other has been tried.
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

# Fail-fast canary: before the long scrape loop, probe stats.ncaa.org through a
# few rotating proxies. NCAA's Akamai edge blocks by IP, returning HTTP 403
# "Access Denied" to a blocked proxy on ANY path -- so if every probed proxy is
# 403/errored, the whole pool is dead and the scrape would silently produce
# nothing for ~30 minutes before crashing at assembly. Abort immediately with a
# clear message instead. Succeeds as soon as ANY proxy returns HTTP 200 (the
# pool can have a mix; rotation handles the rest).
preflight_proxy_check <- function(proxies = get_proxy_ips(),
                                  tries = 5L,
                                  test_url = "https://stats.ncaa.org/") {
  n <- nrow(proxies)
  if (is.null(n) || n == 0) {
    cli::cli_abort(c(
      "Proxy preflight FAILED: empty proxy pool from get_proxy_ips().",
      "x" = "Check PROXY_KEY / PROXY_PKG / PROXY_ENDPOINT are set and the endpoint is reachable."
    ))
  }
  tries <- min(as.integer(tries), n)
  ua <- "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
  results <- character(0)

  for (k in seq_len(tries)) {
    px <- select_proxy(proxies)
    res <- tryCatch({
      req <- httr2::request(test_url)
      req <- do.call(httr2::req_proxy, c(list(req), px))
      req <- httr2::req_user_agent(req, ua)
      req <- httr2::req_timeout(req, 25)
      req <- httr2::req_error(req, is_error = function(resp) FALSE)  # inspect status ourselves
      httr2::resp_status(httr2::req_perform(req))
    }, error = function(e) paste0("ERR: ", conditionMessage(e)))

    results <- c(results, as.character(res))
    if (is.numeric(res) && res == 200) {
      cli::cli_alert_success(
        "Proxy preflight OK: stats.ncaa.org reachable via proxy (HTTP 200) on attempt {k}/{tries}."
      )
      return(invisible(TRUE))
    }
    cli::cli_alert_warning("Preflight attempt {k}/{tries}: {results[k]}")
  }

  cli::cli_abort(c(
    "Proxy preflight FAILED: stats.ncaa.org blocked all {tries} probed proxies.",
    "i" = "Responses: {paste(results, collapse = ', ')}",
    "x" = "NCAA is returning 403/Access Denied (or errors) to the proxy IPs -- the pool is blocked.",
    ">" = "Point PROXY_ENDPOINT/PROXY_KEY/PROXY_PKG at a working (residential/mobile) pool, then re-run."
  ))
}
