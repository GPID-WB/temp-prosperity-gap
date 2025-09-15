# SETUP -----------

## load libraries ---------
library(fastverse)

# remotes::install_github("PIP-Technical-Team/pipapi@DEV")

## data files ---------
version  <- "20250401_2017_01_02_PROD"
version  <- "20250401_2021_01_02_PROD"
version  <- "20250930_2017_01_02_PROD"
version  <- "20250930_2021_01_02_PROD"
ppp_year <- sub("(.+_)([0-9]{4})(_.+)", "\\2", version) |>
  as.numeric()

## ppp year --------
if (ppp_year == 2021) {
  ps <- 28
} else if (ppp_year == 2017) {
  ps <- 25
} else if (ppp_year == 2011) {
  ps <- 22
} else {
  ps <- 25
}




gls <- pipfun::pip_create_globals(
  root_dir   = Sys.getenv("PIP_ROOT_DIR"),
  # out_dir    = fs::path("y:/pip_ingestion_pipeline/temp/"),
  vintage    = version,
  create_dir = FALSE,
  max_year_country   = 2024,
  max_year_aggregate = 2025,
  max_year_lineup    = 2024
)


# functions -----------
remove_var <- \(dt, x) {
  x_in_dt <- names(dt)[names(dt) %in% x]
  if (length(x_in_dt) == 0)
    return(invisible(dt))

  dt[,
     (x_in_dt) := NULL]
  dt
}

rename_var <- \(dt, old, new) {
  stopifnot(length(old) == length(new))
  x_in_dt <- all(old %in% names(dt))
  if (isFALSE(x_in_dt)) {
    cli::cli_alert("no names changed")
    return(invisible(dt))
  }

  setnames(dt, old, new)
}


