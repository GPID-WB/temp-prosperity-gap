# SETUP -----------

## load libraries ---------
library(fastverse)
library(furrr)
library(progressr)

# remotes::install_github("PIP-Technical-Team/pipapi@DEV")

## data files ---------
version  <- "20240326_2017_01_02_PROD"
version  <- "20240429_2017_01_02_INT"
ppp_year <- sub("(.+_)([0-9]{4})(_.+)", "\\2", version) |>
  as.numeric()

## ppp year --------
if (ppp_year == 2017) {
  ps <- 25
} else if (ppp_year == 2011) {
  ps <- 22
} else {
  ps <- 25
}



# get data -----------
data_files <-
  Sys.getenv("PIPAPI_DATA_ROOT_FOLDER_LOCAL") |>
  fs::path(version, "survey_data") |>
  fs::dir_ls(regexp = "fst$", type = "file")

files_names <- data_files |>
  fs::path_file() |>
  fs::path_ext_remove()

names(data_files) <- files_names

## remove GROUP data files ------
to_drop <- which(grepl("GROUP$", files_names))

files_names <- files_names[-to_drop]
data_files  <- data_files[-to_drop]

## load data ---------
tictoc::tic()
ld <- purrr::imap(data_files, \(x, y) {
  fst::read_fst(x, as.data.table = TRUE) |>
    ftransform(id = y)
  },
  .progress = TRUE)
dt <- rowbind(ld)
tictoc::toc()

dt[,
  c("country_code", "year") := tstrsplit(id, split = "_", keep = c(1, 2))]

# df <- copy(dt)
dt <- copy(df)

# Merge CPI -----------

cpi <- pipload::pip_load_aux("cpi")


## counting variable ---------
cpi[, count := fifelse(cpi_data_level == "national", 2, 1)]

## expand data --------
ecpi <-
  cpi[
    # expand data ----------
    rep(1:.N, count)
    ][,
      # differentiate rural from urban -----------
     indx := 1:.N,
     by =  c("country_code",
             "cpi_year",
             "survey_year",
             "survey_acronym")
     ][,
       ## area var to merge -----------
       area := fcase(count == 2 & indx == 1, "rural",
                     count == 2 & indx == 2, "urban",
                     default = "")
       ][,
         area := fifelse(area == "", cpi_data_level, area)
         ][,
           ## filter vars -------------
           .( country_code, cpi_year, survey_year, survey_acronym, cpi, area)
           ]
setnames(ecpi, "cpi_year", "year")
## actual merge ------------
dt <- joyn::joyn(dt, ecpi,
                 by = c("country_code", "year", "area"),
                 match_type = "1:1")



# calculate PG ---------

## by area ---------
pg_area <-
  dt |>
  ftransform(pg = ps/welfare) |>
  fgroup_by(id, area) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## national ---------
pg_national <-
  pg_area |>
  ftransform(area = "national") |>
  fgroup_by(id, area) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## Append both ---------
ft <- rowbind(pg_area, pg_national)

ft[, id := NULL]

setcolorder(ft, c("country_code", "year"))
setorderv(ft, c("country_code", "area", "year"))

# Save results-----------

## output dir -----------

outdir <-
  "p:/03.pip/estimates/temp-Prosperity_Gap/" |>
  fs::dir_create(version, "svy_years")

## save in different formats ---
filename <- "PG_svy_year"

fs::path(outdir, filename, ext = "fst") |>
  fst::write_fst(ft, path = _, compress = 0)

fs::path(outdir, filename, ext = "dta") |>
  haven::write_dta(ft, path = _)

