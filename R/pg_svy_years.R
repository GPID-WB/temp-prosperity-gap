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


# calculate PG ---------

## by area ---------
pg_area <-
  dt |>
  ftransform(pg = ps/welfare_ppp) |>
  fgroup_by(id, area, welfare_type, survey_year) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## national ---------
pg_national <-
  pg_area |>
  ftransform(area = "national") |>
  fgroup_by(id, area, welfare_type, survey_year) |>
  fselect(pg, sum.weight) |>
  fmean(sum.weight, stub = FALSE) |>
  frename(sum.weight = sum.sum.weight) |>
  fungroup()

## Append both ---------
ft <- rowbind(pg_area, pg_national)

ft[,
   c("country_code", "year") := tstrsplit(id, split = "_", keep = c(1, 2))
   ][, id := NULL]

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

