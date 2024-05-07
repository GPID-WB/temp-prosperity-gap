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

if (ppp_year == 2017) {
  ps <- 25
} else if (ppp_year == 2011) {
  ps <- 22
} else {
  ps <- 25
}

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


tictoc::tic()
ld <- purrr::imap(data_files, \(x, y) {
  fst::read_fst(x, as.data.table = TRUE) |>
    ftransform(id = y)
  },
  .progress = TRUE)
dt <- rowbind(ld)
tictoc::toc()


pg_area <-
  dt |>
  ftransform(pg = ps/welfare) |>
  fgroup_by(id, area) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

pg_national <-
  pg_area |>
  ftransform(area = "national") |>
  fgroup_by(id, area) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

rowbind(pg_area, pg_national)

