# SETUP -----------

## load libraries ---------
library(fastverse)

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
  # root_dir |>
  # fs::path(version) |>
  fs::dir_ls(regexp = "fst$", type = "file")

files_names <- data_files |>
  fs::path_file() |>
  fs::path_ext_remove()

names(data_files) <- files_names

## remove GROUP data files ------
to_drop <- grepl("GROUP$", files_names)

files_names <- files_names[!to_drop]
data_files  <- data_files[!to_drop]

## load data ---------
tictoc::tic()
ld <- purrr::imap(data_files, \(x, y) {
  fst::read_fst(x, as.data.table = TRUE) |>
    ftransform(cache_id = y)
},
.progress = TRUE)
dt <- rowbind(ld, fill = TRUE)
tictoc::toc()

## Add national to countries without area ----------

dt[, area := as.character(area)
   ][,
     area := fifelse(is.na(area), "national", area)]


## get means in ppp --------------
mn <-
  Sys.getenv("PIPAPI_DATA_ROOT_FOLDER_LOCAL") |>
  fs::path(version, "estimations", "survey_means.fst") |>
  fst::read_fst( as.data.table = TRUE)

## expand mean data --------
emn <-
  mn[!is.na(cpi) & !is.na(ppp)
  ][,
    count := fifelse(reporting_level == "national", 3, 1)
  ][
    # expand data ----------
    rep(1:.N, count)
  ][,
    # differentiate rural from urban -----------
    indx := 1:.N,
    by =  cache_id
  ][,
    ## area var to merge -----------
    area := fcase(count == 3 & indx == 1, "rural",
                  count == 3 & indx == 2, "urban",
                  count == 3 & indx == 3, "national",
                  default = "")
  ][,
    area := fifelse(area == "", reporting_level, area)
  ][,
    ## filter vars -------------
    .( cache_id, survey_year, area, survey_mean_lcu, survey_mean_ppp)
  ]

## actual merge ---------
df <- joyn::joyn(dt, emn,
                 by = c("cache_id", "area"),
                 match_type = "m:1",
                 keep = "inner",
                 reportvar = FALSE)

# nomean <- df[.joyn == "x", unique(cache_id)]
# nodata <- df[.joyn == "y", unique(cache_id)]

## deflate to ppp ----------------
df[,
   weflare_ppp := welfare * (survey_mean_lcu/survey_mean_ppp)]

# calculate PG ---------

## by area ---------
pg_area <-
  df |>
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

