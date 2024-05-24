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




gls <- pipfun::pip_create_globals(
  root_dir   = Sys.getenv("PIP_ROOT_DIR"),
  # out_dir    = fs::path("y:/pip_ingestion_pipeline/temp/"),
  vintage    = version,
  create_dir = FALSE,
  max_year_country   = 2023,
  max_year_aggregate = 2022
)




# get data -----------
data_files <-
  gls$CACHE_SVY_DIR_PC |>
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


# files_names <- files_names[sample(1:100, 10)]
# data_files  <- data_files[files_names]


## load data ---------

cols <- c("cache_id", "survey_year", "area", "welfare_ppp", "weight")

rfst <- purrr::possibly(fst::read_fst)

tictoc::tic()
ld <- purrr::map(data_files, \(x) {
  rfst(x,
       as.data.table = TRUE,
       columns = cols)
},
.progress = TRUE)
dt <- rowbind(ld, fill = TRUE)
tictoc::toc()

## Add national to countries without area ----------

dt[, area := as.character(area)
][,
  area := fifelse(is.na(area) | area == "", "national", area)]



# calculate PG ---------

## by area ---------
pg_area <-
  dt |>
  ftransform(pg = ps/welfare_ppp) |>
  fgroup_by(cache_id, survey_year, area) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## national ---------
pg_national <-
  pg_area |>
  ftransform(area = "national") |>
  fgroup_by(cache_id, survey_year, area) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## Append both ---------
ft <- rowbind(pg_area, pg_national) |>
  funique()

ft[,
   c("country_code", "year", "welfare_type") :=
     tstrsplit(cache_id, split = "_", keep = c(1, 2, 5))
][,
  cache_id := NULL]

setcolorder(ft, c("country_code", "year"))
setorderv(ft,
          c("country_code", "area", "year", "welfare_type", "weight"),
          c(1, 1, 1, 1, -1))

dups <- fduplicated(gv(ft, c("country_code", "year", "survey_year", "welfare_type", "area")))

# duplicates
dp <- ft[dups]
# single values
sv <- ft[!dups]



# Save results-----------

## output dir -----------

outdir <-
  "p:/03.pip/estimates/temp-Prosperity_Gap/" |>
  fs::dir_create(version, "svy_years")

## save in different formats ---
filename <- "PG_svy_year"

fs::path(outdir, filename, ext = "fst") |>
  fst::write_fst(sv, path = _, compress = 0)

fs::path(outdir, filename, ext = "dta") |>
  haven::write_dta(sv, path = _)

