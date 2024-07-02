
source("R/setup.R")

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

