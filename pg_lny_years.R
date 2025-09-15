
source("R/setup.R")

# get data -----------
data_files <-
  fs::path("E:/PIP/pipapi_data",
           version,
           "lineup_data") |>
  # root_dir |>
  # fs::path(version) |>
  fs::dir_ls(regexp = "fst$", type = "file")

files_names <- data_files |>
  fs::path_file() |>
  fs::path_ext_remove()

names(data_files) <- files_names

# files_names <- files_names[sample(1:100, 10)]
# data_files  <- data_files[files_names]


## load data ---------

cols <- c("reporting_level", "welfare", "weight", "index")


tictoc::tic()
ld <- lapply(cli::cli_progress_along(data_files),
                 \(i) {
                   x <- data_files[i]
                   y <- files_names[i]
                   dt <- fst::read_fst(x,
                        as.data.table = TRUE,
                        columns = cols)
                   dt[index > 0
                      ][,
                        id := y
                        ][, index := NULL]
})
dt <- rowbind(ld, fill = TRUE)
tictoc::toc()



# calculate PG ---------

## by area ---------
pg_area <-
  dt |>
  ftransform(pg = ps/welfare) |>
  fgroup_by(id, reporting_level) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## national ---------
pg_national <- dt |>
  fsubset(reporting_level != "national") |>
  ftransform(pg = ps/welfare) |>
  ftransform(reporting_level = "national") |>
  fgroup_by(id, reporting_level) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()


## Append both ---------
ft <- rowbind(pg_area, pg_national) |>
  funique()

ft[,
   c("country_code", "year") :=
     tstrsplit(id, split = "_", keep = c(1, 2))
][,
  id := NULL]

setcolorder(ft, c("country_code", "year"))
setorderv(ft,
          c("country_code", "reporting_level", "year", "weight"),
          c(1, 1, 1, -1))

dups <- gv(ft, c("country_code", "year", "reporting_level")) |>
  fduplicated()

# duplicates
dp <- ft[dups]
# single values
sv <- ft[!dups]



# Save results-----------

## output dir -----------

outdir <-
  "p:/03.pip/estimates/temp-Prosperity_Gap/" |>
  fs::dir_create(version, "lny_years")

## save in different formats ---
filename <- "PG_lny_year"

fs::path(outdir, filename, ext = "fst") |>
  fst::write_fst(sv, path = _, compress = 0)

fs::path(outdir, filename, ext = "dta") |>
  haven::write_dta(sv, path = _)

