
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

data_files <- as.list(data_files) |>
  setNames(files_names)


# files_names <- files_names[sample(1:100, 10)]
# data_files  <- data_files[files_names]

manual <- FALSE
if (manual) {
  ws <- grep("SVK_198[1-4]", files_names)
  data_files <- data_files[ws]
  files_names <- files_names[ws]
}


## load data ---------

cols <- c("reporting_level", "welfare", "weight", "index")

tictoc::tic()
ld <- lapply(cli::cli_progress_along(data_files),
                 \(i) {
                   x <- data_files[[i]]
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
  funique() |>
  na_omit()

ft[,
   c("country_code", "reporting_year") :=
     tstrsplit(id, split = "_", keep = c(1, 2))
][,
  reporting_year := as.numeric(reporting_year)
  ][,
  id := NULL]

setcolorder(ft, c("country_code", "reporting_year"))
setorderv(ft,
          c("country_code", "reporting_level", "reporting_year", "weight"),
          c(1, 1, 1, -1))

dups <- gv(ft, c("country_code", "reporting_year", "reporting_level")) |>
  fduplicated()

# duplicates
dp <- ft[dups]
dp[]
# single values
sv <- ft[!dups]



# Save results-----------

## output dir -----------

outdir <-
  "p:/03.pip/estimates/temp-Prosperity_Gap/" |>
  fs::dir_create(version, "lny_years")

## save in different formats ---
filename <-  fs::path(outdir, "pg_lnp", ext = "fst")

if (manual) {
  fpg <- fst::read_fst(filename, as.data.table = TRUE)

}





fst::write_fst(sv,filename, compress = 0)


fs::path_ext_set(filename, "dta") |>
    haven::write_dta(sv, path = _)


ts <- fst::read_fst(filename, as.data.table = TRUE)
ts[country_code == "DOM" & reporting_year > 2022]
