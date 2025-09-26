
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

data_files <- as.list(data_files) |>
  setNames(files_names)

## remove GROUP data files ------
to_drop <- grepl("GROUP$", files_names)

files_names <- files_names[!to_drop]
data_files  <- data_files[!to_drop]



# add metadata
svy <- lkup$svy_lkup[, .(path,
                         cache_id,
                         country_code,
                         reporting_level,
                         reporting_year,
                         welfare_type)]
## load data ---------

cols <- c("cache_id", "area", "welfare_ppp", "weight")

rfst <- purrr::possibly(fst::read_fst)

tictoc::tic()
ld <- lapply(cli::cli_progress_along(files_names),
             \(i) {
               x <- files_names[i]

               ry <- svy[cache_id == x,
                         unique(reporting_year)]


               rl <- svy[cache_id == x,
                         unique(reporting_level)]

               path <- data_files[[x]]

               DT <- rfst(path,
                         as.data.table = TRUE,
                         columns = cols)


               if ("national" %in% rl) {
                 DT[, reporting_level := "national"]
               } else {
                 DT[, reporting_level := area]
               }

              DT[, reporting_year := ry
                 ][, area := NULL
                   ]
              DT

})

tictoc::toc()
names(ld) <- files_names


dt <- rowbind(ld, fill = TRUE)


# calculate PG ---------

## by reporting_level ---------
pg_area <-
  dt |>
  ftransform(pg = ps/welfare_ppp) |>
  fgroup_by(cache_id, reporting_year, reporting_level) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()

## national ---------
pg_national <- dt |>
  fsubset(reporting_level != "national") |>
  ftransform(pg = ps/welfare_ppp) |>
  ftransform(reporting_level = "national") |>
  fgroup_by(cache_id, reporting_level, reporting_year) |>
  fselect(pg, weight) |>
  fmean(weight, stub = FALSE) |>
  fungroup()



## Append both ---------
ft <- rowbind(pg_area, pg_national) |>
  funique(cols = c("cache_id", "pg")) |>
  setkey(NULL)

ft[,
   c("country_code", "welfare_type") :=
     tstrsplit(cache_id, split = "_", keep = c(1, 5))
][,
  cache_id := NULL] |>
  setcolorder(c("country_code", "reporting_year")) |>
  setorderv(
    c(
      "country_code",
      "reporting_level",
      "reporting_year",
      "welfare_type",
      "weight"
    ),
    c(1, 1, 1, 1, -1)
  )


# Save results-----------

## output dir -----------

outdir <-
  "p:/03.pip/estimates/temp-Prosperity_Gap/" |>
  fs::dir_create(version, "svy_years")

## save in different formats ---
filename <-  fs::path(outdir, "pg_svy", ext = "fst")


fst::write_fst(ft, filename, compress = 0)

fs::path_ext_set(filename, "dta") |>
  haven::write_dta(ft, path = _)



ts <- fst::read_fst(filename, as.data.table = TRUE)

ts[is.na(pg)]
