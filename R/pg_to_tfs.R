source("R/setup.R")

# Loading -------
dir <-
  "y:/pip_ingestion_pipeline/pc_data/prosperity_gap" |>
  fs::path(version)


files <- c("pg_lnp", "pg_svy")

.x <- "pg_svy"
.x <- "pg_lnp"

l <- lapply(files, \(.x) {

  df <- fread(fs::path(dir, .x, ext = "csv"),
              stringsAsFactors = FALSE)

  remove_var(df, c("survey_year", "population"))
  rename_var(df, "year", "reporting_year")




  # saving --------
  gls$OUT_AUX_DIR_PC |>
    fs::path(.x, ext = "fst") |>
    fst::write_fst(df, path = _)


  gls$OUT_AUX_DIR_PC |>
    fs::path(.x, ext = "qs") |>
    qs::qsave(df, file = _)


})

