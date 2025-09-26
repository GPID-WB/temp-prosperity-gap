source("R/setup.R")
# SETUP -----------

indir <-
  "p:/03.pip/estimates/temp-Prosperity_Gap/" |>
  fs::dir_create(version)


# y_dir  <- fs::path(gls$OUT_AUX_DIR_PC)
data_dir <- Sys.getenv("PIPAPI_DATA_ROOT_FOLDER_LOCAL")
e_dir <- data_dir  |>
  fs::path(version)


# ppp years
ppp_year <- py <- version |>
  gsub("(.*)_([0-9]{4})(_.*)", "\\2", x = _) |>
  as.numeric()



# SVY ---------------
svy <- fs::path(indir, "svy_years", "pg_svy.fst" ) |>
  fst::read_fst(as.data.table = TRUE)

svy[,
    c("weight") := NULL
    ][,
      reporting_year := as.numeric(reporting_year)
    ]

svy[, welfare_type := fifelse(welfare_type == "CON",
                              "consumption",
                              "income")]


# LNY -------------------
lnp <- fs::path(indir, "lny_years", "pg_lnp.fst" ) |>
  fst::read_fst(as.data.table = TRUE)

refy_cols <-  c("country_code", "reporting_year", "reporting_level", "welfare_type")
wt <- lkup$refy_lkup[, ..refy_cols]

lnp <- joyn::joyn(lnp, wt,
                    by = c("country_code",
                           "reporting_year",
                           "reporting_level"),
                    match_type = "1:1",
                    keep = "left",
                  reportvar = FALSE)

lnp[,
      c("weight") := NULL
      ][,
        reporting_year := as.numeric(reporting_year)
      ]

# treat countries with reporting_level differnet to national.
ur_lnp <- lnp[reporting_level != "national",
              c("country_code", "reporting_year", "welfare_type")] |>
  unique() |>
  _[,
    reporting_level := "national"]

lnp <- joyn::joyn(lnp, ur_lnp,
                  by = c("country_code",
                         "reporting_year",
                         "reporting_level"),
                  match_type = "1:1",
                  keep = "left",
                  reportvar = FALSE,
                  update_values = TRUE)


# Save files -----------

fst::write_fst(svy, fs::path(gls$OUT_AUX_DIR_PC, "pg_svy.fst"))
fst::write_fst(lnp, fs::path(gls$OUT_AUX_DIR_PC, "pg_lnp.fst"))

fst::write_fst(svy, fs::path(e_dir, "_aux", "pg_svy.fst"))
fst::write_fst(lnp, fs::path(e_dir, "_aux", "pg_lnp.fst"))

