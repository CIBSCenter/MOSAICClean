################################################################################
## Primary data cleaning script for MOSAIC prospective in-hospital data
##  collection
################################################################################

## -- THIS WILL NEED TO BE UPDATED ---------------------------------------------
## At initial run, only the first 94 patients will be cleaned. The transition to
##  dynamic data pulling in REDCap means that we are currently behind on data
##  entry for later patients; listing queries for these patients will be
##  unproductive until data entry is caught up.
last_pt <- 94

## load tidyverse, lubridate for data management
library(tidyverse)
library(lubridate)

## -- Source helper functions --------------------------------------------------
source("R/dataclean_helpers.R")

## -- API/httr setup -----------------------------------------------------------
library(httr) ## for working with REDCap API
rc_url <- "https://redcap.vanderbilt.edu/api/"

## -- Read in data dictionary for in-hospital project --------------------------
## Will be used for variable labels, limits
ih_ddict <- httr::POST(
  url = rc_url,
  body = list(
    token = Sys.getenv("MOSAIC_IH"),
    ## API token gives you permission to get data
    content = "metadata",          ## export *metadata* (data dictionary)
    format = "csv"                 ## export as *CSV*
  )
) %>%
  post_to_df()

## Get vector of date variables (all export in ISO 8601)
date_vars <- ih_datadict %>%
  filter(
    text_validation_type_or_show_slider_number %in% c("date_mdy", "date_ymd")
  ) %>%
  pull(field_name)

dttm_vars <- ih_datadict %>%
  filter(
    text_validation_type_or_show_slider_number %in% c("datetime_mdy")
  ) %>%
  pull(field_name)

## -- Export data --------------------------------------------------------------
## Data collected ONLY on day of enrollment
day1_df <- post_to_df(
  httr::POST(
    url = rc_url,
    body = list(
      token = Sys.getenv("MOSAIC_IH"),
      content = "record",   ## export *records*
      format = "csv",       ## export as *CSV*
      ## Q: Which forms to export? A: Those collected *only* day of enrollment
      forms = paste(
        c(
          "enrollment_qualification_form",
          "contact_information",
          "prehospital_function_assessment_form",
          "dates_tracking_form",
          "enrollment_data_collection_form_mds",
          "enrollment_nutrition_data_form"
        ),
        collapse = ","
      ),
      fields = c("id"),     ## additional fields
      events = "enrollment_trial_d_arm_1", ## baseline visit event only
      rawOrLabel = "label", ## export factor *labels* vs numeric codes
      exportCheckboxLabel = TRUE ## export ckbox *labels* vs Unchecked/Checked
    )
  )
) %>%
  ## Remove any test patients from dataset
  filter(!str_detect(tolower(id), "test")) %>%
  ## Convert date/time variables to proper formats
  mutate_at(vars(one_of(date_vars)), ymd) %>%
  mutate_at(vars(one_of(dttm_vars)), ~ date(ymd_hm(.))) %>%
  ## TEMPORARY: Select only patients up till last_pt (set above)
  separate(id, into = c("site", "ptnum"), sep = "-", remove = FALSE) %>%
  filter(as.numeric(ptnum) <= last_pt)

################################################################################
## Enrollment Qualification Form
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
enrqual_codes <- tribble(
  ~ code,      ~ msg,
  ## Protocol present? (Only one like this; not worth separate missing matrix)
  "protocol",  "Missing current approved protocol at enrollment",
  ## Inclusion criteria
  "inc_date",  "Inclusion criteria date is missing, prior to March 2017, or after date script run",
  "inc_adult", "Inclusion criteria: adult patient either missing or marked No",
  "inc_icu",   "Inclusion criteria: in ICU either missing or marked No",
  "inc_organ", "Inclusion criteria: qualifying organ failure either missing or marked No",
  "inc_organ_present", "Patient had qualifying organ failure, but no specific organ failures marked present",
  "inc_mv",    "Patient marked as having both invasive MV and NIPPV at inclusion",
  ## Exclusion criteria: Should usually be present and No; there may be exceptions
  "exc_rapid", "Exclusion: rapidly resolving organ failure either missing or marked Yes; please correct or confirm",
  "exc_5days", "Exclusion: cumulative hospital days either missing or marked Yes; please correct or confirm",
  "exc_indep", "Exclusion: inability to live independently either missing or marked Yes; please correct or confirm",
  "exc_neuro", "Exclusion: severe neurologic injury either missing or marked Yes; please correct or confirm",
  "exc_bmi",   "Exclusion: BMI > 50 either missing or marked Yes; please correct or confirm",
  "exc_subst", "Exclusion: active substance abuse either missing or marked Yes; please correct or confirm",
  "exc_fu",    "Exclusion: blind, deaf, English either missing or marked Yes; please correct or confirm",
  "exc_morib", "Exclusion: expected death within 24h either missing or marked Yes; please correct or confirm",
  "exc_pris",  "Exclusion: prisoner either missing or marked Yes; please correct or confirm",
  "exc_200mi", "Exclusion: >200 miles from Nashville either missing or marked Yes; please correct or confirm",
  "exc_home",  "Exclusion: homeless with no secondary contact either missing or marked Yes; please correct or confirm",
  "exc_coenr", "Exclusion: co-enrollment either missing or marked Yes; please correct or confirm",
  "exc_mdref", "Exclusion: attending refusal either missing or marked Yes; please correct or confirm",
  "exc_ptref", "Exclusion: patient/surrogate refusal either missing or marked Yes; please correct or confirm",
  "exc_screen", "Exclusion: >72h before screening either missing or marked Yes; please correct or confirm",
  "exc_surr",  "Exclusion: >72h before surrogate available either missing or marked Yes; please correct or confirm"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in day1_df; columns = # potential issues
enrqual_issues <- matrix(
  FALSE, ncol = nrow(enrqual_codes), nrow = nrow(day1_df)
)
colnames(enrqual_issues) <- enrqual_codes$code
rownames(enrqual_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

## -- Determine true/false for each potential issue ----------------------------
## Usual format: `df_issues[, "issue_name"] <- [condition]`

enrqual_issues[, "protocol"] <- is.na(day1_df$protocol)

## -- Inclusion criteria -------------------------------------------------------
## Date met inclusion criteria: must be present, within range of 3/1/2017-today
enrqual_issues[, "inc_date"] <- with(day1_df, {
  is.na(incl_dttm) |
  incl_dttm < as.Date("2017-03-01") |
    incl_dttm > Sys.Date()
})

## Inclusion criteria: All three must be present and yes to enroll
enrqual_issues[, "inc_adult"] <- with(day1_df, {
  is.na(adult_random) | !(adult_random == "Yes")
})
enrqual_issues[, "inc_icu"] <- with(day1_df, {
  is.na(in_icu_random) | !(in_icu_random == "Yes")
})
enrqual_issues[, "inc_organ"] <- with(day1_df, {
  is.na(organ_fail) | !(organ_fail == "Yes")
})

## If organ failure present at inclusion, at least one type must be present
enrqual_issues[, "inc_organ_present"] <- with(day1_df, {
  organ_fail == "Yes" &
    rowSums(
      !is.na(day1_df[, grep("^organ\\_fail\\_present\\_[0-9]$", names(day1_df))])
    ) == 0
})

## Patient cannot be on both invasive and noninvasive ventilation
enrqual_issues[, "inc_mv"] <- with(day1_df, {
  !is.na(organ_fail_present_1) & !is.na(organ_fail_present_2)
})

## -- Exclusion criteria -------------------------------------------------------
enrqual_issues[, "exc_rapid"] <- with(day1_df, {
  is.na(organ_resolve_exc) | organ_resolve_exc == "Yes"
})
enrqual_issues[, "exc_5days"] <- with(day1_df, {
  is.na(cum_days_exc) | cum_days_exc == "Yes"
})
enrqual_issues[, "exc_indep"] <- with(day1_df, {
  is.na(indep_exc) | indep_exc == "Yes"
})
enrqual_issues[, "exc_neuro"] <- with(day1_df, {
  is.na(neuro_exc) | neuro_exc == "Yes"
})
enrqual_issues[, "exc_bmi"] <- with(day1_df, {
  is.na(bmi_exc) | bmi_exc == "Yes"
})
enrqual_issues[, "exc_subst"] <- with(day1_df, {
  is.na(sub_psyc_exc) | sub_psyc_exc == "Yes"
})
enrqual_issues[, "exc_fu"] <- with(day1_df, {
  is.na(blind_deaf_exc) | blind_deaf_exc == "Yes"
})
enrqual_issues[, "exc_morib"] <- with(day1_df, {
  is.na(moribund_exc) | moribund_exc == "Yes"
})
enrqual_issues[, "exc_pris"] <- with(day1_df, {
  is.na(prison_exc) | prison_exc == "Yes"
})
enrqual_issues[, "exc_200mi"] <- with(day1_df, {
  is.na(miles_exc) | miles_exc == "Yes"
})
enrqual_issues[, "exc_home"] <- with(day1_df, {
  is.na(homeless_exc) | homeless_exc == "Yes"
})
enrqual_issues[, "exc_coenr"] <- with(day1_df, {
  is.na(coenroll_exc) | coenroll_exc == "Yes"
})
enrqual_issues[, "exc_mdref"] <- with(day1_df, {
  is.na(attending_refused_exc) | attending_refused_exc == "Yes"
})
enrqual_issues[, "exc_ptref"] <- with(day1_df, {
  is.na(ptsurr_refusal_exc) | ptsurr_refusal_exc == "Yes"
})
enrqual_issues[, "exc_screen"] <- with(day1_df, {
  is.na(elig_expired_exc) | elig_expired_exc == "Yes"
})
enrqual_issues[, "exc_surr"] <- with(day1_df, {
  is.na(nosurr_exc) | nosurr_exc == "Yes"
})

## -- Create a final data.frame of errors + messages ---------------------------
enrqual_errors <- create_error_df(
  error_matrix = enrqual_issues, error_codes = as.data.frame(enrqual_codes)
)

enrqual_final <- enrqual_errors %>%
  mutate(form = "Enrollment Qualification")

################################################################################
## Combine all queries and export to output/ for uploading
################################################################################

## Combine all queries into a single data.frame
error_dfs <- list(
  enrqual_final
)

## Create variables needed to identify specific queries
all_issues <- bind_rows(error_dfs) %>%
  ## Separate ID, event name from id column
  separate(id, into = c("id", "event"), sep = "; ") %>%
  ## Create a unique query number for all issues for each patient
  group_by(id) %>%
  mutate(querynum = 1:n()) %>%
  ungroup() %>%
  ## Create total query ID: patient ID + date + querynum
  mutate(querydate = format(Sys.Date(), "%Y-%m-%d")) %>%
  unite("queryid", id, querydate, querynum, sep = "_", remove = FALSE) %>%
  ## Select variables in order needed for data clean database
  dplyr::select(queryid, id, querydate, form, event, msg)

## TODO: Remove queries that have already been addressed and are unfixable/
##       errors
## WAITING ON: data clean database to be built and populated; data clean to take
##  place

## -- Write final info to output/ ----------------------------------------------
write_csv(all_issues, path = "output/testclean.csv")
