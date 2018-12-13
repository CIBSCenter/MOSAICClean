################################################################################
## Primary data cleaning script for MOSAIC prospective in-hospital data
##  collection
################################################################################

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
  mutate_at(vars(one_of(dttm_vars)), ~ date(ymd_hm(.)))

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
  ## Inclusion criteria
  "inc_date",  "Inclusion criteria date is missing, prior to March 2017, or after date script run",
  "inc_adult", "Inclusion criteria: adult patient either missing or marked No",
  "inc_icu",   "Inclusion criteria: in ICU either missing or marked No",
  "inc_organ", "Inclusion criteria: qualifying organ failure either missing or marked No",
  "inc_organ_present", "Patient had qualifying organ failure, but no specific organ failures marked present",
  "inc_mv",    "Patient marked as having both invasive MV and NIPPV at inclusion"
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

## -- Create a final data.frame of errors + messages ---------------------------
enrqual_errors <- create_error_df(
  error_matrix = enrqual_issues, error_codes = as.data.frame(enrqual_codes)
)