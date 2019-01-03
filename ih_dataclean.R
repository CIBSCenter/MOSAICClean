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

## -- Set dates for specific events --------------------------------------------
## Used to restrict which patients get checked for certain conditions
epic_date <- as.Date("2018-05-15")
safety_date <- as.Date("2018-02-26")

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
date_vars <- ih_ddict %>%
  filter(
    text_validation_type_or_show_slider_number %in% c("date_mdy", "date_ymd")
  ) %>%
  pull(field_name)

dttm_vars <- ih_ddict %>%
  filter(
    text_validation_type_or_show_slider_number %in% c("datetime_mdy")
  ) %>%
  pull(field_name)

## -- Export data --------------------------------------------------------------
## day1_df and daily_df will be used to clean multiple forms; other data.frames
##  are used for specific forms/time points (eg, CADUCEUS only filled out on
##  days 1/3/5)
reconsent_levels <- c(
  "Yes, agreed to continue (signed ICD)",
  "No, withdrew from further participation (signed ICD)"
)

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
          "enrollment_nutrition_data_form",
          "dna_log"
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
  ## Add indicators for whether each pre-hospital assessment was done
  ##  (could be either fully or partially)
  mutate_at(
    vars(one_of(str_subset(names(.), "\\_comp\\_ph$"))),
    ~ str_detect(., "^Yes")
  ) %>%
  mutate(
    ## Indicators for current, former smoker (could be one or both)
    current_smoker = str_detect(tolower(gq_smoke), "current"),
    former_smoker = str_detect(tolower(gq_smoke), "former"),
    ## Indicator for whether patient self- or reconsented at any point
    pt_consented =
      (!is.na(reconsent_ph1) & reconsent_ph1 %in% reconsent_levels) |
      (!is.na(reconsent_ph2) & reconsent_ph2 %in% reconsent_levels) |
      (!is.na(consent_self) & consent_self == "Yes"),
    ## Indicator for whether patient died *in the hospital*
    ##  (death is recorded in the same spot whether it occurs in hospital or
    ##   in follow-up)
    died_inhosp = !is.na(death) & death == "Yes" &
      (is.na(hospdis) | hospdis == "No"),
    ## Create variable for last in-hospital date
    last_inhosp = case_when(
      !is.na(hospdis_dttm) ~ hospdis_dttm,
      !is.na(death_dttm)   ~ death_dttm,
      !is.na(studywd_dttm) ~ studywd_dttm,
      TRUE                 ~ as.Date(NA)
    )
  ) %>%
  ## TEMPORARY: Select only patients up till last_pt (set above)
  separate(id, into = c("site", "ptnum"), sep = "-", remove = FALSE) %>%
  filter(as.numeric(ptnum) <= last_pt)

## Data collected daily throughout study (daily data, PAD, etc)
daily_df <- post_to_df(
  httr::POST(
    url = rc_url,
    body = list(
      token = Sys.getenv("MOSAIC_IH"),
      content = "record",   ## export *records*
      format = "csv",       ## export as *CSV*
      ## Export forms collected daily during hospitalization
      forms = paste(
        c(
          "daily_data_collection_form_mds",
          "daily_pad_assessment",
          "icu_mobility_scale_form",
          "accelerometer_bedside_form",
          "accelerometer_placement_form",
          "sedline_daily_form",
          "daily_data_collection_form_extended"
        ),
        collapse = ","
      ),
      fields = c("id,redcap_event_name"),     ## additional fields
      rawOrLabel = "label", ## export factor *labels* vs numeric codes
      exportCheckboxLabel = TRUE ## export ckbox *labels* vs Unchecked/Checked
    )
  )
) %>%
  ## Remove any test patients from dataset and "Discharge Day" from events
  ##  (only forms done on Discharge Day are specimens and CADUCEUS; this is
  ##  easier than listing all other forms in API call above)
  filter(
    !str_detect(tolower(id), "test"),
    !(redcap_event_name == "Discharge Day")
  ) %>%
  ## Convert date/time variables to proper formats
  mutate_at(vars(one_of(date_vars)), ymd) %>%
  mutate_at(vars(one_of(dttm_vars)), ~ date(ymd_hm(.))) %>%
  ## TEMPORARY: Select only patients up till last_pt (set above)
  separate(id, into = c("site", "ptnum"), sep = "-", remove = FALSE) %>%
  filter(as.numeric(ptnum) <= last_pt)

## CADUCEUS, filled out on days 1/3/5 for all patients
## Step 1: Create dummy dataset to make sure all patients have a record for all
##  three days
cad_dummy <- data.frame(
  id = rep(all_ids, each = 3),
  redcap_event_name = rep(
    c("Enrollment /Trial Day 1", "Trial Day 3", "Trial Day 5"),
    length(all_ids)
  ),
  stringsAsFactors = FALSE
)

cad_df <- post_to_df(
  httr::POST(
    url = rc_url,
    body = list(
      token = Sys.getenv("MOSAIC_IH"),
      content = "record",   ## export *records*
      format = "csv",       ## export as *CSV*
      forms = "caduceus_2",
      fields = c("id"),     ## additional fields
      events = paste(
        c("enrollment_trial_d_arm_1","trial_day_3_arm_1","trial_day_5_arm_1"),
        collapse = ","
      ),
      rawOrLabel = "label", ## export factor *labels* vs numeric codes
      exportCheckboxLabel = TRUE ## export ckbox *labels* vs Unchecked/Checked
    )
  )
) %>%
  ## Remove any test patients from dataset
  filter(!str_detect(tolower(id), "test")) %>%
  ## Join with CADUCEUS dummy dataset
  right_join(cad_dummy, by = c("id", "redcap_event_name")) %>%
  ## Add on protocol - CADUCEUS added with protocol 1.02
  right_join(day1_df %>% dplyr::select(id, protocol), by = "id") %>%
  ## TEMPORARY: Select only patients up till last_pt (set above)
  separate(id, into = c("site", "ptnum"), sep = "-", remove = FALSE) %>%
  filter(
    as.numeric(ptnum) <= last_pt,
    ## Also remove any patients on protocol 1.01 - CADUCEUS not added until 1.02
    !(!is.na(protocol) & protocol == "Protocol v1.01")
  )

## Family Capacitation Survey, administered on event 7
famcap_df <- post_to_df(
  httr::POST(
    url = rc_url,
    body = list(
      token = Sys.getenv("MOSAIC_IH"),
      content = "record",   ## export *records*
      format = "csv",       ## export as *CSV*
      forms = paste(c("family_capcitation_survery"), collapse = ","),
      fields = c("id"),     ## additional fields
      events = "trial_day_7_arm_1", ## study day 7 event only
      rawOrLabel = "label", ## export factor *labels* vs numeric codes
      exportCheckboxLabel = TRUE ## export ckbox *labels* vs Unchecked/Checked
    )
  )
) %>%
  ## Remove any test patients from dataset
  filter(!str_detect(tolower(id), "test")) %>%
  ## Add on protocol - family capacitation added with protocol 1.02
  right_join(day1_df %>% dplyr::select(id, protocol), by = "id") %>%
  ## Add event name for patients with no form done
  mutate(redcap_event_name = "Trial Day 7") %>%
  ## Add indicators for whether each survey was done, fully or partially
  mutate_at(vars(famcap_comp), ~ str_detect(., "^Yes")) %>%
  ## TEMPORARY: Select only patients up till last_pt (set above)
  separate(id, into = c("site", "ptnum"), sep = "-", remove = FALSE) %>%
  filter(
    as.numeric(ptnum) <= last_pt,
    ## Also remove any patients on protocol 1.01 - survey not added until 1.02
    !(!is.na(protocol) & protocol == "Protocol v1.01")
  )

## Save dfs to a test data file in case API/wifi aren't working
redcap_dfs <- str_subset(ls(), "^[a-z,0-9]+\\_df$")
walk(
  redcap_dfs,
  ~ saveRDS(get(.), file = sprintf("testdata/%s.rds", .))
)

################################################################################
## Create Daily Dummy Dataset
##
## Several forms are filled out on a daily basis while patients are in the
##  hospital. A few forms, like the specimen log and CADUCEUS, are filled out on
##  specific study days, even *after* patients have died, been discharged, or
##  have withdrawn. This is good for record-keeping, but creates falsely missing
##  data for forms like PAD and daily data on those records not actually during
##  a hospitalization.
## Therefore, we create a dummy dataset to indicate patient status on each day
##  during the study period, with indicators for hospitalization, death,
##  discharge, and withdrawal. (A patient could withdraw, and then be known to
##  have died or been discharged, so it is possible for >1 of these indicators
##  to be true.)
## FUTURE WORK might include additional indicators for whether the patient was
##  in the ICU on a given day. There is no single variable in this dataset which
##  indicates this, so it would have to be determined by the dates tracking form.
################################################################################

all_ids <- sort(unique(c(day1_df$id, daily_df$id, famcap_df$id)))
all_events <- c("Enrollment /Trial Day 1", paste("Trial Day", 2:28))
  ## discharge day is not included - it falls on one of the above events, or
  ## after Trial Day 28 if the patient had a long hospitalization

dummy_df <- data.frame(
  id = rep(all_ids, each = length(all_events)),
  redcap_event_name = rep(all_events, length(all_ids)),
  stringsAsFactors = FALSE
) %>%
  ## Merge on relevant dates for each event: enrollment, discharge, death, w/d
  left_join(
    dplyr::select(
      day1_df, id, enroll_dttm, hospdis_dttm, death_dttm, studywd_dttm,
      studywd_writing_2
    ),
    by = "id"
  ) %>%
  ## Create new variable for study date, beginning with enrollment date/day 1
  ## In addition to helping determine status on each day, this will help clean
  ##  things like correct PAD assessment dates and daily data collection dates
  group_by(id) %>%
  mutate(
    ## Study day/study date
    study_day = 1:n(),
    study_date = enroll_dttm + study_day - 1,
    ## Indicators for various states
    ## NOTE: Transition days (eg, day of discharge) are counted as meeting
    ##  criteria for any applicable state. For example, if a patient dies in
    ##  the hospital on February 1, both `inhosp` and `deceased` will be TRUE
    ##  for that day.
    inhosp = case_when(
      study_date < enroll_dttm                          ~ as.logical(NA),
      !is.na(death_dttm) & study_date > death_dttm      ~ FALSE,
      !is.na(hospdis_dttm) & study_date > hospdis_dttm  ~ FALSE,
      !is.na(death_dttm) & study_date <= death_dttm     ~ TRUE,
      !is.na(hospdis_dttm) & study_date <= hospdis_dttm ~ TRUE,
      !is.na(studywd_writing_2)                         ~ as.logical(NA),
      TRUE                                              ~ TRUE
    ),
    deceased = case_when(
      study_date < enroll_dttm                          ~ as.logical(NA),
      !is.na(death_dttm) & study_date >= death_dttm     ~ TRUE,
      !is.na(studywd_writing_2)                         ~ as.logical(NA),
      TRUE                                              ~ FALSE
    ),
    withdrawn = case_when(
      study_date < enroll_dttm                          ~ as.logical(NA),
      !is.na(studywd_dttm) & study_date >= studywd_dttm ~ TRUE,
      TRUE                                              ~ FALSE
    ),
    ## Is this a transition day between states? eg, date of hospital discharge
    transition_day = case_when(
      !is.na(hospdis_dttm) & study_date == hospdis_dttm ~ TRUE,
      !is.na(death_dttm) & study_date == death_dttm     ~ TRUE,
      !is.na(studywd_dttm) & study_date == studywd_dttm ~ TRUE,
      TRUE                                              ~ FALSE
    )
  ) %>%
  ungroup()
  
## Update daily_df to include correct events per above, including all days the
## patient was hospitalized but not withdrawn
daily_df <- left_join(
  filter(dummy_df, inhosp, !withdrawn) %>%
    dplyr::select(id, redcap_event_name, study_date, transition_day),
  daily_df %>% mutate(in_daily = TRUE),
  by = c("id", "redcap_event_name")
)

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
## Contact Information
################################################################################

## -- Missingness checks -------------------------------------------------------
contact_missvars <- c(
  "title", "sex", "first_name", "last_name",
  # "name_common", ## removed by request 1/2/2019
  "ssn", "homeless"
)
contact_missing <- check_missing(
  df = day1_df, variables = contact_missvars, ddict = ih_ddict
)

## -- Custom checks ------------------------------------------------------------
## List of checks + messages
contact_codes <- tribble(
  ~ code,        ~ msg,
  "street",      "Patient not homeless, but missing street address",
  "city",        "Patient not homeless, but missing city",
  "state",       "Patient not homeless, but missing state",
  "zip",         "Patient not homeless, but missing ZIP",
  "preferred",   "No preferred method of contact checked",
  "pref_home",   "Preferred contact = home phone, but no home phone entered",
  "pref_cell",   "Preferred contact = mobile phone, but no mobile phone entered",
  "pref_work",   "Preferred contact = work phone, but no work phone entered",
  "pref_email",  "Preferred contact = email, but no email address entered",
  "pref_social", "Preferred contact = social media, but no info entered",
  "pref_other",  "Preferred contact = other, but no other phone entered"
) %>%
  as.data.frame()

## Initialize matrix
contact_issues <- matrix(
  FALSE, ncol = nrow(contact_codes), nrow = nrow(day1_df)
)
colnames(contact_issues) <- contact_codes$code
rownames(contact_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

## Street address for patients marked as not homeless
contact_issues[, "street"] <- with(day1_df, {
  !is.na(homeless) & homeless == "No" & is.na(pt_street)
})
contact_issues[, "city"] <- with(day1_df, {
  !is.na(homeless) & homeless == "No" & is.na(pt_city)
})
contact_issues[, "state"] <- with(day1_df, {
  !is.na(homeless) & homeless == "No" & is.na(pt_state)
})
contact_issues[, "zip"] <- with(day1_df, {
  !is.na(homeless) & homeless == "No" & is.na(pt_zip)
})

## Preferred method of contact
contact_issues[, "preferred"] <-
  rowSums(!is.na(day1_df[, grep("^pt\\_pref\\_[0-9]+$", names(day1_df))])) == 0

## Phone numbers/email are checked for formatting automatically by REDCap
## THANK YOU REDCAP
contact_issues[, "pref_home"] <- with(day1_df, {
  !is.na(pt_pref_0) & is.na(pt_home_phone)
})
contact_issues[, "pref_cell"] <- with(day1_df, {
  !is.na(pt_pref_1) & is.na(pt_mobile_phone)
})
contact_issues[, "pref_work"] <- with(day1_df, {
  !is.na(pt_pref_2) & is.na(pt_work_phone)
})
contact_issues[, "pref_email"] <- with(day1_df, {
  !is.na(pt_pref_3) & is.na(pt_email)
})
contact_issues[, "pref_social"] <- with(day1_df, {
  !is.na(pt_pref_4) & is.na(pt_socialmedia)
})
contact_issues[, "pref_other"] <- with(day1_df, {
  !is.na(pt_pref_99) & is.na(pt_other_phone)
})

## -- Create a final data.frame of errors + messages ---------------------------
contact_errors <- create_error_df(
  error_matrix = contact_issues, error_codes = contact_codes
)

contact_final <- bind_rows(contact_missing, contact_errors) %>%
  mutate(form = "Contact Information")

################################################################################
## Pre-Hospital Function
################################################################################

## -- Missingness checks -------------------------------------------------------
## NOTE: PASE, BDI added in later protocols, should not be present for all pts
prehosp_missvars <- c(
  str_subset(names(day1_df), "[^bdi|pase\\_]\\_comp\\_ph$")
)
prehosp_missing <- check_missing(
  df = day1_df, variables = prehosp_missvars, ddict = ih_ddict
)

## -- Custom checks ------------------------------------------------------------
## List of checks + messages
prehosp_codes <- tribble(
  ~ code,        ~ msg,
  ## -- General questions ------------------------------------------------------
  ## Assessment not done
  "gq_rsn",       "Missing reason general questions not completed",
  "gq_rsn_other", "Missing explanation of other reason general questions not done",
  ## Assessment done
  "gq_whom",            "Missing who completed general questions",
  "gq_edu",             "Missing years of education",
  "gq_marital",         "Missing marital status",
  "gq_living",          "Missing living status",
  "gq_living_other",    "Missing explanation of other living status",
  "gq_primary",         "Missing primary dwelling",
  "gq_primary_other",   "Missing explanation of other primary dwelling",
  "gq_owned",           "Missing whether primary dwelling is owned",
  "gq_deaf",            "Missing whether patient is deaf or has difficulty hearing",
  "gq_blind",           "Missing whether patient is blind or has difficulty seeing",
  "gq_smoking",         "Missing whether patient is a current or former smoker",
  "gq_cigday_now",      "Current smoker, but missing current cigarettes per day",
  "gq_yrssmoke_now",    "Current smoker, but missing current years of smoking",
  "gq_whenquit",        "Former smoker, but missing date quit",
  "gq_cigday_former",   "Former smoker, but missing former cigarettes per day",
  "gq_yrssmoke_former", "Former smoker, but missing former years of smoking",
  "gq_smokeless",       "Missing whether patient has used smokeless tobacco",
  "gq_nicotine",        "Missing whether patient uses nicotine replacement therapy",
  "gq_illicit",         "Missing illicit drug use history",
  "gq_illicit_now",     "Missing drug(s) patient is currently using",
  "gq_illicit_now_other", "Missing explanation of other current illicit drugs",
  "gq_illicit_former",  "Missing drug(s) patient formerly used",
  "gq_illicit_former_other", "Missing explanation of other former illicit drug(s)",
  ## -- PASE ---------------------------------------------------------------------
  ## NOTE: The PASE was added with protocol 1.02
  "pase_done",         "Missing whether PASE was completed",
  "pase_rsn",          "Missing reason PASE not completed",
  "pase_other",        "Missing explanation of other reason PASE not completed",
  "pase_whom",         "Missing who completed PASE",
  "pase_leisure",      "Missing PASE leisure activity",
  "pase_leisure_act",  "Missing description of sitting activities",
  "pase_leisure_hrs",  "Missing hours per day of sitting activities",
  "pase_walk",         "Missing PASE walk outside home",
  "pase_walk_hrs",     "Missing hours per day of walking",
  "pase_light",        "Missing PASE light sports",
  "pase_light_act",    "Missing description of light sports activities",
  "pase_light_hrs",    "Missing hours per day of light sports",
  "pase_mod",          "Missing PASE moderate sports",
  "pase_mod_act",      "Missing description of moderate sports activities",
  "pase_mod_hrs",      "Missing hours per day of moderate sports",
  "pase_stren",        "Missing PASE strenuous sports",
  "pase_stren_act",    "Missing description of strenuous sports activities",
  "pase_stren_hrs",    "Missing hours per day of strenuous sports",
  "pase_strength",     "Missing PASE strength exercises",
  "pase_strength_act", "Missing description of strength exercises",
  "pase_strength_hrs", "Missing hours per day of strength exercises",
  "pase_house_light",  "Missing PASE light household work",
  "pase_house_heavy",  "Missing PASE heavy household work",
  "pase_repairs",      "Missing PASE home repairs",
  "pase_yard",         "Missing PASE lawn work",
  "pase_garden",       "Missing PASE outdoor gardening",
  "pase_caring",       "Missing PASE caregiving",
  "pase_work",         "Missing PASE work for pay or volunteer",
  "pase_work_hrs",     "Missing hours per week worked or volunteered",
  "pase_work_act",     "Missing amount of physical activity for work or volunteering",
  ## -- Basic/Instrumental Activities of Daily Living --------------------------
  "biadl_rsn",         "Missing reason BIADL not completed",
  "biadl_other",       "Missing explanation of other reason BIADL not completed",
  "biadl_whom",        "Missing who completed BIADL",
  "biadl_bathe_help",  "Missing BIADL whether patient needed help to bathe",
  "biadl_bathe_diff",  "Missing BIADL whether patient had difficulty bathing",
  "biadl_dress_help",  "Missing BIADL whether patient needed help to dress",
  "biadl_dress_diff",  "Missing BIADL whether patient had difficulty dressing",
  "biadl_chair_help",  "Missing BIADL whether patient needed help to get in/out of chair",
  "biadl_chair_diff",  "Missing BIADL whether patient had difficulty getting in/out of chair",
  "biadl_walk_help",   "Missing BIADL whether patient needed help to walk around house",
  "biadl_walk_diff",   "Missing BIADL whether patient had difficulty walking around house",
  "biadl_eat_help",    "Missing BIADL whether patient needed help to eat",
  "biadl_eat_diff",    "Missing BIADL whether patient had difficulty eating",
  "biadl_groom_help",  "Missing BIADL whether patient needed help to groom",
  "biadl_groom_diff",  "Missing BIADL whether patient had difficulty grooming",
  "biadl_toilet_help", "Missing BIADL whether patient needed help to use the toilet",
  "biadl_toilet_diff", "Missing BIADL whether patient had difficulty toileting",
  "biadl_qumi_help",   "Missing BIADL whether patient needed help to walk 0.25 mile",
  "biadl_qumi_diff",   "Missing BIADL whether patient had difficulty walking 0.25 mile",
  "biadl_stair_help",  "Missing BIADL whether patient needed help to bathe",
  "biadl_stair_diff",  "Missing BIADL whether patient had difficulty bathing",
  "biadl_carry_help",  "Missing BIADL whether patient needed help to lift or carry",
  "biadl_carry_diff",  "Missing BIADL whether patient had difficulty lifting",
  "biadl_shop_help",   "Missing BIADL whether patient needed help to shop",
  "biadl_shop_diff",   "Missing BIADL whether patient had difficulty shopping",
  "biadl_house_help",  "Missing BIADL whether patient needed help with housework",
  "biadl_house_diff",  "Missing BIADL whether patient had difficulty with housework",
  "biadl_meal_help",   "Missing BIADL whether patient needed help with meal prep",
  "biadl_meal_diff",   "Missing BIADL whether patient had difficulty with meal prep",
  "biadl_meds_help",   "Missing BIADL whether patient needed help with meds",
  "biadl_meds_diff",   "Missing BIADL whether patient had difficulty with meds",
  "biadl_money_help",  "Missing BIADL whether patient needed help with finances",
  "biadl_money_diff",  "Missing BIADL whether patient had difficulty with finances",
  "biadl_farwalk",     "Missing BIADL how far patient walks on an average day",
  "biadl_numblocks",   "Missing BIADL number of blocks patient walks on an average day",
  "biadl_drive",       "Missing BIADL whether patient has driven a car",
  ## -- Life Space -------------------------------------------------------------
  "ls_rsn",           "Missing reason Life Space not completed",
  "ls_other",         "Missing explanation of other reason Life Space not completed",
  "ls_whom",          "Missing who completed Life Space",
  "ls_aids_any",      "Missing LS whether any aids are used",
  "ls_aids_conflict", "Both none and >=1 aid are marked in LS",
  "ls_room",          "Missing LS whether patient has been to other room",
  "ls_room_often",    "Missing LS how often patient visited other room",
  "ls_room_equip",    "Missing LS whether special equipment needed to get to other room",
  "ls_room_help",     "Missing LS whether help needed to get to other room",
  "ls_out",           "Missing LS whether patient has been outside",
  "ls_out_often",     "Missing LS how often patient went outside",
  "ls_out_equip",     "Missing LS whether special equipment needed to get outside",
  "ls_out_help",      "Missing LS whether help needed to get outside",
  "ls_neigh",         "Missing LS whether patient has been in neighborhood",
  "ls_neigh_often",   "Missing LS how often patient visited neighborhood",
  "ls_neigh_equip",   "Missing LS whether special equipment needed to get to neighborhood",
  "ls_neigh_help",    "Missing LS whether help needed to get to neighborhood",
  "ls_town",          "Missing LS whether patient has been to town",
  "ls_town_often",    "Missing LS how often patient visited town",
  "ls_town_equip",    "Missing LS whether special equipment needed to get to town",
  "ls_town_help",     "Missing LS whether help needed to get to town",
  "ls_far",           "Missing LS whether patient has been out of town",
  "ls_far_often",     "Missing LS how often patient gone out of town",
  "ls_far_equip",     "Missing LS whether special equipment needed to get out of town",
  "ls_far_help",      "Missing LS whether help needed to get out of town",
  ## -- Employment -------------------------------------------------------------
  "emp_rsn",           "Missing reason employment questions not completed",
  "emp_other",         "Missing explanation of other reason employment not completed",
  "emp_whom",          "Missing who completed employment",
  "emp_status",        "Missing employment status",
  "emp_notwork",       "Missing what patient was doing if not working",
  "emp_ftpt",          "Missing whether employment was full time or part time",
  "emp_hrs",           "Missing how many hours worked per week",
  "emp_occ",           "Missing occupation or type of work",
  "emp_occcode",       "Missing occupation code",
  "emp_occcode_other", "Missing explanation of unclassified occupation code",
  ## -- AUDIT ------------------------------------------------------------------
  "audit_rsn",   "Missing reason AUDIT not completed",
  "audit_other", "Missing explanation of other reason AUDIT not completed",
  "audit_whom",  "Missing who completed AUDIT",
  "audit_1",     "Missing AUDIT question 1",
  "audit_2",     "Patient drinks at least monthly, but missing AUDIT question 2",
  "audit_3",     "Missing AUDIT question 3",
  "audit_4",     "Missing AUDIT question 4",
  "audit_5",     "Missing AUDIT question 5",
  "audit_6",     "Missing AUDIT question 6",
  "audit_7",     "Missing AUDIT question 7",
  "audit_8",     "Missing AUDIT question 8",
  "audit_9",     "Missing AUDIT question 9",
  "audit_10",    "Missing AUDIT question 10",
  ## -- IQCODE -----------------------------------------------------------------
  "iqcode_rsn",   "Missing reason IQCODE not completed",
  "iqcode_other", "Missing explanation of other reason IQCODE not completed",
  ## IQCODE can *only* be completed by surrogate/caregiver, per validation
  # "iqcode_whom",  "Missing who completed IQCODE",
  "iqcode_1",     "Missing IQCODE question 1",
  "iqcode_2",     "Missing IQCODE question 2",
  "iqcode_3",     "Missing IQCODE question 3",
  "iqcode_4",     "Missing IQCODE question 4",
  "iqcode_5",     "Missing IQCODE question 5",
  "iqcode_6",     "Missing IQCODE question 6",
  "iqcode_7",     "Missing IQCODE question 7",
  "iqcode_8",     "Missing IQCODE question 8",
  "iqcode_9",     "Missing IQCODE question 9",
  "iqcode_10",    "Missing IQCODE question 10",
  "iqcode_11",    "Missing IQCODE question 11",
  "iqcode_12",    "Missing IQCODE question 12",
  "iqcode_13",    "Missing IQCODE question 13",
  "iqcode_14",    "Missing IQCODE question 14",
  "iqcode_15",    "Missing IQCODE question 15",
  "iqcode_16",    "Missing IQCODE question 16",
  ## -- BDI --------------------------------------------------------------------
  ## NOTE: The BDI was added with protocol 1.05
  "bdi_done",  "Missing whether BDI was completed",
  "bdi_rsn",   "Missing reason BDI not completed",
  "bdi_other", "Missing explanation of other reason BDI not completed",
  "bdi_whom",  "Missing who completed BDI",
  "bdi_1",     "Missing BDI question 1",
  "bdi_2",     "Missing BDI question 2",
  "bdi_3",     "Missing BDI question 3",
  "bdi_4",     "Missing BDI question 4",
  "bdi_5",     "Missing BDI question 5",
  "bdi_6",     "Missing BDI question 6",
  "bdi_7",     "Missing BDI question 7",
  "bdi_8",     "Missing BDI question 8",
  "bdi_9",     "Missing BDI question 9",
  "bdi_10",    "Missing BDI question 10",
  "bdi_11",    "Missing BDI question 11",
  "bdi_12",    "Missing BDI question 12",
  "bdi_13",    "Missing BDI question 13",
  "bdi_14",    "Missing BDI question 14",
  "bdi_15",    "Missing BDI question 15",
  "bdi_16",    "Missing BDI question 16",
  "bdi_17",    "Missing BDI question 17",
  "bdi_18",    "Missing BDI question 18",
  "bdi_19",    "Missing BDI question 19",
  "bdi_20",    "Missing BDI question 20",
  "bdi_21",    "Missing BDI question 21",
  ## -- Zarit ------------------------------------------------------------------
  "zarit_rsn",   "Missing reason Zarit not completed",
  "zarit_other", "Missing explanation of other reason Zarit not completed",
  "zarit_whom",  "Missing who completed Zarit",
  "zarit_1",     "Missing Zarit question 1",
  "zarit_2",     "Missing Zarit question 2",
  "zarit_3",     "Missing Zarit question 3",
  "zarit_4",     "Missing Zarit question 4",
  "zarit_5",     "Missing Zarit question 5",
  "zarit_6",     "Missing Zarit question 6",
  "zarit_7",     "Missing Zarit question 7",
  "zarit_8",     "Missing Zarit question 8",
  "zarit_9",     "Missing Zarit question 9",
  "zarit_10",     "Missing Zarit question 10",
  "zarit_11",     "Missing Zarit question 11",
  "zarit_12",     "Missing Zarit question 12",
  ## -- Memory/Behavior --------------------------------------------------------
  "mb_rsn",   "Missing reason Memory/Behavior not completed",
  "mb_other", "Missing explanation of other reason Memory/Behavior not completed",
  "mb_1",     "Missing Memory/Behavior question 1, yes or no",
  "mb_1a",    "Missing Memory/Behavior question 1a, whether bothered",
  "mb_2",     "Missing Memory/Behavior question 2, yes or no",
  "mb_2a",    "Missing Memory/Behavior question 2a, whether bothered",
  "mb_3",     "Missing Memory/Behavior question 3, yes or no",
  "mb_3a",    "Missing Memory/Behavior question 3a, whether bothered",
  "mb_4",     "Missing Memory/Behavior question 4, yes or no",
  "mb_4a",    "Missing Memory/Behavior question 4a, whether bothered",
  "mb_5",     "Missing Memory/Behavior question 5, yes or no",
  "mb_5a",    "Missing Memory/Behavior question 5a, whether bothered",
  "mb_6",     "Missing Memory/Behavior question 6, yes or no",
  "mb_6a",    "Missing Memory/Behavior question 6a, whether bothered",
  "mb_7",     "Missing Memory/Behavior question 7, yes or no",
  "mb_7a",    "Missing Memory/Behavior question 7a, whether bothered",
  "mb_8",     "Missing Memory/Behavior question 8, yes or no",
  "mb_8a",    "Missing Memory/Behavior question 8a, whether bothered",
  "mb_9",     "Missing Memory/Behavior question 9, yes or no",
  "mb_9a",    "Missing Memory/Behavior question 9a, whether bothered",
  "mb_10",    "Missing Memory/Behavior question 10, yes or no",
  "mb_10a",   "Missing Memory/Behavior question 10a, whether bothered",
  "mb_11",    "Missing Memory/Behavior question 11, yes or no",
  "mb_11a",   "Missing Memory/Behavior question 11a, whether bothered",
  "mb_12",    "Missing Memory/Behavior question 12, yes or no",
  "mb_12a",   "Missing Memory/Behavior question 12a, whether bothered",
  "mb_13",    "Missing Memory/Behavior question 13, yes or no",
  "mb_13a",   "Missing Memory/Behavior question 13a, whether bothered",
  "mb_14",    "Missing Memory/Behavior question 14, yes or no",
  "mb_14a",   "Missing Memory/Behavior question 14a, whether bothered",
  "mb_15",    "Missing Memory/Behavior question 15, yes or no",
  "mb_15a",   "Missing Memory/Behavior question 15a, whether bothered",
  "mb_16",    "Missing Memory/Behavior question 16, yes or no",
  "mb_16a",   "Missing Memory/Behavior question 16a, whether bothered",
  "mb_17",    "Missing Memory/Behavior question 17, yes or no",
  "mb_17a",   "Missing Memory/Behavior question 17a, whether bothered",
  "mb_18",    "Missing Memory/Behavior question 18, yes or no",
  "mb_18a",   "Missing Memory/Behavior question 18a, whether bothered",
  "mb_19",    "Missing Memory/Behavior question 19, yes or no",
  "mb_19a",   "Missing Memory/Behavior question 19a, whether bothered",
  "mb_20",    "Missing Memory/Behavior question 20, yes or no",
  "mb_20a",   "Missing Memory/Behavior question 20a, whether bothered",
  "mb_21",    "Missing Memory/Behavior question 21, yes or no",
  "mb_21a",   "Missing Memory/Behavior question 21a, whether bothered",
  "mb_22",    "Missing Memory/Behavior question 22, yes or no",
  "mb_22a",   "Missing Memory/Behavior question 22a, whether bothered",
  "mb_23",    "Missing Memory/Behavior question 23, yes or no",
  "mb_23a",   "Missing Memory/Behavior question 23a, whether bothered",
  "mb_24",    "Missing Memory/Behavior question 24, yes or no",
  "mb_24a",   "Missing Memory/Behavior question 24a, whether bothered"
) %>%
  as.data.frame()

## Initialize matrix
prehosp_issues <- matrix(
  FALSE, ncol = nrow(prehosp_codes), nrow = nrow(day1_df)
)
colnames(prehosp_issues) <- prehosp_codes$code
rownames(prehosp_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

## -- General questions --------------------------------------------------------
## Questions not done
prehosp_issues[, "gq_rsn"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph == "No" & is.na(gq_comp_ph_rsn)
})
prehosp_issues[, "gq_rsn_other"] <- with(day1_df, {
  !is.na(gq_comp_ph_rsn) &
    gq_comp_ph_rsn == "Other (explain)" & is.na(gq_comp_ph_other)
})

## Questions done
prehosp_issues[, "gq_whom"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_who_ph)
})

prehosp_issues[, "gq_edu"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_education)
})
prehosp_issues[, "gq_marital"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_marital)
})
prehosp_issues[, "gq_living"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_living_status)
})
prehosp_issues[, "gq_living_other"] <- with(day1_df, {
  !is.na(gq_living_status) & gq_living_status == "Other" &
    is.na(gq_living_status_other)
})
prehosp_issues[, "gq_primary"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_pt_dwell)
})
prehosp_issues[,"gq_primary_other"] <- with(day1_df, {
  !is.na(gq_pt_dwell) & gq_pt_dwell == "Other" & is.na(gq_pt_dwell_other)
})
prehosp_issues[, "gq_owned"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_dwell_owned)
})
prehosp_issues[, "gq_deaf"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_deaf)
})
prehosp_issues[, "gq_blind"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_blind)
})
prehosp_issues[, "gq_smoking"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_smoke)
})
prehosp_issues[, "gq_cigday_now"] <- with(day1_df, {
  !is.na(gq_smoke) & gq_smoke == "Current only" &
    is.na(gq_smoke_current_num) & is.na(gq_smoke_current_num_na)
})
prehosp_issues[, "gq_yrssmoke_now"] <- with(day1_df, {
  !is.na(gq_smoke) & gq_smoke == "Current only" &
    is.na(gq_smoke_current_years) & is.na(gq_smoke_current_years_na)
})
prehosp_issues[, "gq_whenquit"] <- with(day1_df, {
  !is.na(gq_smoke) & gq_smoke == "Former only" &
    is.na(gq_smoke_former_quit) & is.na(gq_smoke_former_quit_na)
})
prehosp_issues[, "gq_cigday_former"] <- with(day1_df, {
  !is.na(gq_smoke) & gq_smoke == "Former only" &
    is.na(gq_smoke_former_num) & is.na(gq_smoke_former_num_na)
})
prehosp_issues[, "gq_yrssmoke_former"] <- with(day1_df, {
  !is.na(gq_smoke) & gq_smoke == "Former only" &
    is.na(gq_smoke_former_years) & is.na(gq_smoke_former_years_na)
})
prehosp_issues[, "gq_smokeless"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_tobacco)
})
prehosp_issues[, "gq_nicotine"] <- with(day1_df, {
  !is.na(gq_comp_ph) & gq_comp_ph & is.na(gq_nicotine)
})
prehosp_issues[, "gq_illicit"] <-
  rowSums(!is.na(day1_df[, paste0("gq_illicit_drug_", c(1, 2, 0, 99))])) == 0
prehosp_issues[, "gq_illicit_now"] <- with(day1_df, {
  !is.na(gq_illicit_drug_1) &
    rowSums(!is.na(day1_df[, paste0("gq_illicit_curr_", 1:5)])) == 0
})
prehosp_issues[, "gq_illicit_now_other"] <- with(day1_df, {
  !is.na(gq_illicit_curr_5) & is.na(gq_illicit_curr_other)
})
prehosp_issues[, "gq_illicit_former"] <- with(day1_df, {
  !is.na(gq_illicit_drug_1) &
    rowSums(!is.na(day1_df[, paste0("gq_illicit_form_", 1:5)])) == 0
})
prehosp_issues[, "gq_illicit_former_other"] <- with(day1_df, {
  !is.na(gq_illicit_form_5) & is.na(gq_illicit_form_other)
})

## -- PASE ---------------------------------------------------------------------
## NOTE: The PASE was added with protocol 1.02

## Was PASE done?
prehosp_issues[, "pase_done"] <- with(day1_df, {
  !is.na(protocol) & protocol != "Protocol v1.01" & is.na(pase_comp_ph)
})

## PASE not done
prehosp_issues[, "pase_rsn"] <- with(day1_df, {
  !is.na(pase_comp_ph) & !pase_comp_ph & is.na(pase_comp_ph_rsn)
})
prehosp_issues[, "pase_other"] <- with(day1_df, {
  !is.na(pase_comp_ph_rsn) & pase_comp_ph_rsn == "Other (explain)" &
    is.na(pase_comp_ph_other)
})

## PASE done
prehosp_issues[, "pase_whom"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_who_ph)
})

prehosp_issues[, "pase_leisure"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_1)
})
## Initial question answered seldom, sometimes, or often -> two followups
prehosp_issues[, "pase_leisure_act"] <- with(day1_df, {
  !is.na(pase_1) & str_detect(pase_1, "\\(.+\\)$") &
    (is.na(pase_1a) | pase_1a == "")
})
prehosp_issues[, "pase_leisure_hrs"] <- with(day1_df, {
  !is.na(pase_1) & str_detect(pase_1, "\\(.+\\)$") & is.na(pase_1b)
})

prehosp_issues[, "pase_walk"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_2)
})
prehosp_issues[, "pase_walk_hrs"] <- with(day1_df, {
  !is.na(pase_2) & str_detect(pase_2, "\\(.+\\)$") & is.na(pase_2a)
})

prehosp_issues[, "pase_light"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_3)
})
prehosp_issues[, "pase_light_act"] <- with(day1_df, {
  !is.na(pase_3) & str_detect(pase_3, "\\(.+\\)$") &
    ((is.na(pase_3a) | pase_3a == "") & is.na(pase_3a2))
})
prehosp_issues[, "pase_light_hrs"] <- with(day1_df, {
  !is.na(pase_3) & str_detect(pase_3, "\\(.+\\)$") & is.na(pase_3b)
})

prehosp_issues[, "pase_mod"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_4)
})
prehosp_issues[, "pase_mod_act"] <- with(day1_df, {
  !is.na(pase_4) & str_detect(pase_4, "\\(.+\\)$") &
    ((is.na(pase_4a) | pase_4a == "") & is.na(pase_4a2))
})
prehosp_issues[, "pase_mod_hrs"] <- with(day1_df, {
  !is.na(pase_4) & str_detect(pase_4, "\\(.+\\)$") & is.na(pase_4b)
})

prehosp_issues[, "pase_stren"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_5)
})
prehosp_issues[, "pase_stren_act"] <- with(day1_df, {
  !is.na(pase_5) & str_detect(pase_5, "\\(.+\\)$") &
    ((is.na(pase_5a) | pase_5a == "") & is.na(pase_5a2))
})
prehosp_issues[, "pase_stren_hrs"] <- with(day1_df, {
  !is.na(pase_5) & str_detect(pase_5, "\\(.+\\)$") & is.na(pase_5b)
})

prehosp_issues[, "pase_strength"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_6)
})
prehosp_issues[, "pase_strength_act"] <- with(day1_df, {
  !is.na(pase_6) & str_detect(pase_6, "\\(.+\\)$") &
    ((is.na(pase_6a) | pase_6a == "") & is.na(pase_6a2))
})
prehosp_issues[, "pase_strength_hrs"] <- with(day1_df, {
  !is.na(pase_6) & str_detect(pase_6, "\\(.+\\)$") & is.na(pase_6b)
})

prehosp_issues[, "pase_house_light"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_7)
})
prehosp_issues[, "pase_house_heavy"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_8)
})

prehosp_issues[, "pase_repairs"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_9a)
})
prehosp_issues[, "pase_yard"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_9b)
})
prehosp_issues[, "pase_garden"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_9c)
})
prehosp_issues[, "pase_caring"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_9d)
})
prehosp_issues[, "pase_work"] <- with(day1_df, {
  !is.na(pase_comp_ph) & pase_comp_ph & is.na(pase_10)
})
prehosp_issues[, "pase_work_hrs"] <- with(day1_df, {
  !is.na(pase_10) & pase_10 == "Yes" &
    ((is.na(pase_10a) | pase_10a == "") & is.na(pase_10a2))
})
prehosp_issues[, "pase_work_act"] <- with(day1_df, {
  !is.na(pase_10) & pase_10 == "Yes" & is.na(pase_10b)
})

## -- BIADL --------------------------------------------------------------------

## BIADL not done
prehosp_issues[, "biadl_rsn"] <- with(day1_df, {
  !is.na(adl_comp_ph) & !adl_comp_ph & is.na(adl_comp_ph_rsn)
})
prehosp_issues[, "biadl_other"] <- with(day1_df, {
  !is.na(adl_comp_ph_rsn) & adl_comp_ph_rsn == "Other (explain)" &
    is.na(adl_comp_ph_other)
})

## BIADL done
prehosp_issues[, "biadl_whom"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_who_ph)
})

## Most questions have two parts: a) Need help? b) Have difficulty?
## Part b only filled out if part a = No help needed
prehosp_issues[, "biadl_bathe_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_1a)
})
prehosp_issues[, "biadl_bathe_diff"] <- with(day1_df, {
  !is.na(adl_1a) & adl_1a == "No help needed" & is.na(adl_1b)
})

prehosp_issues[, "biadl_dress_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_2a)
})
prehosp_issues[, "biadl_dress_diff"] <- with(day1_df, {
  !is.na(adl_2a) & adl_2a == "No help needed" & is.na(adl_2b)
})

prehosp_issues[, "biadl_chair_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_3a)
})
prehosp_issues[, "biadl_chair_diff"] <- with(day1_df, {
  !is.na(adl_3a) & adl_3a == "No help needed" & is.na(adl_3b)
})

prehosp_issues[, "biadl_walk_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_4a)
})
prehosp_issues[, "biadl_walk_diff"] <- with(day1_df, {
  !is.na(adl_4a) & adl_4a == "No help needed" & is.na(adl_4b)
})

prehosp_issues[, "biadl_eat_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_5a)
})
prehosp_issues[, "biadl_eat_diff"] <- with(day1_df, {
  !is.na(adl_5a) & adl_5a == "No help needed" & is.na(adl_5b)
})

prehosp_issues[, "biadl_groom_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_6a)
})
prehosp_issues[, "biadl_groom_diff"] <- with(day1_df, {
  !is.na(adl_6a) & adl_6a == "No help needed" & is.na(adl_6b)
})

prehosp_issues[, "biadl_toilet_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_7a)
})
prehosp_issues[, "biadl_toilet_diff"] <- with(day1_df, {
  !is.na(adl_7a) & adl_7a == "No help needed" & is.na(adl_7b)
})

prehosp_issues[, "biadl_qumi_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_8a)
})
prehosp_issues[, "biadl_qumi_diff"] <- with(day1_df, {
  !is.na(adl_8a) & adl_8a == "No help needed" & is.na(adl_8b)
})

prehosp_issues[, "biadl_stair_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_9a)
})
prehosp_issues[, "biadl_stair_diff"] <- with(day1_df, {
  !is.na(adl_9a) & adl_9a == "No help needed" & is.na(adl_9b)
})

prehosp_issues[, "biadl_carry_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_10a)
})
prehosp_issues[, "biadl_carry_diff"] <- with(day1_df, {
  !is.na(adl_10a) & adl_10a == "No help needed" & is.na(adl_10b)
})

prehosp_issues[, "biadl_shop_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_11a)
})
prehosp_issues[, "biadl_shop_diff"] <- with(day1_df, {
  !is.na(adl_11a) & adl_11a == "No help needed" & is.na(adl_11b)
})

prehosp_issues[, "biadl_house_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_12a)
})
prehosp_issues[, "biadl_house_diff"] <- with(day1_df, {
  !is.na(adl_12a) & adl_12a == "No help needed" & is.na(adl_12b)
})

prehosp_issues[, "biadl_meal_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_13a)
})
prehosp_issues[, "biadl_meal_diff"] <- with(day1_df, {
  !is.na(adl_13a) & adl_13a == "No help needed" & is.na(adl_13b)
})

prehosp_issues[, "biadl_meds_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_14a)
})
prehosp_issues[, "biadl_meds_diff"] <- with(day1_df, {
  !is.na(adl_14a) & adl_14a == "No help needed" & is.na(adl_14b)
})

prehosp_issues[, "biadl_money_help"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_15a)
})
prehosp_issues[, "biadl_money_diff"] <- with(day1_df, {
  !is.na(adl_15a) & adl_15a == "No help needed" & is.na(adl_15b)
})

prehosp_issues[, "biadl_farwalk"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_16)
})
prehosp_issues[, "biadl_numblocks"] <- with(day1_df, {
  !is.na(adl_16) & adl_16 == "More than 1 block" & is.na(adl_16_blocks)
})

prehosp_issues[, "biadl_drive"] <- with(day1_df, {
  !is.na(adl_comp_ph) & adl_comp_ph & is.na(adl_17)
})

## -- Life Space ---------------------------------------------------------------
## LS not done
prehosp_issues[, "ls_rsn"] <- with(day1_df, {
  !is.na(ls_comp_ph) & !ls_comp_ph & is.na(ls_comp_ph_rsn)
})
prehosp_issues[, "ls_other"] <- with(day1_df, {
  !is.na(ls_comp_ph_rsn) & ls_comp_ph_rsn == "Other (explain)" &
    is.na(ls_comp_ph_other)
})

## LS done
prehosp_issues[, "ls_whom"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph & is.na(ls_who_ph)
})

prehosp_issues[, "ls_aids_any"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph &
    rowSums(!is.na(day1_df[, grep("^ls\\_aid\\_[0-9]+$", names(day1_df))])) == 0
})
prehosp_issues[, "ls_aids_conflict"] <- with(day1_df, {
  !is.na(ls_aid_0) &
    rowSums(!is.na(day1_df[, grep("^ls\\_aid\\_[1-9][0-9]*$", names(day1_df))])) > 0
})

## For each space, if Qa is Yes, b-d should be filled out
prehosp_issues[, "ls_room"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph & is.na(ls_other_room)
})
prehosp_issues[, "ls_room_often"] <- with(day1_df, {
  !is.na(ls_other_room) & ls_other_room == "Yes" & is.na(ls_other_room_often)
})
prehosp_issues[, "ls_room_equip"] <- with(day1_df, {
  !is.na(ls_other_room) & ls_other_room == "Yes" & is.na(ls_other_room_equip)
})
prehosp_issues[, "ls_room_help"] <- with(day1_df, {
  !is.na(ls_other_room) & ls_other_room == "Yes" & is.na(ls_other_room_help)
})

prehosp_issues[, "ls_out"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph & is.na(ls_outside_home)
})
prehosp_issues[, "ls_out_often"] <- with(day1_df, {
  !is.na(ls_outside_home) & ls_outside_home == "Yes" & is.na(ls_outside_home_often)
})
prehosp_issues[, "ls_out_equip"] <- with(day1_df, {
  !is.na(ls_outside_home) & ls_outside_home == "Yes" & is.na(ls_outside_home_equip)
})
prehosp_issues[, "ls_out_help"] <- with(day1_df, {
  !is.na(ls_outside_home) & ls_outside_home == "Yes" & is.na(ls_outside_home_help)
})

prehosp_issues[, "ls_neigh"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph & is.na(ls_neighborhood)
})
prehosp_issues[, "ls_neigh_often"] <- with(day1_df, {
  !is.na(ls_neighborhood) & ls_neighborhood == "Yes" & is.na(ls_neighborhood_often)
})
prehosp_issues[, "ls_neigh_equip"] <- with(day1_df, {
  !is.na(ls_neighborhood) & ls_neighborhood == "Yes" & is.na(ls_neighborhood_equip)
})
prehosp_issues[, "ls_neigh_help"] <- with(day1_df, {
  !is.na(ls_neighborhood) & ls_neighborhood == "Yes" & is.na(ls_neighborhood_equip)
})

prehosp_issues[, "ls_town"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph & is.na(ls_town)
})
prehosp_issues[, "ls_town_often"] <- with(day1_df, {
  !is.na(ls_town) & ls_town == "Yes" & is.na(ls_town_often)
})
prehosp_issues[, "ls_town_equip"] <- with(day1_df, {
  !is.na(ls_town) & ls_town == "Yes" & is.na(ls_town_equip)
})
prehosp_issues[, "ls_town_help"] <- with(day1_df, {
  !is.na(ls_town) & ls_town == "Yes" & is.na(ls_town_equip)
})

prehosp_issues[, "ls_far"] <- with(day1_df, {
  !is.na(ls_comp_ph) & ls_comp_ph & is.na(ls_outside_town)
})
prehosp_issues[, "ls_far_often"] <- with(day1_df, {
  !is.na(ls_outside_town) & ls_outside_town == "Yes" & is.na(ls_outside_town_often)
})
prehosp_issues[, "ls_far_equip"] <- with(day1_df, {
  !is.na(ls_outside_town) & ls_outside_town == "Yes" & is.na(ls_outside_town_equip)
})
prehosp_issues[, "ls_far_help"] <- with(day1_df, {
  !is.na(ls_outside_town) & ls_outside_town == "Yes" & is.na(ls_outside_town_equip)
})

## -- Employment ---------------------------------------------------------------
## Employment not done
prehosp_issues[, "emp_rsn"] <- with(day1_df, {
  !is.na(emp_comp_ph) & !emp_comp_ph & is.na(emp_comp_ph_rsn)
})
prehosp_issues[, "emp_other"] <- with(day1_df, {
  !is.na(emp_comp_ph_rsn) & emp_comp_ph_rsn == "Other (explain)" &
    is.na(emp_comp_ph_other)
})

## Employment done
prehosp_issues[, "emp_whom"] <- with(day1_df, {
  !is.na(emp_comp_ph) & emp_comp_ph & is.na(emp_who_ph)
})

prehosp_issues[, "emp_status"] <- with(day1_df, {
  !is.na(emp_comp_ph) & emp_comp_ph & is.na(emp_status_ph)
})
prehosp_issues[, "emp_notwork"] <- with(day1_df, {
  !is.na(emp_status_ph) & emp_status_ph == "Not working" & is.na(emp_unemp_ph)
})
prehosp_issues[, "emp_ftpt"] <- with(day1_df, {
  !is.na(emp_status_ph) & emp_status_ph == "Working" & is.na(emp_work_status_ph)
})
prehosp_issues[, "emp_hrs"] <- with(day1_df, {
  !is.na(emp_status_ph) & emp_status_ph == "Working" & is.na(emp_hours_ph)
})
prehosp_issues[, "emp_occ"] <- with(day1_df, {
  !is.na(emp_status_ph) & emp_status_ph == "Working" & is.na(emp_hours_na_ph)
}) ## `emp_hours_na_ph` is actually free text asking for desc of occupation
prehosp_issues[, "emp_occcode"] <- with(day1_df, {
  !is.na(emp_status_ph) & emp_status_ph == "Working" & is.na(emp_occ_ph)
})
prehosp_issues[, "emp_occcode_other"] <- with(day1_df, {
  !is.na(emp_occ_ph) & emp_occ_ph == "25. Other not classified above" &
    is.na(emp_occ_other_ph)
})

## -- AUDIT --------------------------------------------------------------------
## AUDIT not done
prehosp_issues[, "audit_rsn"] <- with(day1_df, {
  !is.na(audit_comp_ph) & !audit_comp_ph & is.na(audit_comp_ph_rsn)
})
prehosp_issues[, "audit_other"] <- with(day1_df, {
  !is.na(audit_comp_ph_rsn) & audit_comp_ph_rsn == "Other (explain)" &
    is.na(audit_comp_ph_other)
})

## AUDIT done
prehosp_issues[, "audit_whom"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_who_ph)
})

prehosp_issues[, "audit_1"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_1_ph)
})
## Q2 only filled out if patient drinks >= monthly
prehosp_issues[, "audit_2"] <- with(day1_df, {
  !is.na(audit_1_ph) & audit_1_ph %in% c(
    "Monthly or less", "2-3 times a week", "2-4 times a month",
    "4 or more times a week"
  ) &
    is.na(audit_2_ph)
})
prehosp_issues[, "audit_3"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_3_ph)
})
prehosp_issues[, "audit_4"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_4_ph)
})
prehosp_issues[, "audit_5"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_5_ph)
})
prehosp_issues[, "audit_6"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_6_ph)
})
prehosp_issues[, "audit_7"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_7_ph)
})
prehosp_issues[, "audit_8"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_8_ph)
})
prehosp_issues[, "audit_9"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_9_ph)
})
prehosp_issues[, "audit_10"] <- with(day1_df, {
  !is.na(audit_comp_ph) & audit_comp_ph & is.na(audit_10_ph)
})

## -- IQCODE -------------------------------------------------------------------
## IQCODE not done
prehosp_issues[, "iqcode_rsn"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & !iqcode_comp_ph & is.na(iqcode_comp_ph_rsn)
})
prehosp_issues[, "iqcode_other"] <- with(day1_df, {
  !is.na(iqcode_comp_ph_rsn) & iqcode_comp_ph_rsn == "Other (explain)" &
    is.na(iqcode_comp_ph_other)
})

## IQCODE done
prehosp_issues[, "iqcode_1"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_1_ph)
})
prehosp_issues[, "iqcode_2"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_2_ph)
})
prehosp_issues[, "iqcode_3"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_3_ph)
})
prehosp_issues[, "iqcode_4"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_4_ph)
})
prehosp_issues[, "iqcode_5"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_5_ph)
})
prehosp_issues[, "iqcode_6"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_6_ph)
})
prehosp_issues[, "iqcode_7"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_7_ph)
})
prehosp_issues[, "iqcode_8"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_8_ph)
})
prehosp_issues[, "iqcode_9"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_9_ph)
})
prehosp_issues[, "iqcode_10"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_10_ph)
})
prehosp_issues[, "iqcode_11"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_11_ph)
})
prehosp_issues[, "iqcode_12"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_12_ph)
})
prehosp_issues[, "iqcode_13"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_13_ph)
})
prehosp_issues[, "iqcode_14"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_14_ph)
})
prehosp_issues[, "iqcode_15"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_15_ph)
})
prehosp_issues[, "iqcode_16"] <- with(day1_df, {
  !is.na(iqcode_comp_ph) & iqcode_comp_ph & is.na(iqcode_16_ph)
})

## -- BDI ----------------------------------------------------------------------
## NOTE: BDI was added with protocol 1.05

## Was BDI done?
prehosp_issues[, "bdi_done"] <- with(day1_df, {
  !is.na(protocol) & protocol %in% c(paste0("Protocol v.1.0", c(5))) &
    is.na(bdi_comp_ph)
})

## BDI not done
prehosp_issues[, "bdi_rsn"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & !bdi_comp_ph & is.na(bdi_comp_ph_rsn)
})
prehosp_issues[, "bdi_other"] <- with(day1_df, {
  !is.na(bdi_comp_ph_rsn) & bdi_comp_ph_rsn == "Other (explain)" &
    is.na(bdi_comp_ph_other)
})

## BDI done
prehosp_issues[, "bdi_whom"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_who_ph)
})

prehosp_issues[, "bdi_1"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_1_ph)
})
prehosp_issues[, "bdi_2"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_2_ph)
})
prehosp_issues[, "bdi_3"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_3_ph)
})
prehosp_issues[, "bdi_4"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_4_ph)
})
prehosp_issues[, "bdi_5"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_5_ph)
})
prehosp_issues[, "bdi_6"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_6_ph)
})
prehosp_issues[, "bdi_7"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_7_ph)
})
prehosp_issues[, "bdi_8"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_8_ph)
})
prehosp_issues[, "bdi_9"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_9_ph)
})
prehosp_issues[, "bdi_10"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_10_ph)
})
prehosp_issues[, "bdi_11"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_11_ph)
})
prehosp_issues[, "bdi_12"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_12_ph)
})
prehosp_issues[, "bdi_13"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_13_ph)
})
prehosp_issues[, "bdi_14"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_14_ph)
})
prehosp_issues[, "bdi_15"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_15_ph)
})
prehosp_issues[, "bdi_16"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_16_ph)
})
prehosp_issues[, "bdi_17"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_17_ph)
})
prehosp_issues[, "bdi_18"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_18_ph)
})
prehosp_issues[, "bdi_19"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_19_ph)
})
prehosp_issues[, "bdi_20"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_20_ph)
})
prehosp_issues[, "bdi_21"] <- with(day1_df, {
  !is.na(bdi_comp_ph) & bdi_comp_ph & is.na(bdi_21_ph)
})

## -- Zarit --------------------------------------------------------------------
## Zarit not done
prehosp_issues[, "zarit_rsn"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & !zarit_comp_ph & is.na(zarit_comp_ph_rsn)
})
prehosp_issues[, "zarit_other"] <- with(day1_df, {
  !is.na(zarit_comp_ph_rsn) & zarit_comp_ph_rsn == "Other (explain)" &
    is.na(zarit_comp_ph_other)
})

## Zarit done
prehosp_issues[, "zarit_whom"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph &
    (is.na(caregiver_ph) | caregiver_ph == "")
})

prehosp_issues[, "zarit_1"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_1)
})
prehosp_issues[, "zarit_2"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_2)
})
prehosp_issues[, "zarit_3"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_3)
})
prehosp_issues[, "zarit_4"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_4)
})
prehosp_issues[, "zarit_5"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_5)
})
prehosp_issues[, "zarit_6"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_6)
})
prehosp_issues[, "zarit_7"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_7)
})
prehosp_issues[, "zarit_8"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_8)
})
prehosp_issues[, "zarit_9"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_9)
})
prehosp_issues[, "zarit_10"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_10)
})
prehosp_issues[, "zarit_11"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_11)
})
prehosp_issues[, "zarit_12"] <- with(day1_df, {
  !is.na(zarit_comp_ph) & zarit_comp_ph & is.na(zarit_12)
})

## -- Memory/Behavior ----------------------------------------------------------
## M/B not done
prehosp_issues[, "mb_rsn"] <- with(day1_df, {
  !is.na(memory_comp_ph) & !memory_comp_ph & is.na(memory_comp_ph_rsn)
})
prehosp_issues[, "mb_other"] <- with(day1_df, {
  !is.na(memory_comp_ph_rsn) & memory_comp_ph_rsn == "Other (explain)" &
    is.na(memory_comp_ph_other)
})

## Memory/Behavior done
prehosp_issues[, "mb_1"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_1a)
})
prehosp_issues[, "mb_1a"] <- with(day1_df, {
  !is.na(membehav_1a) & membehav_1a == "Yes" & is.na(membehav_1b)
})
prehosp_issues[, "mb_2"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_2a)
})
prehosp_issues[, "mb_2a"] <- with(day1_df, {
  !is.na(membehav_2a) & membehav_2a == "Yes" & is.na(membehav_2b)
})
prehosp_issues[, "mb_3"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_3a)
})
prehosp_issues[, "mb_3a"] <- with(day1_df, {
  !is.na(membehav_3a) & membehav_3a == "Yes" & is.na(membehav_3b)
})
prehosp_issues[, "mb_4"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_4a)
})
prehosp_issues[, "mb_4a"] <- with(day1_df, {
  !is.na(membehav_4a) & membehav_4a == "Yes" & is.na(membehav_4b)
})
prehosp_issues[, "mb_5"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_5a)
})
prehosp_issues[, "mb_5a"] <- with(day1_df, {
  !is.na(membehav_5a) & membehav_5a == "Yes" & is.na(membehav_5b)
})
prehosp_issues[, "mb_6"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_6a)
})
prehosp_issues[, "mb_6a"] <- with(day1_df, {
  !is.na(membehav_6a) & membehav_6a == "Yes" & is.na(membehav_6b)
})
prehosp_issues[, "mb_7"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_7a)
})
prehosp_issues[, "mb_7a"] <- with(day1_df, {
  !is.na(membehav_7a) & membehav_7a == "Yes" & is.na(membehav_7b)
})
prehosp_issues[, "mb_8"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_8a)
})
prehosp_issues[, "mb_8a"] <- with(day1_df, {
  !is.na(membehav_8a) & membehav_8a == "Yes" & is.na(membehav_8b)
})
prehosp_issues[, "mb_9"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_9a)
})
prehosp_issues[, "mb_9a"] <- with(day1_df, {
  !is.na(membehav_9a) & membehav_9a == "Yes" & is.na(membehav_9b)
})
prehosp_issues[, "mb_10"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_10a)
})
prehosp_issues[, "mb_10a"] <- with(day1_df, {
  !is.na(membehav_10a) & membehav_10a == "Yes" & is.na(membehav_10b)
})
prehosp_issues[, "mb_11"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_11a)
})
prehosp_issues[, "mb_11a"] <- with(day1_df, {
  !is.na(membehav_11a) & membehav_11a == "Yes" & is.na(membehav_11b)
})
prehosp_issues[, "mb_12"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_12a)
})
prehosp_issues[, "mb_12a"] <- with(day1_df, {
  !is.na(membehav_12a) & membehav_12a == "Yes" & is.na(membehav_12b)
})
prehosp_issues[, "mb_13"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_13a)
})
prehosp_issues[, "mb_13a"] <- with(day1_df, {
  !is.na(membehav_13a) & membehav_13a == "Yes" & is.na(membehav_13b)
})
prehosp_issues[, "mb_14"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_14a)
})
prehosp_issues[, "mb_14a"] <- with(day1_df, {
  !is.na(membehav_14a) & membehav_14a == "Yes" & is.na(membehav_14b)
})
prehosp_issues[, "mb_15"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_15a)
})
prehosp_issues[, "mb_15a"] <- with(day1_df, {
  !is.na(membehav_15a) & membehav_15a == "Yes" & is.na(membehav_15b)
})
prehosp_issues[, "mb_16"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_16a)
})
prehosp_issues[, "mb_16a"] <- with(day1_df, {
  !is.na(membehav_16a) & membehav_16a == "Yes" & is.na(membehav_16b)
})
prehosp_issues[, "mb_17"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_17a)
})
prehosp_issues[, "mb_17a"] <- with(day1_df, {
  !is.na(membehav_17a) & membehav_17a == "Yes" & is.na(membehav_17b)
})
prehosp_issues[, "mb_18"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_18a)
})
prehosp_issues[, "mb_18a"] <- with(day1_df, {
  !is.na(membehav_18a) & membehav_18a == "Yes" & is.na(membehav_18b)
})
prehosp_issues[, "mb_19"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_19a)
})
prehosp_issues[, "mb_19a"] <- with(day1_df, {
  !is.na(membehav_19a) & membehav_19a == "Yes" & is.na(membehav_19b)
})
prehosp_issues[, "mb_20"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_20a)
})
prehosp_issues[, "mb_20a"] <- with(day1_df, {
  !is.na(membehav_20a) & membehav_20a == "Yes" & is.na(membehav_20b)
})
prehosp_issues[, "mb_21"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_21a)
})
prehosp_issues[, "mb_21a"] <- with(day1_df, {
  !is.na(membehav_21a) & membehav_21a == "Yes" & is.na(membehav_21b)
})
prehosp_issues[, "mb_22"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_22a)
})
prehosp_issues[, "mb_22a"] <- with(day1_df, {
  !is.na(membehav_22a) & membehav_22a == "Yes" & is.na(membehav_22b)
})
prehosp_issues[, "mb_23"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_23a)
})
prehosp_issues[, "mb_23a"] <- with(day1_df, {
  !is.na(membehav_23a) & membehav_23a == "Yes" & is.na(membehav_23b)
})
prehosp_issues[, "mb_24"] <- with(day1_df, {
  !is.na(memory_comp_ph) & memory_comp_ph & is.na(membehav_24a)
})
prehosp_issues[, "mb_24a"] <- with(day1_df, {
  !is.na(membehav_24a) & membehav_24a == "Yes" & is.na(membehav_24b)
})

## -- Create a final data.frame of errors + messages ---------------------------
prehosp_errors <- create_error_df(
  error_matrix = prehosp_issues, error_codes = prehosp_codes
)

prehosp_final <- bind_rows(prehosp_missing, prehosp_errors) %>%
  mutate(form = "Pre-Hospital Function")

################################################################################
## Dates Tracking Form
################################################################################

## -- Missingness checks -------------------------------------------------------
dt_missvars <- c(
  "enroll_dttm", "consent_self", "vcc_check", "death", "studywd", "hospdis",
  "dnr", "trach", "accel_rl", "accel_num", "int_num", "noninv_num",
  "hosp_admin_dttm", "icuadm_1_dttm", "icu_readmit_number"
)
dt_missing <- check_missing(
  df = day1_df, variables = dt_missvars, ddict = ih_ddict
)

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
dt_codes <- tribble(
  ~ code,      ~ msg,
  ## -- Consent ----------------------------------------------------------------
  ## Surrogate
  "sur_date",        "Missing date of surrogate consent",
  "sur_contact",     "Missing surrogate consent for contact for future studies",
  "sur_dna_this",    "Missing surrogate consent for DNA in this study",
  "sur_dna_future",  "Missing surrogate consent for DNA in future research",
  "sur_consent_doc", "Surrogate consent document was not uploaded",
  "sur_icf",         "Missing whether surrogate ICF note filled out in EPIC",
  ## Self/reconsent
  "pt_reconsent_ph1",           "Missing whether patient reconsented in hospital",
  "pt_reconsent_ph1_other",     "Missing explanation for reason patient not reconsented in hospital",
  "pt_icf",                     "Missing whether patient ICF note filled out in EPIC",
  "pt_icd_date",                "Missing date patient signed ICD",
  "pt_contact",                 "Missing patient consent for contact for future studies",
  "pt_dna_this",                "Missing patient consent for DNA in this study",
  "pt_dna_future",              "Missing patient consent for DNA in future research",
  "pt_consent_doc",             "Patient consent document was not uploaded",
  "pt_reconsent_ph2",           "Missing whether patient was reconsented in follow-up",
  "pt_reconsent_ph2_other",     "Missing explanation for reason patient not reconsented in follow-up",
  "pt_reconsent_again",         "Missing whether patient needed consent again on additional form",
  "pt_reconsent_again_icd",     "Missing date patient signed ICD for reconsent",
  "pt_reconsent_again_contact", "Missing reconsent for contact for future studies",
  "pt_reconsent_dna_this",      "Missing reconsent for DNA in this study",
  "pt_reconsent_dna_future",    "Missing reconsent for DNA in future research",
  "pt_reconsent_doc",           "Patient reconsent document was not uploaded",
  ## -- Death ------------------------------------------------------------------
  "death_epic", "Missing whether EPIC RR form was updated with patient death",
  "death_dttm", "Missing date and time of patient death",
  "death_supp", "Missing whether death was due to withdrawal of life support",
  "death_summ", "Missing summary of death",
  ## -- Withdrawal -------------------------------------------------------------
  "wd_epic",          "Missing whether EPIC RR was updated with patient withdrawal",
  "wd_dttm",          "Missing date and time of withdrawal",
  "wd_who",           "Missing who withdrew patient",
  "wd_comm",          "Missing how withdrawal was communicated",
  "wd_comm_other",    "Missing explanation of other method of communicating withdrawal",
  "wd_summ",          "Missing summary of withdrawal reason",
  "wd_writing",       "Missing what patient or surrogate requested in writing for withdrawal",
  "wd_writing_other", "MIssing explanation of other request for withdrawal",
  ## -- Discharge --------------------------------------------------------------
  "hospdis_epic",         "Missing whether EPIC RR was updated with hospital discharge",
  "hospdis_safety",       "No info about potential safety issues for follow-up (if no known problems, please mark No issues identified)",
  "hospdis_safety_other", "Missing explanation of other follow-up safety concerns",
  "hospdis_dttm",         "Missing date and time of discharge",
  "hospdis_cg",           "Missing whether primary caregiver changed from Pre-Hospital Form",
  "hospdis_cg_new",       "Missing name of new primary caregiver",
  "hospdis_cg_contact",   "Missing contact phone for new primary caregiver",
  "hospdis_dcloc",        "Missing location of hospital discharge",
  "hospdis_dcloc_other",  "Missing explanation of other hospital discharge location",
  "hospdis_fac_contact",  "Missing contact information for facility patient discharged to",
  "hospdis_mvdc",         "Missing whether patient on mechanical ventilation at discharge",
  ## -- Hospitalization --------------------------------------------------------
  "coenrolled",    "Missing whether patient was co-enrolled in other studies (check No if none)",
  "coenr_mends2",  "Co-enrolled in MENDS2, but missing ID",
  "coenr_mindusa", "Co-enrolled in MINDUSA, but missing ID",
  "coenr_insight", "Co-enrolled in INSIGHT, but missing ID",
  "coenr_other",   "Missing explanation of other study co-enrolled in",
  "dnrdate",       "Missing date and time of DNR or DNI",
  "trachdate",     "Missing date and time of tracheostomy",
  ## -- Accelerometer placement ------------------------------------------------
  "accel_rsn",             "Missing reason accelerometer was never applied",
  "accel_rsn_other",       "Missing explanation for other reason accelerometer never applied",
  "accel_wrist_1",         "Missing serial number of wrist accelerometer 1",
  "accel_wrist_2",         "Missing serial number of wrist accelerometer 2",
  "accel_wrist_3",         "Missing serial number of wrist accelerometer 3",
  "accel_wrist_4",         "Missing serial number of wrist accelerometer 4",
  "accel_ankle_1",         "Missing serial number of ankle accelerometer 1",
  "accel_ankle_2",         "Missing serial number of ankle accelerometer 2",
  "accel_ankle_3",         "Missing serial number of ankle accelerometer 3",
  "accel_ankle_4",         "Missing serial number of ankle accelerometer 4",
  "accel_init_1",          "Missing date and time of accelerometer initiation 1",
  "accel_init_2",          "Missing date and time of accelerometer initiation 2",
  "accel_init_3",          "Missing date and time of accelerometer initiation 3",
  "accel_init_4",          "Missing date and time of accelerometer initiation 4",
  "accel_wrist_dc_1",      "Missing whether wrist initiation 1 was discontinued",
  "accel_wrist_dc_2",      "Missing whether wrist initiation 2 was discontinued",
  "accel_wrist_dc_3",      "Missing whether wrist initiation 3 was discontinued",
  "accel_wrist_dc_4",      "Missing whether wrist initiation 4 was discontinued",
  "accel_wrist_dc_dttm_1", "Missing date and time of wrist discontinuation 1",
  "accel_wrist_dc_dttm_2", "Missing date and time of wrist discontinuation 2",
  "accel_wrist_dc_dttm_3", "Missing date and time of wrist discontinuation 3",
  "accel_wrist_dc_dttm_4", "Missing date and time of wrist discontinuation 4",
  "accel_ankle_dc_1",      "Missing whether ankle initiation 1 was discontinued",
  "accel_ankle_dc_2",      "Missing whether ankle initiation 2 was discontinued",
  "accel_ankle_dc_3",      "Missing whether ankle initiation 3 was discontinued",
  "accel_ankle_dc_4",      "Missing whether ankle initiation 4 was discontinued",
  "accel_ankle_dc_dttm_1", "Missing date and time of ankle discontinuation 1",
  "accel_ankle_dc_dttm_2", "Missing date and time of ankle discontinuation 2",
  "accel_ankle_dc_dttm_3", "Missing date and time of ankle discontinuation 3",
  "accel_ankle_dc_dttm_4", "Missing date and time of ankle discontinuation 4",
  "accel_upload_1",        "Missing whether device data from initiation 1 was uploaded",
  "accel_upload_2",        "Missing whether device data from initiation 2 was uploaded",
  "accel_upload_3",        "Missing whether device data from initiation 3 was uploaded",
  "accel_upload_4",        "Missing whether device data from initiation 4 was uploaded",
  ## -- Mechanical ventilation -------------------------------------------------
  ## Invasive
  "int_dttm_1",    "Missing date and time of invasive MV initiation 1",
  "int_dc_1",      "Missing whether invasive MV initiation 1 discontinued",
  "int_dc_dttm_1", "Missing date and time of invasive MV discontinuation 1",
  "int_dttm_2",    "Missing date and time of invasive MV initiation 2",
  "int_dc_2",      "Missing whether invasive MV initiation 2 discontinued",
  "int_dc_dttm_2", "Missing date and time of invasive MV discontinuation 2",
  "int_dttm_3",    "Missing date and time of invasive MV initiation 3",
  "int_dc_3",      "Missing whether invasive MV initiation 3 discontinued",
  "int_dc_dttm_3", "Missing date and time of invasive MV discontinuation 3",
  "int_dttm_4",    "Missing date and time of invasive MV initiation 4",
  "int_dc_4",      "Missing whether invasive MV initiation 4 discontinued",
  "int_dc_dttm_4", "Missing date and time of invasive MV discontinuation 4",
  "int_dttm_5",    "Missing date and time of invasive MV initiation 5",
  "int_dc_5",      "Missing whether invasive MV initiation 5 discontinued",
  "int_dc_dttm_5", "Missing date and time of invasive MV discontinuation 5",
  "int_dttm_6",    "Missing date and time of invasive MV initiation 6",
  "int_dc_6",      "Missing whether invasive MV initiation 6 discontinued",
  ## Actually no variable for extubation 6?
  # "int_dc_dttm_6", "Missing date and time of invasive MV discontinuation 6",
  ## Noninvasive
  "noninv_dttm_1",    "Missing date and time of noninvasive MV initiation 1",
  "noninv_dc_1",      "Missing whether noninvasive MV initiation 1 discontinued",
  "noninv_dc_dttm_1", "Missing date and time of noninvasive MV discontinuation 1",
  "noninv_dttm_2",    "Missing date and time of noninvasive MV initiation 2",
  "noninv_dc_2",      "Missing whether noninvasive MV initiation 2 discontinued",
  "noninv_dc_dttm_2", "Missing date and time of noninvasive MV discontinuation 2",
  "noninv_dttm_3",    "Missing date and time of noninvasive MV initiation 3",
  "noninv_dc_3",      "Missing whether noninvasive MV initiation 3 discontinued",
  "noninv_dc_dttm_3", "Missing date and time of noninvasive MV discontinuation 3",
  "noninv_dttm_4",    "Missing date and time of noninvasive MV initiation 4",
  "noninv_dc_4",      "Missing whether noninvasive MV initiation 4 discontinued",
  "noninv_dc_dttm_4", "Missing date and time of noninvasive MV discontinuation 4",
  "noninv_dttm_5",    "Missing date and time of noninvasive MV initiation 5",
  "noninv_dc_5",      "Missing whether noninvasive MV initiation 5 discontinued",
  "noninv_dc_dttm_5", "Missing date and time of noninvasive MV discontinuation 5",
  "noninv_dttm_6",    "Missing date and time of noninvasive MV initiation 6",
  "noninv_dc_6",      "Missing whether noninvasive MV initiation 6 discontinued",
  "noninv_dc_dttm_6", "Missing date and time of noninvasive MV discontinuation 6",
  ## -- Hospitalization --------------------------------------------------------
  "icu_dc_1",  "No death recorded, but patient missing ICU discharge 1",
  "icu_adm_2", "Missing ICU admission 2 date and time",
  "icu_dc_2",  "Missing ICU discharge 2 date and time",
  "icu_adm_3", "Missing ICU admission 3 date and time",
  "icu_dc_3",  "Missing ICU discharge 3 date and time",
  "icu_adm_4", "Missing ICU admission 4 date and time",
  "icu_dc_4",  "Missing ICU discharge 4 date and time",
  "icu_adm_5", "Missing ICU admission 5 date and time",
  "icu_dc_5",  "Missing ICU discharge 5 date and time",
  "icu_adm_6", "Missing ICU admission 6 date and time",
  "icu_dc_6",  "Missing ICU discharge 6 date and time"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in day1_df; columns = # potential issues
dt_issues <- matrix(
  FALSE, ncol = nrow(dt_codes), nrow = nrow(day1_df)
)
colnames(dt_issues) <- dt_codes$code
rownames(dt_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

## -- Consent ------------------------------------------------------------------
## Surrogate
dt_issues[, "sur_date"] <- with(day1_df, {
  !is.na(consent_self) & consent_self == "No" & is.na(sur_consent_date)
})
dt_issues[, "sur_contact"] <- with(day1_df, {
  !is.na(consent_self) & consent_self == "No" & is.na(sur_consent_study)
})
dt_issues[, "sur_dna_this"] <- with(day1_df, {
  !is.na(consent_self) & consent_self == "No" & is.na(sur_consent_dna_1)
})
dt_issues[, "sur_dna_future"] <- with(day1_df, {
  !is.na(consent_self) & consent_self == "No" & is.na(sur_consent_dna_2)
})
dt_issues[, "sur_consent_doc"] <- with(day1_df, {
  !is.na(consent_self) & consent_self == "No" & is.na(sur_consent_upload)
})
dt_issues[, "sur_icf"] <- with(day1_df, {
  !is.na(enroll_dttm) & enroll_dttm >= epic_date &
    !is.na(consent_self) & consent_self == "No" & is.na(icf_chart_reminder_1)
})

## Self/reconsent
dt_issues[, "pt_reconsent_ph1"] <- with(day1_df, {
  !is.na(consent_self) & consent_self == "No" & is.na(reconsent_ph1)
})
dt_issues[, "pt_reconsent_ph1_other"] <- with(day1_df, {
  !is.na(reconsent_ph1) & reconsent_ph1 == "No, other (explain)" &
    (is.na(reconsent_ph1_other) | reconsent_ph1_other == "")
})
dt_issues[, "pt_icf"] <- with(day1_df, {
  !is.na(enroll_dttm) & enroll_dttm >= epic_date &
    pt_consented & is.na(reconsent_ph1_starpanel_1)
})
dt_issues[, "pt_icd_date"] <- with(day1_df, {
  pt_consented & is.na(pt_consent_date)
})
dt_issues[, "pt_contact"] <- with(day1_df, {
  pt_consented & is.na(pt_consent_study)
})
dt_issues[, "pt_dna_this"] <- with(day1_df, {
  pt_consented & is.na(pt_consent_dna_1)
})
dt_issues[, "pt_dna_future"] <- with(day1_df, {
  pt_consented & is.na(pt_consent_dna_2)
})
dt_issues[, "pt_consent_doc"] <- with(day1_df, {
  pt_consented & is.na(pt_consent_upload)
})
dt_issues[, "pt_reconsent_ph2"] <- with(day1_df, {
  !is.na(reconsent_ph1) &
    reconsent_ph1 %in% c(
      "Not able due to cognitive inability", "No, other (explain)"
    ) & is.na(reconsent_ph2)
})
dt_issues[, "pt_reconsent_ph2_other"] <- with(day1_df, {
  !is.na(reconsent_ph2) & reconsent_ph2 == "No, other (explain)" &
    (is.na(reconsent_ph2_other) | reconsent_ph2_other == "")
})
## If patient reconsented during follow-up they might need to reconsent again?
dt_issues[, "pt_reconsent_again"] <- with(day1_df, {
  !is.na(reconsent_ph2) &
    reconsent_ph2 == "Yes, agreed to continue (signed ICD)" &
    is.na(reconsent_again_ph2)
})
dt_issues[, "pt_reconsent_again_icd"] <- with(day1_df, {
  !is.na(reconsent_again_ph2) & reconsent_again_ph2 == "Yes" &
    is.na(reconsent_again_ph2_date)
})
dt_issues[, "pt_reconsent_again_contact"] <- with(day1_df, {
  !is.na(reconsent_again_ph2) & reconsent_again_ph2 == "Yes" &
    is.na(reconsent_again_ph2_study)
})
dt_issues[, "pt_reconsent_dna_this"] <- with(day1_df, {
  !is.na(reconsent_again_ph2) & reconsent_again_ph2 == "Yes" &
    is.na(reconsent_again_ph2_dna_1)
})
dt_issues[, "pt_reconsent_dna_future"] <- with(day1_df, {
  !is.na(reconsent_again_ph2) & reconsent_again_ph2 == "Yes" &
    is.na(reconsent_again_ph2_dna_2)
})
dt_issues[, "pt_reconsent_doc"] <- with(day1_df, {
  !is.na(reconsent_again_ph2) & reconsent_again_ph2 == "Yes" &
    is.na(reconsent_again_ph2_form)
})

## -- Death --------------------------------------------------------------------
dt_issues[, "death_epic"] <- with(day1_df, {
  !is.na(enroll_dttm) & enroll_dttm >= epic_date &
    !is.na(death) & death == "Yes" & is.na(death_starpanel_1)
})
dt_issues[, "death_dttm"] <- with(day1_df, {
  !is.na(death) & death == "Yes" & is.na(death_dttm)
})
dt_issues[, "death_supp"] <- with(day1_df, {
  !is.na(death) & death == "Yes" & is.na(death_wdtrt)
})
dt_issues[, "death_summ"] <- with(day1_df, {
  !is.na(death) & death == "Yes" & (is.na(death_summary) | death_summary == "")
})

## -- Withdrawal ---------------------------------------------------------------
dt_issues[, "wd_epic"] <- with(day1_df, {
  !is.na(enroll_dttm) & enroll_dttm >= epic_date &
    !is.na(studywd) & studywd == "Yes" & is.na(studywd_starpanel_1)
})
dt_issues[, "wd_dttm"] <- with(day1_df, {
  !is.na(studywd) & studywd == "Yes" & is.na(studywd_dttm)
})
dt_issues[, "wd_who"] <- with(day1_df, {
  !is.na(studywd) & studywd == "Yes" & is.na(studywd_who)
})
dt_issues[, "wd_comm"] <- with(day1_df, {
  !is.na(studywd) & studywd == "Yes" & is.na(studywd_how)
})
dt_issues[, "wd_comm_other"] <- with(day1_df, {
  !is.na(studywd_how) & studywd_how == "Other" &
    (is.na(studywd_other) | studywd_other == "")
})
dt_issues[, "wd_summ"] <- with(day1_df, {
  !is.na(studywd) & studywd == "Yes" & (is.na(studywd_rsn) | studywd_rsn == "")
})
dt_issues[, "wd_writing"] <- with(day1_df, {
  !is.na(studywd_how) & studywd_how == "In writing" &
    rowSums(!is.na(day1_df[, grep("^studywd\\_writing\\_[0-9]+$", names(day1_df))])) == 0
})
dt_issues[, "wd_writing_other"] <- with(day1_df, {
  !is.na(studywd_writing_5) &
    (is.na(studywd_writing_other) | studywd_writing_other == "")
})

## -- Discharge ----------------------------------------------------------------
dt_issues[, "hospdis_epic"] <- with(day1_df, {
  !is.na(enroll_dttm) & enroll_dttm >= epic_date &
    !is.na(hospdis) & hospdis == "Yes" & is.na(hospdis_starpanel_1)
})
dt_issues[, "hospdis_safety"] <- with(day1_df, {
  !is.na(enroll_dttm) & enroll_dttm >= safety_date &
    !is.na(hospdis) & hospdis == "Yes" &
    rowSums(!is.na(day1_df[, grep("^fu\\_safety\\_[0-9]+$", names(day1_df))])) == 0
})
dt_issues[, "hospdis_safety_other"] <- with(day1_df, {
  !is.na(fu_safety_7) & (is.na(fu_safety_explain) | fu_safety_explain == "")
})
dt_issues[, "hospdis_dttm"] <- with(day1_df, {
  !is.na(hospdis) & hospdis == "Yes" & is.na(hospdis_dttm)
})
dt_issues[, "hospdis_cg"] <- with(day1_df, {
  !is.na(hospdis) & hospdis == "Yes" & is.na(hospdis_primary_change)
})
dt_issues[, "hospdis_cg_new"] <- with(day1_df, {
  !is.na(hospdis_primary_change) & hospdis_primary_change == "Yes" &
    (is.na(hospdis_primary_name) | hospdis_primary_name == "")
})
dt_issues[, "hospdis_cg_contact"] <- with(day1_df, {
  !is.na(hospdis_primary_change) & hospdis_primary_change == "Yes" &
    (is.na(hospdis_primary_phone) | hospdis_primary_phone == "")
})
dt_issues[, "hospdis_dcloc"] <- with(day1_df, {
  !is.na(hospdis) & hospdis == "Yes" & is.na(hospdis_loc)
})
dt_issues[, "hospdis_dcloc_other"] <- with(day1_df, {
  !is.na(hospdis_loc) & hospdis_loc == "Other" &
    (is.na(hospdis_loc_other) | hospdis_loc_other == "")
})
dt_issues[, "hospdis_fac_contact"] <- with(day1_df, {
  !is.na(hospdis_loc) & hospdis_loc != "Home" &
    (is.na(hospdc_info) | hospdc_info == "")
})
dt_issues[, "hospdis_mvdc"] <- with(day1_df, {
  !is.na(hospdis) & hospdis == "Yes" & is.na(hospdis_vent)
})

## -- Hospitalization ----------------------------------------------------------
dt_issues[, "coenrolled"] <- rowSums(
  !is.na(day1_df[, grep("^coenroll\\_[0-9]+$", names(day1_df))])
) == 0
dt_issues[, "coenr_mends2"] <- with(day1_df, {
  !is.na(coenroll_6) & is.na(mends2_studyid)
})
dt_issues[, "coenr_mindusa"] <- with(day1_df, {
  !is.na(coenroll_7) & is.na(mindusa_studyid)
})
dt_issues[, "coenr_insight"] <- with(day1_df, {
  !is.na(coenroll_8) & is.na(insight_studyid)
})
dt_issues[, "coenr_other"] <- with(day1_df, {
  !is.na(coenroll_99) & (is.na(coenroll_other) | coenroll_other == "")
})
dt_issues[, "dnrdate"] <- with(day1_df, {
  !is.na(dnr) & dnr == "Yes" & is.na(dnr_dttm)
})
dt_issues[, "trachdate"] <- with(day1_df, {
  !is.na(trach) & trach == "Yes" & is.na(trach_dttm)
})

## -- Accelerometers -----------------------------------------------------------
dt_issues[, "accel_rsn"] <- with(day1_df, {
  !is.na(accel_num) & accel_num == 0 & is.na(accel_never_rsn)
})
dt_issues[, "accel_rsn_other"] <- with(day1_df, {
  !is.na(accel_never_rsn) & accel_never_rsn == "Other (explain)" &
    (is.na(accel_never_other) | accel_never_other == "")
})

## Initiation 1
dt_issues[, "accel_wrist_1"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 1 & is.na(accel_wrist_1)
})
dt_issues[, "accel_wrist_dc_1"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 1 & is.na(wrist_disc_yn_1)
})
dt_issues[, "accel_wrist_dc_dttm_1"] <- with(day1_df, {
  !is.na(wrist_disc_yn_1) & is.na(wrist_disc_dttm_1)
})
dt_issues[, "accel_ankle_1"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 1 & is.na(accel_ankle_1)
})
dt_issues[, "accel_ankle_dc_1"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 1 & is.na(ankle_disc_yn_1)
})
dt_issues[, "accel_ankle_dc_dttm_1"] <- with(day1_df, {
  !is.na(wrist_disc_yn_1) & is.na(ankle_disc_dttm_1)
})
dt_issues[, "accel_init_1"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 1 & is.na(accel_int_dttm_1)
})
dt_issues[, "accel_upload_1"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 1 & is.na(upload_data_1_1)
})

## Initiation 2
dt_issues[, "accel_wrist_2"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 2 & is.na(accel_wrist_2)
})
dt_issues[, "accel_wrist_dc_2"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 2 & is.na(wrist_disc_yn_2)
})
dt_issues[, "accel_wrist_dc_dttm_2"] <- with(day1_df, {
  !is.na(wrist_disc_yn_2) & is.na(wrist_disc_dttm_2)
})
dt_issues[, "accel_ankle_2"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 2 & is.na(accel_ankle_2)
})
dt_issues[, "accel_ankle_dc_2"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 2 & is.na(ankle_disc_yn_2)
})
dt_issues[, "accel_ankle_dc_dttm_2"] <- with(day1_df, {
  !is.na(wrist_disc_yn_2) & is.na(ankle_disc_dttm_2)
})
dt_issues[, "accel_init_2"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 2 & is.na(accel_int_dttm_2)
})
dt_issues[, "accel_upload_2"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 2 & is.na(upload_data_2_1)
})

## Initiation 3
dt_issues[, "accel_wrist_3"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 3 & is.na(accel_wrist_3)
})
dt_issues[, "accel_wrist_dc_3"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 3 & is.na(wrist_disc_yn_3)
})
dt_issues[, "accel_wrist_dc_dttm_3"] <- with(day1_df, {
  !is.na(wrist_disc_yn_3) & is.na(wrist_disc_dttm_3)
})
dt_issues[, "accel_ankle_3"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 3 & is.na(accel_ankle_3)
})
dt_issues[, "accel_ankle_dc_3"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 3 & is.na(ankle_disc_yn_3)
})
dt_issues[, "accel_ankle_dc_dttm_3"] <- with(day1_df, {
  !is.na(wrist_disc_yn_3) & is.na(ankle_disc_dttm_3)
})
dt_issues[, "accel_init_3"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 3 & is.na(accel_int_dttm_3)
})
dt_issues[, "accel_upload_3"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 3 & is.na(upload_data_3_1)
})

## Initiation 4
dt_issues[, "accel_wrist_4"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 4 & is.na(accel_wrist_4)
})
dt_issues[, "accel_wrist_dc_4"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 4 & is.na(wrist_disc_yn_4)
})
dt_issues[, "accel_wrist_dc_dttm_4"] <- with(day1_df, {
  !is.na(wrist_disc_yn_4) & is.na(wrist_disc_dttm_4)
})
dt_issues[, "accel_ankle_4"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 4 & is.na(accel_ankle_4)
})
dt_issues[, "accel_ankle_dc_4"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 4 & is.na(ankle_disc_yn_4)
})
dt_issues[, "accel_ankle_dc_dttm_4"] <- with(day1_df, {
  !is.na(wrist_disc_yn_4) & is.na(ankle_disc_dttm_4)
})
dt_issues[, "accel_init_4"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 4 & is.na(accel_int_dttm_4)
})
dt_issues[, "accel_upload_4"] <- with(day1_df, {
  !is.na(accel_num) & accel_num >= 4 & is.na(upload_data_4_1)
})

## -- Mechanical ventilation ---------------------------------------------------
## Invasive
dt_issues[, "int_dttm_1"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 1 & is.na(int_1_dttm)
})
dt_issues[, "int_dc_1"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 1 & is.na(ext_1)
})
dt_issues[, "int_dc_dttm_1"] <- with(day1_df, {
  !is.na(ext_1) & ext_1 == "Yes" & is.na(ext_1_dttm)
})

dt_issues[, "int_dttm_2"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 2 & is.na(int_2_dttm)
})
dt_issues[, "int_dc_2"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 2 & is.na(ext_2)
})
dt_issues[, "int_dc_dttm_2"] <- with(day1_df, {
  !is.na(ext_2) & ext_2 == "Yes" & is.na(ext_2_dttm)
})

dt_issues[, "int_dttm_3"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 3 & is.na(int_1_dttm)
})
dt_issues[, "int_dc_3"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 3 & is.na(ext_3)
})
dt_issues[, "int_dc_dttm_3"] <- with(day1_df, {
  !is.na(ext_3) & ext_3 == "Yes" & is.na(ext_3_dttm)
})

dt_issues[, "int_dttm_4"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 4 & is.na(int_4_dttm)
})
dt_issues[, "int_dc_4"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 4 & is.na(ext_4)
})
dt_issues[, "int_dc_dttm_4"] <- with(day1_df, {
  !is.na(ext_4) & ext_4 == "Yes" & is.na(ext_4_dttm)
})

dt_issues[, "int_dttm_5"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 5 & is.na(int_5_dttm)
})
dt_issues[, "int_dc_5"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 5 & is.na(ext_5)
})
dt_issues[, "int_dc_dttm_5"] <- with(day1_df, {
  !is.na(ext_5) & ext_5 == "Yes" & is.na(ext_5_dttm)
})

dt_issues[, "int_dttm_6"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 6 & is.na(int_6_dttm)
})
dt_issues[, "int_dc_6"] <- with(day1_df, {
  !is.na(int_num) & int_num >= 6 & is.na(ext_6)
})
## Actually no variable for extubation 6?
# dt_issues[, "int_dc_dttm_6"] <- with(day1_df, {
#   !is.na(ext_6) & ext_6 == "Yes" & is.na(ext_6_dttm)
# })

## Noninvasive
dt_issues[, "noninv_dttm_1"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 1 & is.na(noninv_1_dttm)
})
dt_issues[, "noninv_dc_1"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 1 & is.na(noninv_1_dc)
})
dt_issues[, "noninv_dc_dttm_1"] <- with(day1_df, {
  !is.na(noninv_1_dc) & noninv_1_dc == "Yes" & is.na(noninv_1_dc_dttm)
})

dt_issues[, "noninv_dttm_2"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 2 & is.na(noninv_2_dttm)
})
dt_issues[, "noninv_dc_2"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 2 & is.na(noninv_2_dc)
})
dt_issues[, "noninv_dc_dttm_2"] <- with(day1_df, {
  !is.na(noninv_2_dc) & noninv_2_dc == "Yes" & is.na(noninv_2_dc_dttm)
})

dt_issues[, "noninv_dttm_3"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 3 & is.na(noninv_3_dttm)
})
dt_issues[, "noninv_dc_3"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 3 & is.na(noninv_3_dc)
})
dt_issues[, "noninv_dc_dttm_3"] <- with(day1_df, {
  !is.na(noninv_3_dc) & noninv_3_dc == "Yes" & is.na(noninv_3_dc_dttm)
})

dt_issues[, "noninv_dttm_4"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 4 & is.na(noninv_4_dttm)
})
dt_issues[, "noninv_dc_4"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 4 & is.na(noninv_4_dc)
})
dt_issues[, "noninv_dc_dttm_4"] <- with(day1_df, {
  !is.na(noninv_4_dc) & noninv_4_dc == "Yes" & is.na(noninv_4_dc_dttm)
})

dt_issues[, "noninv_dttm_5"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 5 & is.na(noninv_5_dttm)
})
dt_issues[, "noninv_dc_5"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 5 & is.na(noninv_5_dc)
})
dt_issues[, "noninv_dc_dttm_5"] <- with(day1_df, {
  !is.na(noninv_5_dc) & noninv_5_dc == "Yes" & is.na(noninv_5_dc_dttm)
})

dt_issues[, "noninv_dttm_6"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 6 & is.na(noninv_6_dttm)
})
dt_issues[, "noninv_dc_6"] <- with(day1_df, {
  !is.na(noninv_num) & noninv_num >= 6 & is.na(noninv_6_dc)
})
dt_issues[, "noninv_dc_dttm_6"] <- with(day1_df, {
  !is.na(noninv_6_dc) & noninv_6_dc == "Yes" & is.na(noninv_6_dc_dttm)
})

## -- Hospitalization ----------------------------------------------------------
dt_issues[, "icu_dc_1"] <- with(day1_df, {
  ## ICU discharge 1 should be present as long as patient remained alive during
  ##  hospitalization
  !died_inhosp & is.na(icudis_1_dttm)
})
dt_issues[, "icu_adm_2"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 0 & is.na(icuadm_2_dttm)
})
dt_issues[, "icu_dc_2"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 0 &
    (
      ## Readmitted at least one additional time
      icu_readmit_number > 1 |
        ## Not readmitted, but did not die during hospitalization
        !died_inhosp
    ) &
    is.na(icudis_2_dttm)
})
dt_issues[, "icu_adm_3"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 1 & is.na(icuadm_3_dttm)
})
dt_issues[, "icu_dc_3"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 1 &
    (
      ## Readmitted at least one additional time
      icu_readmit_number > 2 |
        ## Not readmitted, but did not die during hospitalization
        !died_inhosp
    ) &
    is.na(icudis_3_dttm)
})
dt_issues[, "icu_adm_4"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 2 & is.na(icuadm_4_dttm)
})
dt_issues[, "icu_dc_4"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 2 &
    (
      ## Readmitted at least one additional time
      icu_readmit_number > 3 |
        ## Not readmitted, but did not die during hospitalization
        !died_inhosp
    ) &
    is.na(icudis_4_dttm)
})
dt_issues[, "icu_adm_5"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 3 & is.na(icuadm_5_dttm)
})
dt_issues[, "icu_dc_5"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 3 &
    (
      ## Readmitted at least one additional time
      icu_readmit_number > 4 |
        ## Not readmitted, but did not die during hospitalization
        !died_inhosp
    ) &
    is.na(icudis_5_dttm)
})
dt_issues[, "icu_adm_6"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 4 & is.na(icuadm_6_dttm)
})
dt_issues[, "icu_dc_6"] <- with(day1_df, {
  !is.na(icu_readmit_number) & icu_readmit_number > 4 &
    (
      ## Readmitted at least one additional time
      icu_readmit_number > 5 |
        ## Not readmitted, but did not die during hospitalization
        !died_inhosp
    ) &
    is.na(icudis_6_dttm)
})

## -- Create a final data.frame of errors + messages ---------------------------
dt_errors <- create_error_df(
  error_matrix = dt_issues, error_codes = dt_codes
)

dt_final <- bind_rows(dt_missing, dt_errors) %>%
  mutate(form = "Dates Tracking")

################################################################################
## Enrollment Data (MDS)
################################################################################

## -- Missingness checks -------------------------------------------------------
enrollmds_missvars <- c(
  "mrn", "dob", "gender", "insurance", "workerscomp_insurance", "hispanic",
  "frailty_scale", "icu_rsn"
)
enrollmds_missing <- check_missing(
  df = day1_df, variables = enrollmds_missvars, ddict = ih_ddict
)

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
enrollmds_codes <- tribble(
  ~ code,            ~ msg,
  "medicare",        "Patient on Medicare, but missing number",
  "race",            "No race indicated",
  "race_other",      "Missing description of other race",
  "charlson_none_1", "No conditions marked for Charlson category 1 (check None if none apply)",
  "charlson_conf_1", "None cannot be marked if at least one condition is marked for Charlson category 1",
  "charlson_none_2", "No conditions marked for Charlson category 2 (check None if none apply)",
  "charlson_conf_2", "None cannot be marked if at least one condition is marked for Charlson category 2",
  "charlson_none_3", "No conditions marked for Charlson category 3 (check None if none apply)",
  "charlson_conf_3", "None cannot be marked if at least one condition is marked for Charlson category 3",
  "charlson_none_4", "No conditions marked for Charlson category 4 (check None if none apply)",
  "charlson_conf_4", "None cannot be marked if at least one condition is marked for Charlson category 4",
  "apache_none",     "No conditions marked for APACHE organ insufficiency (check None if none apply)",
  "apache_conf",     "None cannot be marked if at least one condition is marked for APACHE organ insufficiency",
  "icu_rsn_other",   "Missing description of other ICU admitting diagnosis"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in day1_df; columns = # potential issues
enrollmds_issues <- matrix(
  FALSE, ncol = nrow(enrollmds_codes), nrow = nrow(day1_df)
)
colnames(enrollmds_issues) <- enrollmds_codes$code
rownames(enrollmds_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

enrollmds_issues[, "medicare"] <- with(day1_df, {
  !is.na(insurance) & str_detect(insurance, "Medicare") & is.na(medicare_num)
})
enrollmds_issues[, "race"] <- rowSums(
  !is.na(day1_df[, grep("^race\\_[0-9]+$", names(day1_df))])
) == 0
enrollmds_issues[, "race_other"] <- with(day1_df, {
  !is.na(race_14) & (is.na(race_other) | race_other == "")
})
enrollmds_issues[, "charlson_none_1"] <- rowSums(
  !is.na(day1_df[, grep("^charlson\\_1\\_[0-9]+$", names(day1_df))])
) == 0
enrollmds_issues[, "charlson_conf_1"] <- !is.na(day1_df$charlson_1_0) &
  rowSums(!is.na(day1_df[, grep("^charlson\\_1\\_[0-9]+$", names(day1_df))])) > 1
enrollmds_issues[, "charlson_none_2"] <- rowSums(
  !is.na(day1_df[, grep("^charlson\\_2\\_[0-9]+$", names(day1_df))])
) == 0
enrollmds_issues[, "charlson_conf_2"] <- !is.na(day1_df$charlson_2_0) &
  rowSums(!is.na(day1_df[, grep("^charlson\\_2\\_[0-9]+$", names(day1_df))])) > 1
enrollmds_issues[, "charlson_none_3"] <- rowSums(
  !is.na(day1_df[, grep("^charlson\\_3\\_[0-9]+$", names(day1_df))])
) == 0
enrollmds_issues[, "charlson_conf_3"] <- !is.na(day1_df$charlson_3_0) &
  rowSums(!is.na(day1_df[, grep("^charlson\\_3\\_[0-9]+$", names(day1_df))])) > 1
enrollmds_issues[, "charlson_none_4"] <- rowSums(
  !is.na(day1_df[, grep("^charlson\\_4\\_[0-9]+$", names(day1_df))])
) == 0
enrollmds_issues[, "charlson_conf_4"] <- !is.na(day1_df$charlson_4_0) &
  rowSums(!is.na(day1_df[, grep("^charlson\\_4\\_[0-9]+$", names(day1_df))])) > 1
enrollmds_issues[, "apache_none"] <- rowSums(
  !is.na(day1_df[, grep("^chronic\\_dis\\_[0-9]+$", names(day1_df))])
) == 0
enrollmds_issues[, "apache_conf"] <- !is.na(day1_df$charlson_1_0) &
  rowSums(!is.na(day1_df[, grep("^chronic\\_dis\\_[0-9]+$", names(day1_df))])) > 1
enrollmds_issues[, "icu_rsn_other"] <- with(day1_df, {
  !is.na(icu_rsn) & icu_rsn == "Other" &
    (is.na(icu_rsn_other) | icu_rsn_other == "")
})

## -- Create a final data.frame of errors + messages ---------------------------
enrollmds_errors <- create_error_df(
  error_matrix = enrollmds_issues, error_codes = enrollmds_codes
)

enrollmds_final <- bind_rows(enrollmds_missing, enrollmds_errors) %>%
  mutate(form = "Enrollment Data (MDS)")

################################################################################
## Enrollment Nutrition Data
################################################################################

## -- Missingness checks -------------------------------------------------------
nutr_missvars <- c(
  "mrn", "dob", "gender", "insurance", "workerscomp_insurance", "hispanic",
  "frailty_scale", "icu_rsn"
)
nutr_missing <- check_missing(
  df = day1_df, variables = nutr_missvars, ddict = ih_ddict
)

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
nutr_codes <- tribble(
  ~ code,             ~ msg,
  "days_enroll",      "Missing how many days patient was in the ICU before enrollment",
  "day1_date",        "Missing date of pre-enrollment day 1",
  "day1_tpn",         "Missing whether patient received TPN on pre-enrollment day 1",
  "day1_goal_kcal",   "Missing calorie goal for pre-enrollment day 1",
  "day1_total_kcal",  "Missing total calories for pre-enrollment day 1",
  "day1_goal_aa",     "Missing amino acid goal for pre-enrollment day 1",
  "day1_total_aa",    "Missing amino acid total for pre-enrollment day 1",
  "day1_tube",        "Missing whether patient received tube feeding on pre-enrollment day 1",
  "day1_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 1",
  "day1_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 1",
  "day1_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 1",
  "day1_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 1",
  "day1_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 1",
  "day1_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 1",
  "day1_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 1",
  "day2_date",        "Missing date of pre-enrollment day 2",
  "day2_tpn",         "Missing whether patient received TPN on pre-enrollment day 2",
  "day2_goal_kcal",   "Missing calorie goal for pre-enrollment day 2",
  "day2_total_kcal",  "Missing total calories for pre-enrollment day 2",
  "day2_goal_aa",     "Missing amino acid goal for pre-enrollment day 2",
  "day2_total_aa",    "Missing amino acid total for pre-enrollment day 2",
  "day2_tube",        "Missing whether patient received tube feeding on pre-enrollment day 2",
  "day2_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 2",
  "day2_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 2",
  "day2_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 2",
  "day2_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 2",
  "day2_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 2",
  "day2_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 2",
  "day2_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 2",
  "day3_date",        "Missing date of pre-enrollment day 3",
  "day3_tpn",         "Missing whether patient received TPN on pre-enrollment day 3",
  "day3_goal_kcal",   "Missing calorie goal for pre-enrollment day 3",
  "day3_total_kcal",  "Missing total calories for pre-enrollment day 3",
  "day3_goal_aa",     "Missing amino acid goal for pre-enrollment day 3",
  "day3_total_aa",    "Missing amino acid total for pre-enrollment day 3",
  "day3_tube",        "Missing whether patient received tube feeding on pre-enrollment day 3",
  "day3_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 3",
  "day3_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 3",
  "day3_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 3",
  "day3_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 3",
  "day3_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 3",
  "day3_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 3",
  "day3_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 3",
  "day4_date",        "Missing date of pre-enrollment day 4",
  "day4_tpn",         "Missing whether patient received TPN on pre-enrollment day 4",
  "day4_goal_kcal",   "Missing calorie goal for pre-enrollment day 4",
  "day4_total_kcal",  "Missing total calories for pre-enrollment day 4",
  "day4_goal_aa",     "Missing amino acid goal for pre-enrollment day 4",
  "day4_total_aa",    "Missing amino acid total for pre-enrollment day 4",
  "day4_tube",        "Missing whether patient received tube feeding on pre-enrollment day 4",
  "day4_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 4",
  "day4_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 4",
  "day4_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 4",
  "day4_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 4",
  "day4_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 4",
  "day4_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 4",
  "day4_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 4",
  "day5_date",        "Missing date of pre-enrollment day 5",
  "day5_tpn",         "Missing whether patient received TPN on pre-enrollment day 5",
  "day5_goal_kcal",   "Missing calorie goal for pre-enrollment day 5",
  "day5_total_kcal",  "Missing total calories for pre-enrollment day 5",
  "day5_goal_aa",     "Missing amino acid goal for pre-enrollment day 5",
  "day5_total_aa",    "Missing amino acid total for pre-enrollment day 5",
  "day5_tube",        "Missing whether patient received tube feeding on pre-enrollment day 5",
  "day5_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 5",
  "day5_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 5",
  "day5_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 5",
  "day5_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 5",
  "day5_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 5",
  "day5_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 5",
  "day5_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 5",
  "day6_date",        "Missing date of pre-enrollment day 6",
  "day6_tpn",         "Missing whether patient received TPN on pre-enrollment day 6",
  "day6_goal_kcal",   "Missing calorie goal for pre-enrollment day 6",
  "day6_total_kcal",  "Missing total calories for pre-enrollment day 6",
  "day6_goal_aa",     "Missing amino acid goal for pre-enrollment day 6",
  "day6_total_aa",    "Missing amino acid total for pre-enrollment day 6",
  "day6_tube",        "Missing whether patient received tube feeding on pre-enrollment day 6",
  "day6_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 6",
  "day6_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 6",
  "day6_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 6",
  "day6_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 6",
  "day6_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 6",
  "day6_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 6",
  "day6_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 6",
  "day7_date",        "Missing date of pre-enrollment day 7",
  "day7_tpn",         "Missing whether patient received TPN on pre-enrollment day 7",
  "day7_goal_kcal",   "Missing calorie goal for pre-enrollment day 7",
  "day7_total_kcal",  "Missing total calories for pre-enrollment day 7",
  "day7_goal_aa",     "Missing amino acid goal for pre-enrollment day 7",
  "day7_total_aa",    "Missing amino acid total for pre-enrollment day 7",
  "day7_tube",        "Missing whether patient received tube feeding on pre-enrollment day 7",
  "day7_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 7",
  "day7_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 7",
  "day7_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 7",
  "day7_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 7",
  "day7_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 7",
  "day7_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 7",
  "day7_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 7",
  "day8_date",        "Missing date of pre-enrollment day 8",
  "day8_tpn",         "Missing whether patient received TPN on pre-enrollment day 8",
  "day8_goal_kcal",   "Missing calorie goal for pre-enrollment day 8",
  "day8_total_kcal",  "Missing total calories for pre-enrollment day 8",
  "day8_goal_aa",     "Missing amino acid goal for pre-enrollment day 8",
  "day8_total_aa",    "Missing amino acid total for pre-enrollment day 8",
  "day8_tube",        "Missing whether patient received tube feeding on pre-enrollment day 8",
  "day8_tube_types",  "Missing number of types of tube feeds on pre-enrollment day 8",
  "day8_tube_type_1", "Missing first type of tube feeding on pre-enrollment day 8",
  "day8_tube_vol_1",  "Missing first volume of tube feeding on pre-enrollment day 8",
  "day8_tube_type_2", "Missing second type of tube feeding on pre-enrollment day 8",
  "day8_tube_vol_2",  "Missing second volume of tube feeding on pre-enrollment day 8",
  "day8_tube_type_3", "Missing third type of tube feeding on pre-enrollment day 8",
  "day8_tube_vol_3",  "Missing third volume of tube feeding on pre-enrollment day 8"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in day1_df; columns = # potential issues
nutr_issues <- matrix(
  FALSE, ncol = nrow(nutr_codes), nrow = nrow(day1_df)
)
colnames(nutr_issues) <- nutr_codes$code
rownames(nutr_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

nutr_issues[, "days_enroll"] <- is.na(day1_df$nutri_days)

## Day 1
nutr_issues[, "day1_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 1 & is.na(nutri_pre_1)
})
nutr_issues[, "day1_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 1 & is.na(nutri_pre_1_tpn_yn)
})
nutr_issues[, "day1_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_1_tpn_yn) & nutri_pre_1_tpn_yn == "Yes" & is.na(tpn_cal_goal_1)
})
nutr_issues[, "day1_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_1_tpn_yn) & nutri_pre_1_tpn_yn == "Yes" & is.na(tpn_cal_tot_1)
})
nutr_issues[, "day1_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_1_tpn_yn) & nutri_pre_1_tpn_yn == "Yes" & is.na(tpn_goal_amino_1)
})
nutr_issues[, "day1_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_1_tpn_yn) & nutri_pre_1_tpn_yn == "Yes" & is.na(tpn_tot_amino_1)
})
nutr_issues[, "day1_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 1 & is.na(tube_yn_1)
})
nutr_issues[, "day1_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_1) & tube_yn_1 == "Yes" & is.na(tube_num_1)
})
nutr_issues[, "day1_tube_type_1"] <- with(day1_df, {
  !is.na(tube_num_1) & tube_num_1 >= 1 & is.na(tube_type_1_1)
})
nutr_issues[, "day1_tube_vol_1"] <- with(day1_df, {
  !is.na(tube_num_1) & tube_num_1 >= 1 & is.na(tube_vol_1_1)
})
nutr_issues[, "day1_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_1) & tube_num_1 >= 2 & is.na(tube_type_2_1)
})
nutr_issues[, "day1_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_1) & tube_num_1 >= 2 & is.na(tube_vol_2_1)
})
nutr_issues[, "day1_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_1) & tube_num_1 >= 3 & is.na(tube_type_3_1)
})
nutr_issues[, "day1_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_1) & tube_num_1 >= 3 & is.na(tube_vol_3_1)
})

## Day 2
nutr_issues[, "day2_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 2 & is.na(nutri_pre_2)
})
nutr_issues[, "day2_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 2 & is.na(nutri_pre_2_tpn_yn)
})
nutr_issues[, "day2_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_2_tpn_yn) & nutri_pre_2_tpn_yn == "Yes" & is.na(tpn_cal_goal_2)
})
nutr_issues[, "day2_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_2_tpn_yn) & nutri_pre_2_tpn_yn == "Yes" & is.na(tpn_cal_tot_2)
})
nutr_issues[, "day2_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_2_tpn_yn) & nutri_pre_2_tpn_yn == "Yes" & is.na(tpn_goal_amino_2)
})
nutr_issues[, "day2_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_2_tpn_yn) & nutri_pre_2_tpn_yn == "Yes" & is.na(tpn_tot_amino_2)
})
nutr_issues[, "day2_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 2 & is.na(tube_yn_2)
})
nutr_issues[, "day2_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_2) & tube_yn_2 == "Yes" & is.na(tube_num_2)
})
nutr_issues[, "day2_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_2) & tube_num_2 >= 2 & is.na(tube_type_2_2)
})
nutr_issues[, "day2_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_2) & tube_num_2 >= 2 & is.na(tube_vol_2_2)
})
nutr_issues[, "day2_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_2) & tube_num_2 >= 2 & is.na(tube_type_2_2)
})
nutr_issues[, "day2_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_2) & tube_num_2 >= 2 & is.na(tube_vol_2_2)
})
nutr_issues[, "day2_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_2) & tube_num_2 >= 3 & is.na(tube_type_3_2)
})
nutr_issues[, "day2_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_2) & tube_num_2 >= 3 & is.na(tube_vol_3_2)
})

## Day 3
nutr_issues[, "day3_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 3 & is.na(nutri_pre_3)
})
nutr_issues[, "day3_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 3 & is.na(nutri_pre_3_tpn_yn)
})
nutr_issues[, "day3_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_3_tpn_yn) & nutri_pre_3_tpn_yn == "Yes" & is.na(tpn_cal_goal_3)
})
nutr_issues[, "day3_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_3_tpn_yn) & nutri_pre_3_tpn_yn == "Yes" & is.na(tpn_cal_tot_3)
})
nutr_issues[, "day3_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_3_tpn_yn) & nutri_pre_3_tpn_yn == "Yes" & is.na(tpn_goal_amino_3)
})
nutr_issues[, "day3_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_3_tpn_yn) & nutri_pre_3_tpn_yn == "Yes" & is.na(tpn_tot_amino_3)
})
nutr_issues[, "day3_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 3 & is.na(tube_yn_3)
})
nutr_issues[, "day3_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_3) & tube_yn_3 == "Yes" & is.na(tube_num_3)
})
nutr_issues[, "day3_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_3) & tube_num_3 >= 3 & is.na(tube_type_3_3)
})
nutr_issues[, "day3_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_3) & tube_num_3 >= 3 & is.na(tube_vol_3_3)
})
nutr_issues[, "day3_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_3) & tube_num_3 >= 2 & is.na(tube_type_2_3)
})
nutr_issues[, "day3_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_3) & tube_num_3 >= 2 & is.na(tube_vol_2_3)
})
nutr_issues[, "day3_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_3) & tube_num_3 >= 3 & is.na(tube_type_3_3)
})
nutr_issues[, "day3_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_3) & tube_num_3 >= 3 & is.na(tube_vol_3_3)
})

## Day 4
nutr_issues[, "day4_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 4 & is.na(nutri_pre_4)
})
nutr_issues[, "day4_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 4 & is.na(nutri_pre_4_tpn_yn)
})
nutr_issues[, "day4_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_4_tpn_yn) & nutri_pre_4_tpn_yn == "Yes" & is.na(tpn_cal_goal_4)
})
nutr_issues[, "day4_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_4_tpn_yn) & nutri_pre_4_tpn_yn == "Yes" & is.na(tpn_cal_tot_4)
})
nutr_issues[, "day4_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_4_tpn_yn) & nutri_pre_4_tpn_yn == "Yes" & is.na(tpn_goal_amino_4)
})
nutr_issues[, "day4_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_4_tpn_yn) & nutri_pre_4_tpn_yn == "Yes" & is.na(tpn_tot_amino_4)
})
nutr_issues[, "day4_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 4 & is.na(tube_yn_4)
})
nutr_issues[, "day4_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_4) & tube_yn_4 == "Yes" & is.na(tube_num_4)
})
nutr_issues[, "day4_tube_type_1"] <- with(day1_df, {
  !is.na(tube_num_4) & tube_num_4 >= 4 & is.na(tube_type_1_4)
})
nutr_issues[, "day4_tube_vol_1"] <- with(day1_df, {
  !is.na(tube_num_4) & tube_num_4 >= 4 & is.na(tube_vol_1_4)
})
nutr_issues[, "day4_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_4) & tube_num_4 >= 2 & is.na(tube_type_2_4)
})
nutr_issues[, "day4_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_4) & tube_num_4 >= 2 & is.na(tube_vol_2_4)
})
nutr_issues[, "day4_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_4) & tube_num_4 >= 3 & is.na(tube_type_3_4)
})
nutr_issues[, "day4_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_4) & tube_num_4 >= 3 & is.na(tube_vol_3_4)
})

## Day 5
nutr_issues[, "day5_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 5 & is.na(nutri_pre_5)
})
nutr_issues[, "day5_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 5 & is.na(nutri_pre_5_tpn_yn)
})
nutr_issues[, "day5_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_5_tpn_yn) & nutri_pre_5_tpn_yn == "Yes" & is.na(tpn_cal_goal_5)
})
nutr_issues[, "day5_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_5_tpn_yn) & nutri_pre_5_tpn_yn == "Yes" & is.na(tpn_cal_tot_5)
})
nutr_issues[, "day5_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_5_tpn_yn) & nutri_pre_5_tpn_yn == "Yes" & is.na(tpn_goal_amino_5)
})
nutr_issues[, "day5_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_5_tpn_yn) & nutri_pre_5_tpn_yn == "Yes" & is.na(tpn_tot_amino_5)
})
nutr_issues[, "day5_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 5 & is.na(tube_yn_5)
})
nutr_issues[, "day5_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_5) & tube_yn_5 == "Yes" & is.na(tube_num_5)
})
nutr_issues[, "day5_tube_type_1"] <- with(day1_df, {
  !is.na(tube_num_5) & tube_num_5 >= 5 & is.na(tube_type_1_5)
})
nutr_issues[, "day5_tube_vol_1"] <- with(day1_df, {
  !is.na(tube_num_5) & tube_num_5 >= 5 & is.na(tube_vol_1_5)
})
nutr_issues[, "day5_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_5) & tube_num_5 >= 2 & is.na(tube_type_2_5)
})
nutr_issues[, "day5_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_5) & tube_num_5 >= 2 & is.na(tube_vol_2_5)
})
nutr_issues[, "day5_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_5) & tube_num_5 >= 3 & is.na(tube_type_3_5)
})
nutr_issues[, "day5_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_5) & tube_num_5 >= 3 & is.na(tube_vol_3_5)
})

## Day 6
nutr_issues[, "day6_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 6 & is.na(nutri_pre_6)
})
nutr_issues[, "day6_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 6 & is.na(nutri_pre_6_tpn_yn)
})
nutr_issues[, "day6_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_6_tpn_yn) & nutri_pre_6_tpn_yn == "Yes" & is.na(tpn_cal_goal_6)
})
nutr_issues[, "day6_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_6_tpn_yn) & nutri_pre_6_tpn_yn == "Yes" & is.na(tpn_cal_tot_6)
})
nutr_issues[, "day6_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_6_tpn_yn) & nutri_pre_6_tpn_yn == "Yes" & is.na(tpn_goal_amino_6)
})
nutr_issues[, "day6_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_6_tpn_yn) & nutri_pre_6_tpn_yn == "Yes" & is.na(tpn_tot_amino_6)
})
nutr_issues[, "day6_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 6 & is.na(tube_yn_6)
})
nutr_issues[, "day6_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_6) & tube_yn_6 == "Yes" & is.na(tube_num_6)
})
nutr_issues[, "day6_tube_type_1"] <- with(day1_df, {
  !is.na(tube_num_6) & tube_num_6 >= 6 & is.na(tube_type_1_6)
})
nutr_issues[, "day6_tube_vol_1"] <- with(day1_df, {
  !is.na(tube_num_6) & tube_num_6 >= 6 & is.na(tube_vol_1_6)
})
nutr_issues[, "day6_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_6) & tube_num_6 >= 2 & is.na(tube_type_2_6)
})
nutr_issues[, "day6_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_6) & tube_num_6 >= 2 & is.na(tube_vol_2_6)
})
nutr_issues[, "day6_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_6) & tube_num_6 >= 3 & is.na(tube_type_3_6)
})
nutr_issues[, "day6_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_6) & tube_num_6 >= 3 & is.na(tube_vol_3_6)
})

## Day 7
nutr_issues[, "day7_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 7 & is.na(nutri_pre_7)
})
nutr_issues[, "day7_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 7 & is.na(nutri_pre_7_tpn_yn)
})
nutr_issues[, "day7_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_7_tpn_yn) & nutri_pre_7_tpn_yn == "Yes" & is.na(tpn_cal_goal_7)
})
nutr_issues[, "day7_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_7_tpn_yn) & nutri_pre_7_tpn_yn == "Yes" & is.na(tpn_cal_tot_7)
})
nutr_issues[, "day7_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_7_tpn_yn) & nutri_pre_7_tpn_yn == "Yes" & is.na(tpn_goal_amino_7)
})
nutr_issues[, "day7_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_7_tpn_yn) & nutri_pre_7_tpn_yn == "Yes" & is.na(tpn_tot_amino_7)
})
nutr_issues[, "day7_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 7 & is.na(tube_yn_7)
})
nutr_issues[, "day7_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_7) & tube_yn_7 == "Yes" & is.na(tube_num_7)
})
nutr_issues[, "day7_tube_type_1"] <- with(day1_df, {
  !is.na(tube_num_7) & tube_num_7 >= 7 & is.na(tube_type_1_7)
})
nutr_issues[, "day7_tube_vol_1"] <- with(day1_df, {
  !is.na(tube_num_7) & tube_num_7 >= 7 & is.na(tube_vol_1_7)
})
nutr_issues[, "day7_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_7) & tube_num_7 >= 2 & is.na(tube_type_2_7)
})
nutr_issues[, "day7_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_7) & tube_num_7 >= 2 & is.na(tube_vol_2_7)
})
nutr_issues[, "day7_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_7) & tube_num_7 >= 3 & is.na(tube_type_3_7)
})
nutr_issues[, "day7_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_7) & tube_num_7 >= 3 & is.na(tube_vol_3_7)
})

## Day 1
nutr_issues[, "day8_date"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 8 & is.na(nutri_pre_8)
})
nutr_issues[, "day8_tpn"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 8 & is.na(nutri_pre_8_tpn_yn)
})
nutr_issues[, "day8_goal_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_8_tpn_yn) & nutri_pre_8_tpn_yn == "Yes" & is.na(tpn_cal_goal_8)
})
nutr_issues[, "day8_total_kcal"] <- with(day1_df, {
  !is.na(nutri_pre_8_tpn_yn) & nutri_pre_8_tpn_yn == "Yes" & is.na(tpn_cal_tot_8)
})
nutr_issues[, "day8_goal_aa"] <- with(day1_df, {
  !is.na(nutri_pre_8_tpn_yn) & nutri_pre_8_tpn_yn == "Yes" & is.na(tpn_goal_amino_8)
})
nutr_issues[, "day8_total_aa"] <- with(day1_df, {
  !is.na(nutri_pre_8_tpn_yn) & nutri_pre_8_tpn_yn == "Yes" & is.na(tpn_tot_amino_8)
})
nutr_issues[, "day8_tube"] <- with(day1_df, {
  !is.na(nutri_days) & nutri_days >= 8 & is.na(tube_yn_8)
})
nutr_issues[, "day8_tube_types"] <- with(day1_df, {
  !is.na(tube_yn_8) & tube_yn_8 == "Yes" & is.na(tube_num_8)
})
nutr_issues[, "day8_tube_type_1"] <- with(day1_df, {
  !is.na(tube_num_8) & tube_num_8 >= 8 & is.na(tube_type_1_8)
})
nutr_issues[, "day8_tube_vol_1"] <- with(day1_df, {
  !is.na(tube_num_8) & tube_num_8 >= 8 & is.na(tube_vol_1_8)
})
nutr_issues[, "day8_tube_type_2"] <- with(day1_df, {
  !is.na(tube_num_8) & tube_num_8 >= 2 & is.na(tube_type_2_8)
})
nutr_issues[, "day8_tube_vol_2"] <- with(day1_df, {
  !is.na(tube_num_8) & tube_num_8 >= 2 & is.na(tube_vol_2_8)
})
nutr_issues[, "day8_tube_type_3"] <- with(day1_df, {
  !is.na(tube_num_8) & tube_num_8 >= 3 & is.na(tube_type_3_8)
})
nutr_issues[, "day8_tube_vol_3"] <- with(day1_df, {
  !is.na(tube_num_8) & tube_num_8 >= 3 & is.na(tube_vol_3_8)
})

## -- Create a final data.frame of errors + messages ---------------------------
nutr_errors <- create_error_df(
  error_matrix = nutr_issues, error_codes = nutr_codes
)

nutr_final <- nutr_errors %>%
  mutate(form = "Enrollment Nutrition Data")

################################################################################
## Daily Data Collection (MDS)
################################################################################

## -- Missingness checks -------------------------------------------------------
dailymds_missvars <- c(
  "daily_date", "vent_today", "phys_restrain", "remove_device", "ptot_daily",
  "tube_daily_yn"
)
dailymds_missing <- check_missing(
  df = daily_df, variables = dailymds_missvars, ddict = ih_ddict
)

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
dailymds_codes <- tribble(
  ~ code,        ~ msg,
  "daily_date_right", "Date of Daily Data Collection (MDS) does not match dates tracking; please check form date and/or enrollment date",
  ## ABC
  "sedation",      "Missing whether patient received sedation",
  "sat",           "On sedation, but missing whether patient received an SAT",
  "sat_rsn",       "Missing reason patient did not get SAT",
  "sat_rsn_other", "Missing explanation of other reason patient did not get SAT",
  "sbt",           "On MV, but missing whether patient received an SBT",
  "sbt_rsn",       "Missing reason patient did not get SBT",
  "sbt_rsn_other", "Missing explanation of other reason patient did not get SBT",
  "sat_sbt",       "Missing whether patient off sedation during SBT",
  ## Agitation
  "device_which", "Missing which devices patient removed",
  "device_other", "Missing explanation of other device removed",
  ## PT/OT
  "ptot_actual", "Missing whether ordered PT/OT actually occurred",
  ## Nutrition
  "tube_types",     "Missing how many types of tube feeding",
  "tube_type_1",    "No option marked for type of tube feeding 1",
  "tube_vol_1",     "Missing volume of tube feeding 1",
  "tube_type_2",    "No option marked for type of tube feeding 2",
  "tube_vol_2",     "Missing volume of tube feeding 2",
  "tpn_cals_goal",  "Missing calorie goal for TPN",
  "tpn_cals_total", "Missing total calories for TPN",
  "tpn_aa_goal",    "Missing amino acid goal for TPN",
  "tpn_aa_total",   "Missing total amino acid for TPN"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
dailymds_issues <- matrix(
  FALSE, ncol = nrow(dailymds_codes), nrow = nrow(daily_df)
)
colnames(dailymds_issues) <- dailymds_codes$code
rownames(dailymds_issues) <- with(daily_df, {
  paste(id, redcap_event_name, sep = '; ') })

dailymds_issues[, "daily_date_right"] <- with(daily_df, {
  !is.na(daily_date) & !is.na(study_date) & !(study_date == daily_date)
})
## ABC
dailymds_issues[, "sedation"] <- with(daily_df, {
  !is.na(vent_today) & vent_today == "Yes" & is.na(sedation_today)
})
dailymds_issues[, "sat"] <- with(daily_df, {
  !is.na(sedation_today) & sedation_today == "Yes" & is.na(sat_today)
})
dailymds_issues[, "sat_rsn"] <- with(daily_df, {
  !is.na(sat_today) & sat_today == "No" &
    rowSums(
      !is.na(daily_df[, grep("^sat\\_rsn\\_[0-9]+$", names(daily_df))])
    ) == 0
})
dailymds_issues[, "sat_rsn_other"] <- with(daily_df, {
  !is.na(sat_rsn_99) & (is.na(sat_other) | sat_other == "")
})
dailymds_issues[, "sbt"] <- with(daily_df, {
  !is.na(vent_today) & vent_today == "Yes" & is.na(sbt_today)
})
dailymds_issues[, "sbt_rsn"] <- with(daily_df, {
  !is.na(sbt_today) & sbt_today == "No" &
    rowSums(
      !is.na(daily_df[, grep("^sbt\\_rsn\\_[0-9]+$", names(daily_df))])
    ) == 0
})
dailymds_issues[, "sbt_rsn_other"] <- with(daily_df, {
  !is.na(sbt_rsn_99) & (is.na(sbt_other) | sbt_other == "")
})
dailymds_issues[, "sat_sbt"] <- with(daily_df, {
  !is.na(sbt_today) & sbt_today == "Yes" & is.na(abc_paired)
})

## Agitation
dailymds_issues[, "device_which"] <- with(daily_df, {
  !is.na(remove_device) & remove_device == "Yes" &
    rowSums(
      !is.na(daily_df[, grep("^devices\\_removed\\_[0-9]+$", names(daily_df))])
    ) == 0
})
dailymds_issues[, "device_other"] <- with(daily_df, {
  !is.na(devices_removed_99) & (is.na(device_other) | device_other == "")
})

## PT/OT
dailymds_issues[, "ptot_actual"] <- with(daily_df, {
  !is.na(ptot_daily) & ptot_daily == "Yes" & is.na(ptot_y_daily)
})

## Nutrition
dailymds_issues[, "tube_types"] <- with(daily_df, {
  !is.na(tube_daily_yn) & tube_daily_yn == "Yes" & is.na(tube_daily_num)
})
dailymds_issues[, "tube_type_1"] <- with(daily_df, {
  !is.na(tube_daily_num) & tube_daily_num >= 1 &
    rowSums(
      !is.na(daily_df[, grep("^tube\\_daily\\_type\\_1\\_[0-9]+$", names(daily_df))])
    ) == 0
})
dailymds_issues[, "tube_vol_1"] <- with(daily_df, {
  !is.na(tube_daily_num) & tube_daily_num >= 1 & is.na(tube_daily_vol_1)
})
dailymds_issues[, "tube_type_2"] <- with(daily_df, {
  !is.na(tube_daily_num) & tube_daily_num >= 2 &
    rowSums(
      !is.na(daily_df[, grep("^tube\\_daily\\_type\\_2\\_[0-9]+$", names(daily_df))])
    ) == 0
})
dailymds_issues[, "tube_vol_2"] <- with(daily_df, {
  !is.na(tube_daily_num) & tube_daily_num >= 2 & is.na(tube_daily_vol_2)
})
dailymds_issues[, "tpn_cals_goal"] <- with(daily_df, {
  !is.na(tpn_daily_yn) & tpn_daily_yn == "Yes" & is.na(cal_goal)
})
dailymds_issues[, "tpn_cals_total"] <- with(daily_df, {
  !is.na(tpn_daily_yn) & tpn_daily_yn == "Yes" & is.na(cal_tot)
})
dailymds_issues[, "tpn_aa_goal"] <- with(daily_df, {
  !is.na(tpn_daily_yn) & tpn_daily_yn == "Yes" & is.na(amino_goal)
})
dailymds_issues[, "tpn_aa_total"] <- with(daily_df, {
  !is.na(tpn_daily_yn) & tpn_daily_yn == "Yes" & is.na(amino_tot)
})

## -- Create a final data.frame of errors + messages ---------------------------
dailymds_errors <- create_error_df(
  error_matrix = dailymds_issues, error_codes = dailymds_codes
)

dailymds_final <- bind_rows(dailymds_missing, dailymds_errors) %>%
  mutate(form = "Daily Data Collection (MDS)")

################################################################################
## PAD Form
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
pad_codes <- tribble(
  ~ code,        ~ msg,
  "assess_date_na",    "Missing date of PAD assessment",
  "assess_date_right", "Date of PAD assessment does not match dates tracking; please check PAD date and/or enrollment date",
  "assess_num",        "Missing number of assessments done today",
  ## Should be 2 assessments in the ICU and 1 outside the ICU. In the interest
  ## of time, this check was not incorporated, because it will require lots of
  ## work with the dates tracking information. This is a place for future
  ## improvement.
  ## No assessments done today
  "assess_none_rass",       "Missing reason RASS not done",
  "assess_none_rass_other", "Missing explanation for other reason RASS not done",
  "assess_none_cam",        "Missing reason CAM-ICU not done",
  "assess_none_cam_other",  "Missing explanation for other reason CAM-ICU not done",
  "assess_none_cpot",       "Missing reason CPOT not done",
  "assess_none_cpot_other", "Missing explanation for other reason CPOT not done",
  ## >=1 assessment done today
  "assess_1_time",        "Missing time of first assessment",
  "assess_1_who",         "Missing who completed first PAD assessment",
  "assess_1_rass",        "Missing RASS from first assessment",
  "assess_1_rass_nd",     "Missing reason first RASS was not done",
  "assess_1_rass_other",  "Missing explanation for other reason first RASS not done",
  "assess_1_rass_target", "Missing first target RASS",
  "assess_1_cpot",        "Missing CPOT from first assessment",
  "assess_1_cpot_nd",     "Missing reason first CPOT was not done",
  "assess_1_cpot_other",  "Missing explanation for other reason first CPOT not done",
  "assess_1_cam",         "Missing CAM-ICU from first assessment",
  "assess_1_cam_nd",      "Missing reason first CAM-ICU was not done",
  "assess_1_cam_f1",      "Missing first CAM-ICU Feature 1",
  "assess_1_cam_f2",      "Missing first CAM-ICU Feature 2",
  "assess_1_cam_f4a",     "Missing first CAM-ICU Feature 4a",
  "assess_1_cam_f4b",     "Missing first CAM-ICU Feature 4b",
  ## >=2 assessments done today
  "assess_2_time",        "Missing time of second assessment",
  "assess_2_who",         "Missing who completed second PAD assessment",
  "assess_2_rass",        "Missing RASS from second assessment",
  "assess_2_rass_nd",     "Missing reason second RASS was not done",
  "assess_2_rass_other",  "Missing explanation for other reason second RASS not done",
  "assess_2_rass_target", "Missing second target RASS",
  "assess_2_cpot",        "Missing CPOT from second assessment",
  "assess_2_cpot_nd",     "Missing reason second CPOT was not done",
  "assess_2_cpot_other",  "Missing explanation for other reason second CPOT not done",
  "assess_2_cam",         "Missing CAM-ICU from second assessment",
  "assess_2_cam_nd",      "Missing reason second CAM-ICU was not done",
  "assess_2_cam_f1",      "Missing second CAM-ICU Feature 1",
  "assess_2_cam_f2",      "Missing second CAM-ICU Feature 2",
  "assess_2_cam_f4a",     "Missing second CAM-ICU Feature 4a",
  "assess_2_cam_f4b",     "Missing second CAM-ICU Feature 4b"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
pad_issues <- matrix(
  FALSE, ncol = nrow(pad_codes), nrow = nrow(daily_df)
)
colnames(pad_issues) <- pad_codes$code
rownames(pad_issues) <- with(daily_df, {
  paste(id, redcap_event_name, sep = '; ') })

pad_issues[, "assess_date_na"] <- is.na(daily_df$assess_date)
pad_issues[, "assess_date_right"] <- with(daily_df, {
  !is.na(assess_date) & !is.na(study_date) & !(study_date == assess_date)
})
pad_issues[, "assess_num"] <- is.na(daily_df$assess_number)

## Should be 2 assessments in the ICU and 1 outside the ICU. In the interest
## of time, this check was not incorporated, because it will require lots of
## work with the dates tracking information. This is a place for future
## improvement.

## No assessments done today
pad_issues[, "assess_none_rass"] <- with(daily_df, {
  !is.na(assess_number) & assess_number == 0 & is.na(rass_inc_rsn_0)
})
pad_issues[, "assess_none_rass"] <- with(daily_df, {
  !is.na(rass_inc_rsn_0) & rass_inc_rsn_0 == "F. Other (see NTF)" &
    (is.na(rass_inc_other_0) | rass_inc_other_0 == "")
})
pad_issues[, "assess_none_cpot"] <- with(daily_df, {
  !is.na(assess_number) & assess_number == 0 & is.na(cpot_inc_rsn_0)
})
pad_issues[, "assess_none_cpot"] <- with(daily_df, {
  !is.na(cpot_inc_rsn_0) & cpot_inc_rsn_0 == "F. Other (see NTF)" &
    (is.na(cpot_inc_other_0) | cpot_inc_other_0 == "")
})
pad_issues[, "assess_none_cam"] <- with(daily_df, {
  !is.na(assess_number) & assess_number == 0 & is.na(cam_inc_rsn_0)
})
pad_issues[, "assess_none_cam"] <- with(daily_df, {
  !is.na(cam_inc_rsn_0) & cam_inc_rsn_0 == "F. Other (see NTF)" &
    (is.na(cam_inc_other_0) | cam_inc_other_0 == "")
})

## >=1 assessment done today
pad_issues[, "assess_1_time"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 1 & is.na(assess_dttm_1)
})
pad_issues[, "assess_1_who"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 1 & is.na(assess_who_1)
})
pad_issues[, "assess_1_rass"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 1 & is.na(rass_actual_1)
})
pad_issues[, "assess_1_rass_nd"] <- with(daily_df, {
  !is.na(rass_actual_1) & rass_actual_1 == "Not done" & is.na(rass_inc_rsn_1)
})
pad_issues[, "assess_1_rass_other"] <- with(daily_df, {
  !is.na(rass_inc_rsn_1) & rass_inc_rsn_1 == "E. Other (see NTF)" &
    (is.na(rass_inc_other_1) | rass_inc_other_1 == "")
})
pad_issues[, "assess_1_rass_target"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 1 & is.na(rass_target_1)
})
pad_issues[, "assess_1_cpot"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 1 & is.na(cpot_1)
})
pad_issues[, "assess_1_cpot_nd"] <- with(daily_df, {
  !is.na(cpot_1) & cpot_1 == "Not done" & is.na(cpot_inc_rsn_1)
})
pad_issues[, "assess_1_cpot_other"] <- with(daily_df, {
  !is.na(cpot_inc_rsn_1) & cpot_inc_rsn_1 == "E. Other (see NTF)" &
    (is.na(cpot_inc_other_1) | cpot_inc_other_1 == "")
})
pad_issues[, "assess_1_cam"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 1 & is.na(cam_1)
})
pad_issues[, "assess_1_cam_nd"] <- with(daily_df, {
  !is.na(cam_1) & cam_1 == "Not Done" & (is.na(cam_na_1) | cam_na_1 == "")
})
pad_issues[, "assess_1_cam_f1"] <- with(daily_df, {
  !is.na(cam_1) & cam_1 %in% c("Negative", "Postive") & is.na(cam_f1_1)
})
pad_issues[, "assess_1_cam_f2"] <- with(daily_df, {
  !is.na(cam_1) & cam_1 %in% c("Negative", "Postive") & is.na(cam_f2_1)
})
pad_issues[, "assess_1_cam_f4a"] <- with(daily_df, {
  !is.na(cam_1) & cam_1 %in% c("Negative", "Postive") & is.na(cam_f4a_1)
})
pad_issues[, "assess_1_cam_f4b"] <- with(daily_df, {
  !is.na(cam_1) & cam_1 %in% c("Negative", "Postive") & is.na(cam_f4b_1)
})

## >=2 assessments done today
pad_issues[, "assess_2_time"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 2 & is.na(assess_dttm_2)
})
pad_issues[, "assess_2_who"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 2 & is.na(assess_who_2)
})
pad_issues[, "assess_2_rass"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 2 & is.na(rass_actual_2)
})
pad_issues[, "assess_2_rass_nd"] <- with(daily_df, {
  !is.na(rass_actual_2) & rass_actual_2 == "Not done" & is.na(rass_inc_rsn_2)
})
pad_issues[, "assess_2_rass_other"] <- with(daily_df, {
  !is.na(rass_inc_rsn_2) & rass_inc_rsn_2 == "E. Other (see NTF)" &
    (is.na(rass_inc_other_2) | rass_inc_other_2 == "")
})
pad_issues[, "assess_2_rass_target"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 2 & is.na(rass_target_2)
})
pad_issues[, "assess_2_cpot"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 2 & is.na(cpot_2)
})
pad_issues[, "assess_2_cpot_nd"] <- with(daily_df, {
  !is.na(cpot_2) & cpot_2 == "Not done" & is.na(cpot_inc_rsn_2)
})
pad_issues[, "assess_2_cpot_other"] <- with(daily_df, {
  !is.na(cpot_inc_rsn_2) & cpot_inc_rsn_2 == "E. Other (see NTF)" &
    (is.na(cpot_inc_other_2) | cpot_inc_other_2 == "")
})
pad_issues[, "assess_2_cam"] <- with(daily_df, {
  !is.na(assess_number) & assess_number >= 2 & is.na(cam_2)
})
pad_issues[, "assess_2_cam_nd"] <- with(daily_df, {
  !is.na(cam_2) & cam_2 == "Not Done" & (is.na(cam_na_2) | cam_na_2 == "")
})
pad_issues[, "assess_2_cam_f1"] <- with(daily_df, {
  !is.na(cam_2) & cam_2 %in% c("Negative", "Postive") & is.na(cam_f1_2)
})
pad_issues[, "assess_2_cam_f2"] <- with(daily_df, {
  !is.na(cam_2) & cam_2 %in% c("Negative", "Postive") & is.na(cam_f2_2)
})
pad_issues[, "assess_2_cam_f4a"] <- with(daily_df, {
  !is.na(cam_2) & cam_2 %in% c("Negative", "Postive") & is.na(cam_f4a_2)
})
pad_issues[, "assess_2_cam_f4b"] <- with(daily_df, {
  !is.na(cam_2) & cam_2 %in% c("Negative", "Postive") & is.na(cam_f4b_2)
})

## -- Create a final data.frame of errors + messages ---------------------------
pad_errors <- create_error_df(
  error_matrix = pad_issues, error_codes = pad_codes
)

pad_final <- pad_errors %>%
  mutate(form = "Daily PAD Assessment")

################################################################################
## ICU Mobility Scale Form
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
mobility_codes <- tribble(
  ~ code,        ~ msg,
  "mob_date_na",    "Missing date of ICU Mobility Scale assessment",
  "mob_date_right", "Date of ICU Mobility Scale does not match dates tracking; please check form date and/or enrollment date",
  "mob_level",      "Missing highest level achieved on ICU Mobility Scale"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
mobility_issues <- matrix(
  FALSE, ncol = nrow(mobility_codes), nrow = nrow(daily_df)
)
colnames(mobility_issues) <- mobility_codes$code
rownames(mobility_issues) <- with(daily_df, {
  paste(id, redcap_event_name, sep = '; ') })

mobility_issues[, "mob_date_na"] <- is.na(daily_df$ice_mob_date)
mobility_issues[, "mob_date_right"] <- with(daily_df, {
  !is.na(ice_mob_date) & !is.na(study_date) & !(study_date == ice_mob_date)
})
mobility_issues[, "mob_level"] <- is.na(daily_df$ice_mob_scale)

## -- Create a final data.frame of errors + messages ---------------------------
mobility_errors <- create_error_df(
  error_matrix = mobility_issues, error_codes = mobility_codes
)

mobility_final <- mobility_errors %>%
  mutate(form = "ICU Mobility Scale")

################################################################################
## Accelerometer Bedside Form
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
accelbed_codes <- tribble(
  ~ code,        ~ msg,
  "accelbed_date_na",    "Missing date of Accelerometer Bedside form",
  "accelbed_date_right", "Date of Accelerometer Bedside form does not match dates tracking; please check form date and/or enrollment date",
  "accelbed_times",      "Missing number of times device replaced or removed",
  ## Removal/replacement 1
  "accelbed_1_which",      "Missing whether instance 1 was removal or replacement",
  "accelbed_1_time_known", "Missing whether time of replacement 1 was known",
  "accelbed_1_time",       "Missing date and time of replacement 1",
  "accelbed_1_device",     "Missing whether replacement 1 was wrist or ankle",
  "accelbed_1_wrist",      "Missing whether replacement 1 was left or right wrist",
  "accelbed_1_ankle",      "Missing whether replacement 1 was left or right ankle",
  "accelbed_1_rsn",        "Missing reason for device removal 1",
  "accelbed_1_other",      "Missing explanation for other reason for device removal 1",
  ## Removal/replacement 2
  "accelbed_2_which",      "Missing whether instance 2 was removal or replacement",
  "accelbed_2_time_known", "Missing whether time of replacement 2 was known",
  "accelbed_2_time",       "Missing date and time of replacement 2",
  "accelbed_2_device",     "Missing whether replacement 2 was wrist or ankle",
  "accelbed_2_wrist",      "Missing whether replacement 2 was left or right wrist",
  "accelbed_2_ankle",      "Missing whether replacement 2 was left or right ankle",
  "accelbed_2_rsn",        "Missing reason for device removal 2",
  "accelbed_2_other",      "Missing explanation for other reason for device removal 2",
  ## Removal/replacement 3
  "accelbed_3_which",      "Missing whether instance 3 was removal or replacement",
  "accelbed_3_time_known", "Missing whether time of replacement 3 was known",
  "accelbed_3_time",       "Missing date and time of replacement 3",
  "accelbed_3_device",     "Missing whether replacement 3 was wrist or ankle",
  "accelbed_3_wrist",      "Missing whether replacement 3 was left or right wrist",
  "accelbed_3_ankle",      "Missing whether replacement 3 was left or right ankle",
  "accelbed_3_rsn",        "Missing reason for device removal 3",
  "accelbed_3_other",      "Missing explanation for other reason for device removal 3",
  ## Removal/replacement 4
  "accelbed_4_which",      "Missing whether instance 4 was removal or replacement",
  "accelbed_4_time_known", "Missing whether time of replacement 4 was known",
  "accelbed_4_time",       "Missing date and time of replacement 4",
  "accelbed_4_device",     "Missing whether replacement 4 was wrist or ankle",
  "accelbed_4_wrist",      "Missing whether replacement 4 was left or right wrist",
  "accelbed_4_ankle",      "Missing whether replacement 4 was left or right ankle",
  "accelbed_4_rsn",        "Missing reason for device removal 4",
  "accelbed_4_other",      "Missing explanation for other reason for device removal 4",
  ## Removal/replacement 5
  "accelbed_5_which",      "Missing whether instance 5 was removal or replacement",
  "accelbed_5_time_known", "Missing whether time of replacement 5 was known",
  "accelbed_5_time",       "Missing date and time of replacement 5",
  "accelbed_5_device",     "Missing whether replacement 5 was wrist or ankle",
  "accelbed_5_wrist",      "Missing whether replacement 5 was left or right wrist",
  "accelbed_5_ankle",      "Missing whether replacement 5 was left or right ankle",
  "accelbed_5_rsn",        "Missing reason for device removal 5",
  "accelbed_5_other",      "Missing explanation for other reason for device removal 5",
  ## Removal/replacement 6
  "accelbed_6_which",      "Missing whether instance 6 was removal or replacement",
  "accelbed_6_time_known", "Missing whether time of replacement 6 was known",
  "accelbed_6_time",       "Missing date and time of replacement 6",
  "accelbed_6_device",     "Missing whether replacement 6 was wrist or ankle",
  "accelbed_6_wrist",      "Missing whether replacement 6 was left or right wrist",
  "accelbed_6_ankle",      "Missing whether replacement 6 was left or right ankle",
  "accelbed_6_rsn",        "Missing reason for device removal 6",
  "accelbed_6_other",      "Missing explanation for other reason for device removal 6",
  ## Removal/replacement 7
  "accelbed_7_which",      "Missing whether instance 7 was removal or replacement",
  "accelbed_7_time_known", "Missing whether time of replacement 7 was known",
  "accelbed_7_time",       "Missing date and time of replacement 7",
  "accelbed_7_device",     "Missing whether replacement 7 was wrist or ankle",
  "accelbed_7_wrist",      "Missing whether replacement 7 was left or right wrist",
  "accelbed_7_ankle",      "Missing whether replacement 7 was left or right ankle",
  "accelbed_7_rsn",        "Missing reason for device removal 7",
  "accelbed_7_other",      "Missing explanation for other reason for device removal 7",
  ## Removal/replacement 8
  "accelbed_8_which",      "Missing whether instance 8 was removal or replacement",
  "accelbed_8_time_known", "Missing whether time of replacement 8 was known",
  "accelbed_8_time",       "Missing date and time of replacement 8",
  "accelbed_8_device",     "Missing whether replacement 8 was wrist or ankle",
  "accelbed_8_wrist",      "Missing whether replacement 8 was left or right wrist",
  "accelbed_8_ankle",      "Missing whether replacement 8 was left or right ankle",
  "accelbed_8_rsn",        "Missing reason for device removal 8",
  "accelbed_8_other",      "Missing explanation for other reason for device removal 8"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
accelbed_issues <- matrix(
  FALSE, ncol = nrow(accelbed_codes), nrow = nrow(daily_df)
)
colnames(accelbed_issues) <- accelbed_codes$code
rownames(accelbed_issues) <- with(daily_df, {
  paste(id, redcap_event_name, sep = '; ') })

accelbed_issues[, "accelbed_date_na"] <- is.na(daily_df$bed_date)
accelbed_issues[, "accelbed_date_right"] <- with(daily_df, {
  !is.na(bed_date) & !is.na(study_date) & !(study_date == bed_date)
})
accelbed_issues[, "accelbed_times"] <- is.na(daily_df$bed_device_num)

## Removal/replacement 1
accelbed_issues[, "accelbed_1_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 1 & is.na(bed_device_move_place_1)
})
accelbed_issues[, "accelbed_1_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 1 & is.na(bed_accel_time_known_1)
})
accelbed_issues[, "accelbed_1_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_1) & bed_accel_time_known_1 == "Yes" &
    is.na(bed_accel_dttm_1)
})
accelbed_issues[, "accelbed_1_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 1 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_1\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_1_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_1) & bed_device_move_place_1 == "Placed" &
    !is.na(bed_wrist_ankle_1_1) & is.na(bed_wrist_rl_1)
})
accelbed_issues[, "accelbed_1_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_1) & bed_device_move_place_1 == "Placed" &
    !is.na(bed_wrist_ankle_1_2) & is.na(bed_ankle_rl_1)
})
accelbed_issues[, "accelbed_1_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_1) & bed_device_move_place_1 == "Removed" &
    is.na(bed_remove_why_1)
})
accelbed_issues[, "accelbed_1_other"] <- with(daily_df, {
  !is.na(bed_remove_why_1) & bed_remove_why_1 == "Other" &
    (is.na(bed_remove_other_1) | bed_remove_other_1 == "")
})

## Removal/replacement 2
accelbed_issues[, "accelbed_2_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 2 & is.na(bed_device_move_place_2)
})
accelbed_issues[, "accelbed_2_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 2 & is.na(bed_accel_time_known_2)
})
accelbed_issues[, "accelbed_2_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_2) & bed_accel_time_known_2 == "Yes" &
    is.na(bed_accel_dttm_2)
})
accelbed_issues[, "accelbed_2_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 2 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_2\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_2_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_2) & bed_device_move_place_2 == "Placed" &
    !is.na(bed_wrist_ankle_2_1) & is.na(bed_wrist_rl_2)
})
accelbed_issues[, "accelbed_2_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_2) & bed_device_move_place_2 == "Placed" &
    !is.na(bed_wrist_ankle_2_2) & is.na(bed_ankle_rl_2)
})
accelbed_issues[, "accelbed_2_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_2) & bed_device_move_place_2 == "Removed" &
    is.na(bed_remove_why_2)
})
accelbed_issues[, "accelbed_2_other"] <- with(daily_df, {
  !is.na(bed_remove_why_2) & bed_remove_why_2 == "Other" &
    (is.na(bed_remove_other_2) | bed_remove_other_2 == "")
})

## Removal/replacement 3
accelbed_issues[, "accelbed_3_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 3 & is.na(bed_device_move_place_3)
})
accelbed_issues[, "accelbed_3_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 3 & is.na(bed_accel_time_known_3)
})
accelbed_issues[, "accelbed_3_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_3) & bed_accel_time_known_3 == "Yes" &
    is.na(bed_accel_dttm_3)
})
accelbed_issues[, "accelbed_3_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 3 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_3\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_3_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_3) & bed_device_move_place_3 == "Placed" &
    !is.na(bed_wrist_ankle_3_1) & is.na(bed_wrist_rl_3)
})
accelbed_issues[, "accelbed_3_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_3) & bed_device_move_place_3 == "Placed" &
    !is.na(bed_wrist_ankle_3_2) & is.na(bed_ankle_rl_3)
})
accelbed_issues[, "accelbed_3_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_3) & bed_device_move_place_3 == "Removed" &
    is.na(bed_remove_why_3)
})
accelbed_issues[, "accelbed_3_other"] <- with(daily_df, {
  !is.na(bed_remove_why_3) & bed_remove_why_3 == "Other" &
    (is.na(bed_remove_other_3) | bed_remove_other_3 == "")
})

## Removal/replacement 4
accelbed_issues[, "accelbed_4_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 4 & is.na(bed_device_move_place_4)
})
accelbed_issues[, "accelbed_4_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 4 & is.na(bed_accel_time_known_4)
})
accelbed_issues[, "accelbed_4_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_4) & bed_accel_time_known_4 == "Yes" &
    is.na(bed_accel_dttm_4)
})
accelbed_issues[, "accelbed_4_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 4 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_4\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_4_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_4) & bed_device_move_place_4 == "Placed" &
    !is.na(bed_wrist_ankle_4_1) & is.na(bed_wrist_rl_4)
})
accelbed_issues[, "accelbed_4_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_4) & bed_device_move_place_4 == "Placed" &
    !is.na(bed_wrist_ankle_4_2) & is.na(bed_ankle_rl_4)
})
accelbed_issues[, "accelbed_4_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_4) & bed_device_move_place_4 == "Removed" &
    is.na(bed_remove_why_4)
})
accelbed_issues[, "accelbed_4_other"] <- with(daily_df, {
  !is.na(bed_remove_why_4) & bed_remove_why_4 == "Other" &
    (is.na(bed_remove_other_4) | bed_remove_other_4 == "")
})

## Removal/replacement 5
accelbed_issues[, "accelbed_5_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 5 & is.na(bed_device_move_place_5)
})
accelbed_issues[, "accelbed_5_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 5 & is.na(bed_accel_time_known_5)
})
accelbed_issues[, "accelbed_5_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_5) & bed_accel_time_known_5 == "Yes" &
    is.na(bed_accel_dttm_5)
})
accelbed_issues[, "accelbed_5_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 5 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_5\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_5_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_5) & bed_device_move_place_5 == "Placed" &
    !is.na(bed_wrist_ankle_5_1) & is.na(bed_wrist_rl_5)
})
accelbed_issues[, "accelbed_5_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_5) & bed_device_move_place_5 == "Placed" &
    !is.na(bed_wrist_ankle_5_2) & is.na(bed_ankle_rl_5)
})
accelbed_issues[, "accelbed_5_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_5) & bed_device_move_place_5 == "Removed" &
    is.na(bed_remove_why_5)
})
accelbed_issues[, "accelbed_5_other"] <- with(daily_df, {
  !is.na(bed_remove_why_5) & bed_remove_why_5 == "Other" &
    (is.na(bed_remove_other_5) | bed_remove_other_5 == "")
})

## Removal/replacement 6
accelbed_issues[, "accelbed_6_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 6 & is.na(bed_device_move_place_6)
})
accelbed_issues[, "accelbed_6_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 6 & is.na(bed_accel_time_known_6)
})
accelbed_issues[, "accelbed_6_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_6) & bed_accel_time_known_6 == "Yes" &
    is.na(bed_accel_dttm_6)
})
accelbed_issues[, "accelbed_6_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 6 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_6\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_6_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_6) & bed_device_move_place_6 == "Placed" &
    !is.na(bed_wrist_ankle_6_1) & is.na(bed_wrist_rl_6)
})
accelbed_issues[, "accelbed_6_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_6) & bed_device_move_place_6 == "Placed" &
    !is.na(bed_wrist_ankle_6_2) & is.na(bed_ankle_rl_6)
})
accelbed_issues[, "accelbed_6_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_6) & bed_device_move_place_6 == "Removed" &
    is.na(bed_remove_why_6)
})
accelbed_issues[, "accelbed_6_other"] <- with(daily_df, {
  !is.na(bed_remove_why_6) & bed_remove_why_6 == "Other" &
    (is.na(bed_remove_other_6) | bed_remove_other_6 == "")
})

## Removal/replacement 7
accelbed_issues[, "accelbed_7_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 7 & is.na(bed_device_move_place_7)
})
accelbed_issues[, "accelbed_7_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 7 & is.na(bed_accel_time_known_7)
})
accelbed_issues[, "accelbed_7_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_7) & bed_accel_time_known_7 == "Yes" &
    is.na(bed_accel_dttm_7)
})
accelbed_issues[, "accelbed_7_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 7 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_7\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_7_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_7) & bed_device_move_place_7 == "Placed" &
    !is.na(bed_wrist_ankle_7_1) & is.na(bed_wrist_rl_7)
})
accelbed_issues[, "accelbed_7_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_7) & bed_device_move_place_7 == "Placed" &
    !is.na(bed_wrist_ankle_7_2) & is.na(bed_ankle_rl_7)
})
accelbed_issues[, "accelbed_7_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_7) & bed_device_move_place_7 == "Removed" &
    is.na(bed_remove_why_7)
})
accelbed_issues[, "accelbed_7_other"] <- with(daily_df, {
  !is.na(bed_remove_why_7) & bed_remove_why_7 == "Other" &
    (is.na(bed_remove_other_7) | bed_remove_other_7 == "")
})

## Removal/replacement 8
accelbed_issues[, "accelbed_8_which"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 8 & is.na(bed_device_move_place_8)
})
accelbed_issues[, "accelbed_8_time_known"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 8 & is.na(bed_accel_time_known_8)
})
accelbed_issues[, "accelbed_8_time"] <- with(daily_df, {
  !is.na(bed_accel_time_known_8) & bed_accel_time_known_8 == "Yes" &
    is.na(bed_accel_dttm_8)
})
accelbed_issues[, "accelbed_8_device"] <- with(daily_df, {
  !is.na(bed_device_num) & bed_device_num >= 8 &
    rowSums(
      !is.na(daily_df[, grep("^bed\\_wrist\\_ankle\\_8\\_[1|2]$", names(daily_df))])
    ) == 0
})
accelbed_issues[, "accelbed_8_wrist"] <- with(daily_df, {
  !is.na(bed_device_move_place_8) & bed_device_move_place_8 == "Placed" &
    !is.na(bed_wrist_ankle_8_1) & is.na(bed_wrist_rl_8)
})
accelbed_issues[, "accelbed_8_ankle"] <- with(daily_df, {
  !is.na(bed_device_move_place_8) & bed_device_move_place_8 == "Placed" &
    !is.na(bed_wrist_ankle_8_2) & is.na(bed_ankle_rl_8)
})
accelbed_issues[, "accelbed_8_rsn"] <- with(daily_df, {
  !is.na(bed_device_move_place_8) & bed_device_move_place_8 == "Removed" &
    is.na(bed_remove_why_8)
})
accelbed_issues[, "accelbed_8_other"] <- with(daily_df, {
  !is.na(bed_remove_why_8) & bed_remove_why_8 == "Other" &
    (is.na(bed_remove_other_8) | bed_remove_other_8 == "")
})

## -- Create a final data.frame of errors + messages ---------------------------
accelbed_errors <- create_error_df(
  error_matrix = accelbed_issues, error_codes = accelbed_codes
)

accelbed_final <- accelbed_errors %>%
  mutate(form = "Accelerometer Bedside Form")

################################################################################
## Accelerometer Placement Form
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
accelplace_codes <- tribble(
  ~ code,        ~ msg,
  "accelplace_date_na",    "Missing date of Accelerometer Placement Form",
  "accelplace_date_right", "Date of Accelerometer Placement does not match dates tracking; please check form date and/or enrollment date",
  "accelplace_ever",       "Missing whether patient ever wore an accelerometer today",
  "accelplace_rsn",        "Missing reason patient never wore accelerometer today",
  "accelplace_rsn_other",  "No explanation for other or unknown reason patient never wore accelerometer today",
  "accelplace_icu",        "Missing whether patient had ICU orders",
  ## Placement check 1 (all days)
  # "accelplace_1_wrist_num", "Missing wrist device number at placement check 1", ## no longer in database
  "accelplace_1_yn",               "Missing whether placement check 1 was completed",
  "accelplace_1_rsn",              "Missing reason placement check 1 not completed",
  "accelplace_1_other",            "No explanation for other reason placement check 1 not completed",
  "accelplace_1_time",             "Missing time of placement check 1",
  ## Wrist
  "accelplace_1_wrist_yn",         "Missing whether patient was wearing wrist device at placement check 1",
  "accelplace_1_wrist_dom",        "Missing whether wrist device was on dominant hand at placement check 1",
  "accelplace_1_wrist_rsn",        "Missing reason wrist device was not worn at placement check 1",
  "accelplace_1_wrist_other",      "No explanation for other reason wrist device not worn at placement check 1",
  "accelplace_1_wrist_narsn",      "Missing reason wrist device was not assessed at placement check 1",
  "accelplace_1_wrist_naother",    "No explanation for other reason wrist device was not assessed at placement check 1",
  "accelplace_1_wrist_skin",       "Missing skin assessment on wrist for placement check 1",
  "accelplace_1_wrist_skin_na",    "Missing reason skin not assessed on wrist at placement check 1",
  "accelplace_1_wrist_skin_other", "No explanation for other reason skin not assessed on wrist at placement check 1",
  ## Ankle
  "accelplace_1_ankle_yn",         "Missing whether patient was wearing ankle device at placement check 1",
  "accelplace_1_ankle_dom",        "Missing whether ankle device was on right ankle at placement check 1",
  "accelplace_1_ankle_rsn",        "Missing reason ankle device was not worn at placement check 1",
  "accelplace_1_ankle_other",      "No explanation for other reason ankle device not worn at placement check 1",
  "accelplace_1_ankle_narsn",      "Missing reason ankle device was not assessed at placement check 1",
  "accelplace_1_ankle_naother",    "No explanation for other reason ankle device was not assessed at placement check 1",
  "accelplace_1_ankle_skin",       "Missing skin assessment on ankle for placement check 1",
  "accelplace_1_ankle_skin_na",    "Missing reason skin not assessed on ankle at placement check 1",
  "accelplace_1_ankle_skin_other", "No explanation for other reason skin not assessed on ankle at placement check 1",
  ## Placement check 2 (days with ICU orders)
  "accelplace_2_yn",               "ICU orders today, but missing whether placement check 2 was completed",
  "accelplace_2_rsn",              "Missing reason placement check 2 not completed",
  "accelplace_2_other",            "No explanation for other reason placement check 2 not completed",
  "accelplace_2_time",             "Missing time of placement check 2",
  ## Wrist
  "accelplace_2_wrist_yn",         "Missing whether patient was wearing wrist device at placement check 2",
  "accelplace_2_wrist_dom",        "Missing whether wrist device was on dominant hand at placement check 2",
  "accelplace_2_wrist_rsn",        "Missing reason wrist device was not worn at placement check 2",
  "accelplace_2_wrist_other",      "No explanation for other reason wrist device not worn at placement check 2",
  "accelplace_2_wrist_narsn",      "Missing reason wrist device was not assessed at placement check 2",
  "accelplace_2_wrist_naother",    "No explanation for other reason wrist device was not assessed at placement check 2",
  "accelplace_2_wrist_skin",       "Missing skin assessment on wrist for placement check 2",
  "accelplace_2_wrist_skin_na",    "Missing reason skin not assessed on wrist at placement check 2",
  "accelplace_2_wrist_skin_other", "No explanation for other reason skin not assessed on wrist at placement check 2",
  ## Ankle
  "accelplace_2_ankle_yn",         "Missing whether patient was wearing ankle device at placement check 2",
  "accelplace_2_ankle_dom",        "Missing whether ankle device was on right ankle at placement check 2",
  "accelplace_2_ankle_rsn",        "Missing reason ankle device was not worn at placement check 2",
  "accelplace_2_ankle_other",      "No explanation for other reason ankle device not worn at placement check 2",
  "accelplace_2_ankle_narsn",      "Missing reason ankle device was not assessed at placement check 2",
  "accelplace_2_ankle_naother",    "No explanation for other reason ankle device was not assessed at placement check 2",
  "accelplace_2_ankle_skin",       "Missing skin assessment on ankle for placement check 2",
  "accelplace_2_ankle_skin_na",    "Missing reason skin not assessed on ankle at placement check 2",
  "accelplace_2_ankle_skin_other", "No explanation for other reason skin not assessed on ankle at placement check 2"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
accelplace_issues <- matrix(
  FALSE, ncol = nrow(accelplace_codes), nrow = nrow(daily_df)
)
colnames(accelplace_issues) <- accelplace_codes$code
rownames(accelplace_issues) <- with(daily_df, {
  paste(id, redcap_event_name, sep = '; ') })

accelplace_issues[, "accelplace_date_na"] <- is.na(daily_df$coord_accel_date)
accelplace_issues[, "accelplace_date_right"] <- with(daily_df, {
  !is.na(coord_accel_date) & !is.na(study_date) & !(study_date == coord_accel_date)
})
accelplace_issues[, "accelplace_ever"] <- is.na(daily_df$coord_ever)
accelplace_issues[, "accelplace_rsn"] <- with(daily_df, {
  !is.na(coord_ever) & coord_ever == "No" & is.na(coord_ever_rsn_no)
})
accelplace_issues[, "accelplace_rsn_other"] <- with(daily_df, {
  !is.na(coord_ever_rsn_no) &
    coord_ever_rsn_no == "Other/Unknown noncompliance (explain)" &
    (is.na(coord_ever_other) | coord_ever_other == "")
})
accelplace_issues[, "accelplace_icu"] <- is.na(daily_df$coord_icu_yn)

## Placement check 1 (all days)
accelplace_issues[, "accelplace_1_yn"] <- is.na(daily_df$coord_assess_1)
accelplace_issues[, "accelplace_1_rsn"] <- with(daily_df, {
  !is.na(coord_assess_1) & coord_assess_1 == "No" & is.na(coord_assess_no_1)
})
accelplace_issues[, "accelplace_1_other"] <- with(daily_df, {
  !is.na(coord_assess_no_1) & coord_assess_no_1 == "Other" &
    (is.na(coord_assess_other_1) | coord_assess_other_1 == "")
})
accelplace_issues[, "accelplace_1_rsn"] <- with(daily_df, {
  !is.na(coord_assess_1) & coord_assess_1 == "Yes" & is.na(coord_dttm_1)
})

## Wrist
accelplace_issues[, "accelplace_1_wrist_yn"] <- with(daily_df, {
  !is.na(coord_assess_1) & coord_assess_1 == "Yes" & is.na(coord_wrist_1)
})
 ## Device worn
accelplace_issues[, "accelplace_1_wrist_dom"] <- with(daily_df, {
  !is.na(coord_wrist_1) & coord_wrist_1 == "Yes" & is.na(coord_wrist_dom_1)
})
accelplace_issues[, "accelplace_1_wrist_skin"] <- with(daily_df, {
  !is.na(coord_wrist_1) & coord_wrist_1 == "Yes" & is.na(coord_wrist_skin_1)
})
accelplace_issues[, "accelplace_1_wrist_skin_na"] <- with(daily_df, {
  !is.na(coord_wrist_skin_1) & coord_wrist_skin_1 == "Not assessed" &
    is.na(coord_wrist_skin_na_1)
})
accelplace_issues[, "accelplace_1_wrist_skin_other"] <- with(daily_df, {
  !is.na(coord_wrist_skin_na_1) & coord_wrist_skin_na_1 == "Other" &
    (is.na(coord_wrist_skin_other_1) | coord_wrist_skin_other_1 == "")
})
 ## Device not worn
accelplace_issues[, "accelplace_1_wrist_rsn"] <- with(daily_df, {
  !is.na(coord_wrist_1) & coord_wrist_1 == "No" & is.na(coord_wrist_no_1)
})
accelplace_issues[, "accelplace_1_wrist_other"] <- with(daily_df, {
  !is.na(coord_wrist_no_1) & coord_wrist_no_1 == "Other" &
    (is.na(coord_wrist_no_other_1) | coord_wrist_no_other_1 == "")
})
 ## Wrist not assessed
accelplace_issues[, "accelplace_1_wrist_narsn"] <- with(daily_df, {
  !is.na(coord_wrist_1) & coord_wrist_1 == "Not assessed" & is.na(coord_wrist_na_1)
})
accelplace_issues[, "accelplace_1_wrist_naother"] <- with(daily_df, {
  !is.na(coord_wrist_na_1) & coord_wrist_na_1 == "Other" &
    (is.na(coord_wrist_na_other_1) | coord_wrist_na_other_1 == "")
})

## Ankle
accelplace_issues[, "accelplace_1_ankle_yn"] <- with(daily_df, {
  !is.na(coord_assess_1) & coord_assess_1 == "Yes" & is.na(coord_ankle_1)
})
 ## Device worn
accelplace_issues[, "accelplace_1_ankle_dom"] <- with(daily_df, {
  !is.na(coord_ankle_1) & coord_ankle_1 == "Yes" & is.na(coord_ankle_dom_1)
})
accelplace_issues[, "accelplace_1_ankle_skin"] <- with(daily_df, {
  !is.na(coord_ankle_1) & coord_ankle_1 == "Yes" & is.na(coord_ankle_skin_1)
})
accelplace_issues[, "accelplace_1_ankle_skin_na"] <- with(daily_df, {
  !is.na(coord_ankle_skin_1) & coord_ankle_skin_1 == "Not assessed" &
    is.na(coord_ankle_skin_na_1)
})
accelplace_issues[, "accelplace_1_ankle_skin_other"] <- with(daily_df, {
  !is.na(coord_ankle_skin_na_1) & coord_ankle_skin_na_1 == "Other" &
    (is.na(coord_ankle_skin_other_1) | coord_ankle_skin_other_1 == "")
})
 ## Device not worn
accelplace_issues[, "accelplace_1_ankle_rsn"] <- with(daily_df, {
  !is.na(coord_ankle_1) & coord_ankle_1 == "No" & is.na(coord_ankle_no_1)
})
accelplace_issues[, "accelplace_1_ankle_other"] <- with(daily_df, {
  !is.na(coord_ankle_no_1) & coord_ankle_no_1 == "Other" &
    (is.na(coord_ankle_no_other_1) | coord_ankle_no_other_1 == "")
})
 ## Ankle not assessed
accelplace_issues[, "accelplace_1_ankle_narsn"] <- with(daily_df, {
  !is.na(coord_ankle_1) & coord_ankle_1 == "Not assessed" & is.na(coord_ankle_na_1)
})
accelplace_issues[, "accelplace_1_ankle_naother"] <- with(daily_df, {
  !is.na(coord_ankle_na_1) & coord_ankle_na_1 == "Other" &
    (is.na(coord_ankle_na_other_1) | coord_ankle_na_other_1 == "")
})

## Placement check 2
##  (days with ICU orders; questions only visible if accelerometer ever worn)
accelplace_issues[, "accelplace_2_yn"] <- with(daily_df, {
  !is.na(coord_ever) & coord_ever == "Yes" &
    !is.na(coord_icu_yn) & coord_icu_yn == "Yes" &
    is.na(coord_assess_2)
})
accelplace_issues[, "accelplace_2_rsn"] <- with(daily_df, {
  !is.na(coord_assess_2) & coord_assess_2 == "No" & is.na(coord_assess_no_2)
})
accelplace_issues[, "accelplace_2_other"] <- with(daily_df, {
  !is.na(coord_assess_no_2) & coord_assess_no_2 == "Other" &
    (is.na(coord_assess_other_2) | coord_assess_other_2 == "")
})
accelplace_issues[, "accelplace_2_rsn"] <- with(daily_df, {
  !is.na(coord_assess_2) & coord_assess_2 == "Yes" & is.na(coord_dttm_2)
})

## Wrist
accelplace_issues[, "accelplace_2_wrist_yn"] <- with(daily_df, {
  !is.na(coord_assess_2) & coord_assess_2 == "Yes" & is.na(coord_wrist_2)
})
## Device worn
accelplace_issues[, "accelplace_2_wrist_dom"] <- with(daily_df, {
  !is.na(coord_wrist_2) & coord_wrist_2 == "Yes" & is.na(coord_wrist_dom_2)
})
accelplace_issues[, "accelplace_2_wrist_skin"] <- with(daily_df, {
  !is.na(coord_wrist_2) & coord_wrist_2 == "Yes" & is.na(coord_wrist_skin_2)
})
accelplace_issues[, "accelplace_2_wrist_skin_na"] <- with(daily_df, {
  !is.na(coord_wrist_skin_2) & coord_wrist_skin_2 == "Not assessed" &
    is.na(coord_wrist_skin_na_2)
})
accelplace_issues[, "accelplace_2_wrist_skin_other"] <- with(daily_df, {
  !is.na(coord_wrist_skin_na_2) & coord_wrist_skin_na_2 == "Other" &
    (is.na(coord_wrist_skin_other_2) | coord_wrist_skin_other_2 == "")
})
## Device not worn
accelplace_issues[, "accelplace_2_wrist_rsn"] <- with(daily_df, {
  !is.na(coord_wrist_2) & coord_wrist_2 == "No" & is.na(coord_wrist_no_2)
})
accelplace_issues[, "accelplace_2_wrist_other"] <- with(daily_df, {
  !is.na(coord_wrist_no_2) & coord_wrist_no_2 == "Other" &
    (is.na(coord_wrist_no_other_2) | coord_wrist_no_other_2 == "")
})
## Wrist not assessed
accelplace_issues[, "accelplace_2_wrist_narsn"] <- with(daily_df, {
  !is.na(coord_wrist_2) & coord_wrist_2 == "Not assessed" & is.na(coord_wrist_na_2)
})
accelplace_issues[, "accelplace_2_wrist_naother"] <- with(daily_df, {
  !is.na(coord_wrist_na_2) & coord_wrist_na_2 == "Other" &
    (is.na(coord_wrist_na_other_2) | coord_wrist_na_other_2 == "")
})

## Ankle
accelplace_issues[, "accelplace_2_ankle_yn"] <- with(daily_df, {
  !is.na(coord_assess_2) & coord_assess_2 == "Yes" & is.na(coord_ankle_2)
})
## Device worn
accelplace_issues[, "accelplace_2_ankle_dom"] <- with(daily_df, {
  !is.na(coord_ankle_2) & coord_ankle_2 == "Yes" & is.na(coord_ankle_dom_2)
})
accelplace_issues[, "accelplace_2_ankle_skin"] <- with(daily_df, {
  !is.na(coord_ankle_2) & coord_ankle_2 == "Yes" & is.na(coord_ankle_skin_2)
})
accelplace_issues[, "accelplace_2_ankle_skin_na"] <- with(daily_df, {
  !is.na(coord_ankle_skin_2) & coord_ankle_skin_2 == "Not assessed" &
    is.na(coord_ankle_skin_na_2)
})
accelplace_issues[, "accelplace_2_ankle_skin_other"] <- with(daily_df, {
  !is.na(coord_ankle_skin_na_2) & coord_ankle_skin_na_2 == "Other" &
    (is.na(coord_ankle_skin_other_2) | coord_ankle_skin_other_2 == "")
})
## Device not worn
accelplace_issues[, "accelplace_2_ankle_rsn"] <- with(daily_df, {
  !is.na(coord_ankle_2) & coord_ankle_2 == "No" & is.na(coord_ankle_no_2)
})
accelplace_issues[, "accelplace_2_ankle_other"] <- with(daily_df, {
  !is.na(coord_ankle_no_2) & coord_ankle_no_2 == "Other" &
    (is.na(coord_ankle_no_other_2) | coord_ankle_no_other_2 == "")
})
## ankle not assessed
accelplace_issues[, "accelplace_2_ankle_narsn"] <- with(daily_df, {
  !is.na(coord_ankle_2) & coord_ankle_2 == "Not assessed" & is.na(coord_ankle_na_2)
})
accelplace_issues[, "accelplace_2_ankle_naother"] <- with(daily_df, {
  !is.na(coord_ankle_na_2) & coord_ankle_na_2 == "Other" &
    (is.na(coord_ankle_na_other_2) | coord_ankle_na_other_2 == "")
})

## -- Create a final data.frame of errors + messages ---------------------------
accelplace_errors <- create_error_df(
  error_matrix = accelplace_issues, error_codes = accelplace_codes
)

accelplace_final <- accelplace_errors %>%
  mutate(form = "Accelerometer Placement")

################################################################################
## DNA Log (filled out once, included with Enrollment/Trial Day 1)
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
dna_codes <- tribble(
  ~ code,           ~ msg,
  "dna_drawn",      "Missing whether a DNA specimen was drawn",
  "dna_rsn",        "Missing reason DNA specimen not drawn",
  "dna_rsn_other",  "No explanation for other reason DNA specimen was not drawn",
  "dna_date_miss",  "Missing date DNA specimen was drawn",
  "dna_date_range", "Date DNA was drawn is either prior to enrollment or after last in-hospital date",
  "dna_trans",      "Missing whether patient had a blood transfusion within 6m of DNA draw",
  "dna_mail",       "Missing whether DNA specimen was mailed to DNA Core Lab"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
dna_issues <- matrix(
  FALSE, ncol = nrow(dna_codes), nrow = nrow(day1_df)
)
colnames(dna_issues) <- dna_codes$code
rownames(dna_issues) <- with(day1_df, {
  paste(id, redcap_event_name, sep = '; ') })

dna_issues[, "dna_drawn"] <- is.na(day1_df$dna_specimen)
dna_issues[, "dna_rsn"] <- with(day1_df, {
  !is.na(dna_specimen) & dna_specimen == "No" & is.na(dna_no_rsn)
})
dna_issues[, "dna_rsn_other"] <- with(day1_df, {
  !is.na(dna_no_rsn) & dna_no_rsn == "Other" & (is.na(dna_other) | dna_other == "")
})
dna_issues[, "dna_date_miss"] <- with(day1_df, {
  !is.na(dna_specimen) & dna_specimen == "Yes" & is.na(dna_date)
})
dna_issues[, "dna_date_range"] <- with(day1_df, {
  ifelse(is.na(dna_date), FALSE,
         (!is.na(enroll_dttm) & dna_date < enroll_dttm) |
           (!is.na(last_inhosp) & dna_date > last_inhosp))
})
dna_issues[, "dna_trans"] <- with(day1_df, {
  !is.na(dna_specimen) & dna_specimen == "Yes" & is.na(transfusion)
})
dna_issues[, "dna_mail"] <- with(day1_df, {
  !is.na(dna_specimen) & dna_specimen == "Yes" & is.na(dna_mailed)
})

## -- Create a final data.frame of errors + messages ---------------------------
dna_errors <- create_error_df(
  error_matrix = dna_issues, error_codes = dna_codes
)

dna_final <- dna_errors %>%
  mutate(form = "DNA Log")

################################################################################
## Family Capacitation Survey (added with protocol 1.02)
################################################################################

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
famcap_codes <- tribble(
  ~ code,        ~ msg,
  "famcap_done", "Protocol >1.01, but missing whether Family Capacitation Survey done",
  "famcap_rsn",  "Missing reason Family Capacitation Survey not done",
  "famcap_1",    "Missing Family Capacitation Survey question 1",
  "famcap_2",    "Missing Family Capacitation Survey question 2",
  "famcap_3",    "Missing Family Capacitation Survey question 3",
  "famcap_4",    "Missing Family Capacitation Survey question 4",
  "famcap_5",    "Missing Family Capacitation Survey question 5",
  "famcap_6",    "Missing Family Capacitation Survey question 6",
  "famcap_7",    "Missing Family Capacitation Survey question 7",
  "famcap_8",    "Missing Family Capacitation Survey question 8",
  "famcap_9",    "Missing Family Capacitation Survey question 9",
  "famcap_10",   "Missing Family Capacitation Survey question 10",
  "famcap_11",   "Missing Family Capacitation Survey question 11",
  "famcap_12",   "Missing Family Capacitation Survey question 12",
  "famcap_13",   "Missing Family Capacitation Survey question 13",
  "famcap_14",   "Missing Family Capacitation Survey question 14",
  "famcap_15",   "Missing Family Capacitation Survey question 15",
  "famcap_16",   "Missing Family Capacitation Survey question 16",
  "famcap_17",   "Missing Family Capacitation Survey question 17"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in day1_df; columns = # potential issues
famcap_issues <- matrix(
  FALSE, ncol = nrow(famcap_codes), nrow = nrow(famcap_df)
)
colnames(famcap_issues) <- famcap_codes$code
rownames(famcap_issues) <- with(famcap_df, {
  paste(id, redcap_event_name, sep = '; ') })

## FCS not done
famcap_issues[, "famcap_done"] <- with(famcap_df, { is.na(famcap_comp) })
famcap_issues[, "famcap_rsn"] <- with(famcap_df, {
  !is.na(famcap_comp) & !famcap_comp & is.na(famcap_comp_rsn)
})

## FCS done
famcap_issues[, "famcap_1"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_1)
})
famcap_issues[, "famcap_2"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_2)
})
famcap_issues[, "famcap_3"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_3)
})
famcap_issues[, "famcap_4"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_4)
})
famcap_issues[, "famcap_5"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_5)
})
famcap_issues[, "famcap_6"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_6)
})
famcap_issues[, "famcap_7"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_7)
})
famcap_issues[, "famcap_8"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_8)
})
famcap_issues[, "famcap_9"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_9)
})
famcap_issues[, "famcap_10"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_10)
})
famcap_issues[, "famcap_11"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_11)
})
famcap_issues[, "famcap_12"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_12)
})
famcap_issues[, "famcap_13"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_13)
})
famcap_issues[, "famcap_14"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_14)
})
famcap_issues[, "famcap_15"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_15)
})
famcap_issues[, "famcap_16"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_16)
})
famcap_issues[, "famcap_17"] <- with(famcap_df, {
  !is.na(famcap_comp) & famcap_comp & is.na(famcap_17)
})

## -- Create a final data.frame of errors + messages ---------------------------
famcap_errors <- create_error_df(
  error_matrix = famcap_issues, error_codes = famcap_codes
)

famcap_final <- famcap_errors %>%
  mutate(form = "Family Capacitation Survey")

################################################################################
## CADUCEUS (completed on days 1, 3, 5, even after death/discharge/WD)
################################################################################

## -- Create error codes + corresponding messages for all issues *except* ------
## -- fields that are simply missing or should fall within specified limits ----

## Codes: Short, like variable names
## Messages: As clear as possible to the human reader

## tribble = row-wise data.frame; easier to match code + message
cad_codes <- tribble(
  ~ code,          ~ msg,
  "cad_date",      "Missing date of CADUCEUS blood draw",
  "cad_draw",      "Missing whether CADUCEUS values were tested",
  "cad_rsn",       "Missing reason CADUCEUS values were not tested",
  "cad_other",     "No explanation for other reason CADUCEUS values not tested",
  "cad_ache_ul",   "Missing AChE U/L",
  "cad_ache_ughb", "Missing AChE U/gHb",
  "cad_bche",      "Missing BChE (U/L)"
) %>%
  as.data.frame() ## But create_error_df() doesn't handle tribbles

## Create empty matrix to hold all potential issues
## Rows = # rows in daily_df; columns = # potential issues
cad_issues <- matrix(
  FALSE, ncol = nrow(cad_codes), nrow = nrow(cad_df)
)
colnames(cad_issues) <- cad_codes$code
rownames(cad_issues) <- with(cad_df, {
  paste(id, redcap_event_name, sep = '; ') })

cad_issues[, "cad_date"] <- is.na(cad_df$cad_date)
cad_issues[, "cad_draw"] <- is.na(cad_df$cad_yn)
cad_issues[, "cad_rsn"] <- with(cad_df, {
  !is.na(cad_yn) & cad_yn == "No" & is.na(cad_no)
})
cad_issues[, "cad_other"] <- with(cad_df, {
  !is.na(cad_no) & cad_no == "Other" & (is.na(cad_no_other) | cad_no_other == "")
})
cad_issues[, "cad_ache_ul"] <- with(cad_df, {
  !is.na(cad_yn) & cad_yn == "Yes" & is.na(cad_ache)
})
cad_issues[, "cad_ache_ughb"] <- with(cad_df, {
  !is.na(cad_yn) & cad_yn == "Yes" & is.na(cad_ache_2)
})
cad_issues[, "cad_bche"] <- with(cad_df, {
  !is.na(cad_yn) & cad_yn == "Yes" & is.na(cad_bche)
})

## -- Create a final data.frame of errors + messages ---------------------------
cad_errors <- create_error_df(
  error_matrix = cad_issues, error_codes = cad_codes
)

cad_final <- cad_errors %>%
  mutate(form = "CADUCEUS")






################################################################################
## Combine all queries and export to output/ for uploading
################################################################################

## Combine all queries into a single data.frame
error_dfs <- list(
  enrqual_final,
  contact_final,
  prehosp_final,
  dt_final,
  enrollmds_final,
  nutr_final,
  dailymds_final,
  pad_final,
  mobility_final,
  accelbed_final,
  accelplace_final,
  dna_final,
  cad_final,
  famcap_final
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
