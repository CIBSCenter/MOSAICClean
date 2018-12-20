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
  ## Add indicators for whether each pre-hospital assessment was done
  ##  (could be either fully or partially)
  mutate_at(
    vars(one_of(str_subset(names(day1_df), "\\_comp\\_ph$"))),
    ~ str_detect(., "^Yes")
  ) %>%
  ## Add indicators for current, former smoker (could be one or both)
  mutate(
    current_smoker = str_detect(tolower(gq_smoke), "current"),
    former_smoker = str_detect(tolower(gq_smoke), "former")
  ) %>%
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
## Contact Information
################################################################################

## -- Missingness checks -------------------------------------------------------
contact_missvars <- c(
  "title", "sex", "first_name", "last_name", "name_common", "ssn", "homeless"
)
contact_missing <- check_missing(
  df = day1_df, variables = contact_missvars, ddict = ih_datadict
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
  df = day1_df, variables = prehosp_missvars, ddict = ih_datadict
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
  "iqcode_16",    "Missing IQCODE question 16"
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

## -- Create a final data.frame of errors + messages ---------------------------
prehosp_errors <- create_error_df(
  error_matrix = prehosp_issues, error_codes = prehosp_codes
)

prehosp_final <- bind_rows(prehosp_missing, prehosp_errors) %>%
  mutate(form = "Pre-Hospital Function")

################################################################################
## Combine all queries and export to output/ for uploading
################################################################################

## Combine all queries into a single data.frame
error_dfs <- list(
  enrqual_final,
  contact_final
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
