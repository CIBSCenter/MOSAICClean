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
      (!is.na(consent_self) & consent_self == "Yes")
  ) %>%
  ## TEMPORARY: Select only patients up till last_pt (set above)
  separate(id, into = c("site", "ptnum"), sep = "-", remove = FALSE) %>%
  filter(as.numeric(ptnum) <= last_pt)

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
  "accel_upload_4",        "Missing whether device data from initiation 4 was uploaded"
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
  !is.na(studywd) & studywd == "Yes" &
    rowSums(!is.na(day1_df[, grep("^studywd\\_writing\\_[0-9]+$", names(day1_df))])) == 0
})
dt_issues[, "wd_writing_other"] <- with(day1_df, {
  !is.na(studywd_writing_5) &
    (is.na(studywd_writing_other) | studywd_writing_other == "")
})

## -- Discharge ----------------------------------------------------------------
dt_issues[, "hospdis_epic"] <- with(day1_df, {
  !is.na(hospdis) & hospdis == "Yes" & is.na(hospdis_starpanel_1)
})
dt_issues[, "hospdis_safety"] <- with(day1_df, {
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

## -- Create a final data.frame of errors + messages ---------------------------
dt_errors <- create_error_df(
  error_matrix = dt_issues, error_codes = dt_codes
)

dt_final <- bind_rows(dt_missing, dt_errors) %>%
  mutate(form = "Dates Tracking")

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
## Combine all queries and export to output/ for uploading
################################################################################

## Combine all queries into a single data.frame
error_dfs <- list(
  enrqual_final,
  contact_final,
  prehosp_final,
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
