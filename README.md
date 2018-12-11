# MOSAIC Data Clean

This set of scripts allows ongoing monitoring and cleaning of prospective data
collection for the MOSAIC prospective cohort study ([clinicaltrials.gov listing](https://clinicaltrials.gov/ct2/show/NCT03115840)).

> This is a work in progress. A full example of the process can be found in Jennifer Thompson's [data cleaning & monitoring tutorial](https://github.com/jenniferthompson/DataCleanExample).

### Configuration

All REDCap API tokens for an individual user are assumed to be stored in the user's `.Renviron` file within this working directory, in the following format:

`MOSAIC_IH=ABCDEFGHIJKLMNOPQRSTUVWXYZ`

R will use these tokens in combination with the `httr` package to
programmatically access the appropriate REDCap databases.

### File Structure

- Main directory
    - `ih_dataclean.R`: Primary script; sources additional scripts from `/R`
    - `.Renviron`: Contains API tokens used to access REDCap projects
- `/R`
    - `old_queries.R`: Exports previously noted queries in order to remove
    those which cannot or need not be corrected. **NOT CURRENTLY AVAILABLE**
    - `dataclean_helpers.R`: Helper functions used in primary data cleaning
    scripts
- `/output`: Destination for CSV files containing queries from each run of
`ih_data_clean.R`. One file per site per date.
