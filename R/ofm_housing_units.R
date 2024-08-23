#' OFM Housing Unit Data by Type by County and Region
#'
#' This function pulls and cleans OFM post-censal housing unit data by county from 1980 to present
#' 
#' @param dec Number of decimals to round data - defaults to 0
#' @return tibble of housing unit data by county and region from 1980 to present
#' 
#' @importFrom rlang .data
#' 
#' @examples
#' housing_unit_data <- ofm_county_housing_unit_data(dec = 0)
#' 
#' @export
#'
ofm_county_housing_unit_data <- function(dec = 0) {
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # File locations for OFM downloads 
  ofm_hu_file="x:/DSA/population-trends/data/housing/ofm_april1_postcensal_estimates_housing_1980_1990-present.xlsx"
  
  # Geography order
  geo_ord <- c("King", "Kitsap", "Pierce", "Snohomish", "Region")
  
  # 1980 to present
  print(stringr::str_glue("Processing OFM post-censal housing estimates from 1980 to present"))
  ofm_data <- dplyr::as_tibble(openxlsx::read.xlsx(ofm_hu_file, detectDates = FALSE, skipEmptyRows = TRUE, startRow = 4, colNames = TRUE, sheet = "Housing Units")) |>
    dplyr::filter(.data$Jurisdiction %in% c("King County","Kitsap County","Pierce County","Snohomish County")) |>
    tidyr::pivot_longer(cols=dplyr::contains("Housing"), names_to="temp", values_to="estimate") |>
    dplyr::select(-"Line", -"Filter", -"County") |>
    tidyr::separate(col = .data$temp, sep = 4, into = c("year", "variable"), remove = TRUE) |>
    dplyr::mutate(variable = stringr::str_remove_all(.data$variable, ".Census.Count.of.")) |>
    dplyr::mutate(variable = stringr::str_remove_all(.data$variable, ".Postcensal.Estimate.of.")) |>
    dplyr::mutate(variable = stringr::str_remove_all(.data$variable, ".Census-Based.Estimate.of.")) |>
    dplyr::mutate(variable = stringr::str_remove_all(.data$variable, "\U00B9")) |>
    dplyr::mutate(variable = stringr::str_replace_all(.data$variable, "\\.", " ")) |>
    dplyr::mutate(estimate = stringr::str_replace_all(.data$estimate, "\\.", " ")) |>
    dplyr::mutate(year = as.numeric(.data$year), estimate = as.numeric(.data$estimate)) |>
    dplyr::mutate(estimate = tidyr::replace_na(.data$estimate, 0)) |>
    dplyr::mutate(variable = stringr::str_replace_all(.data$variable, "One Unit Housing Units", "sf")) |>
    dplyr::mutate(variable = stringr::str_replace_all(.data$variable, "Two or More Housing Units", "mf")) |>
    dplyr::mutate(variable = stringr::str_replace_all(.data$variable, "Mobile Home and Special Housing Units", "mh")) |>
    dplyr::mutate(variable = stringr::str_replace_all(.data$variable, "Total Housing Units", "total")) |>
    dplyr::rename(geography = "Jurisdiction")
  
  print(stringr::str_glue("Summarizing OFM post-censal housing estimates from 1980 to present for PSRC region"))
  region <- ofm_data |>
    dplyr::group_by(.data$year, .data$variable) |>
    dplyr::summarise(estimate = sum(.data$estimate)) |>
    dplyr::as_tibble() |>
    dplyr::mutate(geography = "Region")
  
  # Final Cleanup
  print(stringr::str_glue("Combining County and Region tibbles and cleaning up"))
  tbl <- dplyr::bind_rows(ofm_data, region) |>
    tidyr::pivot_wider(names_from = "variable", values_from = "estimate") |>
    dplyr::group_by(.data$geography) |>
    dplyr::mutate(total_chg = round(.data$total - dplyr::lag(.data$total), dec)) |>
    dplyr::mutate(sf_chg = round(.data$sf - dplyr::lag(.data$sf), dec)) |>
    dplyr::mutate(sf_per = .data$sf_chg / .data$total_chg) |>
    dplyr::mutate(mf_chg = round(.data$mf - dplyr::lag(.data$mf), dec)) |>
    dplyr::mutate(mf_per = .data$mf_chg / .data$total_chg) |>
    dplyr::mutate(mh_chg = round(.data$mh - dplyr::lag(.data$mh), dec)) |>
    dplyr::mutate(mh_per = .data$mh_chg / .data$total_chg) |>
    dplyr::select("year", "geography",
                  "sf", "sf_chg", "sf_per",
                  "mf", "mf_chg", "mf_per",
                  "mh", "mh_chg", "mh_per",
                  "total", "total_chg") |>
    dplyr::mutate(geography = stringr::str_remove_all(.data$geography, " County")) |>
    dplyr::mutate(geography = factor(.data$geography, levels = geo_ord)) |>
    dplyr::arrange(.data$geography, .data$year)
  
  return(tbl)
  
}
