#' OFM Population Data by County and Region
#'
#' This function pulls and cleans OFM inter-censal population data by county from 1960 to 2020 and
#' combines it with the post-censal population data by county from 2020 onward
#' 
#' @param dec Number of decimals to round data - defaults to 0
#' @return tibble of population data by county and region from 1960 to present
#' 
#' @importFrom rlang .data
#' 
#' @examples
#' pop_data <- ofm_county_population_data(dec = 0)
#' 
#' @export
#'
ofm_county_population_data <- function(dec = 0) {
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # File locations for OFM downloads
  ofm_pre_2020 <- "X:/DSA/population-trends/data/population/ofm_april1_intercensal_estimates_county_1960-2020.xlsx"
  ofm_post_2020 <- "X:/DSA/population-trends/data/population/ofm_april1_population_final.xlsx"
  
  # Geography order
  geo_ord <- c("King", "Kitsap", "Pierce", "Snohomish", "Region")
  
  # 1960 to 2020
  print(stringr::str_glue("Processing OFM inter-censal population estimates from 1960 to 2020"))
  o <- dplyr::as_tibble(openxlsx::read.xlsx(ofm_pre_2020, detectDates = FALSE, skipEmptyRows = TRUE, startRow = 4, colNames = TRUE, sheet = "Population")) |>
    dplyr::filter(.data$County %in% c("King","Kitsap","Pierce","Snohomish")) |>
    tidyr::pivot_longer(cols=tidyselect::contains("Population"), names_to="year", values_to="population") |>
    dplyr::mutate(year = stringr::str_replace(.data$year, "Census.Count.of.Total.Population.", "")) |>
    dplyr::mutate(year = stringr::str_replace(.data$year, "Intercensal.Estimate.of.Total.Population.", "")) |>
    dplyr::mutate(dplyr::across(c('year','population'), as.numeric)) |>
    dplyr::rename(geography="County") |>
    dplyr::select("year", "geography", "population")
  
  print(stringr::str_glue("Summarizing OFM inter-censal population estimates from 1960 to 2020 for PSRC region"))
  r <- o |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(population = sum(.data$population)) |>
    dplyr::as_tibble() |>
    dplyr::mutate(geography = "Region")
  
  print(stringr::str_glue("Combining County OFM inter-censal population estimates from 1990 to 2000"))
  tbl <- dplyr::bind_rows(o, r)
  rm(r, o)
  
  # 2020 to present
  print(stringr::str_glue("Processing OFM post-censal population estimates from 2020 to present"))
  o <- dplyr::as_tibble(openxlsx::read.xlsx(ofm_post_2020, detectDates = FALSE, skipEmptyRows = TRUE, startRow = 5, colNames = TRUE, sheet = "Population")) |>
    dplyr::filter(.data$County %in% c("King","Kitsap","Pierce","Snohomish") & .data$Filter == 1) |>
    tidyr::pivot_longer(cols=tidyselect::contains("Population"), names_to="year", values_to="population") |>
    dplyr::mutate(year = stringr::str_replace(.data$year, ".Population.Census", ""), year = stringr::str_replace(.data$year, ".Population.Estimate\U00B9", ""), year = stringr::str_replace(.data$year, ".Population.Estimate", ""), Jurisdiction = stringr::str_replace(.data$Jurisdiction, " \\(part\\)", "")) |>
    dplyr::mutate(dplyr::across(c('year','population'), as.numeric)) |>
    dplyr::rename(geography="County") |>
    dplyr::filter(.data$year != 2020) |>
    dplyr::select("year", "geography", "population")
  
  print(stringr::str_glue("Summarizing OFM post-censal population estimates from 2020 to present for PSRC region"))
  r <- o |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(population = sum(.data$population)) |>
    dplyr::as_tibble() |>
    dplyr::mutate(geography = "Region")
  
  print(stringr::str_glue("Combining County OFM inter-censal population estimates from 1960 to 2020 to post-censal estimates after 2020"))
  tbl <- dplyr::bind_rows(tbl, o, r)
  rm(r, o)
  
  # Final Cleanup
  print(stringr::str_glue("Adding Change data and cleaning up"))
  tbl <- tbl |>
    dplyr::mutate(geography = factor(.data$geography, levels = geo_ord)) |>
    dplyr::arrange(.data$geography, .data$year) |>
    dplyr::group_by(.data$geography) |>
    dplyr::mutate(population_chg = round(.data$population - dplyr::lag(.data$population), dec)) |>
    dplyr::mutate(population_per = .data$population_chg / .data$population) |>
    dplyr::select("year", "geography", "population", "population_chg", "population_per")
  
  return(tbl)
  
}
