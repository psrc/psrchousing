#' OFM Population and Housing Unit Data near High-Capacity Transit
#'
#' This function pulls and cleans OFM parcelized population and housing unit data near HCT
#' 
#' @param dec Number of decimals to round data - defaults to 0
#' @param yrs Vector of 4 digit years to pull data for - default to 2010 through 2023
#' @param hct_title Name of HCT buffer in Elmer to use - deautls to VISION 2050 HCT buffers
#' @return tibble of population and housing units near HCT
#' 
#' @importFrom rlang .data
#' 
#' @examples
#' pop_housing_near_hct <- ofm_pop_housing_near_hct(yrs = c(2023))
#' 
#' @export
#'
ofm_pop_housing_near_hct <- function(yrs = c(seq(2010, 2023, by=1)), hct_title = "hct_station_area_vision_2050", dec = 0) {
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # Geography order
  geo_ord <- c("King", "Kitsap", "Pierce", "Snohomish", "Region")
  
  housing_data <- NULL
  for (y in yrs) {
    
    # Determine which OFM vintage to use based on the estimate year
    if (y < 2020) {
      ofm_vintage <- 2020
    } else if (y < 2023) {
      ofm_vintage <- 2022
    } else {ofm_vintage <- y}
    
    # Build query for psrcelmer and get parcel data from Elmer
    print(stringr::str_glue("Loading {y} OFM based parcelized estimates of total population and housing units"))
    q <- paste0("SELECT parcel_dim_id, estimate_year, total_pop, housing_units, occupied_housing_units from ofm.parcelized_saep_facts WHERE ofm_vintage = ", ofm_vintage, " AND estimate_year = ", y, "")
    p <- psrcelmer::get_query(sql = q)
    
    # Parcel Dimensions
    print(stringr::str_glue("Loading {y} parcel dimensions from Elmer"))
    if (y >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
    
    # Build query for psrcelmer and get parcel dimensions data from Elmer
    q <- paste0("SELECT parcel_dim_id, parcel_id, county_name, ", hct_title, " from small_areas.parcel_dim WHERE base_year = ", parcel_yr, " ")
    d <- psrcelmer::get_query(sql = q) |> dplyr::rename(hct = tidyselect::all_of(hct_title), county="county_name")
    
    # Add HCT Areas to Parcels
    print(stringr::str_glue("Joining parcel dimensions from Elmer with parcel data"))
    p <- dplyr::left_join(p, d, by="parcel_dim_id")
    
    # Region summaries
    print(stringr::str_glue("Summarize {y} population and housing units for HCT areas by region"))
    region <- p |>
      dplyr::select("hct", population = "total_pop", "housing_units", households = "occupied_housing_units") |>
      dplyr::group_by(.data$hct) |>
      dplyr::summarise(population = round(sum(.data$population), dec), housing_units = round(sum(.data$housing_units), dec), households = round(sum(.data$households), dec)) |>
      dplyr::as_tibble() |>
      tidyr::pivot_wider(names_from = "hct", values_from = c("population", "housing_units", "households"))|>
      dplyr::mutate(geography = "Region", year = y)
    
    print(stringr::str_glue("Summarize {y} total population and housing units for region"))
    region_all <- p |>
      dplyr::select(population = "total_pop", "housing_units", households = "occupied_housing_units") |>
      dplyr::mutate(hct = "total") |>
      dplyr::group_by(.data$hct) |>
      dplyr::summarise(population = round(sum(.data$population), dec), housing_units = round(sum(.data$housing_units), dec), households = round(sum(.data$households), dec)) |>
      dplyr::as_tibble() |>
      tidyr::pivot_wider(names_from = "hct", values_from = c("population", "housing_units", "households"))|>
      dplyr::mutate(geography = "Region", year = y)
    
    print(stringr::str_glue("Combine {y} population and housing units for region with HTC areas and clean up columns"))
    region <- dplyr::left_join(region, region_all, by=c("geography", "year")) |>
      dplyr::select("year", "geography", 
             pop = "population_total", pop_in_hct = "population_in station area", pop_out_hct = "population_not in station area",
             hu = "housing_units_total", hu_in_hct = "housing_units_in station area", hu_out_hct = "housing_units_not in station area",
             hh = "households_total", hh_in_hct = "households_in station area", hh_out_hct = "households_not in station area")
    
    # County summaries
    print(stringr::str_glue("Summarize {y} population and housing units for HCT areas by county"))
    county <- p |>
      dplyr::select(geography = "county", "hct", population = "total_pop", "housing_units", households = "occupied_housing_units") |>
      dplyr::group_by(.data$geography, .data$hct) |>
      dplyr::summarise(population = round(sum(.data$population), dec), housing_units = round(sum(.data$housing_units), dec), households = round(sum(.data$households), dec)) |>
      dplyr::as_tibble() |>
      tidyr::pivot_wider(names_from = "hct", values_from = c("population", "housing_units", "households"))|>
      dplyr::mutate(year = y)
    
    print(stringr::str_glue("Summarize {y} total population and housing units by county"))
    county_all <- p |>
      dplyr::select(geography = "county", population = "total_pop", "housing_units", households = "occupied_housing_units") |>
      dplyr::group_by(.data$geography) |>
      dplyr::summarise(population = round(sum(.data$population), dec), housing_units = round(sum(.data$housing_units), dec), households = round(sum(.data$households), dec)) |>
      dplyr::as_tibble() |>
      dplyr::mutate(year = y)
    
    print(stringr::str_glue("Combine {y} population and housing units by county with HTC areas and clean up columns"))
    county <- dplyr::left_join(county, county_all, by=c("geography", "year")) |>
      dplyr::select("year", "geography", 
                    pop = "population", pop_in_hct = "population_in station area", pop_out_hct = "population_not in station area",
                    hu = "housing_units", hu_in_hct = "housing_units_in station area", hu_out_hct = "housing_units_not in station area",
                    hh = "households", hh_in_hct = "households_in station area", hh_out_hct = "households_not in station area")
    
    print(stringr::str_glue("Combine {y} population and housing units by county and region"))
    tbl <- dplyr::bind_rows(region, county) 
    
    if (is.null(housing_data)) {housing_data <- tbl} else {housing_data <- dplyr::bind_rows(housing_data, tbl)}
    
  }
  
  print(stringr::str_glue("Calculating the annual change"))
  final_tbl <- housing_data |>
    dplyr::group_by(.data$geography) |>
    # Calculate annual change for population
    dplyr::mutate(pop_chg = round(.data$pop - dplyr::lag(.data$pop), dec)) |>
    dplyr::mutate(pop_in_hct_chg = round(.data$pop_in_hct - dplyr::lag(.data$pop_in_hct), dec)) |>
    dplyr::mutate(pop_out_hct_chg = round(.data$pop_out_hct - dplyr::lag(.data$pop_out_hct), dec)) |>
    dplyr::mutate(pop_in_hct_per = .data$pop_in_hct_chg / .data$pop_chg) |>
    dplyr::mutate(pop_out_hct_per = .data$pop_out_hct_chg / .data$pop_chg) |>
    # Calculate annual change for housing units
    dplyr::mutate(hu_chg = round(.data$hu - dplyr::lag(.data$hu), dec)) |>
    dplyr::mutate(hu_in_hct_chg = round(.data$hu_in_hct - dplyr::lag(.data$hu_in_hct), dec)) |>
    dplyr::mutate(hu_out_hct_chg = round(.data$hu_out_hct - dplyr::lag(.data$hu_out_hct), dec)) |>
    dplyr::mutate(hu_in_hct_per = .data$hu_in_hct_chg / .data$hu_chg) |>
    dplyr::mutate(hu_out_hct_per = .data$hu_out_hct_chg / .data$hu_chg) |>
    # Calculate annual change for households
    dplyr::mutate(hh_chg = round(.data$hh - dplyr::lag(.data$hh), dec)) |>
    dplyr::mutate(hh_in_hct_chg = round(.data$hh_in_hct - dplyr::lag(.data$hh_in_hct), dec)) |>
    dplyr::mutate(hh_out_hct_chg = round(.data$hh_out_hct - dplyr::lag(.data$hh_out_hct), dec)) |>
    dplyr::mutate(hh_in_hct_per = .data$hh_in_hct_chg / .data$hh_chg) |>
    dplyr::mutate(hh_out_hct_per = .data$hh_out_hct_chg / .data$hh_chg) |>
    # Clean up column order
    dplyr::select("year", "geography",
                  "pop", "pop_in_hct", "pop_out_hct", "pop_chg", "pop_in_hct_chg", "pop_in_hct_per", "pop_out_hct_chg", "pop_out_hct_per",
                  "hu", "hu_in_hct", "hu_out_hct", "hu_chg", "hu_in_hct_chg", "hu_in_hct_per", "hu_out_hct_chg", "hu_out_hct_per",
                  "hh", "hh_in_hct", "hh_out_hct", "hh_chg", "hh_in_hct_chg", "hh_in_hct_per", "hh_out_hct_chg", "hh_out_hct_chg") |>
    dplyr::mutate(geography = factor(.data$geography, levels = geo_ord)) |>
    dplyr::arrange(.data$geography, .data$year)
  
  return(final_tbl)
  
}
