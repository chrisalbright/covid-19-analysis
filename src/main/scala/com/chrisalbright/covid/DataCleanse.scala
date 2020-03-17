package com.chrisalbright.covid

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

object DataCleanse extends Serializable {
  @transient lazy val oldYYfmt = DateTimeFormatter.ofPattern("M/d/yy H:m")
  @transient lazy val oldYYYYfmt = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
  @transient lazy val newfmt = DateTimeFormatter.ISO_DATE_TIME
  private val NA = "None Available"
  private val countryMapping = Map(
    "Iran (Islamic Republic of)" -> "Iran",
    "occupied Palestinian territory" -> "Palestine",
    "Viet Nam" -> "Vietnam",
    "Korea, South" -> "South Korea",
    "Republic of Korea" -> "South Korea",
    "Taiwan*" -> "Taiwan",
    "Taipei and environs" -> "Taiwan",
    "Russian Federation" -> "Russia",
    "Republic of Moldova" -> "Moldova",
    "Republic of Ireland" -> "Ireland",
    "Mainland China" -> "China",
    "UK" -> "United Kingdom",
    "US" -> "United States",
    "Macao SAR" -> "Macau",
    "Hong Kong SAR" -> "Hong Kong",
    "Czechia" -> "Czech Republic"
  )
  private val stateMapping: Map[String, String] = Map(
    "AL" -> "Alabama",
    "AK" -> "Alaska",
    "AZ" -> "Arizona",
    "AR" -> "Arkansas",
    "CA" -> "California",
    "CO" -> "Colorado",
    "CT" -> "Connecticut",
    "DE" -> "Delaware",
    "FL" -> "Florida",
    "GA" -> "Georgia",
    "HI" -> "Hawaii",
    "ID" -> "Idaho",
    "IL" -> "Illinois",
    "IN" -> "Indiana",
    "IA" -> "Iowa",
    "KS" -> "Kansas",
    "KY" -> "Kentucky",
    "LA" -> "Louisiana",
    "ME" -> "Maine",
    "MD" -> "Maryland",
    "MA" -> "Massachusetts",
    "MI" -> "Michigan",
    "MN" -> "Minnesota",
    "MS" -> "Mississippi",
    "MO" -> "Missouri",
    "MT" -> "Montana",
    "NE" -> "Nebraska",
    "NV" -> "Nevada",
    "NH" -> "New Hampshire",
    "NJ" -> "New Jersey",
    "NM" -> "New Mexico",
    "NY" -> "New York",
    "NC" -> "North Carolina",
    "ND" -> "North Dakota",
    "OH" -> "Ohio",
    "OK" -> "Oklahoma",
    "OR" -> "Oregon",
    "PA" -> "Pennsylvania",
    "RI" -> "Rhode Island",
    "SC" -> "South Carolina",
    "SD" -> "South Dakota",
    "TN" -> "Tennessee",
    "TX" -> "Texas",
    "UT" -> "Utah",
    "VT" -> "Vermont",
    "VA" -> "Virginia",
    "WA" -> "Washington",
    "WV" -> "West Virginia",
    "WI" -> "Wisconsin",
    "WY" -> "Wyoming",
    "Unassigned Location (From Diamond Princess)" -> "Diamond Princess",
    "From Diamond Princess" -> "Diamond Princess"
  )
  def lookupCountry(name: String) = countryMapping.getOrElse(name, name)
  def lookupState(name: String): String = stateMapping.getOrElse(name, name)
  def convertStateAndCountry(state: Option[String], country: Option[String]): Country = (state, country) match {
    case (None, None) => Country(NA, NA, NA)
    case (Some(state: String), Some(country: String)) if state.contains(",") =>
      val split: Array[String] = state.split(", ")
      Country(lookupCountry(country), lookupState(split(1)), split(0).replace("County", "").trim)
    case (Some(state: String), Some(country: String)) => Country(lookupCountry(country), lookupState(state), NA)
    case (None, Some("UK")) => Country(lookupCountry("UK"), "UK", NA)
    case (None, Some(country: String)) => Country(lookupCountry(country), lookupCountry(country), NA)
  }
  def convertUK(country: String): (String, String) = country match {
    case "UK" => ("United Kingdom", "UK")
    case other => (other, "")
  }
  def convertTimestampFmt(timestampString: String, format: DateTimeFormatter): Timestamp = {
    val dt: LocalDateTime = LocalDateTime.parse(timestampString, format)
    Timestamp.from(dt.toInstant(ZoneOffset.UTC))
  }
  def convertTimestamp(timestampString: String): Timestamp = timestampString match {
    case str: String if str.matches("""\d?\d/\d?\d/\d\d \d?\d:\d?\d""") => convertTimestampFmt(str, oldYYfmt)
    case str: String if str.matches("""\d?\d/\d?\d/\d\d\d\d \d?\d:\d?\d""") => convertTimestampFmt(str, oldYYYYfmt)
    case str: String => convertTimestampFmt(str, newfmt)
  }

}
