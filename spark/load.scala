import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.language.postfixOps

case class Country(country: String, state: String, county: String)
case class CovidDaily(recordDate: Date, state: String, country: String, confirmed: BigInt, deaths: BigInt, recovered: BigInt)
case class CovidEs(id: Int, recordDate: java.util.Date, state: String, country: String, totalConfirmed: Int, newConfirmed: Int, totalDeaths: Int, newDeaths: Int, totalRecovered: Int, newRecovered: Int)
object DataCleanse extends Serializable {
  val countryMapping = Map(
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
  val stateMapping: Map[String, String] = Map(
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
      "Unassigned Location (From Diamond Princess)" -> "Diamond Princess"),
      "From Diamond Princess" -> "Diamond Princess")

}

object COVID extends Serializable {

  import DataCleanse._

  val stateName = "State"
  val countryName = "Country"
  val customSchema = StructType(Array(
    StructField("Province/State", StringType, true),
    StructField("Country/Region", StringType, true),
    StructField("Last Update", StringType, true),
    StructField("Confirmed", IntegerType, true),
    StructField("Deaths", IntegerType, true),
    StructField("Recovered", IntegerType, true),
    StructField("Latitude", DoubleType, true),
    StructField("Longitude", DoubleType, true)
  ))

  @transient lazy val oldYYfmt = DateTimeFormatter.ofPattern("M/d/yy H:m")
  @transient lazy val oldYYYYfmt = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
  @transient lazy val newfmt = DateTimeFormatter.ISO_DATE_TIME

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val covidSrc: DataFrame =
    spark.read.schema(customSchema)
      .option("header", true)
      .csv("/data/csse_covid_19_data/csse_covid_19_daily_reports/*.csv")
      .withColumn("recordDate", to_date(regexp_extract(input_file_name(), """.+\/(\d\d-\d\d-\d\d\d\d).csv""", 1), "MM-dd-yyyy"))

  private val NA = "None Available"
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

  def convertRow(row: Row): CovidDaily = {
    val Row(state, country, lastUpdate, confirmed, deaths, recovered, latitude, longitude, recordDate: Date) = row
    val cs: Country = convertStateAndCountry(Option(state.asInstanceOf[String]), Option(country.asInstanceOf[String]))
    val lastUpdateTimestamp: Timestamp = convertTimestamp(lastUpdate.asInstanceOf[String])
    def convertInt(x: Any): Int = Option(x).map(_.toString.toInt).getOrElse(0)
    def convertDouble(x: Any): Double = Option(x).map(_.toString.toDouble).getOrElse(-1.0)
    CovidDaily(recordDate, cs.state, cs.country, convertInt(confirmed), convertInt(deaths), convertInt(recovered)) //, convertDouble(longitude), convertDouble(latitude))
  }

  private val covid0 =
    covidSrc.map(convertRow)
      .groupBy($"state", $"country", $"recordDate")
      .agg(
        sum("confirmed") as "confirmed",
        sum("deaths") as "deaths",
        sum("recovered") as "recovered")
      .cache()

  val states = covid0.select($"state", $"country").distinct()
  val dates = covid0.select($"recordDate").distinct()
  val allDates = dates.crossJoin(states)

  // TODO: need to default measure columns to last non-zero value
  val covid = allDates
    .join(
      covid0,
      Seq("state", "country", "recordDate"),
      "left_outer")
    .na.fill(Map("confirmed" -> 0, "deaths" -> 0, "recovered" -> 0))
    .sort($"country", $"state", $"recordDate" desc)
    .withColumn("confirmed", $"confirmed" cast IntegerType)
    .withColumn("deaths", $"deaths" cast IntegerType)
    .withColumn("recovered", $"recovered" cast IntegerType)
    .withColumn("id", hash($"state", $"country", $"recordDate"))
    .cache()

  //  val covidPivot: DataFrame = covid.withColumn("lastUpdate", $"lastUpdate" cast "date")
  //    .groupBy($"country") // $"county", $"state",
  //    .pivot($"lastUpdate")
  //    .max("confirmed")

  @transient
  lazy val covidDeltaWindow = Window
    .partitionBy($"state", $"country")
    .orderBy($"recordDate")
  val covidDeltas = covid
    .withColumn("prev_confirmed", lag($"confirmed", 1, 0).over(covidDeltaWindow))
    .withColumn("prev_deaths", lag($"deaths", 1, 0).over(covidDeltaWindow))
    .withColumn("prev_recovered", lag($"recovered", 1, 0).over(covidDeltaWindow))
    .withColumn("newConfirmed", $"confirmed" - $"prev_confirmed")
    .withColumn("newDeaths", $"deaths" - $"prev_deaths")
    .withColumn("newRecovered", $"recovered" - $"prev_recovered")
    .drop("prev_confirmed", "prev_deaths", "prev_recovered")

  private def saveToEs(df: DataFrame): Unit = {
    import org.elasticsearch.spark.rdd.EsSpark

    val x = df.rdd.map {
      case Row(state: String, country: String, recordDate: Date, confirmed: Int, deaths: Int, recovered: Int, id: Int, newConfirmed: Int, newDeaths: Int, newRecovered: Int) =>
        CovidEs(
          id = id,
          recordDate = recordDate,
          state = state,
          country = country,
          totalConfirmed = confirmed,
          newConfirmed = newConfirmed,
          totalRecovered = recovered,
          newRecovered = newRecovered,
          totalDeaths = deaths,
          newDeaths = newDeaths
        )
    }

    EsSpark
      .saveToEs(x, "covid/deltas",
        Map(
          "es.nodes" -> "elastic-node",
          "es.mapping.rich.date" -> "true",
          "es.mapping.id" -> "id"
        ))
  }

  def publish(): Unit = saveToEs(covidDeltas)
  
}