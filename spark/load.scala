import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import org.elasticsearch.spark.sql._

import scala.language.postfixOps

case class Country(country: String, state: String, county: String)
case class CovidDaily(county: String, state: String, country: String, lastUpdate: Timestamp, confirmed: Int, deaths: Int, recovered: Int)

object COVID extends Serializable {
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

  val countryMapping = Map(
    "Iran (Islamic Republic of)" -> "Iran",
    "occupied Palestinian territory" -> "Palestine",
    "Viet Nam" -> "Vietnam",
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
  val stateAbbrevString: String =
    """Alabama - AL
      |Alaska - AK
      |Arizona - AZ
      |Arkansas - AR
      |California - CA
      |Colorado - CO
      |Connecticut - CT
      |Delaware - DE
      |Florida - FL
      |Georgia - GA
      |Hawaii - HI
      |Idaho - ID
      |Illinois - IL
      |Indiana - IN
      |Iowa - IA
      |Kansas - KS
      |Kentucky - KY
      |Louisiana - LA
      |Maine - ME
      |Maryland - MD
      |Massachusetts - MA
      |Michigan - MI
      |Minnesota - MN
      |Mississippi - MS
      |Missouri - MO
      |Montana - MT
      |Nebraska - NE
      |Nevada - NV
      |New Hampshire - NH
      |New Jersey - NJ
      |New Mexico - NM
      |New York - NY
      |North Carolina - NC
      |North Dakota - ND
      |Ohio - OH
      |Oklahoma - OK
      |Oregon - OR
      |Pennsylvania - PA
      |Rhode Island - RI
      |South Carolina - SC
      |South Dakota - SD
      |Tennessee - TN
      |Texas - TX
      |Utah - UT
      |Vermont - VT
      |Virginia - VA
      |Washington - WA
      |West Virginia - WV
      |Wisconsin - WI
      |Wyoming - WY""".stripMargin
  val stateMapping: Map[String, String] = stateAbbrevString.split('\n').map(s => s.split('-').map(_.trim)).map(arr => arr(1) -> arr(0)).toMap
  @transient lazy val oldYYfmt = DateTimeFormatter.ofPattern("M/d/yy H:m")
  @transient lazy val oldYYYYfmt = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
  @transient lazy val newfmt = DateTimeFormatter.ISO_DATE_TIME

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val covidSrc: DataFrame = spark.read.schema(customSchema).option("header", true).csv("/data/csse_covid_19_data/csse_covid_19_daily_reports/*.csv")

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
    case (None, Some(country: String)) => Country(lookupCountry(country), NA, NA)
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
    val Row(state, country, lastUpdate, confirmed, deaths, recovered, latitude, longitude) = row
    val cs: Country = convertStateAndCountry(Option(state.asInstanceOf[String]), Option(country.asInstanceOf[String]))
    val lastUpdateTimestamp: Timestamp = convertTimestamp(lastUpdate.asInstanceOf[String])
    def convertInt(x: Any): Int = Option(x).map(_.toString.toInt).getOrElse(-1)
    def convertDouble(x: Any): Double = Option(x).map(_.toString.toDouble).getOrElse(-1.0)
    CovidDaily(cs.county, cs.state, cs.country, lastUpdateTimestamp, convertInt(confirmed), convertInt(deaths), convertInt(recovered)) //, convertDouble(longitude), convertDouble(latitude))
  }

  val covid: Dataset[CovidDaily] =
    covidSrc.map(convertRow)
      .distinct()
  //  val covidPivot: DataFrame = covid.withColumn("lastUpdate", $"lastUpdate" cast "date")
  //    .groupBy($"country") // $"county", $"state",
  //    .pivot($"lastUpdate")
  //    .max("confirmed")
  @transient lazy val covidDeltaWindow = Window
    .partitionBy($"county", $"state", $"country")
    .orderBy($"lastUpdate")
  val covidDeltas = covid.withColumn("lastUpdate", date_trunc("Day", $"lastUpdate"))
    .withColumn("prev_confirmed", lag($"confirmed", 1, 0).over(covidDeltaWindow))
    .withColumn("prev_deaths", lag($"deaths", 1, 0).over(covidDeltaWindow))
    .withColumn("prev_recovered", lag($"recovered", 1, 0).over(covidDeltaWindow))
    .withColumn("newConfirmed", $"confirmed" - $"prev_confirmed")
    .withColumn("newDeaths", $"deaths" - $"prev_deaths")
    .withColumn("newRecovered", $"recovered" - $"prev_recovered")
    .drop("prev_confirmed", "prev_deaths", "prev_recovered")

  def saveToEs(df: DataFrame): Unit = {
    df.saveToEs("covid/deltas", Map("es.nodes" -> "elastic-node", "es.mapping.rich.date" -> "true"))
  }
}