import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class CountyAndState(state: String, county: String)
case class CovidDaily(county: String, state: String, country: String, lastUpdate: Timestamp, confirmed: Int, deaths: Int, recovered: Int, latitude: Double, longitude: Double)

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
  val stateCodes: Map[String, String] = stateAbbrevString.split('\n').map(s => s.split('-').map(_.trim)).map(arr => arr(1) -> arr(0)).toMap.withDefaultValue("Unknown")
  @transient lazy val oldYYfmt = DateTimeFormatter.ofPattern("M/d/yy H:m")
  @transient lazy val oldYYYYfmt = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
  @transient lazy val newfmt = DateTimeFormatter.ISO_DATE_TIME

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  private val covidSrc: DataFrame = spark.read.schema(customSchema).option("header", true).csv("/data/csse_covid_19_data/csse_covid_19_daily_reports/*.csv")

  def convertState(state: Option[String]): CountyAndState = state match {
    case None => CountyAndState("None Available", "None Available")
    case Some(str: String) if str.contains(",") =>
      val split: Array[String] = str.split(", ")
      CountyAndState(split(1), split(0))
    case Some(str: String) => CountyAndState(str, "None Available")
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
    val cs: CountyAndState = convertState(Option(state.asInstanceOf[String]))
    val lastUpdateTimestamp: Timestamp = convertTimestamp(lastUpdate.asInstanceOf[String])
    val safeCountry: String = Option(country.asInstanceOf[String]).getOrElse("None Available")
    def convertInt(x: Any): Int = Option(x).map(_.toString.toInt).getOrElse(-1)
    def convertDouble(x: Any): Double = Option(x).map(_.toString.toDouble).getOrElse(-1.0)
    CovidDaily(cs.county, cs.state, safeCountry, lastUpdateTimestamp, convertInt(confirmed), convertInt(deaths), convertInt(recovered), convertDouble(longitude), convertDouble(latitude))
  }

  val covid: Dataset[CovidDaily] = covidSrc.map(convertRow)
}