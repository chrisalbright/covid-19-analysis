package com.chrisalbright.covid

import java.sql.{Date, Timestamp}

import com.chrisalbright.covid.DataCleanse.{convertStateAndCountry, convertTimestamp}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

class CovidAnalysis(spark: SparkSession, options: Map[String, String] = Map(
  "es.nodes" -> "elastic-node",
  "es.mapping.rich.date" -> "true",
  "es.mapping.id" -> "id"
)) extends Serializable {

  import spark.implicits._

  lazy val covidSrc: DataFrame =
    spark.read.schema(CovidAnalysis.customSchema)
      .option("header", true)
      .csv("/data/csse_covid_19_data/csse_covid_19_daily_reports/*.csv")
      .withColumn("recordDate", to_date(regexp_extract(input_file_name(), """.+\/(\d\d-\d\d-\d\d\d\d).csv""", 1), "MM-dd-yyyy"))

  def convertRow(row: Row): CovidDaily = {
    val Row(state, country, lastUpdate, confirmed, deaths, recovered, latitude, longitude, recordDate: Date) = row
    val cs: Country = convertStateAndCountry(Option(state.asInstanceOf[String]), Option(country.asInstanceOf[String]))
    val lastUpdateTimestamp: Timestamp = convertTimestamp(lastUpdate.asInstanceOf[String])
    def convertInt(x: Any): Int = Option(x).map(_.toString.toInt).getOrElse(0)
    def convertDouble(x: Any): Double = Option(x).map(_.toString.toDouble).getOrElse(-1.0)
    CovidDaily(recordDate, cs.state, cs.country, convertInt(confirmed), convertInt(deaths), convertInt(recovered)) //, convertDouble(longitude), convertDouble(latitude))
  }

  private lazy val covid0 =
    covidSrc.map(convertRow)
      .groupBy($"state", $"country", $"recordDate")
      .agg(
        sum("confirmed") as "confirmed",
        sum("deaths") as "deaths",
        sum("recovered") as "recovered")
      .cache()

  private lazy val states = covid0.select($"state", $"country").distinct()
  private lazy val dates = covid0.select($"recordDate").distinct()
  private lazy val allDates = dates.crossJoin(states)

  lazy val covid = allDates
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

  @transient
  private lazy val covidDeltaWindow = Window
    .partitionBy($"state", $"country")
    .orderBy($"recordDate")

  lazy val covidDeltas = covid
    .withColumn("prev_confirmed", lag($"confirmed", 1, 0).over(covidDeltaWindow))
    .withColumn("prev_deaths", lag($"deaths", 1, 0).over(covidDeltaWindow))
    .withColumn("prev_recovered", lag($"recovered", 1, 0).over(covidDeltaWindow))
    .withColumn("newConfirmed", $"confirmed" - $"prev_confirmed")
    .withColumn("newDeaths", $"deaths" - $"prev_deaths")
    .withColumn("newRecovered", $"recovered" - $"prev_recovered")
    .drop("prev_confirmed", "prev_deaths", "prev_recovered")

  def publish(rdd: RDD[CovidEs]): Unit = EsSpark.saveToEs(rdd, "covid/deltas", options)

  private def prepareDeltas(covidDeltas: DataFrame): RDD[CovidEs] = covidDeltas.rdd.map {
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
  
  def publishDeltas(): Unit = {
    publish(prepareDeltas(covidDeltas))
  }

  def saveDeltasCSV(): Unit = {
    covidDeltas
      .coalesce(1)
      .withColumnRenamed("recovered", "totalRecovered")
      .withColumnRenamed("deaths", "totalDeaths")
      .withColumnRenamed("confirmed", "totalConfirmed")
      .write.mode("overwrite")
      .option("header", true)
      .csv("/output/covid.csv")
  }
}

object CovidAnalysis {
  val stateName = "State"
  val countryName = "Country"
  val customSchema = StructType(Array(
    StructField("Province/State", StringType, nullable = true),
    StructField("Country/Region", StringType, nullable = true),
    StructField("Last Update", StringType, nullable = true),
    StructField("Confirmed", IntegerType, nullable = true),
    StructField("Deaths", IntegerType, nullable = true),
    StructField("Recovered", IntegerType, nullable = true),
    StructField("Latitude", DoubleType, nullable = true),
    StructField("Longitude", DoubleType, nullable = true)
  ))
}