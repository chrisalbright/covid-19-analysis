package com.chrisalbright.covid

import org.apache.spark.sql.SparkSession

object CovidAnalysisJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val covidAnalysis = new CovidAnalysis(spark)
    args.lift(0) match {
      case Some("es") => covidAnalysis.publishDeltas()
      case Some("csv") => covidAnalysis.saveDeltasCSV()
      case Some(_)|None => ""
    }
    spark.stop()
    sys.exit()
  }
}
