package com.chrisalbright.covid

import org.apache.spark.sql.SparkSession

object CovidAnalysisJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val covidAnalysis = new CovidAnalysis(spark)
    covidAnalysis.publishDeltas()
  }

}
