package com.chrisalbright

import java.sql.Date

package object covid {
  case class Country(country: String, state: String, county: String)
  case class CovidDaily(recordDate: Date, state: String, country: String, confirmed: BigInt, deaths: BigInt, recovered: BigInt)
  case class CovidEs(id: Int, recordDate: java.util.Date, state: String, country: String, totalConfirmed: Int, newConfirmed: Int, totalDeaths: Int, newDeaths: Int, totalRecovered: Int, newRecovered: Int)

}
