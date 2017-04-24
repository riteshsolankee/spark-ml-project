package com.assignment.flightdata

import com.util.InitSpark
import org.apache.spark.sql.functions._
import com.util.Util
import org.apache.spark.sql.types.DoubleType


/**
  * Created by ritesh on 23/04/17.
  */
object FlightData extends InitSpark{

  def main(args: Array[String]) = {
    import spark.implicits._

//    var flightData = sc.textFile("file:///Users/ritesh/Documents/DataScience/advanceBigData/Assignment1/2007.csv")

    val flightDF =
      spark.read.option("header","true")
        .csv("/Users/ritesh/Documents/DataScience/advanceBigData/Assignment1/2007.csv")
    var flightDelayDF =
      flightDF.select("Year","Month", "DayofMonth", "DayOfWeek",
        "FlightNum","ArrDelay", "DepDelay","Cancelled",
        "CancellationCode", "Diverted")

//    flightDelayDF.select("Year").distinct().show()
//    flightDelayDF.select("Month").distinct().show()
//    flightDelayDF.select("DayofMonth").distinct().show()
//    flightDelayDF.select("DayOfWeek").distinct().show()
//    flightDelayDF.select("CancellationCode").distinct().show()
//    println(flightDelayDF.filter($"FlightNum" === "NA").count())
//    println("Total Count of ArrDelay column:- " + flightDelayDF.count())
//    println("Total Count of DepDelay column:- " + flightDelayDF.count())

//    println("Count of NA in ArrDelay column:- " + flightDelayDF.filter($"ArrDelay" === "NA").count())
//    println("Count of NA in DepDelay column:- " + flightDelayDF.filter($"DepDelay" === "NA").count())
//    println("Count of NA in both DepDelay & ArrDelay columns:- " + flightDelayDF.filter($"DepDelay" === "NA" && $"ArrDelay" === "NA").count())
//    flightDelayDF.show
//    println("Count of NA in both DepDelay & ArrDelay columns:- " + flightDF.filter($"DepDelay" === "NA" && $"ArrDelay" === "NA").count())

//    flightDF.filter($"DepDelay" === "NA" && $"ArrDelay" === "NA").show
//    flightDF.filter($"DepDelay" === "NA" && $"ArrDelay" =!= "NA").show
//    flightDF.filter($"DepDelay" =!= "NA" && $"ArrDelay" === "NA").show
    //filtering all the records where the cancellation record is set to '1'
    flightDelayDF = flightDelayDF.filter($"Cancelled" =!= "1")
    //There are records where 'Departure' delay  not null and  arrival delay is not applicable
    //Therefore Creating different DF fpr arrival and departure
    var arrivalDelayDF =
      flightDelayDF
        .select("Year","Month", "DayofMonth", "DayOfWeek",
          "FlightNum","ArrDelay","Cancelled","CancellationCode", "Diverted")
        .filter($"ArrDelay" =!= "NA")
    arrivalDelayDF = Util.castColumnTo(arrivalDelayDF, "ArrDelay", DoubleType)
    arrivalDelayDF =arrivalDelayDF.groupBy("DayOfWeek").agg(avg("ArrDelay").alias("ArrivalDelayAverage")).withColumnRenamed("DayOfWeek", "ArrDayOfWeek")
//    arrivalDelayDF.show

    var departureDelayDF =
      flightDelayDF
        .select("Year","Month", "DayofMonth", "DayOfWeek", "FlightNum",
          "DepDelay","Cancelled","CancellationCode", "Diverted")
        .filter($"DepDelay" =!= "NA")
    departureDelayDF = Util.castColumnTo(departureDelayDF, "DepDelay", DoubleType)
    departureDelayDF = departureDelayDF.groupBy("DayOfWeek").agg(avg("DepDelay").alias("DepartureDelayAverage"))
//    departureDelayDF.show

    val resultDF =
      arrivalDelayDF.join(
        departureDelayDF,
          arrivalDelayDF("ArrDayOfWeek") === departureDelayDF("DayOfWeek"))
        .orderBy("DayOfWeek")
        .select("DayOfWeek", "ArrivalDelayAverage", "DepartureDelayAverage")
    resultDF.show
  }
}
