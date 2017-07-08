package com.assignment.flightdata

import com.assignment.flightdata.FlightData.{reader, spark}
import com.util.{InitSpark, Util}
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
  * Created by ritesh on 06/07/17.
  */
object FlightDelayWithCsvRead extends InitSpark{

  def main(args: Array[String]) = {
    import spark.implicits._

    val threshold = 50.00
    val flightDF =
      reader.csv("/Users/ritesh/Documents/DataScience/advanceBigData/Assignment1/2007.csv")
    flightDF.show()

    // Start - delaypercentage calculation
    var delayDF = flightDF.select( "DepDelay","Origin","Cancelled")

    delayDF = Util.castColumnTo(delayDF, "DepDelay", DoubleType)

    delayDF = delayDF.filter($"Cancelled" !== "1")
    delayDF = delayDF.filter($"ArrDelay" !== "NA")

    delayDF = delayDF.withColumn("isDepartureDelayed", delayDF("DepDelay") > 0.0)

    delayDF = Util.castColumnTo(delayDF, "isDepartureDelayed", IntegerType)

    delayDF.show()
    val resultDF =
      delayDF.groupBy("Origin")
        .agg(((sum("isDepartureDelayed")/count("isDepartureDelayed"))*100).alias("percentageDelay"))
        .filter($"percentageDelay" > lit(threshold))

    resultDF.show()
    // End - Delay percentage calculation
  }

}
