package com.assignment.flightdata

import com.util.InitSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, lit, sum, when,col}
import org.apache.spark.sql.types.{DataType, DoubleType}

/**
  * Created by ritesh on 16/05/17.
  */
case class DepartureDelay(depDelay : String, origin : String, cancelled : String)

object FlightDelayPercentageCaseClass extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

    //threshold for filter the delayed filght
    val threshold = 70.00
    var data = sc.textFile("file:///Users/ritesh/Documents/DataScience/advanceBigData/Assignment1/2007.csv")
    data.take(10).foreach(println)

    // Remove header
    data = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    var delayDF =
      data.map(str => {
        val strArr = str.split(",")
        DepartureDelay(strArr(15), strArr(16), strArr(21))
      }).toDF("DepDelay", "Origin", "Cancelled")

    delayDF.show()
    //Filter NA and cancelled records
    delayDF = delayDF.filter(delayDF("Cancelled") !== "1")
    delayDF = delayDF.filter(delayDF("DepDelay") !== "NA")

    delayDF = castColumnTo(delayDF, "DepDelay", DoubleType)
    // Get delayed departure as separate column with values as '1' - dealyed , '0' - not delayed
    delayDF = delayDF.withColumn("isDepartureDelayed", when(col("DepDelay") > 0.0, 1).otherwise(0))

    delayDF.show()
    //Calculate delay percentage grouped by origin
    val percentageDF =
      delayDF.groupBy("Origin")
        .agg(((sum("isDepartureDelayed")/count("isDepartureDelayed"))*100).alias("percentageDelay"))

    println("Total Count: " + percentageDF.count())

    val resultDF = percentageDF.filter(percentageDF("percentageDelay") > lit(threshold)).sort("percentageDelay")
    resultDF.show(resultDF.count().toInt,false)
  }

  /**
    * Cast the dataframe column type to the specified data type
    *
    * @param df - Dataframe
    * @param colName - Columns names
    * @param tpe - Dataframe Data types
    * @return
    */
  def castColumnTo(df: DataFrame, colName: String, tpe: DataType ) : DataFrame = {
    df.withColumn(colName, df(colName).cast(tpe) )
  }
}
