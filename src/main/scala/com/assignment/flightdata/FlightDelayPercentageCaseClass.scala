package com.assignment.flightdata

import com.util.InitSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}

/**
  * Created by ritesh on 16/05/17.
  */
case class DepartureDelay(depDelay : String, origin : String, cancelled : String)

object FlightDelayPercentageCaseClass extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

    val threshold = 50.00
    var delayDF = createSubsetDataFrame("Users/ritesh/Documents/DataScience/advanceBigData/Assignment1/2007.csv")
    delayDF.show()
    //Filter NA and cancelled records
    delayDF = delayDF.filter($"Cancelled" !== "1")
    delayDF = delayDF.filter($"DepDelay" !== "NA")

    delayDF = castColumnTo(delayDF, "DepDelay", DoubleType)
    // Get delayed departure as separate column with values as '1' - dealyed , '0' - not delayed
    delayDF = delayDF.withColumn("isDepartureDelayed", delayDF("DepDelay") > 0.0)
    delayDF = castColumnTo(delayDF, "isDepartureDelayed", IntegerType)

    delayDF.show()
    //Calculate delay percentage grouped by origin
    val resultDF =
      delayDF.groupBy("Origin")
        .agg(((sum("isDepartureDelayed")/count("isDepartureDelayed"))*100).alias("percentageDelay"))
        .filter($"percentageDelay" > lit(threshold))

    resultDF.show()
  }

  /**
    *
    * @param path
    * @return
    */
  def createSubsetDataFrame(path : String) : DataFrame = {
    var data = sc.textFile("file:///" + path)
    data.take(10).foreach(println)

    // Remove header
    data = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    import sqlContext.implicits._
    data.map(str => {
      val strArr = str.split(",")
      DepartureDelay(strArr(15), strArr(16), strArr(21))
    }).toDF("DepDelay", "Origin", "Cancelled")
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
