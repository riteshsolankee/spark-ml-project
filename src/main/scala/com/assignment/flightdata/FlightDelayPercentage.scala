package com.assignment.flightdata

import com.util.{InitSpark}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Created by ritesh on 15/05/17.
  */
object FlightDelayPercentage extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

    //Create data frame from CSV file
    val flightDF = createFlightDataFrame("Users/ritesh/Documents/DataScience/advanceBigData/Assignment1/2007.csv")
//    val threshold = args(0).toDouble
    val threshold = 50.00

    // Take only DepartureDelay, Origin and Cancelled data
    var delayDF = flightDF.select( "DepDelay","Origin","Cancelled")

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
    * Reads CSV file and create Dataframe
    *
    * @return
    */
  def createFlightDataFrame (path : String): DataFrame = {
    var temp = sc.textFile("file:///" + path)
    temp.take(10).foreach(println)

    // Remove header
    temp = temp.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    // Define column name and its datatype
    val schemaString = "Year:string,Month:string,DayofMonth:string,DayOfWeek:string,DepTime:string,CRSDepTime:string," +
      "ArrTime:string,CRSArrTime:string,UniqueCarrier:string,FlightNum:string,TailNum:string,ActualElapsedTime:string," +
      "CRSElapsedTime:string,AirTime:string,ArrDelay:string,DepDelay:string,Origin:string,Dest:string,Distance:string," +
      "TaxiIn:string,TaxiOut:string,Cancelled:string,CancellationCode:string,Diverted:string,CarrierDelay:string," +
      "WeatherDelay:string,NASDelay:string,SecurityDelay:string,LateAircraftDelay:string"

    import org.apache.spark.sql.types._

    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName.split(":")(0),
          getFieldTypeInSchema(fieldName.split(":")(1)), true)))

    print("==>" + schema)
    import org.apache.spark.sql._

    val rowRDDx = temp.map(p => {
      var list: mutable.Seq[Any] = collection.mutable.Seq.empty[Any]
      var index = 0
      var tokens = p.split(",")
      tokens.foreach(value => {
        var valType = schema.fields(index).dataType
        var returnVal: Any = null
        valType match {
          case IntegerType => returnVal = value.toString.toInt
          case DoubleType => returnVal = value.toString.toDouble
          case LongType => returnVal = value.toString.toLong
          case FloatType => returnVal = value.toString.toFloat
          case ByteType => returnVal = value.toString.toByte
          case StringType => returnVal = value.toString
          case TimestampType => returnVal = value.toString
        }
        list = list :+ returnVal
        index += 1
      })
      Row.fromSeq(list)
    })
    val df = sqlContext.createDataFrame(rowRDDx, schema)

    df.show()
    df
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

  /**
    *
    * @param ftype
    * @return
    */
  def getFieldTypeInSchema(ftype: String): DataType = {

    ftype match {
      case "int" => return IntegerType
      case "double" => return DoubleType
      case "long" => return LongType
      case "float" => return FloatType
      case "string" => return StringType
      case default => return StringType
    }
  }

}
