package com.util

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._

/**
  * Created by ritesh on 23/04/17.
  */
object Util {
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
      case "byte" => return ByteType
      case "string" => return StringType
      case "date" => return TimestampType
      case "timestamp" => return StringType
      case "uuid" => return StringType
      case "decimal" => return DoubleType
      case "boolean" => BooleanType
      case "counter" => IntegerType
      case "bigint" => IntegerType
      case "text" => return StringType
      case "ascii" => return StringType
      case "varchar" => return StringType
      case "varint" => return IntegerType
      case default => return StringType
    }
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
