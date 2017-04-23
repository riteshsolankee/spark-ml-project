package template.spark

import com.util.InitSpark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.util._
import org.apache.spark.sql.catalyst.expressions.NaNvl

import scala.collection.mutable

final case class Person(firstName: String, lastName: String,
                        country: String, age: Int)

object Main extends InitSpark {
  def main(args: Array[String]) = {
    import spark.implicits._

    val version = spark.version
    println("SPARK VERSION = " + version)

    val sumHundred = spark.range(1, 101).reduce(_ + _)
    println(f"Sum 1 to 100 = $sumHundred")

    println("Reading from csv file: people-example.csv")
    val persons = reader.csv("people-example.csv").as[Person]
    persons.show(2)
    val averageAge = persons.agg(avg("age"))
                     .first.get(0).asInstanceOf[Double]
    println(f"Average Age: $averageAge%.2f")

//    val macdonald =
//      spark.read.option("header","true")
//        .csv("/Users/ritesh/Documents/DataScience/advanceBigData/mcdonald_menu.csv")
//    macdonald.show(false)
    findMaxAndAverage

    close
  }

  /**
    *
    */
  def findMaxAndAverage = {
    var temp = sc.textFile("file:///Users/ritesh/Documents/DataScience/advanceBigData/mcdonald_menu.csv")
    temp.take(10).foreach(println)

    temp = temp.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    temp = temp.map(row => row.replace(", ", "- "))
    val schemaString = "1:string,2:string,3:string,4:double,5:double,6:double,7:double,8:double,9:double,10:double,11:double,12:double,13:double,14:double,15:double,16:double,17:double,18:double,19:double,20:double,21:double,22:double,23:double,24:double"

//    string.replaceAll(start + ".*" + end, "")

    import org.apache.spark.sql.types._


    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName.split(":")(0),
          Util.getFieldTypeInSchema(fieldName.split(":")(1)), true)))

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
          case IntegerType => returnVal = if(value != "NA") value.toString.toInt else Integer.MIN_VALUE
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

    var df1 = df.orderBy(desc("4")).groupBy("1").agg(max("4").alias("Max"), avg("4").alias("Average"))
    val df2 = df.select("1", "2", "4").withColumnRenamed("1", "Category").withColumnRenamed("2", "SubCategory")
    val resultDF = df2.join(df1, (df2("Category") === df1("1") && df2("4") === df1("Max"))).select("Category", "SubCategory", "Max", "Average")

    resultDF.show(false)
  }

}
