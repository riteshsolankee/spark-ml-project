package com.sample

import com.util.InitSpark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
  * Created by ritesh on 27/05/17.
  */

object SampleApp extends InitSpark{

  def main(args:Array[String]) {

    val txtFile ="/Users/ritesh/IdeaProjects/spark-ml-project/src/main/scala/com/sample/SampleApp.scala"
//    val conf = new SparkConf().setAppName("Sample Application")
//    val sc = new SparkContext(conf)
    val txtFileLines = sc.textFile(txtFile, 2).cache()
    val numAs = txtFileLines.filter(line => line.contains("val")).count()
    println("Lines with val: %s".format(numAs))
  }
}