package com

import com.util.InitSpark

/**
  * Created by ritesh on 13/05/17.
  */
object TestDataFrame extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

//    val data = Array(1, 2, 3, 4, 5)
//    val distData = sc.parallelize(data)
//
//    distData.collect().foreach(println)
//
//    distData.take(2).foreach(println)


    val str = "In this case for each element, an instance of the parser class will be created, the element will be processed and then the instance will be destroyed in time but this instance will not be used for other elements. So if you are working with an RDD of 12 elements distributed among 4 partitions, the parser instance will be created 12 times. And as you know creating an instance is a very expensive operation so it will take time."

//    val lines = sc.parallelize(str.split(" "))

    val lines = sc.textFile("file:///Users/ritesh/Documents/DataScience/advanceBigData/wordCount.txt")

    lines.foreach(println)

    val lineLengths = lines.map(s => s.length)

    lineLengths.collect().foreach(println)

    val totalLength = lineLengths.reduce((a,b) => a + b)

    println(totalLength)

    val pairs = lines.map(s => (s,1)).reduceByKey((a,b) => a+b)

    pairs.collect().foreach(println)

    val linesWithKey = lines.filter(line => line.contains("Spark"))
    linesWithKey.foreach(println)

    println(lines.map(line => line.split(" ").size).reduce((a,b) => if(a >b) a else b ))
    println(lines.map(line => line.split(" ").size).reduce((a,b) => Math.max(a ,b)  ))
    println(lines.map(line => line.split(" ").size).reduce((a,b) => Math.min(a ,b)  ))

    println("=============1: ")
    lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a + b).foreach(println)

    println("=============2: ")

    lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a + b).sortByKey().foreach(println)
    println("=============3: ")
    lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a + b).map(item => item.swap).sortByKey(false).foreach(println)
    println("=============4:")
    lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a + b).map(item => item.swap).sortByKey(false).map(item => item.swap).foreach(println)


    val parl = sc.parallelize(1 to 9)
    parl.foreach(println)

    println("===========================:")

    parl.collect().foreach(println)

    val parl2 = sc.parallelize(5 to 15)
    val parunion = parl.union(parl2)

    parunion.collect()

    println("===========================:")
    parl.union(parl2).distinct().collect().foreach(println)

    println("===========JOIN=====================")

    val names1 = sc.parallelize(List("abe","abby", "apple" )).map(a => (a,1))
    val names2 = sc.parallelize(List("apple","beatty", "beatrice" )).map(a => (a,1))

    println("===========================:")
    names1.collect().foreach(println)
    names2.collect().foreach(println)
    println("===========================:")

    names1.join(names2).collect().foreach(println)
    println("===========================:")
    names1.leftOuterJoin(names2).collect().foreach(println)
    println("===========================:")
    names1.rightOuterJoin(names2).collect().foreach(println)

//    names1.rightOuterJoin(names2).repartition(1).saveAsTextFile("file:///Users/ritesh/Documents/DataScience/advanceBigData/wordCount")


    val teams = sc.parallelize(List("twin", "brewers", "cubs", "white sox", "indian"))

    teams.takeSample(true, 3).foreach(print)


    val hockyTeams = sc.parallelize(List("wild", "blackhawks", "red wings", "wild", "oilers", "whales", "jets", "wild"))

    println(hockyTeams.map(k => (k,1)).countByKey())

    //==============
    val babyDF =
      reader.csv("/Users/ritesh/Documents/DataScience/advanceBigData/baby_names.csv")

//    babyDF.show()
//    babyDF.select("First Name", "Count").show()

    val babyRdd = babyDF.rdd.map(name => (name(1), name(4))).countByKey().take(10).foreach(println)

    println(babyDF.rdd.map(name => (name(1), name(4))).countByKey().maxBy(value => value._2))
    //========

    val babyDF1 =
          reader.csv("/Users/ritesh/Documents/DataScience/advanceBigData/baby_names.csv")

    val babyJson = babyDF1.toJSON

    println(babyJson)

    babyDF1.toJSON.take(2).foreach(println)


    babyDF1.write.json("/Users/ritesh/Documents/DataScience/advanceBigData/baby")


  }

}
