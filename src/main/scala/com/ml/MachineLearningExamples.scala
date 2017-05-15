package com.ml

import com.util.InitSpark
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

/**
  * Created by ritesh on 13/05/17.
  */
object MachineLearningExamples extends InitSpark {


  def main(args: Array[String]) = {
    import spark.implicits._

    // Call SVM
    svm
//     linearRegression
//  Create a dense vector (1.0, 0.0, 3.0).
//    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
//    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
//    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
//    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
//    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

//    println(sv2)
  }


  def svm = {
    //Support vector machine
    //Load and parse the data file
    val data = sc.textFile("/Users/ritesh/Documents/DataScience/advanceBigData/svm/sample_svm_data.txt")
    data.map(l => l.split(' ')).take(10).foreach(println)

    val parsedData = data.map { line =>
      val parts = line.split(' ')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(x => x.toDouble)))
      //      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(x => x.toDouble).toArray))
    }

    parsedData.take(10).foreach(println)

    // Run training  algorithm to build the model
    val numIterations = 20
    val model = SVMWithSGD.train(parsedData, numIterations)

    val labelAndPreds = parsedData.map{ point =>
      val prediction = model.predict((point.features))
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble/parsedData.count()
    println("Training Error = " + trainErr)
  }

  // Linear Regression
  def linearRegression = {

    val data = sc.textFile("/Users/ritesh/Documents/DataScience/advanceBigData/linear_regression/lpsa.data")

    val parsedData = data.map{ line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(x => x.toDouble)))

    }
    //Building the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    val valuesAndPreds = parsedData.map{ point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)
  }
}
