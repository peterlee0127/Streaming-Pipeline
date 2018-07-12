/**
  * Created by Peter on 9/8/2016 AD.
  */

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.classification._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object SVM {
    def train(trainingSet:DataFrame,testSet:DataFrame, ssc: StreamingContext): SVMMultiClassOVAModel = {
      val spark = SparkSession
        .builder().master("local[4]").appName("Spark").getOrCreate()
      import spark.implicits._
      val stream = ssc.socketTextStream("localhost", 9999)

      val tokenizer = new Tokenizer()
      .setInputCol("sentence")
    .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeatures")
      .setNumFeatures(15000)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf))
    val pipelineModel = pipeline.fit(trainingSet)

    val t = pipelineModel.transform(trainingSet).select("features", "label").rdd.map( row => LabeledPoint(
      row.getAs[Double]("label"),
      SparseVector.fromML(row.getAs[org.apache.spark.ml.linalg.SparseVector]("features"))
    ))

//    val model = new SVMWithSGD().run(t)
    val model:SVMMultiClassOVAModel = new SVMMultiClassOVAWithSGD().train(t,2000)


    val testD = pipelineModel.transform(testSet).select("features", "label").rdd.map( row => LabeledPoint(
      row.getAs[Double]("label"),
      SparseVector.fromML(row.getAs[org.apache.spark.ml.linalg.SparseVector]("features"))
    ))

    val prediction = model.predict(testD.map(_.features))
    val predictionAndLabel = prediction.zip(testD.map(_.label))

//    predictionAndLabel.collect().foreach(println(_))

       val metrics = new MulticlassMetrics(predictionAndLabel)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }



    return model
      /*
      val LogModel = new LogisticRegressionWithLBFGS()
        .setNumClasses(5)
        .run(t)

      // Compute raw scores on the test set.
      val LogpredictionAndLabels = testD.map { case LabeledPoint(label, features) =>
        val prediction = LogModel.predict(features)
        (prediction, label)
      }

      // Get evaluation metrics.
      val metrics = new MulticlassMetrics(LogpredictionAndLabels)
      val accuracy = metrics.accuracy
      println(s"Accuracy = $accuracy")
*/

//stream.foreachRDD {
//  rdd =>
//    if(!rdd.isEmpty()) {
//      val streamDF = rdd.toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
//      val test = pipelineModel.transform(testSet).select("features", "label").rdd.map( row =>
//        SparseVector.fromML(row.getAs[org.apache.spark.ml.linalg.SparseVector]("features"))
//      )
//      LogModel.predict(test).print()
//
//    }
//}

      //    ssc.start()
      //    ssc.awaitTermination()

//      return LogModel
    }
}
