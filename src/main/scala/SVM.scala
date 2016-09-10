/**
  * Created by Peter on 9/8/2016 AD.
  */

import org.apache.log4j._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}


object SVM {
    def train(trainData:DataFrame): Unit = {
//        val numIterations = 100
//        val model = SVMWithSGD.train(trainData.rdd, numIterations)
//        // Clear the default threshold.
//        model.clearThreshold()
//        // Compute raw scores on the test set.
//        val scoreAndLabels = test.map { point =>
//            val score = model.predict(point.features)
//            (score, point.label)
//        }
//        // Get evaluation metrics.
//        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//        val auROC = metrics.areaUnderROC()
//
//        println("Area under ROC = " + auROC)

    }
}
