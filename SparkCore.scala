/* SimpleApp.scala */
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.clustering.KMeans

//import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.spark.sql._
import scala.collection.mutable.WrappedArray

object SparkCore {
  def setLogger() = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.log4j").setLevel(Level.OFF)
    Logger.getLogger("kafka.utils").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("com.github.fommil.netlib").setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    setLogger()
    val conf = new SparkConf().setAppName("SparkCore")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    val spark = SparkSession
    .builder()
    .appName("Spark SQL Twitter")
    .getOrCreate()

    val numClusters = 10
    val numIterations = 10
    val outputModelDir = "./"
/*
    val path = "file:///home/peter/tw.json"
    val df = spark.read.json(path)
    df.registerTempTable("tweets")
    val en = df.select("lang","text","id").where("lang=='en'")

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(en)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData).select("features").rdd//.map{_.toSeq}

    val k = 18
    val maxIterations = 50
    */
//    val kmeansModel = KMeans.train(rescaledData, k, maxIterations) // --(5)


//    val list = rescaledData.select("features").collect()
//    val dataset = rescaledData
//
//    val kmeans = new KMeans().setK(2).setSeed(1L)
//    val model = kmeans.fit(dataset)
//
//    // Evaluate clustering by computing Within Set Sum of Squared Errors.
//    val WSSSE = model.computeCost(dataset)
//    println(s"Within Set Sum of Squared Errors = $WSSSE")
//
//    // Shows the result.
//    println("Cluster Centers: ")
//    model.clusterCenters.foreach(println)
   /* 
    val data = MLUtils.loadLibSVMFile(sc, "/usr/local/spark/data/mllib/sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    val model = SVMWithSGD.train(training, numIterations)

    model.clearThreshold()
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
        (score, point.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    
    println("Area under ROC = " + auROC)
    */
    //val texts = df.selectExpr("(split(text, ' '))").cast("Seq").as("newText")
    //val texts = en.select("text").explode("text","newtext"){item:String => item.asInstanceOf[String].split(",")}
    //texts.printSchema()

//    val remover = new StopWordsRemover().setInputCol("value").setOutputCol("textout")
  //  val result = remover.transform(texts)
    //result.collect().foreach(println)
    
/*
    val vectors = texts.map(Utils.featurize).cache()
    vectors.count() 
    val model = KMeans.train(vectors, numClusters, numIterations)
    sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)

    val some_tweets = texts.take(1000)
    println("----Example tweets from the clusters")
    for (i <- 0 until numClusters) {
      println(s"\nCLUSTER $i:")
        some_tweets.foreach { t =>
          if (model.predict(Utils.featurize(t)) == i) {
            println(t)
          }
        }
    }
  */
//    val hashtags = result.select("text").map(_.getSeq[Seq[String]](0))
  //  hashtags.map(println("row:"+_))
//    hashtags.foreach(tag=>
//        tag.foreach(_.rdd.map(word => (word(0), 1)).reduceByKey(_+_).foreach(println))
//    )
//    hashtags.collect().flatMap(word => (word(0), 1)).reduceByKey(_+_).foreach(println)
    // .toJavaRDD.map(word => (word, 1)).reduceByKey(_+_).foreach(println)
    // .collect()
    // .map(_.getAs[Seq[String]](0)).foreach(
    //     _.foreach(item=>
    //         item.map(word => (word, 1)).reduceByKey(_+_).foreach(println)
    //         // item
    //     )
    // )
    //    hashtags.foreach(tag=>
    //        tag.foreach(_.rdd.map(word => (word(0), 1)).reduceByKey(_+_).foreach(println))
    //    )
    //    hashtags.collect().flatMap(word => (word(0), 1)).reduceByKey(_+_).foreach(println)
    // .toJavaRDD.map(word => (word, 1)).reduceByKey(_+_).foreach(println)
    // .collect(
    // .flatMap(println(_))
        //_.getAs[Seq[String]](0))
    // hashtags.foreach(println(_))
    // val hashtagsArray:WrappedArray[String] = hashtags
    //.getAs[Seq[String]](0)
    //flatMap(word => (word, 1)).reduceByKey(_+_)
    // hashtagsArray.flatMap(item=> println(item))
    // foreach(println)

    // flatMap(word => (word, 1)).reduceByKey(_+_).foreach(println)
    //.count().show(100)//.foreach(println)


  }
}
