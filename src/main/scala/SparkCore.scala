/* SparkCore.scala
* -Xms500m -Xmx5000m -Xss1m
* */

import java.io.File
import java.util.HashMap

import scala.io.Source._
import scala.util.parsing.json._
import kafka.serializer.StringDecoder
import org.apache.log4j._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel, PredictionModel, Predictor}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StringIndexer, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkCore {
  var numberOfSample = 1000000
  var numberOfFeature = 1000
  var pathPrefix = ""
  var Class = List("")

  def setLogger() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
  }
  def getDirectoryInfo(path:List[String]) = {
    for(i <- 0 until path.length) {
      val count = new File(path(i)).listFiles.length-1
      print(Class(i)+":"+count+", ")
      if(count<numberOfSample) {
        numberOfSample = count
      }
    }
    println("Take "+numberOfSample +" from each domain")
  }
  def getStopWords() : Array[String] = {

    val stopwords = Array("1","2","3","4","5","6","a","an","in","on","with","by","him","her","to","for","and",
    "ve","lot","did","didn","don","d","g","v","j","the","i","am","are","then","too","after","later","s",
    "very","it","me","but","that","there","was","were","about", "of","why","so","be","of","not","is","you",
    "she","he","his","mr","mrs","t","from","how","do","does","doesn","as","this","which","when","m","many",
    "have","has","had","will","first","second","third","our","may","begin","at","th","its","up","down","all",
    "part","if","else", "one","two","three","four","get","ll","can","who","on","off","been","they","new","old",
    "since", "said","most","much","little","o","yes","no","u","once","half","full","ms","see","saw","such","kind",
    "upon","yet","my","we","your","yours","just","here","would","should","can","or","find","met","allow","well","asked","ask","year","week","month","day","ago")
    return stopwords
  }
  def getProducer(): KafkaProducer[String, String] = {
    val prouderProps = new HashMap[String, Object]()
    prouderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prouderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prouderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prouderProps)
    return producer
  }
  def train(trainingSet:DataFrame,testSet:DataFrame) : CrossValidatorModel = {
    var regex = new RegexTokenizer().setInputCol("sentence").setOutputCol("filtered").setPattern("\\w+").setGaps(false)
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures").setNumFeatures(numberOfFeature)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var forest = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setFeatureSubsetStrategy("auto")

    val transformers = Array(
      //new StringIndexer().setInputCol("group").setOutputCol("label"),
      //    new Tokenizer().setInputCol("sentence").setOutputCol("tokens"),
      //    new StopWordsRemover().setStopWords(getStopWords).setCaseSensitive(false).setInputCol("tokens").setOutputCol("filtered"),
      //    new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(100),
      regex,hashingTF,idf,forest
    )

      val pipeLine = new Pipeline().setStages(transformers)
//    val model = pipeLine.fit(trainingSet)
//    val rfModel = model.stages(4).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)

// We use a ParamGridBuilder to construct a grid of parameters to search over.
// With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
// this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(1000, 5000, 10000, 20000, 30000, 60000, 80000))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeLine)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)  // Use 3+ in practice

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeLine)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)



    // Run cross-validation, and choose the best set of parameters.
    val model = cv.fit(trainingSet)

    val bestModel = model.bestModel.asInstanceOf[PipelineModel]
    val stages = bestModel.stages

    val hashingStage = stages(1).asInstanceOf[HashingTF]
    println("numFeatures = " + hashingStage.getNumFeatures)


    val prediction = model.transform(testSet)

    //    val da = prediction.select("label","prediction","filtered")
    //    da.write.format("json").mode("OverWrite").save("prediction")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(prediction)
    println("Accuracy: "+accuracy)
    println("Test Error = " + (1.0 - accuracy))

    return model
  }
  def main(args: Array[String]) = {
    setLogger()
    val spark = SparkSession
      .builder().master("local[4]").appName("Spark").getOrCreate()
    import spark.implicits._

    val configuration = scala.io.Source.fromFile("./configuration.config").getLines().mkString
    val json = JSON.parseFull(configuration)
    val map : Map[String, Option[Any]] = json.get.asInstanceOf[Map[String, Option[Any]]];
    var pathPrefix = map.get("path").get.asInstanceOf[String]
    Class = map.get("Class").get.asInstanceOf[List[String]]
    numberOfSample = map.get("numberOfSample").get.asInstanceOf[Double].toInt
    numberOfFeature = map.get("numberOfFeature").get.asInstanceOf[Double].toInt


//    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

   // val stream = ssc.socketTextStream("localhost", 9999)

//    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
//    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, Set("twitter"))

    var path = Class.map(pathPrefix + _)
    getDirectoryInfo(path)

    var df = spark.read.textFile(path(0)).toDF("sentence").withColumn("label",  when($"sentence".isNotNull, 1.0))
    df = df.limit(numberOfSample)
    var Array(trainingSet,testSet) = df.randomSplit(Array(0.9,0.1), seed = 12345)
    for(i <- 1 until path.length) {
      val labeled = i.toDouble+1.0
      var dataDF = spark.read.textFile(path(i)).toDF("sentence").withColumn("label",  when($"sentence".isNotNull, labeled))
      dataDF = dataDF.limit(numberOfSample)
      val Array(training,test) = dataDF.randomSplit(Array(0.9,0.1), seed = 12345)
      trainingSet = trainingSet.union(training)
      testSet = testSet.union(test)
    }


//    var trainingSet = spark.read.format("libsvm").load("/Users/Peter/AizuLab/s1220150/japan-times-feature-vector-scala/data/sample/multi-class-train-set")
//    var testSet = spark.read.format("libsvm").load("/Users/Peter/AizuLab/s1220150/japan-times-feature-vector-scala/data/sample/multi-class-test-set")
    println("training:"+trainingSet.count()+"  testing:"+testSet.count())

    val model = train(trainingSet,testSet)
//    println("\nStart Reading Stream Text")

//    var producer = getProducer()
//    kafkaStream.foreachRDD {
//        rdd =>
//          if(!rdd.isEmpty()) {
//            val streamDF = rdd.map(_._2).toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
//            val prediction = model.transform(streamDF).select("prediction","sentence")
//            prediction.show()
//
//            val message = new ProducerRecord[String, String]("result", null, prediction.collect().mkString(""))
//            producer.send(message)
//            //probability
//          }
//    }

    /*
    stream.foreachRDD {
      rdd =>
        if(!rdd.isEmpty()) {
          val streamDF = rdd.toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
          val prediction = model.transform(streamDF).select("prediction","sentence","probability")
          prediction.show(false)
        }
    }
    */
//    ssc.start()
//    ssc.awaitTermination()
  }

}
