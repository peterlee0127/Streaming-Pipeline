/* SparkCore.scala
* -Xms500m -Xmx5000m -Xss1m
* */

import java.io.File
import java.util.HashMap

import scala.io.Source._
import scala.util.parsing.json._
import kafka.serializer.StringDecoder
import org.apache.log4j._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.ml.{Pipeline, PipelineModel, PredictionModel, Predictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StringIndexer, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.to_json



object SparkCore {
  var numberOfSample = 1000000
  var numberOfFeature = 1000
  var pathPrefix = ""
  var Class = List("")

  def setLogger() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("zookeeper").setLevel(Level.OFF)
    Logger.getLogger("kafka").setLevel(Level.OFF)
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
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("features").setNumFeatures(numberOfFeature)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val countVector = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(100)

    var forest = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setFeatureSubsetStrategy("auto")

    val desTree = new DecisionTreeClassifier()
    val naiveBayes = new NaiveBayes().setModelType("multinomial")

    val transformers = Array(
      //new StringIndexer().setInputCol("group").setOutputCol("label"),
      //new Tokenizer().setInputCol("sentence").setOutputCol("tokens"),
      //new StopWordsRemover().setStopWords(getStopWords).setCaseSensitive(false).setInputCol("tokens").setOutputCol("filtered"),
      regex,hashingTF,naiveBayes
    )

      val pipeLine = new Pipeline().setStages(transformers)
//    val model = pipeLine.fit(trainingSet)

// We use a ParamGridBuilder to construct a grid of parameters to search over.
// With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
// this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(5000, 10000, 15000))
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
      .setNumFolds(3)  // Use 3+ in practice

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
//  def toOld(v: NewVector): OldVector = v match {
//    case sv: NewSparseVector => OldVectors.sparse(sv.size, sv.indices, sv.values)
//    case dv: NewDenseVector => OldVectors.dense(dv.values)
//  }
  def main(args: Array[String]) = {
    setLogger()
    val spark = SparkSession
      .builder().master("local[8]").appName("Spark").getOrCreate()
    import spark.implicits._

    val configuration = scala.io.Source.fromFile("./configuration.config").getLines().mkString
    val json = JSON.parseFull(configuration)
    val map : Map[String, Option[Any]] = json.get.asInstanceOf[Map[String, Option[Any]]];
    var pathPrefix = map.get("path").get.asInstanceOf[String]
    Class = map.get("Class").get.asInstanceOf[List[String]]
    numberOfSample = map.get("numberOfSample").get.asInstanceOf[Double].toInt
    numberOfFeature = map.get("numberOfFeature").get.asInstanceOf[Double].toInt


    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

   // val stream = ssc.socketTextStream("localhost", 9999)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("twitter"))

    var path = Class.map(pathPrefix + _)
    getDirectoryInfo(path)

    val trainClass = path(0)
      var df = spark.read.textFile(trainClass).toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
      df = df.limit(numberOfSample)
      var Array(trainingSet, testSet) = df.randomSplit(Array(0.8, 0.2))
      for (i <- 1 until path.length) {
          val labeled = i.toDouble
          var dataDF = spark.read.textFile(path(i)).toDF("sentence").withColumn("label", when($"sentence".isNotNull, labeled))
          dataDF = dataDF.limit(numberOfSample)
          val Array(training, test) = dataDF.randomSplit(Array(0.8, 0.2))
          trainingSet = trainingSet.union(training)
          testSet = testSet.union(test)
      }
//      val model = SVM.train(trainingSet ,testSet,ssc)
    val model = train(trainingSet ,testSet)

    val twitterData = Twitter.twitterData
    var twitterTest = spark.createDataFrame(twitterData).toDF("label", "sentence")
   
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

  val twitterPre = model.transform(twitterTest)
  val twitterAcc = evaluator.evaluate(twitterPre)
  println("Twitter.scala Accuracy:"+twitterAcc)



//    for(j <- 0 until path.length) {
//      val trainClass = path(j)
//      var df = spark.read.textFile(trainClass).toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
//      df = df.limit(numberOfSample)
//      var Array(trainingSet, testSet) = df.randomSplit(Array(0.8, 0.2))
//      println("train:"+Class(j))
//      for (i <- 0 until path.length) {
//        if(i!=j) {
//          val labeled = 1.0
//          var dataDF = spark.read.textFile(path(i)).toDF("sentence").withColumn("label", when($"sentence".isNotNull, labeled))
//          dataDF = dataDF.limit(numberOfSample / (path.length - 1))
//          println(dataDF.count() + " " + Class(i))
//          val Array(training, test) = dataDF.randomSplit(Array(0.8, 0.2))
//          trainingSet = trainingSet.union(training)
//          testSet = testSet.union(test)
//        }
//      }
//      SVM.train(trainingSet,testSet)
//    }



  println("training:"+trainingSet.count()+"  testing:"+testSet.count())
//    var trainingSet = spark.read.format("libsvm").load("/Users/Peter/AizuLab/s1220150/japan-times-feature-vector-scala/data/sample/multi-class-train-set")
//    var testSet = spark.read.format("libsvm").load("/Users/Peter/AizuLab/s1220150/japan-times-feature-vector-scala/data/sample/multi-class-test-set")



//    val model = train(trainingSet,testSet)
//    println("\nStart Reading Stream Text")
 val prouderProps = new HashMap[String, Object]()
    prouderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prouderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prouderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    
//    var producer = getProducer()
    kafkaStream.map(_._2).foreachRDD {
        rdd =>
          if(!rdd.isEmpty()) {
            val streamDF = spark.read.json(rdd)//.withColumn("label", when($"sentence".isNotNull, 0.0))
            //val streamDF = rdd.map(_._2).toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
            val prediction = model.transform(streamDF).select("prediction","id","original")
            /*
            prediction.toJSON.foreach{ data =>
             // println(data)
                  val message = new ProducerRecord[String, String]("result", null, data.toString())
                  producer.send(message)
            }
            */
            //prediction.show()
            prediction.toJSON.foreachPartition((partisions: Iterator[String]) => {
                val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prouderProps)
                partisions.foreach((line: String) => {
                  try {
                  producer.send(new ProducerRecord[String, String]("result", line))
                  } catch {
                  case ex: Exception => {
                    println(ex)
                  }
                  }
                  })
                })

            //probability
          }
    }

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
    ssc.start()
    ssc.awaitTermination()
  }

}
