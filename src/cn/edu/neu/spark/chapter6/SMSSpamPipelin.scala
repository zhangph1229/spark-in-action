package cn.edu.neu.spark.chapter6

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object SMSSpamPipelin {
  final val VECTOR_SIZE = 100
  def main(args: Array[String]): Unit = {
    if(args.length < 0){
      println("Usage:SMS Spam Pipeline Parameter")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SMS Spam").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
//    sc.addJar("file://F:/program/java/spark/jar/sms.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //step 1 file --> DataFrame
    val sourceData = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/SMSSpamCollection")
      .map(_.split("\t")).map(line => {
        (line(0), line(1).split(" "))
      })
    val df = sqlContext.createDataFrame(sourceData).toDF("label","message").cache()
    
    //step 2 label: String --> index
    val labelIndex = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df);
    
    //step 3 Word2Vec
    val word2Vec = new Word2Vec()
      .setInputCol("message")
      .setOutputCol("features")
      .setVectorSize(VECTOR_SIZE)
      .setMinCount(1);
    
    //step 4 create object
    val layers = Array[Int](VECTOR_SIZE,6,5,2)
    val mlpc = new MultilayerPerceptronClassifier()
      .setLayers(layers) //Array()
      .setBlockSize(64) //128
      .setSeed(1234L)
      .setMaxIter(10)  //100
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
    
    //step 5 label:index --> String
    val backString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndex.labels);
    
    //step 6 split data 
    val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2))
    
    //step 7 train --> model
    val pipeline = new Pipeline()
      .setStages(Array(labelIndex,word2Vec,mlpc,backString))
    val model = pipeline.fit(trainData)
    
    //step 8 model --> test
    val predictTestData = model.transform(testData)
    predictTestData.printSchema()
    predictTestData.select("message","label","predictedLabel").show(30)
    
    //step 9 evaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val predictAccuracy = evaluator.evaluate(predictTestData)
    println("Tseting Accuracy is %2.4f".format(predictAccuracy * 100) + "%")
  }
}