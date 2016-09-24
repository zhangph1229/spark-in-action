package cn.edu.neu.spark.chapter5

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassificationModel

object ClassificationPipeline {
  def main(args: Array[String]): Unit = {
    if(args.length < 0){
      println("Usage:ClassificationPipleline Parameters: ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Random Forest").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("file://F:/program/java/spark/jar/rf.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //step 1
    val parsedRDD = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/banknote.txt")
        .map(_.split(",")).map(row =>{
          val a = row.map(_.toDouble)
          (a(0),a(1),a(2),a(3),a(4))
        });
    val df = sqlContext.createDataFrame(parsedRDD).toDF("f0","f1","f2","f3","label").cache()
    
    //step 2
    val labelIndex = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df);
    
    //step 3
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0","f1","f2","f3"))
      .setOutputCol("featureVector");
    
    //step 4
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setNumTrees(5);
      
   //step 5
   val labelConvert = new IndexToString()
     .setInputCol("prediction")
     .setOutputCol("predictedLabel")
     .setLabels(labelIndex.labels);
   
   //step 6
   val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2))
   
   //step 7
   val pipeline = new Pipeline().setStages(Array(labelIndex,vectorAssembler,rfClassifier,labelConvert))
   val model = pipeline.fit(trainData)
   
   //step 8
   val predictResDF = model.transform(testData)
   
   //step 9
   predictResDF.select("f0","f1","f2","f3", "label","predictedLabel").show(20)
   
   //step 10
   val evaluator = new MulticlassClassificationEvaluator()
     .setLabelCol("label")
     .setPredictionCol("prediction")
     .setMetricName("precision");
   val predictAccuracy = evaluator.evaluate(predictResDF)
   println("Testing Error = " + (1.0 - predictAccuracy))
   
   //step 11
   val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
   println("Trained RFM is : \n" + rfModel.toString())
   
//   sc.stop()        
  }
}