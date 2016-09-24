package cn.edu.neu.spark.practice

import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.SQLContext

object ClassifierPipeline {
  def main(args: Array[String]): Unit = {
    if(args.length < 0){
      println("Usage:ClassifierPipeline Parameters")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RF Practice").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //step 1 source data --> DataFrame
    val sourceData = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/banknote.txt", 4)
    val parsedTrainData = sourceData.map(_.split(",")).map(line => {
      val tmp = line.map(_.toDouble)
      (tmp(0),tmp(1),tmp(2),tmp(3),tmp(4))
    })
    val df = sqlContext.createDataFrame(parsedTrainData).toDF("f0","f1","f2","f3","label").cache()
    
    //step2 StringIndexer
    val labelIndex = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)
    
    //step 3 提取特征指标数据
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0","f1","f2","f3"))
      .setOutputCol("feature")
   
    //step 4 Create RF
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("feature")
      .setNumTrees(5)
    
    //step 5 Convert label
    val labelConvert = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndex.labels)
    
    //step 6 split data
    val Array(trainData, testData) = df.randomSplit(Array(0.8,0.2))
    
    //step 7 ML Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndex,vectorAssembler,rfClassifier, labelConvert))
    val model = pipeline.fit(trainData)
    
    //step 8 predict test data
    val predict = model.transform(testData)
    
    //step 9 show 
    predict.select("f0","f1","f2","f3","label","predictedLabel").show(20)
    
    //step 10 evaluator
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("predictedLabel")
      .setMetricName("precision")
    val predictionAccuracy = eval.evaluate(predict)
    println("Testing Error = " + (1.0 - predictionAccuracy))
    
    //step 11 save or print model
    val rfm = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    model.save("hdfs://ubuntu1:9000/user/zhangph/output/rfm")
    println("rfm : \n" + rfm.toString())
  }
}