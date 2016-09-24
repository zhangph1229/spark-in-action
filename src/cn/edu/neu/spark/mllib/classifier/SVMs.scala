package cn.edu.neu.spark.mllib.classifier

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.optimization.L1Updater

object SVMs {
  def main(args: Array[String]): Unit = {
    if(args.length < 0){
      println("loss args");
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("SVM").setMaster("spark://ubuntu1:7077");
    val sc = new SparkContext(conf);
    //sc.addJar(System.getProperty("user.dir")+"/cn/edu/neu/spark/mllib/classifier/svms.jar");
    
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "hdfs://ubuntu1:9000/user/zhangph/input/sample_libsvm_data.txt")
    
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    
    // Run training algorithm to build the model
    val numIterations = 5  //迭代次数
    val model = SVMWithSGD.train(training, numIterations)
    
    //配置正则化项，使其成为L1范数，L1范数会使w得分量尽量稀疏，即非零分量个数尽量少。使结果更容易的到稀疏解释。
//    val svmAlg = new SVMWithSGD()
//    svmAlg.optimizer
//      .setNumIterations(200)
//      .setRegParam(0.1)
//      .setUpdater(new L1Updater)
//    val modelL1 = svmAlg.run(training)
    
    // Clear the default threshold.
    model.clearThreshold()
    
    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    
    println("Area under ROC = " + auROC)
   
    // Save and load model
//    model.save(sc, "hdfs://ubuntu1:9000/user/zhangph/output/svm")
//    val sameModel = SVMModel.load(sc, "hdfs://ubuntu1:9000/user/zhangph/output/svm")
  }
}