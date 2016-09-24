package cn.edu.neu.spark.other_dbscan

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.dbscan.DBSCAN

object DBSCANSample {
  val log = LoggerFactory.getLogger(DBSCANSample.getClass)
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("You must pass the arguments: <input file> <output file> <max points> <eps> <minPoints> <master> <other jar>")
      println("master and other jar are additional parameters")
      System.exit(1)
    }
    /**
     * parameter example:
    	hdfs://ubuntu1:9000/user/zhangph/input/dbscan.txt 
    	hdfs://ubuntu1:9000/user/zhangph/output/dbscan 
    	1000 50 3 
    	spark://ubuntu1:7077 
    	file://F:/program/java/spark/jar/dbscan.jar
     * */
    val (src, dest, maxPointsPerPartition, eps, minPoints, master, jar) =
      (args(0), args(1), args(2).toInt, args(3).toFloat, args(4).toInt, args(5), args(6))

    val destOut = dest.split('/').last

    val conf = new SparkConf().setAppName(s"DBSCAN(eps=$eps, min=$minPoints, max=$maxPointsPerPartition) -> $destOut")
      .setMaster(master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.set("spark.storage.memoryFraction", "0.1")
    val sc = new SparkContext(conf)
    sc.addJar(jar)

    val data = sc.textFile(src,120)
    //    val parsedData = data.ma                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  p(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val parsedData = data.map(_.split(","))
      .map(line => line(2) + "," + line(3))
      .map(s =>
        Vectors.dense(s.split(',').map(_.toDouble / 100000)))

    log.info("Trajectory data deal with successsfully.")
    log.info(s"EPS: $eps minPoints: $minPoints")

    val model = DBSCAN.train(
      parsedData,
      eps = eps,
      minPoints = minPoints,
      maxPointsPerPartition = maxPointsPerPartition)

    val res = model.labeledPoints.map(p => s"${p.x},${p.y},${p.cluster}")
   
    val raw = data.map(_.split(",")).map(a => (a(2).toDouble/100000+","+a(3).toDouble/100000,a(0)+","+a(1)+","+a(4)+","+a(5)+","+a(6)))
    val res_s = res.map(_.split(",")).map(a => (a(0)+","+a(1),a(2)))
    val res_l = raw.join(res_s).map{case(a,b)=>(b,a)}
    
    //for debug 
    println("show the cluster info top 10")
    res.take(10).foreach(println)
//    res.saveAsTextFile(dest)
    raw.take(10).foreach(println)
    log.info("-------------raw------------------------")
    res_s.take(10).foreach(println)
    log.info("-------------res_s----------------------")
    res_l.take(100).foreach(println)
    log.info("+++++++++++++final res++++++++++++++++++")
    Thread.sleep(5000)
    
    //save as text file to hdfs or other
//    res_l.saveAsTextFile(dest)
    
    log.info("Stopping Spark Context...")
    sc.stop()
  }
}