package cn.edu.neu.spark.dbscan

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.dbscan.DBSCAN

object DBSCANSample {

  val log = LoggerFactory.getLogger(DBSCANSample.getClass)

  def main(args: Array[String]) {

    if (args.length < 6) {
      System.err.println("You must pass the arguments: <src file> <dest file> <parallelism>")
      System.exit(1)
    }

    val (src, dest, maxPointsPerPartition, eps, minPoints, master, jar) =
      (args(0), args(1), args(2).toInt, args(3).toFloat, args(4).toInt, args(5), args(6))

    val destOut = dest.split('/').last

    val conf = new SparkConf().setAppName(s"DBSCAN(eps=$eps, min=$minPoints, max=$maxPointsPerPartition) -> $destOut")
        .setMaster(master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.storage.memoryFraction", "0.1")
    val sc = new SparkContext(conf)
    sc.addJar(jar)
    
    val data = sc.textFile(src)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    log.info(s"EPS: $eps minPoints: $minPoints")

    val model = DBSCAN.train(
      parsedData,
      eps = eps,
      minPoints = minPoints,
      maxPointsPerPartition = maxPointsPerPartition)

    model.labeledPoints.map(p => s"${p.x},${p.y},${p.cluster}").saveAsTextFile(dest)
    log.info("Stopping Spark Context...")
    sc.stop()

  }
}
