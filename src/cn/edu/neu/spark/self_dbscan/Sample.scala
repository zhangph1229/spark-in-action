package cn.edu.neu.spark.self_dbscan

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sun.rmi.runtime.Log.LogFactory
import org.slf4j.LoggerFactory

object Sample {
  val log = LoggerFactory.getLogger(Sample.getClass)
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("You must pass the arguments: <input file> <output file> <max points> <eps> <minPoints> <master> <other jar>")
      println("master and other jar are additional parameters")
      System.exit(1)
    }
    val (src, dest, maxPointsPerPartition, eps, minPts, master, jar) =
      (args(0), args(1), args(2).toInt, args(3).toFloat, args(4).toInt, args(5), args(6))

    val destOut = dest.split('/').last

    val conf = new SparkConf().setAppName("spark on dbscan").setMaster("spark://ubuntu1:7077");
    val sc = new SparkContext(conf)
    sc.addJar(jar);
    val data = sc.textFile(src).map(_.split(",")).map(p => (p(0).trim.toDouble, p(1).trim.toDouble)).zipWithUniqueId().map(x => (x._2, x._1))
    val cluster: Dbscan = new Dbscan(eps, minPts, data)
    val pre = cluster.predict((data.count + 1,(39.9,116.00)));
    println(pre)
  }
}