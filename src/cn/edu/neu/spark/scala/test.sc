package cn.edu.neu.spark.scala

object test {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
	
	val votes = List(("Scala", 10), ("Java", 20),("Pyton", 3),("Java", 20),("Scala", 2))
                                                  //> votes  : List[(String, Int)] = List((Scala,10), (Java,20), (Pyton,3), (Java,
                                                  //| 20), (Scala,2))
  val groupVotes = votes.groupBy{case(key, _) => key}
                                                  //> groupVotes  : scala.collection.immutable.Map[String,List[(String, Int)]] = M
                                                  //| ap(Scala -> List((Scala,10), (Scala,2)), Pyton -> List((Pyton,3)), Java -> L
                                                  //| ist((Java,20), (Java,20)))
  val sumVotes = groupVotes.map{case(key, value) =>
  	val values = value.map{case(_,v) => v}
  	(key, values.sum) }                       //> sumVotes  : scala.collection.immutable.Map[String,Int] = Map(Scala -> 12, Py
                                                  //| ton -> 3, Java -> 40)
  sumVotes.toSeq.sortBy{case(_, value) => value}.reverse
                                                  //> res0: Seq[(String, Int)] = ArrayBuffer((Java,40), (Scala,12), (Pyton,3))
  
  
  
	
	  
}