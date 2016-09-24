package cn.edu.neu.spark.scala

object review {
				
		
		//test time
		
		//1. tally votes
		val votes = List(("Scala",10),("Java", 20),("Python", 12), ("Java", 2), ("Scala", 3), ("Python", 5))
                                                  //> votes  : List[(String, Int)] = List((Scala,10), (Java,20), (Python,12), (Jav
                                                  //| a,2), (Scala,3), (Python,5))
    val groupVotes = votes.groupBy{case(key, _) => key}
                                                  //> groupVotes  : scala.collection.immutable.Map[String,List[(String, Int)]] = M
                                                  //| ap(Scala -> List((Scala,10), (Scala,3)), Java -> List((Java,20), (Java,2)), 
                                                  //| Python -> List((Python,12), (Python,5)))
    val votesCount = groupVotes.map{case(key, value) =>
    	val valueOnly = value.map{case(_, v) => v}
    	(key , valueOnly.sum)
    }                                             //> votesCount  : scala.collection.immutable.Map[String,Int] = Map(Scala -> 13, 
                                                  //| Java -> 22, Python -> 17)
    votesCount.foreach(println _)                 //> (Scala,13)
                                                  //| (Java,22)
                                                  //| (Python,17)
    val sortVotes = votesCount.toSeq.sortBy{case(_, v) => v}
                                                  //> sortVotes  : Seq[(String, Int)] = ArrayBuffer((Scala,13), (Python,17), (Java
                                                  //| ,22))
    sortVotes.foreach(println)                    //> (Scala,13)
                                                  //| (Python,17)
                                                  //| (Java,22)
		
		/*
	  //2. Expr
		trait Expr
		case class Number(num : Int) extends Expr{}
		case class Sum(e1 : Expr, e2 : Expr) extends Expr{}
		
		def eval(e : Expr)  : Int = e match {
			case num : Number => num.num
			case Sum(e1, e2) => eval(e1) + eval(e2)
		}
		eval(Sum(Number(2), Number(3)))
		println(eval(Sum(Number(1), Sum(Number(2), Number(3)))))
		*/
		
		//3. Factorial
		def fac1(num : Int) : Int = num match {
			case 0 => 1
			case num => num * fac1(num - 1)
		}                                 //> fac1: (num: Int)Int
		fac1(10)                          //> res0: Int = 3628800
		
		def fac2(num : Int) : Int = (1 to num).reduceRight(_*_)
                                                  //> fac2: (num: Int)Int
		fac2(10)                          //> res1: Int = 3628800
		
		//4. Quick Sort
		def qsort(list : List[Int]) : List[Int] = list match{
			case Nil => Nil
			case pivot :: tail => {
				val (smaller, larger) = tail.partition(_ < pivot)
				qsort(smaller) ::: pivot :: qsort(larger)
			}
		}                                 //> qsort: (list: List[Int])List[Int]
		qsort(List(3,4,1,2,9,10,8)).foreach(println)
                                                  //> 1
                                                  //| 2
                                                  //| 3
                                                  //| 4
                                                  //| 8
                                                  //| 9
                                                  //| 10
    
    //5. Fibonacci Number
    def fibs : Stream[Int]= 0 #:: 1 #:: fibs.zip(fibs.tail).map(item => item._1 + item._2)
                                                  //> fibs: => Stream[Int]
    fibs(9)                                       //> res2: Int = 34
		fibs.take(10).foreach(println)    //> 0
                                                  //| 1
                                                  //| 1
                                                  //| 2
                                                  //| 3
                                                  //| 5
                                                  //| 8
                                                  //| 13
                                                  //| 21
                                                  //| 34
		
		/*
		//6. myWhile
		def myWhile(condition : => Boolean)(body : => Unit) {
			if(condition){ body; myWhile(condition)(body)}
		}
		var n : Int = 4
		myWhile(n > 0){
			n = n - 1
			println(n)
		}
		*/
		
		
		
		/*

			//values
			val value : Double = 5
			val tuple = ("jams", "kobe", "cp3", 666)
			val list = List("zhangsan","lisi","wangwu")
		 	var x = 3
		 	x = 5
		 	
		 	//Functions
		 def wordTo(num : Int) : String = num match{
		 	case 0 => "jams"
		 	case 1 => "kobe"
		 	case 2 => "cp3"
		 	case _ => "no"
		 }
		 wordTo(1)
		 
		 val increase : PartialFunction[Any, Any] = {
		 	case x : Int => x + 1
		 }
		 increase(1)
		 val increase1 : PartialFunction[Any, Any] = increase.orElse{
		 	case x : Double => x + 2
		 }
		 increase1(2.0)
		// def mywhile(condition : => Boolean)(body : => Unit) : Unit =
		//   	if(condition) {body;mywhile(condition)(body)	}
		// var n = 5
		// mywhile(n > 0){
		// 	n = n - 1
		// 	println(n)
		// }
		
		//fib
		def fibs : Stream[Int] = 0 #:: 1 #:: fibs.zip(fibs.tail).map(item => item._1 + item._2)
    fibs.take(10).foreach(println)
    def qsort(list : List[Int]) : List[Int] = list match{
    	case Nil => Nil
    	case head :: tail => {
    		val (smaller , higher) = tail.partition(_ < head)
    		qsort(smaller) ::: head :: qsort(higher)
    	}
    }
    //val list = List(3,2,1,4,5,3)
		qsort(List(3,4,5,1,2))
*/
}