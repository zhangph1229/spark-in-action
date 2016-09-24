package cn.edu.neu.spark.scala

object Review {
  //1. tally votes
		val votes = List(("Scala",10),("Java", 20),("Python", 12), ("Java", 2), ("Scala", 3), ("Python", 5))
    val groupVotes = votes.groupBy{case(key, _) => key}
    val votesCount = groupVotes.map{case(key, value) =>
    	val valueOnly = value.map{case(_, v) => v}
    	(key , valueOnly.sum)
    }                                            
    val sortVotes = votesCount.toSeq.sortBy{case(_, v) => v}.reverse                                          
                   
		
	  //2. Expr
		trait Expr
		case class Number(num : Int) extends Expr{}
		case class Sum(e1 : Expr, e2 : Expr) extends Expr{}
		
		def eval(e : Expr)  : Int = e match {
			case num : Number => num.num
			case Sum(e1, e2) => eval(e1) + eval(e2)
		}
		
		//3. Factorial
		def fac1(num : Int) : Int = num match {
			case 0 => 1
			case num => num * fac1(num - 1)
		}                              
		def fac2(num : Int) : Int = (1 to num).reduceRight(_*_)
		                         
		
		//4. Quick Sort
		def qsort(list : List[Int]) : List[Int] = list match{
			case Nil => Nil
			case pivot :: tail => {
				val (smaller, larger) = tail.partition(_ < pivot)
				qsort(smaller) ::: pivot :: qsort(larger)
			}
		}                                 
    
    //5. Fibonacci Number
    def fibs : Stream[Int]= 0 #:: 1 #:: fibs.zip(fibs.tail).map(item => item._1 + item._2)                                               
		
		//6. myWhile
		def myWhile(condition : => Boolean)(body : => Unit) {
			if(condition){ body; myWhile(condition)(body)}
		}

		def main(args : Array[String]) : Unit = {
		  //1. Tally Votes
			println("----1----")
			votesCount.foreach(println _)
		  sortVotes.foreach(println)  
		  
		  println("----2----")
		  //2. Expr
		  eval(Sum(Number(2), Number(3)))
		  println(eval(Sum(Number(1), Sum(Number(2), Number(3)))))
		  
		  println("----3----")
		  //3. Factorial
		  println(fac1(10))
		  println(fac2(10))   
		  
		  println("----4----")
		  //4. quick sort
		  qsort(List(3,4,1,2,9,10,8)).foreach(value => print(" " + value))
		  println
		  
		  println("----5----")
		  //5. Fibonacci Number
		  fibs(9)                                       
		  fibs.take(10).foreach(value => print(value + " ")) 
		  println
		  
		  println("----6----")
      //6. myWhile
      var n: Int = 4
      myWhile(n > 0) {
        n = n - 1
        print(n + " ")
      }
		}
}