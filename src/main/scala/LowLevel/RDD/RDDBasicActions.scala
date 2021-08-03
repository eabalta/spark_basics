package LowLevel.RDD

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession

object RDDBasicActions {
  def main(args: Array[String]): Unit = {
    // Log Config
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Start SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("RDDBasicTransformations")
      .config("spark.executor.memory","8g")
      .config("spark.driver.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext


    val RDD = sc.range(0L,15L,3)

    // collect
    println("collect : ")
    val collected = RDD.collect()
    collected.foreach(println)
    println()

    // count
    println("count : ")
    println(RDD.count())
    println()

    // count
    println("countByValue : ")
    println(RDD.countByValue())
    println()

    // take
    println("take(2) : ")
    RDD.take(2).foreach(println)
    println()

    // top
    println("top(2) : ")
    RDD.top(2).foreach(println)
    println()

    // takeOrdered
    println("takeOrdered : ")
    RDD.takeOrdered(5).foreach(println)
    println()

    // takeSample
    println("takeSample : ")
    RDD.takeSample(withReplacement = false,3).foreach(println)
    println()

    // reduce
    println("reduce : ")
    println(RDD.reduce((x,y) => x+y))

  }
}
