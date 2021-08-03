package LowLevel.RDD

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession

object RDDBasicTransformations {
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

    // One LowLevel.RDD
    println("One LowLevel.RDD Transmissions\t=>")

    val oneRDD = sc.range(0L,15L,3)

    // map
    println("map( x => x * x) : ")
    oneRDD.map(x => x * x).take(10).foreach(println)
    println()

    // filter
    println("filter(x => x % 2 == 0) : ")
    oneRDD.filter( x => x % 2 == 0).take(10).foreach(println)
    println()

    // flatMap
    println("flatMap(_.toUpperCase()) : ")
    val strRDD = sc.parallelize(List("Enes Alper Balta","Alper Balta","Balta","Balta"))
    strRDD.flatMap(_.toUpperCase()).take(10).foreach(println)
    println()

    println("flatMap(x => x.split(\" \")).map(x => x.toUpperCase()) : ")
    strRDD.flatMap(x => x.split(" ")).map(x => x.toUpperCase()).take(10).foreach(println)
    println()

    // distinct
    println("distinct : ")
    strRDD.distinct().take(5).foreach(println)
    println()

    // Two LowLevel.RDD
    println("Two LowLevel.RDD Transmissions\t=>")

    val twoRDD = sc.range(0L,18L,2)

    // union
    println("union : ")
    oneRDD.union(twoRDD).take(25).foreach(println)
    println()

    // intersection
    println("intersection : ")
    oneRDD.intersection(twoRDD).take(25).foreach(println)
    println()

    // substract
    println("oneRDD substract twoRDD : ")
    oneRDD.subtract(twoRDD).take(25).foreach(println)
    println()

    // cartesian
    println("cartesian product : ")
    oneRDD.cartesian(twoRDD).take(15).foreach(println)
    println()
  }
}
