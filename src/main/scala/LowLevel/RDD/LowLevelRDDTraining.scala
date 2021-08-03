package LowLevel.RDD

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession

object LowLevelRDDTraining {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.executor.memory","3g")
      .config("spark.driver.memory","2g")
      .appName("LowLevelRDDTraining")
      .getOrCreate()

    val sc = spark.sparkContext

    val RDD = sc.parallelize(List(3,7,13,15,22,36,7,11,3,25))

    val text = "Enes Alper Balta"
    text.map(_.toUpper)

    var textFile = sc.textFile("./src/main/scala/LowLevelRDD/Ubuntu_Spark_Kurulumu.txt")
    println("count :")
    println(textFile.count())
    println()
    textFile = textFile.flatMap( x => x.split(" ")).map(_.replaceAll("[ “,.!?’–:;'…-]","").trim.toLowerCase()).filter(_.nonEmpty)
    println("word count : ")
    textFile.map((_,1)).reduceByKey(_ + _).map(x=>(x._2,x._1)).sortByKey(ascending = false).take(15).foreach(println)
    println()

    println("intersection : ")
    RDD.intersection(sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))).foreach(println)
    println()

    println("distinct : ")
    val newRDD = RDD.distinct()
    newRDD.foreach(println)
    println()

    println("countByValue : ")
    RDD.countByValue().foreach(println)

  }
}
