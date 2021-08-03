package LowLevel.PairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Filter {
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

    println("Pair LowLevel.RDD filter() Example\t=>\n")

    val retailRDDWithHeader = sc.textFile("./src/main/scala/OnlineRetail.csv")
    println("OnlineRetail.csv")
    retailRDDWithHeader.take(5).foreach(println)

    // Remove Header
    val retailRDD = retailRDDWithHeader.mapPartitionsWithIndex((index,line) => if (index==0) line.drop(1) else line)

    // Quantity > 30
    println("\nQuantity > 30\n")
    retailRDD.filter( x => x.split(";")(3).toInt > 30).take(5).foreach(println)

    // Description contains COFFEE and UnitPrice>20
    println("\nDescription contains COFFEE and UnitPrice>20\n")
    retailRDD.filter(line =>
      line.split(";")(2).contains("COFFEE")
      && line.split(";")(5).trim.replace(",",".").toFloat > 20
    ).take(10).foreach(println)

    // to method
    def coffeePrice20(line:String):Boolean={

      var coffee = line.split(";")(2)
      var unitPrice = line.split(";")(5).trim.replace(",",".").toFloat

      coffee.contains("COFFEE") && unitPrice > 20.0
    }

    println("\nDescription contains COFFEE and UnitPrice>20 with Method\n")
    retailRDD.filter(x => coffeePrice20(x)).take(5).foreach(println)

  }
}




































