package LowLevelDSV

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Accumulators {
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

    val onlineRetailRDD = sc.textFile("./src/main/scala/OnlineRetail.csv").filter(!_.contains("InvoiceNo"))
    println("OnlineRetail.csv\n")
    onlineRetailRDD.take(5).foreach(println)

    // Create Accumulator for UK
    val accUK = sc.longAccumulator


    val total = onlineRetailRDD.map(line => {
      if(line.split(";")(7).contains("United Kingdom")){
        accUK.add(1L)
      }
      accUK.value
    })

    println("\nNumber of UK: ")
    println(total.max())
  }
}
