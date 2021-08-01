package PairRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object Map {
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

    println("Pair RDD map() Example\t=>\n")

    val retailRDD = sc.textFile("./src/main/scala/PairRDD/OnlineRetail.csv").filter(!_.contains("UnitPrice"))

    // find the sum of the charges for canceled sales

    // Find cancelled sales
    // If InvoiceNo starts with 'C' then sale is cancelled
    val cancelledSales = retailRDD.filter(x => x.startsWith("C"))

    // Take Quantity (index 3) and take UnitPrice (index 5) then multiply them, then sum all of them
    println(cancelledSales.map(x => {
      x.split(";")(3).toInt * x.split(";")(5).replace(",",".").toDouble
    }).sum())

  }
}
