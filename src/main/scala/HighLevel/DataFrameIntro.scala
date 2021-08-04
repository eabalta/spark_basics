package HighLevel

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession

object DataFrameIntro {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("IntroductionToDataFrame")
      .config("spark.driver.memory","4g")
      .config("spark.executor.memory","8g")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create DataFrame

    // For necessary to using toDF function
    import spark.implicits._

    // From List
    val dfList = sc.parallelize(List(1,2,3)).toDF()
    dfList.printSchema()
    dfList.show(5)

    // with range function
    val dfRange = spark.range(10,50,2).toDF("numbers")
    dfRange.printSchema()
    dfRange.show(5)

    // from file
    val dfFile = spark.read.format("csv") // or read.option()....csv(path)
      .option("header","true")        // first line is taken as the header
      .option("sep",";")              // signify separator
      .option("inferSchema","true")   // for creating schema  - If you don't write this option, all columns indicated string
      .load("./src/main/scala/OnlineRetail.csv")

    dfFile.printSchema()
    dfFile.take(5).foreach(println) // show alternative
    dfFile.show(5)

    // count Action
    println("OnlineRetail.csv row sie : " + dfFile.count())

    // select Transformation & show Action
    dfFile.select("InvoiceNo","UnitPrice").show(5)

    // Print Spark Plan with explain()
    dfFile.sort("UnitPrice").explain()

    // Dynamic config to change shuffle partition size
    spark.conf.set("spark.sql.shuffle.partitions","5")

    // Transform & Action with new conf

    dfFile.select("Description","Quantity")
      .sort("Quantity")
      .show(5)

  }
}
