package HighLevel
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
object SQLwithCSV {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("SQLCSV")
      .config("spark.driver.memory","4g")
      .config("spark.executor.memory","8g")
      .getOrCreate()

    val sc = spark.sparkContext

    val dfFile = spark.read
      .option("header","true")        // first line is taken as the header
      .option("sep",";")              // signify separator
      .option("inferSchema","true")   // for creating schema  - If you don't write this option, all columns indicated string
      .csv("./src/main/scala/OnlineRetail.csv")

    // File caching after first action function so second action function runs faster than first func.
    dfFile.cache()

    // Create Temporary SQL Table from csv
    dfFile.createOrReplaceTempView("table_csv")

    // SQL Query
    println("First Action Function")
    spark.sql(
      """
       SELECT * FROM table_csv
       """).show(5)

    println("Second Action Function")
    spark.sql(
      """
       SELECT Country,SUM(Quantity) AS sumQ FROM table_csv
        GROUP BY Country
        ORDER BY sumQ DESC
       """).show(5)

  }
}
