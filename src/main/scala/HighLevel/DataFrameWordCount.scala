package HighLevel
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
object DataFrameWordCount {
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

    val fileDataset  = spark.read.textFile("./src/main/scala/omer_seyfettin_forsa_hikaye.txt")

    val fileDataFrame = fileDataset.toDF("values")

    fileDataFrame.show(5,truncate = false)

    val words = fileDataset.flatMap(x => x.split(" "))

    println("Number of words : " + words.count())
    words.show(5)

    // 1

    words.groupBy("value")
      .count()
      .orderBy($"count".desc)
      .show(10)


    // 2
    import org.apache.spark.sql.functions.count
    words.groupBy("value")                              // select the column to be grouped
      .agg(count("value").as("wordFreq"))   // groupping process
      .orderBy($"wordFreq".desc).show(10)           // sorting and show
  }
}
