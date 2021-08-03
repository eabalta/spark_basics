package LowLevel.PairRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object PairRDDOps {
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

    // Find average salary foreach job

    val rdd = sc.textFile("./src/main/scala/simple_data.csv").filter(!_.contains("sirano"))
    println("Dataset")
    rdd.take(5).foreach(println)

    val rdd2 = rdd.map(x => (x.split(",")(3),x.split(",")(5).toInt))
    println("\nSelect Job and Salary")
    rdd2.take(5).foreach(println)

    val rdd3 = rdd2.mapValues(x=>(x,1))
    println("\nAdd Number next to Salary")
    rdd3.take(5).foreach(println)

    val rdd4 = rdd3.reduceByKey((x,y) => (x._1 + y._1,x._2+y._2))
    println("\nSum number of jobs and total salary")
    rdd4.take(5).foreach(println)

    val rdd5 = rdd4.mapValues(x => x._1 / x._2)
    println("\nResult")
    rdd5.take(10).foreach(println)

  }
}
