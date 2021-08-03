package LowLevel.RDD

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkContext yaratmanin iki yolu bulunmaktadir.
    /*

        SparkConf ve SparkContext - Eski

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CreateRDD")
      .setExecutorEnv("spark.driver.memory","2g")
      .setExecutorEnv("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

     */

    val spark = SparkSession.builder()
      .master("local")
      .appName("CreateRDD")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    /*
        SparkConf kullanarak SparkSession olusturma
     val spark = SparkSession.builder
            .config(conf)
            .getOrCreate()
          val sc = spark.sparkContext
     */

    // SparkConf Bilgileri
    println("SparkCof bilgileri: ")
    sc.getConf.getAll.foreach(println)
    println("Executor memory bilgileri: ")
    sc.getExecutorMemoryStatus.foreach(println)


    // Scala Collection ile LowLevel.RDD Olusturma islemleri
    // LowLevel.RDD iki farkli metot ile olusturulabilir.
    // parallelize
    // makeRDD

    println("\nScala Seq")
    val rddByParallelizeCollection = sc.parallelize(Seq(1,2,3,4,5,6,7,8)) // org.apache.spark.rdd.LowLevel.RDD[Int]  ParallelCollectionRDD[0]
    rddByParallelizeCollection.take(3).foreach(println)

    println("\nScala Tuple")
    val rddByParallelizeCollection2 = sc.makeRDD(Seq((1,2),(3,4),(5,6),(7,8))) //  org.apache.spark.rdd.LowLevel.RDD[(Int, Int)]  ParallelCollectionRDD[1]
    rddByParallelizeCollection2.take(3).foreach(println)

    println("\nScala List")
    val rddByParallelizeCollection3 = sc.parallelize(List(10,20,30,40,50,60,70,80)) // org.apache.spark.rdd.LowLevel.RDD[Int]  ParallelCollectionRDD[2]
    rddByParallelizeCollection3.take(3).foreach(println)

    println("\nsparkContext range metodu kullanarak")
    // 10.000 ile 20.000 arasında 100'er artan sayılardan LowLevel.RDD oluştur
    val rddByParallelizeCollection4 = sc.range(10000L, 20000L,100) //  org.apache.spark.rdd.LowLevel.RDD[Long]  MapPartitionsRDD[4]
    rddByParallelizeCollection4.take(3).foreach(println)

    println("\nsql.Row lardan LowLevel.RDD yaratmak")
    val abc = org.apache.spark.sql.Row((1,2),(3,4),(5,6),(7,8)) //  org.apache.spark.sql.Row
    val rddByRows = sc.makeRDD(List(abc)) // org.apache.spark.rdd.LowLevel.RDD[org.apache.spark.sql.Row] = ParallelCollectionRDD[5]
    rddByRows.take(3).foreach(println)

    // Metin dosyasi ile de LowLevel.RDD olusturulabilir.
    println("\ntextFile")
    val rddFromTextFile = sc.textFile("./src/main/scala/LowLevelRDD/omer_seyfettin_forsa_hikaye.txt")
    rddFromTextFile.take(4).foreach(println)
    println("Okunan satir sayisi : " + rddFromTextFile.count())

  }
}
