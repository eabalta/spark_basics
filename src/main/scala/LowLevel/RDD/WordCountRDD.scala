package LowLevel.RDD

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession

object WordCountRDD {
  def main(args: Array[String]): Unit = {

    // Ciktilarimizi gorebilmek adina Spark loglarini kisitliyoruz.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session olusturmak islemi
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("CreateRDD")
      .config("spark.executor.memory","8gb")
      .config("spark.driver.memory","4gb")
      .getOrCreate()

    // Spark Session icerisinden Spark Contexti olusturma

    val sc = spark.sparkContext

    // txt dosyasini okuma islemini gerceklestiriyoruz.
    val storyRDD = sc.textFile("./src/main/scala/LowLevelRDD/omer_seyfettin_forsa_hikaye.txt")

    // Metnimizin kac satirdan olustugunu gormek icin count ile yazdiriyoruz.
    println(storyRDD.count())
    println()

    // Her bir satiri kelimelerine ayirip her bir kelimeyi tuple icerisinde tutuyoruz.
    var words = storyRDD.flatMap(satir => satir.split(" "))

    // Ayirdigimiz kelimeleri duzenliyoruz.
    words = words.map(_.replaceAll("[ “,.!?’–:;'…-]","").trim.toLowerCase())

    // Goruldugu uzere words icerisinde empty stringler kalmis durumda
    words.take(10).foreach(println)
    println()

    // Empty String durumunu cozmek icin de filter metoduyla sadece dolu stringleri aliyoruz.
    words = words.filter(_.nonEmpty)

    // Kelimelerin sayisini tutabilmek icin map fonksiyonuyla her kelimenin bir de sayisini yanina sayi degeri ekliyoruz.
    var wordsCount = words.map((_,1)).reduceByKey(_ + _)

    wordsCount.take(10).foreach(println)
    println()

    var finalCount = wordsCount.map(x => (x._2,x._1))
    println("En Cok Tekrar Eden Kelimeler ;")
    finalCount.sortByKey(ascending = false).take(15).foreach(println)

  }
}
