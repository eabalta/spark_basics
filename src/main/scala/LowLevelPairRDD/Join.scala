package LowLevelPairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Join {
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

    val order_items_rdd = sc.textFile("./src/main/scala/LowLevelPairRDD/order_items.csv").filter(!_.contains("orderItemName"))
    println("\norder_items.csv")
    order_items_rdd.take(5).foreach(println)

    val products_rdd = sc.textFile("./src/main/scala/LowLevelPairRDD/products.csv").filter(!_.contains("productId"))
    println("\nproducts.csv")
    products_rdd.take(5).foreach(println)

    val order_items_pair_rdd = order_items_rdd.map( line => {
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2)
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4)
      val orderItemProductPrice = line.split(",")(5)

      // orderItemProductId anahtar,  kalanlar değer olacak şekilde PairRDD döndürme
      (orderItemProductId, (orderItemName, orderItemOrderId, orderItemQuantity,orderItemSubTotal, orderItemProductPrice))
    })

    println("\norder_items.csv -> PairRDD")
    order_items_pair_rdd.take(5).foreach(println)

    val products_pair_rdd = products_rdd.map( line => {
      val productId = line.split(",")(0)
      val productCategoryId = line.split(",")(1)
      val productName = line.split(",")(2)
      val productDescription = line.split(",")(3)
      val productPrice = line.split(",")(4)
      val productImage = line.split(",")(5)

      (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
    })

    println("\nproducts.csv -> PairRDD")
    products_pair_rdd.take(5).foreach(println)


    val joinedRDD = order_items_pair_rdd.join(products_pair_rdd)
    println("\njoinedRDD")
    joinedRDD.take(10).foreach(println)

    /////// Basit bir kontrol büyük tablo 172.199 satır eğer tüm ürünlerden satış olmuşsa aynı sayı elde edilmeli
    println("\norder_items_rdd satır sayısı: " + order_items_rdd.count())
    println("products_rdd satır sayısı: " + products_rdd.count())
    println("joinedRDD satır sayısı: " + joinedRDD.count())

  }
}
