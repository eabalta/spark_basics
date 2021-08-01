package LowLevelDSV

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
import scala.io.Source

object BroadcastVariables {
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

    println("Finding the top 10 products using broadcast variables -->\n")

    def loadProductstoBroadcast():Map[Int,String]={
      val source = Source.fromFile("./src/main/scala/products.csv")
      val lines = source.getLines().filter(!_.contains("productCategoryId")) // Remove Headers

      var productIdName:Map[Int,String]=Map()

      for(line <- lines){
        val productId = line.split(",")(0).toInt
        val productName = line.split(",")(2)

        productIdName += (productId -> productName)
      }
      productIdName
    }

    // Read file with function then broadcasts
    val broadcastedProducts = sc.broadcast(loadProductstoBroadcast())

    val orderItemsRDD = sc.textFile("./src/main/scala/order_items.csv").filter(!_.contains("orderItemName"))

    val orderItemsPair = orderItemsRDD.map(line => {
      val orderItemProductId = line.split(",")(2).toInt
      val orderItemSubTotal = line.split(",")(4).toFloat

      // orderItemProductId key,  orderItemSubTotal value
      (orderItemProductId, orderItemSubTotal)
    })

    val sortedOrders = orderItemsPair.reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey(ascending = false)

    val sortedOrdersWithName = sortedOrders.map(x => (broadcastedProducts.value(x._2),x._1))
    sortedOrdersWithName.take(10).foreach(println)

  }
}
