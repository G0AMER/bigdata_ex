import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDD_Ex").config("spark.ui.port", 0).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Load sales data into RDD
    val salesRDD: RDD[String] = spark.sparkContext.textFile("hdfs://localhost:9000/bigdata/ada/sales.csv").filter(line => !line.startsWith("sale_id"))

    // Load products data into RDD
    val productsRDD: RDD[String] = spark.sparkContext.textFile("hdfs://localhost:9000/bigdata/ada/products.csv").filter(line => !line.startsWith("product_id"))

    // Splitting lines and creating key-value pairs for both RDDs
    val salesPairRDD: RDD[(String, Array[String])] = salesRDD.map(line => (line.split(",")(1), line.split(",")))
    val productsPairRDD: RDD[(String, Array[String])] = productsRDD.map(line => (line.split(",")(0), line.split(",")))

    // Joining sales and products RDDs by product_id
    val joinedRDD: RDD[(String, (Array[String], Array[String]))] = salesPairRDD.join(productsPairRDD)

    // Calculate total revenue (quantity sold * sale_price) for each product
    val revenueRDD: RDD[(String, Double)] = joinedRDD.map { case (_, (salesInfo, productInfo)) => val quantitySold = salesInfo(2).toInt
      val salePrice = productInfo(3).toDouble
      val revenue = quantitySold * salePrice
      (productInfo(1), revenue)
    }

    // Sort products by descending total revenue
    val sortedRevenueRDD: RDD[(String, Double)] = revenueRDD.reduceByKey(_ + _).sortBy(-_._2)

    // Assuming product category is in index 3 in 'products' RDD
    val categoryRevenueRDD: RDD[(String, Double)] = joinedRDD.map { case (_, (salesInfo, productInfo)) => val quantitySold = salesInfo(2).toInt
      val salePrice = productInfo(3).toDouble
      val revenue = quantitySold * salePrice
      (productInfo(3), revenue)
    }.reduceByKey(_ + _)

    // Group by product category and find the product with maximum revenue in each category
    val bestSellingPerCategoryRDD: RDD[(String, (String, Double))] = joinedRDD.map { case (_, (salesInfo, productInfo)) => val quantitySold = salesInfo(2).toInt
      val salePrice = productInfo(3).toDouble
      val revenue = quantitySold * salePrice
      (productInfo(3), (productInfo(1), revenue))
    }.groupByKey().mapValues(_.maxBy(_._2))

    // Calculate average unit price per product category
    val categoryAvgPriceRDD: RDD[(String, Double)] = joinedRDD.map { case (_, (_, productInfo)) => (productInfo(3), productInfo(4).toDouble)
    }.reduceByKey(_ + _)

    // Print total revenue for each product
    println("Total revenue")
    revenueRDD.foreach(println)

    // Print sorted products by total revenue
    println("\nSorted products by total revenue")
    sortedRevenueRDD.foreach(println)

    // Print total revenue by product category
    println("\nTotal revenue by product category")
    categoryRevenueRDD.foreach(println)

    // Print best-selling product in each category
    println("\nBest-selling product in each category")
    bestSellingPerCategoryRDD.foreach(println)

    // Print average unit price per product category
  }
}