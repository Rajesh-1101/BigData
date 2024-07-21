/*
Problem 17: Imagine you have two Spark DataFrames 'orders' and 'order_items',
representing e-commerce order information and corresponding item details.
Your task is to find the top-selling products, ranked by the total quantity sold,
along with their corresponding categories.

Sample Data:

'orders' DataFrame:
order_id | customer_id | order_date
------------------------------------
1 | 101 | 2023-01-15
2 | 102 | 2023-01-16

'order_items' DataFrame:
order_id | product_id | quantity
----------------------------------
1 | 201 | 3
1 | 202 | 2
2 | 203 | 1
*/
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TopSellingProduct {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Top Selling Product")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ordersData = Seq(
      (1, 101, "2023-01-15"),
      (2, 102, "2023-01-16")
    )

    val orderItemsData = Seq(
      (1, 201, 3),
      (1, 202, 2),
      (2, 203, 1)
    )

    val ordersDF = spark.createDataFrame(ordersData)
      .toDF("order_id", "customer_id", "order_date")
      .withColumn("order_id", $"order_id".cast("int"))
      .withColumn("customer_id", $"customer_id".cast("int"))
      .withColumn("order_date", $"order_date".cast("date"))
    ordersDF.show()

    val orderItemsDF = spark.createDataFrame(orderItemsData)
      .toDF("order_id", "product_id", "quantity")
      .withColumn("order_id", $"order_id".cast("int"))
      .withColumn("product_id", $"product_id".cast("int"))
      .withColumn("quantity", $"order_id".cast("int"))
    orderItemsDF.show()

    val topSellingProduct = ordersDF.join(orderItemsDF, "order_id")
      .groupBy("product_id")
      .agg(sum("quantity").alias("total_quantity"))
      .orderBy(desc("total_quantity"))

    topSellingProduct.show()
    spark.stop()
  }
}
