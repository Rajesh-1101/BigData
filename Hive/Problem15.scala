/*
Problem15: You have a dataset of sales transactions, and you want to find the top-selling
product for each month. Write a scala Spark query to achieve this using window functions.

📊Source Data:
Assume you have a Scala Spark DataFrame named `sales_data` with the following columns:
- `product_id` (String): The unique identifier for each product.
- `sales_date` (Date): The date of the sale.
- `sales_amount` (Double): The amount of the sale.
🔎Here's a sample of the source data:
+-----------+-----------+------------+
| product_id|sales_date |sales_amount|
+-----------+-----------+------------+
| A | 2023-01-15| 100.0 |
| B | 2023-01-20| 150.0 |
| A | 2023-02-10| 120.0 |
| B | 2023-02-15| 180.0 |
| C | 2023-03-05| 200.0 |
| A | 2023-03-10| 250.0 |
+-----------+-----------+------------+


📊Expected Output:
You should generate a result Data Frame that shows the top-selling product for each month.
The result should have the following columns:
- `month` (String): The month for which the top-selling product is calculated.
- `top_product` (String): The product that had the highest total sales for that month.

Here's the expected output for the provided sample data:
+-------+------------+
| month|top_product |
+-------+------------+
|2023-01| B |
|2023-02| B |
|2023-03| A |
+-------+------------+

*/
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object salesTransactions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("salesTransactions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val salesData = Seq(
      ("A", "2023-01-15", 100.0),
      ("B", "2023-01-20", 150.0),
      ("A", "2023-02-10", 120.0),
      ("B", "2023-02-15", 180.0),
      ("C", "2023-03-05", 200.0),
      ("A", "2023-03-10", 250.0)
    ).toDF("product_id", "sales_date", "sales_amount")

    salesData.show(false)

    val salesWithMonth = salesData
      .withColumn("Sales_date", to_date($"sales_date"))
      .withColumn("month", date_format($"sales_date", "yyyy-MM"))
    salesWithMonth.show(false)

    val totalSalePerMonth = salesWithMonth
      .groupBy("month", "product_id")
      .agg(sum($"sales_amount").alias("total_sale"))
    totalSalePerMonth.show(false)

    val windowDF = Window.partitionBy("month").orderBy(desc("total_sale"))

    val rankDF = totalSalePerMonth
      .withColumn("rank", dense_rank().over(windowDF))

    val resultDF = rankDF
      .filter($"rank" === 1)
      .select($"month", $"product_id")
      .withColumnRenamed("product_id", "top_products")

    resultDF.show()

    spark.stop()
  }
}
