/*
Problem3: Given a table of purchases by date, calculate the month-over-month
percentage change in revenue. The output should include the year-month date (YYYY-MM)
and percentage change, rounded to the 2nd decimal point, and sorted from the
beginning to the end of the year.
The percentage change column will be populated from the 2nd month forward and
calculated as ((this month's revenue - last month's revenue) / last month's revenue)*100.

Table Schema :
==========
id : int
created_at : datetime
value : int
purchase_id : int
 */
import jdk.internal.dynalink.support.Guards.isNotNull
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object monthlyRevenueChange{
  def main(args: Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("monthlyRevenueChange")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val purchaseDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("Data/purchases_data.csv")

    // purchaseDF.show()
/*
| id|created_at|value|purchase_id|
+---+----------+-----+-----------+
|  1|2023-04-20|  167|       1001|
|  2|2023-03-27|  472|       1002|
 */
    val monthlyRevenueDF = purchaseDF
      .withColumn("year_month", date_format($"created_at", "yyyy-MM"))
      .groupBy("year_month")
      .agg(sum("value").as("monthly_revenue"))

    // monthlyRevenueDF.show()

    val windowDF = Window.orderBy("year_month")

    val resultDF = monthlyRevenueDF
      .withColumn("prev_month_rev", lag("monthly_revenue", 1).over(windowDF))
      .withColumn("percent_change", round((($"monthly_revenue"- $"prev_month_rev")/($"prev_month_rev")* 100),2))
      .filter($"prev_month_rev".isNotNull)
      .select("year_month", "percent_change")
      .orderBy("year_month")

    resultDF.show()
    spark.stop()
  }
}
