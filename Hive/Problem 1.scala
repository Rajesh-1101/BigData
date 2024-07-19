/*
Problem: Find stores that were opened in the second half of 2021 with more than 20% of their reviews being negative. A review is considered negative when the score given by a customer is below 5. Output the names of the stores together with the ratio of negative reviews to positive ones.

Schema :
======
Table 1: instacart_reviews

id: int
customer_id : int
store_id : int
score: int
------------------------------------------------------------------------------------
Table 2 : instacart_stores

id: int
name: varchar
zipcode: int
opening_date : DateTime
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, when}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val reviewsSchema = Seq(
      (1, 1, 1, 4),
      (2, 1, 1, 5),
      (3, 2, 2, 3),
      (4, 2, 2, 5),
      (5, 3, 3, 2)
    ).toDF("id", "customer_id", "store_id", "score")

    val storesSchema = Seq(
      (1, "Store A", 12345, "2021-08-01"),
      (2, "Store B", 12346, "2021-11-15"),
      (3, "Store C", 12347, "2022-01-10")
    ).toDF("id", "name", "zipcode", "opening_date")

    val stores = storesSchema.withColumn("opening_date", col("opening_date").cast("date"))

    val filterStore = stores.filter(
      col("opening_date").between("2021-07-01", "2021-12-31")
    )

    val storeReview = reviewsSchema.groupBy("store_id")
      .agg(
        sum(when(col("score") < 5, 1).otherwise(0)).alias("negative_reviews"),
        sum(when(col("score") >= 5, 1).otherwise(0)).alias("positive_reviews")
      )

    val storeRatios = storeReview.withColumn("negative_to_positive_ratio",
      col("negative_reviews") * 1.0 / col("positive_reviews")
    )

    val result = storeRatios.join(filterStore, storeRatios("store_id") === filterStore("id"))
      .filter((col("negative_reviews") * 1.0 / (col("negative_reviews") + col("positive_reviews"))) > 0.20)
      .select("name", "negative_to_positive_ratio")

    // Show the result
    result.show()

    spark.stop()
  }
}
