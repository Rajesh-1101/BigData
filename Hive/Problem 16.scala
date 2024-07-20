/*
Problem 16: Imagine you have a messy source table containing customer data, and
you need to clean and transform it using Scala Spark. Here's a simplified version of
the source table.

📊Source Table: `customer_data`**

| customer_id | name | email | phone | registration_date |
|-------------|-----------|------------------------|----------------|------------------|
| 1 | John Doe | john.doe@gmail.com | 123-456-7890 | 2022-01-15 |
| 2 | Jane Smith| jane.smith@hotmail.com | (987)654-3210 | 2021-11-30 |
| 3 | Alice Lee | alice.lee@yahoo.com | 555-5555 | 2023-03-10 |
| 4 | Bob Brown | bob.brown@gmail.com | | 2022-05-20 |

📊 Output
Here's the cleaned and transformed data frame:

| customer_id | full_name | email | phone | registration_date | age |
|-------------|-------------|------------------------|---------------|-------------------|-----|
| 1 | John Doe | john.doe@gmail.com | 1234567890 | 2022-01-15 | 1 |
| 2 | Jane Smith | jane.smith@hotmail.com | 9876543210 | 2021-11-30 | 2 |
| 3 | Alice Lee | alice.lee@yahoo.com | 5555555 | 2023-03-10 | 0 |
| 4 | Bob Brown | bob.brown@gmail.com | N/A | 2022-05-20 | 1 |
*/

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object cleanTransformed {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("cleanTransformed")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("customer_id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("email", StringType, false),
      StructField("phone", StringType, true),
      StructField("registration_date", StringType, false)
    ))

    val sampleData = Seq(
      Row(1, "John Doe", "john.doe@gmail.com", "123-456-7890", "2022-01-15"),
      Row(2, "Jane Smith", "jane.smith@hotmail.com", "(987)654-3210", "2021-11-30"),
      Row(3, "Alice Lee", "alice.lee@yahoo.com", "555-5555", "2023-03-10"),
      Row(4, "Bob Brown", "bob.brown@gmail.com", null, "2022-05-20")
    )

    val sampleDataDF = spark.createDataFrame(
      spark.sparkContext.parallelize(sampleData),
      schema
    )

    sampleDataDF.show(false)

    val cleandDF = sampleDataDF.withColumn("phone", regexp_replace($"phone", "[^\\d]", ""))
      .withColumn("phone", when($"phone" === "","N/A").otherwise($"phone"))
    cleandDF.show()

    val currentYear = year(current_date())
    val registrationYear = year($"registration_date")
    val ageDF = (currentYear - registrationYear).alias("age")

    val finalDF = cleandDF.withColumn("age", ageDF)

    val resultDF = finalDF.select(
      $"customer_id",
      $"name".as("full_name"),
      $"email",
      $"phone",
      $"registration_date",
      $"age"
    )

    resultDF.show()
    spark.stop()
  }
}
