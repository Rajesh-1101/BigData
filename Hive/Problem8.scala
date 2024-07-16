/*
Problem8: Find the duplicate records in the data frame.

Input:
schema = StructType([
 StructField("emp_id", IntegerType(), True),
 StructField("emp_name", StringType(), True),
 StructField("emp_gender", StringType(), True),
 StructField("emp_age", IntegerType(), True),
 StructField("emp_salary", IntegerType(), True),
 StructField("emp_manager", StringType(), True)
])

data = [
 (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
 (2, "Aarav Sharma", "Male", 28, 55000, "Zara Singh"),
 (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel"),
 (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
 (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
 (6, "Naina Verma", "Female", 31, 72000, "Arjun Patel"),
 (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
 (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
 (5, "Aditya Kapoor", "Male", 28, 58000, "Zara Singh"),
 (10, "Anaya Joshi", "Female", 27, 59000, "Aarav Sharma"),
 (11, "Rohan Malhotra", "Male", 36, 73000, "Zara Singh"),
 (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel")
]
df = spark.createDataFrame(data, schema=schema)
df.show()
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object duplicateData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("duplicateData")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val empData = Seq(
      (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
      (2, "Aarav Sharma", "Male", 28, 55000, "Zara Singh"),
      (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel"),
      (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
      (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
      (6, "Naina Verma", "Female", 31, 72000, "Arjun Patel"),
      (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
      (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
      (5, "Aditya Kapoor", "Male", 28, 58000, "Zara Singh"),
      (10, "Anaya Joshi", "Female", 27, 59000, "Aarav Sharma"),
      (11, "Rohan Malhotra", "Male", 36, 73000, "Zara Singh"),
      (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel")
    )

    val empDF = empData.toDF("emp_id", "emp_name", "emp_gender", "emp_age", "emp_salary", "emp_manager")
    empDF.show()

    val groupDF = empDF.groupBy(empDF.columns.map(col): _*).count()
    val resultDF = groupDF.filter(col("count") > 1).drop(col("count"))
    resultDF.show()

    spark.stop()
  }
}
