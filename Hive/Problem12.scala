
/*
Problem12: Solve using REGEXP_REPLACE

INPUT:
1-BERNADO-12-2-GAVI-23-3-GIROUD-34-4-RASHFORD-15

OUTPUT:
+---+-----------+---+
| ID| Name |AGE|
+---+-----------+---+
| 1| BERNADO | 12|
| 2| GAVI | 23|
| 3| GIROUD | 34|
| 4|RASHFORD | 15|
+-+------------+---+
𝐃𝐚𝐭𝐚:
================

# Data to be inserted into the DataFrame

dbutils.fs.put('/FileStore/tables/interview1.txt','1-BERNADO-12-2-GAVI-23-3-GIROUD-34-4-RASHFORD-15')
 */
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReplaceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReplaceExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input data
    val input = "1-BERNADO-12-2-GAVI-23-3-GIROUD-34-4-RASHFORD-15"

    // Create DataFrame with the input string
    val dataDF = Seq(input).toDF("data")
    dataDF.show(false)

    // Transform the data by replacing hyphens with commas
    val transformDF = dataDF.withColumn("data", regexp_replace($"data", "-", ","))
    transformDF.show(false)

    // Split the transformed data into an array
    val splitDF = transformDF.withColumn("data", split($"data", ","))
    splitDF.show(false)

    // Explode the array into individual rows
    val newDF = splitDF.selectExpr("explode(data) as value")
    newDF.show(false)

    // Generate row numbers to facilitate splitting into columns
    val rowNumberDF = newDF.withColumn("rn", monotonically_increasing_id())
    rowNumberDF.show(false)

    // Alias the DataFrames before joining them
    val idDF = rowNumberDF.filter($"rn" % 3 === 0).select($"value".as("ID"), $"rn".as("id_rn"))
    val nameDF = rowNumberDF.filter($"rn" % 3 === 1).select($"value".as("Name"), $"rn".as("name_rn"))
    val ageDF = rowNumberDF.filter($"rn" % 3 === 2).select($"value".as("Age"), $"rn".as("age_rn"))

    // Join the columns together to form the final DataFrame
    val joinedDF = idDF.join(nameDF, idDF("id_rn") + 1 === nameDF("name_rn"))
      .join(ageDF, idDF("id_rn") + 2 === ageDF("age_rn"))
      .select($"ID", $"Name", $"Age")

    // Show the final DataFrame
    joinedDF.show(false)

    // Stop the Spark session
    spark.stop()
  }
}
