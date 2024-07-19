/*
Problem13: How to handle Multi delimiters?

INPUT:
Check the attached pic.

OUTPUT:
+---+----=+---+-------+---------+----+
| ID |NAME|AGE|Physics|Chemistry|Math|
+---+------+---+-------+---------+----+
| 1| A| 20| 31| 32| 34|
| 2| B| 21| 21| 32| 43|
| 3| C| 22| 21| 32| 11|
| 4| D| 23| 10| 12| 12|
+-+---+-------+-----+------+---------+

𝐃𝐚𝐭𝐚:
================

# Data to be inserted into the DataFrame
sampleData=[(1,"A",20,"31|32|34"),
 (2,"B",21,"21|32|43"),
 (3,"C",22,"21|32|11"),
 (4,"D",23,"10|12|12")]
*/
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object multiDelimiters{
  def main(args: Array[String]): Unit= {
    val spark = SparkSession.builder()
      .appName("multiDelimiters")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sampleData = Seq(
      (1,"A",20,"31|32|34"),
      (2,"B",21,"21|32|43"),
      (3,"C",22,"21|32|11"),
      (4,"D",23,"10|12|12")
    )
    val dataDF = spark.createDataFrame(sampleData).toDF("ID","NAME","AGE","MARKS")
    dataDF.show()

    val resultDF = dataDF.withColumn("Physics", split($"MARKS", "\\|")(0))
      .withColumn("Chemistry", split($"MARKS", "\\|")(1))
      .withColumn("Maths", split($"MARKS", "\\|")(2))
      .drop("MARKS")

    resultDF.show()
    spark.stop()
  }
}
