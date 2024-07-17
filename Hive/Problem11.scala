/*
Problem11: Find out only those records that have the correct Phone No

INPUT:
+---+----------+-------------+
| ID | Name | PhoneNo|
+---+----------+-------------+
| 1 |Martinez | U795342iy |
| 2 | Rodri |7903280317 |
| 3 | Mane |sh987122e9 |
+---+----------+-------------+


OUTPUT:
+---+-----+-------------+
| ID | Name | PhoneNo |
+---+-----+-------------+
| 2 |Rodri| 7903280317|
+---+-----+--------------+




𝐒𝐜𝐡𝐞𝐦𝐚 𝐀𝐧𝐝 𝐃𝐚𝐭𝐚:
================
# Define the schema for the DataFrame
schema1=StructType([
 StructField("ID", IntegerType()),
 StructField("Name", StringType()),
 StructField("PhoneNo", StringType())
])

# Data to be inserted into the DataFrame
sampleData1=[(1,"Martinez","U795342iy"),
 (2,"Rodri","7903280317"),
 (3,"Mane","sh987122e9")]
 */
import org.apache.hadoop.fs.Options.HandleOpt.Data
import org.apache.logging.log4j.CloseableThreadContext.Instance
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object phoneNumber {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("phoneNumber")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1,"Martinez","U795342iy"),
      (2,"Rodri","7903280317"),
      (3,"Mane","sh987122e9")
    )
    val dataSchema = StructType(Array(
      StructField("ID", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("PhoneNo", StringType, true)
    ))

    val dataDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data.map(Row.fromTuple)),
      dataSchema
    )
    dataDF.show()
    dataDF.createOrReplaceTempView("DATA2")
    val result = spark.sql("SELECT * FROM DATA2 WHERE PhoneNo rlike '^[0-9]*$'")
    result.show()

    println("another method")
    dataDF.select("*").filter(column("PhoneNo").rlike("^[0-9]*$")).show()
    spark.stop()
  }
}
