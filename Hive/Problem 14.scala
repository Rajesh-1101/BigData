/*
Problem14: Write a spark code to generate the below output for the given input dataset.

input dataset :

+------+---------+----------+-----------+---------+
|EmpId |Name | Locations
+------+---------+----------+-----------+---------+
|1 |Gaurav | Pune , Bangalore, Hyderabad|
|2 |Risabh | Mumbai ,Bangalore, Pune |
+------+---------+----------+-----------+---------+


Required Output:

EmpId Name Location
1 Gaurav Pune
1 Gaurav Bangalore
1 Gaurav Hyderabad
2 Risabh Mumbai
2 Risabh Pune
*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object locationExplode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("locationExplode")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sampleData = Seq(
      (1, "Gaurav",Array("Pune", "Bangalore", "Hyderabad")),
      (2, "Rishabh",Array("Mumbai", "Bangalore", "Pune"))
    )

    val dataDF = sampleData.toDF("ID", "NAME", "CITIES")
    dataDF.show(false)

    val explodeDF = dataDF.select(
      $"ID",
      $"NAME",
      explode($"CITIES").alias("CITY")
    )
    explodeDF.show(false)
    spark.stop()
  }
}
