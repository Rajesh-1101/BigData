/*
Problem7: Find the origin and the destination of each customer.

Input:
flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
(1,'Flight2' , 'Hyderabad' , 'Kochi'),
(1,'Flight3' , 'Kochi' , 'Mangalore'),
(2,'Flight1' , 'Mumbai' , 'Ayodhya'),
(2,'Flight2' , 'Ayodhya' , 'Gorakhpur')
]

_schema = "cust_id int, flight_id string , origin string , destination string"

df_flight = spark.createDataFrame(data = flights_data , schema= _schema)
df_flight.show()

+-------+---------+---------+-----------+
|cust_id|flight_id| origin|destination|
+-------+---------+---------+-----------+
| 1| Flight1| Delhi| Hyderabad|
| 1| Flight2|Hyderabad| Kochi|
| 1| Flight3| Kochi| Mangalore|
| 2| Flight1| Mumbai| Ayodhya|
| 2| Flight2| Ayodhya| Gorakhpur|
+-------+---------+---------+-----------+
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FlightDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FlightDataFrame")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val flightData = Seq(
      (1, "Flight1", "Delhi", "Hyderabad"),
      (1, "Flight2", "Hyderabad", "Kochi"),
      (1, "Flight3", "Kochi", "Mangalore"),
      (2, "Flight1", "Mumbai", "Ayodhya"),
      (2, "Flight2", "Ayodhya", "Gorakhpur")
    )

    val flightDF = flightData.toDF("cust_id", "flight_id", "origin", "destination")
    flightDF.show()

    val winDF = Window.partitionBy("cust_id").orderBy("flight_id")

    val flightRowDF = flightDF.withColumn("row_num", row_number.over(winDF))
    flightRowDF.show()

    val originDF = flightRowDF.filter($"row_num" === 1).select($"cust_id", $"origin".as("origin"))
    originDF.show()

    val destinationDF = flightRowDF.groupBy("cust_id").agg(max("row_num").as("max_row_num"))
      .join(flightRowDF, Seq("cust_id"))
      .filter($"row_num" === $"max_row_num")
      .select($"cust_id", $"destination".as("destination"))
    destinationDF.show()

    val resultDF = originDF.join(destinationDF, Seq("cust_id"))
    resultDF.show()

    spark.stop()
  }
}
