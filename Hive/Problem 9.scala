/*
Problem9: Write a solution to swap the seat ID of every two consecutive students.
If the number of students is odd, the id of the last student is not swapped.

Input:
_data = [(1,'Abbot'),(2,'Doris'),(3,'Emerson'),(4,'Green'),(5,'Jeames')]
_schema = ['id', 'student']
df = spark.createDataFrame(data = _data, schema=_schema)
df.show()
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object duplicateData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("duplicateData")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val stdData = Seq(
      (1,"Abbot"),(2,"Doris"),(3,"Emerson"),(4,"Green"),(5,"Jeames")
    )

    val stdDF = stdData.toDF("id", "student")
    stdDF.show()

    stdDF.createOrReplaceTempView("students")

    val swappedDF = spark.sql(
      """
        |SELECT
        | CASE
        |   WHEN id % 2 = 1 AND LEAD(id) OVER (ORDER BY id) IS NOT NULL THEN LEAD(id) OVER (ORDER BY id)
        |   WHEN iD % 2 = 0 THEN LAG(id) OVER (ORDER BY id)
        |   ELSE id
        |  END AS id,
        |  student
        | FROM students
        | ORDER BY id
        |""".stripMargin
    )

    swappedDF.show()
    stdDF.show()

    spark.stop()
  }
}
