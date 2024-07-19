/*
Problem10: Count rows in each column where NULLs are present

INPUT:
+---+--------+----+
| ID| NAME| AGE|
+---+--------+----+
| 1|Van dijk| 23|
| 2| NULL| 32|
| 3| Fabinho|NULL|
| 4| NULL|NULL|
| 5| Kaka|NULL|
+---+--------+----+
 */
import org.apache.hadoop.fs.Options.HandleOpt.Data
import org.apache.logging.log4j.CloseableThreadContext.Instance
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object countNulls {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("countNulls")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (Some(1), "Van dijk", Some(23)),
      (Some(2), null, Some(32)),
      (Some(3), "Fabinho", None),
      (Some(4), null, None),
      (Some(5), "Kaka", None)
    )

    val dataDF = data.toDF("ID", "NAME", "AGE")
    dataDF.show()

    val countNulls = dataDF.columns.map {
      colName => dataDF.filter(col(colName).isNull).count().asInstanceOf[Long]
    }.zip(dataDF.columns).map(_.swap).toMap

    countNulls.foreach{ case (col, count) => println(s"Column $col has $count NULL values")}
    
    spark.stop()
  }
}
