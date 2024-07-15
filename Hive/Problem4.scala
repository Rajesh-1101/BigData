/*
Problem4: Check the count of null values in each column in the data frame.

Input:
employee = [(1, 'Sagar' ,23),(2, None , 34),(None ,'John' , 46),(5,'Alex', None) , (4,'Alice',None)]
employee_schema = "emp_id int , name string , age int"
emp_df = spark.createDataFrame(data = employee , schema = employee_schema)
emp_df.show()

+------+-----+----+
|emp_id| name| age|
+------+-----+----+
| 1|Sagar| 23|
| 2| null| 34|
| null| John| 46|
| 5| Alex|null|
| 4|Alice|null|
+------+-----+----+

 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NullValueCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NullValueCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define the employee data and schema
    val employee = Seq(
      (Some(1), "Sagar", Some(23)),
      (Some(2), null, Some(34)),
      (None, "John", Some(46)),
      (Some(5), "Alex", None),
      (Some(4), "Alice", None)
    )

    // Define the schema
    val employee_schema = "emp_id int, name string, age int"
    val emp_df = spark.createDataFrame(employee).toDF("emp_id", "name", "age")
    emp_df.show()
    
    val nullValueCount = emp_df.columns.map { columnName =>
      emp_df.filter(col(columnName).isNull || (col(columnName) === lit(null) && col(columnName).isNotNull)).count
    }

    emp_df.columns.zip(nullValueCount).foreach{
      case(columnName, nullValueCount) => println(s"Column $columnName has $nullValueCount null values.")
    }

    spark.stop()
  }
}
