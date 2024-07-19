/*
Problem6: The task is to merge two data frames using Scala Spark.

Input:
data1 = [(1,"Sagar" , "CSE" , "UP" , 80),\
(2,"Shivani" , "IT" , "MH", 86),\
(3,"Muni", "Mech" , "AP", 70)]
data1_schema = ("ID" , "Student_Name" ,"Department_Name" , "City" , "Marks")
df1 = spark.createDataFrame(data= data1 , schema = data1_schema)
df1.show()

data2 = [(4, "Raj" , "CSE" , "MP") , \
(5 , "Kunal" , "Mech" , "RJ") ]
data2_scehma = ("ID" , "Student_Name" , "Department_name" , "City")
df2 = spark.createDataFrame(data = data2 , schema = data2_scehma)
df2.show()

+---+------------+---------------+----+-----+
| ID|Student_Name|Department_Name|City|Marks|
+---+------------+---------------+----+-----+
| 1| Sagar| CSE| UP| 80|
| 2| Shivani| IT| MH| 86|
| 3| Muni| Mech| AP| 70|
+---+------------+---------------+----+-----+

+---+------------+---------------+---------+
| ID|Student_Name|Department_name| City|
+---+------------+---------------+---------+
| 4| Raj| CSE| MP|
| 5| Kunal| Mech|RJ|
+---+------------+---------------+---------+
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mergedDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("mergedDataFrame")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data1 = Seq(
      (1,"Sagar" , "CSE" , "UP" , 80),
      (2,"Shivani" , "IT" , "MH", 86),
      (3,"Muni", "Mech" , "AP", 70)
    )
    val data1_schema = "ID, Student_Name, Department_Name, City, Marks"

    val data1DF = data1.toDF("ID" , "Student_Name" ,"Department_Name" , "City" , "Marks")
    data1DF.show()

    val data2 = Seq(
      (4, "Raj" , "CSE" , "MP") ,
    (5 , "Kunal" , "Mech" , "RJ")
    )
    val data2_schema = "ID , Student_Name , Department_name , City"

    val data2DF = data2.toDF("ID" , "Student_Name" , "Department_name" , "City")
    data2DF.show()

    val data2WithMarksDF = data2DF.withColumn("Marks", lit(null).cast("int"))
    data2WithMarksDF.show()

    val mergedDf = data1DF.union(data2WithMarksDF)
    mergedDf.show()

    spark.stop()
  }
}
