/*
Problem5: Calculate the % Marks for each student. Each subject is worth 100 marks.
Create a result column by following the below condition

1. % Marks greater than or equal to 70 then 'Distinction'
2. % Marks range between 60-69 then 'First Class'
3. % Marks range between 50-59 then 'Second Class'
4. % Marks range between 40-49 then 'Third Class'
5. % Marks Less than or equal to 39 then 'Fail'

Input:
student = [(1,'Steve'),(2,'David'),(3,'Aryan')]
student_schema = "student_id int , student_name string"

marks = [(1,'pyspark',90),
(1,'sql',100),
(2,'sql',70),
(2,'pyspark',60),
(3,'sql',30),
(3,'pyspark',20)
]
marks_schema = "student_id int , subject_name string , marks int"

# Create Student dataFrame
student_df = spark.createDataFrame(data = student , schema = student_schema)
student_df.show()

# Create Marks DataFrame
marks_df = spark.createDataFrame(data = marks , schema = marks_schema)
marks_df.show()

+----------+------------+
|student_id|student_name|
+----------+------------+
| 1| Steve|
| 2| David|
| 3| Aryan|
+----------+------------+

+----------+------------+-----+
|student_id|subject_name|marks|
+----------+------------+-----+
| 1| pyspark| 90|
| 1| sql| 100|
| 2| sql| 70|
| 2| pyspark| 60|
| 3| sql| 30|
| 3| pyspark| 20|
+----------+------------+-----+
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object marksCalculator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("marksCalculator")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val student = Seq(
      (1,"Steve"),(2,"David"),(3,"Aryan")
    )
    val student_schema = "student_id int , student_name string"

    val studentDF = student.toDF("student_id", "student_name")
    studentDF.show()

    val marks = Seq(
      (1,"pyspark",90),
      (1,"sql",100),
      (2,"sql",70),
      (2,"pyspark",60),
      (3,"sql",30),
      (3,"pyspark",20)
    )
    val marks_schema = "student_id int , subject string , marks int"

    val marksDF = marks.toDF("student_id", "subject", "marks")
    marksDF.show()

    val totalMarksDF = marksDF
      .groupBy("student_id")
      .agg(sum("marks").as("total_marks"), (sum("marks") / (count("subject") * 100)).as("percentage"))

    totalMarksDF.show()

    val resultDF = studentDF.join(totalMarksDF, "student_id")
      .withColumn("result", when(col("percentage") >= 0.70, "Distinction")
        .when(col("percentage").between(0.60, 0.69), "First Class")
        .when(col("percentage").between(0.50, 0.59), "Second Class")
        .when(col("percentage").between(0.40, 0.49), "Third Class")
        .otherwise("Fail"))

    resultDF.select("student_id", "student_name", "total_marks", "percentage", "result").show()
    spark.stop()
  }
}
