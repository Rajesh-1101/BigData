//Spark UDF

import spark.implicits._
val cols =Seq("sno", "name")
val data = Seq(("1", "ram"), 
				("2", "shyam"), 
				("3", "guru")
)

val df = data.toDF (cols: _*)
df.show(false)

val Ucase =(strQuote: String) => {
val dt = strQuote.split(" ")
dt.map(f=> f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString(" ")
}

val customUDF =udf(Ucase)

// with DataFrame
df.select(col("sno"), col ("name")).as("name").show(false)
df.select (col("sno"), customUDF (col ("name")).as("name") ).show(false)

// Using it on SQL
spark.udf.register("customUDF", Ucase)
df.createOrReplaceTempView("test_table")
spark.sql("select sno, customUDF (name) from test table").show(false)
