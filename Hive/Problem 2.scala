/*
Problem2: Task: You are given a table of tennis players and their matches that
they could either win (W) or lose (L). Find the longest streak of wins.
A streak is a set of consecutive matches of one player. The streak ends once
a player loses their next match. Output the ID of the player or players and
the length of the streak.

Schema:
======
player_id: int
match_date: datetime
match_result: varchar
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, "2024-01-01", "W"),
      (1, "2024-01-02", "W"),
      (1, "2024-01-03", "L"),
      (1, "2024-01-04", "W"),
      (1, "2024-01-05", "W"),
      (2, "2024-01-01", "L"),
      (2, "2024-01-02", "W"),
      (2, "2024-01-03", "W"),
      (2, "2024-01-04", "W"),
      (2, "2024-01-05", "L")
    ).toDF("player_id", "match_date", "match_result")

    val df = data.withColumn("match_date", $"match_date".cast("date"))

    val sortDF = Window.partitionBy("player_id").orderBy("match_date")

    // Assigning streak groups
    val streakDF = df.withColumn("win_streak",
        when($"match_result" === "W", 1)
          .otherwise(0))
      .withColumn("Streak_id",
        sum(when($"match_result" === "L", 1).otherwise(0))
          .over(sortDF))

    // Aggregating streak lengths

    val resultdf = streakDF.groupBy("player_id", "Streak_id")
      .agg(sum("win_streak").as("streak_length"),
        max("match_date").as("End_date"))
      .filter($"streak_length" > 0)
      .orderBy(desc("streak_length"))

    resultdf.show(false)

    spark.stop()
  }
}
