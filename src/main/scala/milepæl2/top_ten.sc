import org.apache.commons.collections.CollectionUtils.select
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

/*
  What is the top 10 highest rated shows on the whole dataset and which streaming services offers these shows?
 */
val df = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata\\data\\tv-shows.csv")

/*
Selects the streaming services, sorts descendingly by the IMDDb column,
and picks the 10 first tv-shows
Then checks if a streaming service has the value 1, meaning it has the tv-show available.
If it does then the streaming show is added to the topp10 column.
 */

val result = df
  .select("Title", "IMDb", "Netflix", "Hulu", "Prime Video", "Disney+")
  .sort(desc("IMDb"))
  .limit(10)
  .withColumn("Topp10",
    concat_ws(", ",
      when(col("Hulu") === 1, lit("Hulu")),
      when(col("Prime Video") === 1, lit("Prime Video")),
      when(col("Disney+") === 1, lit("Disney+")),
      when(col("Netflix") === 1, lit("Netflix"))
    )
    //Unnecessary columns are dropped.
  ).drop("Netflix", "Hulu", "Prime Video", "Disney+")

result.explain(true)
result.write.mode(SaveMode.Overwrite).format("csv").save("C:\\Users\\marpe\\Documents\\bigdata\\skriv_til")


