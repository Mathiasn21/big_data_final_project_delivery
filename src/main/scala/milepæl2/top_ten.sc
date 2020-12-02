import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{SaveMode, SparkSession}

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

//What is the top 10 highest rated shows on the whole dataset and which streaming services offers these shows?
val df = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata\\data\\tv-shows.csv")

/*
Select relevant data and sort by column IMDb ascending.
Set value 1 if a corresponding streaming service is present, aka does offer this show.
Save data to file in overwrite mode. As data may be altered over time meaning we want this query to overwrite existing data
 */

val result = df
  .select("Title", "IMDb", "Netflix", "Hulu", "Prime Video", "Disney+")
  .sort(desc("IMDb"))
  .limit(10)
  .withColumn("Top10",
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


