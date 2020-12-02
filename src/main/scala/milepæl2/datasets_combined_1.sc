import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

//How many kickstarters was launched and tv-series was produced the same year?
val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()
print("\n\n\n")

//Read tv-show file and drop unnecessary columns.
// Then groups by year, counts and re-names the count column
val tvDf = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata_data\\tv-shows.csv")
  .drop("_c0", "Age", "IMDb", "type", "Netflix",
    "Rotten Tomatoes", "Hulu", "Prime Video", "Disney+")
  .groupBy("Year")
  .count().withColumnRenamed("count", "tv_count")

//Read kickstarter file and drop unnecessary columns
val kickDf = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\kickstarter.csv")
  .drop("currency ", "category ", "main_category ", "backers ",
    "goal ", "deadline ", "pledged ", "state ", "country ", "usd pledged ",
    "_c14", "_c15", "_c16", "_c13")
/*
Add column "date_time" to kickDf and cast column "Launched" to timestamp.
Extrapolate year from "date_time" column
Group by year and count occurrences
 */
 val dateKick = kickDf.withColumn("date_time",
    unix_timestamp(kickDf("launched "), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
   .withColumn("Year_kick", year(col("date_time")))
   .groupBy("Year_kick")
   .count().withColumnRenamed("count", "kickstarter_count")

//Join the dataframes on year and drop redundant column.
val kickTv = tvDf.join(dateKick, tvDf.col("Year")
  .equalTo(dateKick("Year_kick"))).drop("Year_kick")

kickTv.write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")
kickTv.explain(true)
