import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()
print("\n\n\n")

val tvDf = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata_data\\tv-shows.csv")
  .drop("_c0", "Age", "IMDb", "type", "Netflix",
    "Rotten Tomatoes", "Hulu", "Prime Video", "Disney+")
  .groupBy("Year")
  .count().withColumnRenamed("count", "tv_count")

//TODO: IKKE LES NULL VERDIER
val kickDf = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\kickstarter.csv")
  .drop("currency ", "category ", "main_category ", "backers ",
    "goal ", "deadline ", "pledged ", "state ", "country ", "usd pledged ",
    "_c14", "_c15", "_c16", "_c13")

 val dateKick = kickDf.withColumn("date_time",
    unix_timestamp(kickDf("launched "), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
   .withColumn("Year_kick", year(col("date_time")))
   .groupBy("Year_kick")
   .count().withColumnRenamed("count", "kickstarter_count")

val kickTv = tvDf.join(dateKick, tvDf.col("Year")
  .equalTo(dateKick("Year_kick"))).drop("Year_kick")

kickTv.show()
kickTv.explain(true)