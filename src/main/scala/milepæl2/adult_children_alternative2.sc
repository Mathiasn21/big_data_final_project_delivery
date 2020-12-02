import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

//Explicitly import implicits ^^
import spark.implicits._
val sc = spark.sparkContext

/*Which streaming service releases most adult (18+) series
  and which service releases most series for children?
  Alternative 2
 */
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata_data\\tv-shows.csv")

//count occurrences of 18+ series for each streaming service
val prime = df.where(col("Prime Video") === 1 && col("Age") === "18+").count()
val netflix = df.where(col("Netflix") === 1 && col("Age") === "18+").count()
val hulu = df.where(col("Hulu") === 1 && col("Age") === "18+").count()
val disney = df.where(col("Disney+") === 1 && col("Age") === "18+").count()

//count occurrences of All = also for children for each streaming service
val prime2 = df.where(col("Prime Video") === 1 && col("Age") === "all").count()
val netflix2 = df.where(col("Netflix") === 1 && col("Age") === "all").count()
val hulu2 = df.where(col("Hulu") === 1 && col("Age") === "all").count()
val disney2 = df.where(col("Disney+") === 1 && col("Age") === "all").count()

val df_max = List(
  ("prime (18+)", prime),
  ("netflix (18+)", netflix),
  ("hulu (18+)", hulu),
  ("disney (18+)", disney),
  ("prime (all)", prime2),
  ("netflix (all)", netflix2),
  ("hulu (all)", hulu2),
  ("disney+ (all)", disney2)
).toDF("show","count")

df_max.show()

/*Choose series that are for children and adults,
  then sort descending by count and pick the first element
 */
val max_all= df_max.filter($"show".like("%all%")).sort(col("count").desc).limit(1)
val max_18 = df_max.filter($"show".like("%18%")).sort(col("count").desc).limit(1)

/*
  Combine the two dataframes to one dataframe with union,
  then write that dataframe to disk
 */
var newDf = spark.createDataFrame(sc.emptyRDD[Row], max_all.schema)
val first_row = newDf.unionAll(max_all.select($"*"))
val finalDf = first_row.unionAll(max_18.select("*"))
//finalDf.show()
finalDf.write.mode(SaveMode.Overwrite).format("csv").save("/home/student/Documents/prosjekt1/data")
