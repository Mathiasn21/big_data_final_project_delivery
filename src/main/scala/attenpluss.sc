import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._
import spark.implicits

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

val df = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\tv-shows.csv")

/*//DETTE ER KODE SOM FUNKER I SHELL MEN IKKE HER...MEN BARE Å COPY PASTE INN

val df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("tv-shows.csv")

val prime = df.where(col("Prime Video") === 1 && col("Age") === "18+").count()
val netflix = df.where(col("Netflix") === 1 && col("Age") === "18+").count()
val hulu = df.where(col("Hulu") === 1 && col("Age") === "18+").count()
val disney = df.where(col("Disney+") === 1 && col("Age") === "18+").count()

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

val maks_all= df_max.filter($"show".like("%all%")).sort(col("count").desc).take(1)

val maks_atten = df_max.filter($"show".like("%18%")).sort(col("count").desc).take(1)*/

