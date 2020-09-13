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


val file = Files.TvShows
val path = DataFiles.getFilePath(file)
val format = DataFiles.getFileType(file)

var df = spark.read.format(format)
  .option("delimiter", ",")
  .option("header", value = true)
  .option("inferSchema", value = true)
  .load(path)
  .drop("type")

df.describe().show()

def query_1(df: DataFrame): Unit = {
  //TODO: Find average num of shows pr year.
  print("PLAN FOR mean pr år: \n")
  df.groupBy("Year")
    .count()
    .agg(avg("count").as("Avg_year"))
    .explain(true)
  print("\n\n\n")
}

def query_2(df : DataFrame): Unit = {
  //TODO: Hvilken streamingservice har flest/færrest tv-serier tilgjengelig?
  val summedDf = df.agg(
    sum("Hulu").as("Hulu_sum"),
    sum("Disney+").as("Disney_sum"),
    sum("Netflix").as("Netflix_sum"),
    sum("Prime Video").as("Prime_Vid_sum")
  )

  val structs = summedDf.columns.tail.map(
    c => struct(col(c).as("v"), lit(c).as("k"))
  )

  print("\n\n\n\n\n")
  print("max and min:\n")
  summedDf.withColumn("maxCol", greatest(structs: _*).getItem("k"))
    .withColumn("minCol", least(structs: _*).getItem("k")).explain(true)
  print("\n")
}

def query_3(df: DataFrame): Unit = {
  //TODO: Find year has the highest rating shows pr year
  print("\n\n\nQuery for flest shows med høyest rating")
  df.filter("IMDb is not null").groupBy("Year")
    .sum("IMDb").sort(column("sum(IMDb)").desc).explain(true)

}



query_1(df)
query_2(df)
query_3(df)

