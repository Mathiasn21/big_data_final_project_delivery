import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
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
  df.groupBy("Year")
    .count()
    .agg(avg("count").as("Avg_year"))
    .show()
}

def query_2(df : DataFrame): Unit = {
  val summedDf = df.agg(
    sum("Hulu").as("Hulu_sum"),
    sum("Disney+").as("Disney_sum"),
    sum("Netflix").as("Netflix_sum"),
    sum("Prime Video").as("Prime_Vid_sum")
  )

  val structs = summedDf.columns.tail.map(
    c => struct(col(c).as("v"), lit(c).as("k"))
  )

  summedDf.withColumn("maxCol", greatest(structs: _*).getItem("k"))
    .withColumn("minCol", least(structs: _*).getItem("k")).show()
}

def query_3(df: DataFrame): Unit = {
  //TODO: Find year has the highest rating shows pr year
  print("\n\n\nQuery for flest shows med høyest rating")
  val res = df.filter("IMDb is not null").groupBy("Year")
    .sum("IMDb").sort(column("sum(IMDb)").desc).limit(1)
    res.show()
  res.write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")
}

/*def query_3(df: DataFrame): Unit = {
  //TODO: Find year has the highest rating shows pr year - Alternative
  print("\n\n\nQuery for flest shows med høyest rating")
  df.filter("IMDb is not null").groupBy("Year")
    .agg(sum("IMDb").as("rating_sum")).agg(max(column("rating_sum"))).explain(true)
}*/

query_1(df)
query_2(df)
query_3(df)