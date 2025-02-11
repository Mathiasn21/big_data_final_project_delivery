import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


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

//What is the average number tv-shows released pr year?
def query_1(df: DataFrame): Unit = {
  //Count tv shows per year, then calculate the average and write to file
  df.groupBy("Year")
    .count()
    .agg(avg("count").as("Avg_year"))
    .write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")
}

//Which streaming service has the most and least tv-series available?
def query_2(df : DataFrame): Unit = {
  //Sum tv shows related to each streaming service
  val summedDf = df.agg(
    sum("Hulu").as("Hulu_sum"),
    sum("Disney+").as("Disney_sum"),
    sum("Netflix").as("Netflix_sum"),
    sum("Prime Video").as("Prime_Vid_sum")
  )
  val structs = summedDf.columns.tail.map(
    //creates a new struct column that composes of input column v and k
    c => struct(col(c).as("v"), lit(c).as("k"))
  )
  //adds a max and min column to summedDF
  summedDf.withColumn("maxCol", greatest(structs: _*).getItem("k"))
    .withColumn("minCol", least(structs: _*).getItem("k")).show()
}


//What year released the shows with highest rating?
def query_3(df: DataFrame): Unit = {
  //group IMDb by year and calculate sum of all ratings, sort descending and get first value = max value
  val res = df.filter("IMDb is not null").groupBy("Year")
    .sum("IMDb").sort(column("sum(IMDb)").desc).limit(1)
    res.show()
  res.write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")
}

//Using aggregation instead
def query_3_alternative(df: DataFrame): Unit = {
  df.filter("IMDb is not null").groupBy("Year")
    .agg(sum("IMDb").as("rating_sum")).agg(max(column("rating_sum"))).explain(true)
}

query_1(df)
query_2(df)
query_3(df)
query_3_alternative(df)