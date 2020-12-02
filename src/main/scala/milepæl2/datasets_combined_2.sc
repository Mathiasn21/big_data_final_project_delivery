import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

//How many black and white people committed suicide with firearms and how many overdosed from drugs each year?
def loadDf(file: String): DataFrame = (spark.read format "csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file)

val guns_file = "D:\\data\\guns.csv"
val drug_file = "D:\\data\\drug_deaths.csv"

val white = "White"
val black = "Black"
val race_col = col("Race")

//Read guns file
var gunDf = loadDf(guns_file).withColumnRenamed("year", "Date")
//Read drug file and create column date which is casted to a timestamp and extracts year
var drugDf = loadDf(drug_file).drop("DateType").withColumn("Date", year(from_unixtime(unix_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))))

//Filters out empty data and dates before 2014, then sort by year descending
drugDf = drugDf.filter(
    (!col("_c0").contains("(")) &&
      col("Date").isNotNull &&
      col("Date") < lit("2015"))
    .sort(col("Date").desc)

val predicate = race_col.contains(white) or race_col.contains(black)

//Set value in column if it contains value and names column after alias
val countByRace = (column: Column, value: String, alias: String) =>
  count(when(column.contains(value), 1)).as(alias)

/*
Filter dataframes based on race and group by year, then count respective number of occurrences of race.
 */
val overdoseByRace = drugDf.filter(predicate).groupBy(col("Date")).agg(
    countByRace(race_col, black, "Overdose_Black_count"),
    countByRace(race_col, white, "Overdose_White_count")
  )

val gunDeathByRace = gunDf.filter(predicate).groupBy(col("Date")).agg(
    countByRace(race_col, black, "Gun_death_Black_count"),
    countByRace(race_col, white, "Gun_death_White_count")
  )

print(overdoseByRace.join(gunDeathByRace, "Date").coalesce(10).rdd.partitions.length)
//Join dataframe on year and write data to file
overdoseByRace.join(gunDeathByRace, "Date").write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")