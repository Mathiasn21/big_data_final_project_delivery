import java.time.Year

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

/*
How many black and white people committed suicide with firearms and how many overdosed from drugs each year?
 */
def loadDf(file: String): DataFrame = (spark.read format "csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file)

val guns_file = "D:\\data\\guns.csv"
val drug_file = "D:\\data\\drug_deaths.csv"

val white = "White"
val black = "Black"
val race_col = col("Race")

//Reads the guns file
var gunDf = loadDf(guns_file).withColumnRenamed("year", "Date")
//Reads the drug file and adds column date which is cast to a timestamp
var drugDf = loadDf(drug_file).drop("DateType").withColumn("Date", year(from_unixtime(unix_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))))

//Filters out empty data and dates after 2014, then sorts
drugDf = drugDf.filter(
    (!col("_c0").contains("(")) &&
      col("Date").isNotNull &&
      col("Date") < lit("2015"))
    .sort(col("Date").desc)

val predicate = race_col.contains(white) or race_col.contains(black)

//Function that counts on the condition that a column contains a value and names that after the given alias
val countByRace = (column: Column, value: String, alias: String) =>
  count(when(column.contains(value), 1)).as(alias)

/*
New dataframes that filters on whether the race is black or white and groups by date
Then uses the predefined function to calculate the number of deaths for each race per year
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
//joins the two dataframes
overdoseByRace.join(gunDeathByRace, "Date").write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")