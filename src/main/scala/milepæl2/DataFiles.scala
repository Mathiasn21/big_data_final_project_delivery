package milepæl2

import scala.collection.immutable.HashMap

object Files extends Enumeration {
  val Shootings, IRIS_flowers, glass, Movies, TvShows, Kickstarter, DatingProfiles1 = Value
}

object Type extends Enumeration {
  val CSV: Type.Value = Value("csv")
  val JSON: Type.Value = Value("json")
}

object DataFiles{
  val dataLocation = "D:\\data\\"
  val fileHashMap: Map[Files.Value, String] = HashMap(
    Files.IRIS_flowers -> "IRIS_flowers.csv",
    Files.Shootings -> "shootings.csv",
    Files.Movies -> "movies.csv",
    Files.TvShows -> "tv_shows.csv",
    Files.Kickstarter -> "ks-projects-201612.csv",
    Files.DatingProfiles1 -> "dating_profiles\\profiles_1_datingApp.csv"
  )
  val fileTypeHashMap: Map[Files.Value, Type.Value] = HashMap(
    Files.IRIS_flowers -> Type.CSV,
    Files.Shootings -> Type.CSV,
    Files.glass -> Type.CSV,
    Files.Movies -> Type.CSV,
    Files.TvShows -> Type.CSV,
    Files.DatingProfiles1 -> Type.CSV,
    Files.Kickstarter -> Type.CSV
  )

  def getFilePath(file: Files.Value): String = dataLocation + fileHashMap(file)
  def getFileType(file: Files.Value): String = fileTypeHashMap(file).toString
}