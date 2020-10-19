package functionalProgramming

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import cats.syntax.either._
import cats.Id
import functionalProgramming.SparkRealApp.Repository.sparkContext

sealed trait ContextProvider {
  val sparkContext: SparkSession =
    SparkSession
      .builder()
      .appName("App")
      .master("local[1]")
      .getOrCreate()
}
case class Band(id: Long, name: String, homeTown: String, year: Long)
case class Guitar(id: Long, model: String, make: String, guitarType: String)
case class GuitarPlayer(
    id: Long,
    name: String,
    guitars: List[Long],
    band: Long
)
case class GuitarPlayerFinalResult(
    id: Long,
    name: String,
    guitars: List[Guitar],
    band: Band
)

object SparkRealApp {
  type ErrorOr[F[_], T] = Either[Throwable, F[T]]
  type IdErrorOr[T] = ErrorOr[Id, T]
  type DatasetErrorOr[T] = ErrorOr[Dataset, T]
  type TransformDataset[A] = Dataset[A] => Dataset[A]

  sealed trait Repository[T] {
    def readData(
        path: String
    )(implicit encoder: Encoder[T]): DatasetErrorOr[T] =
      sparkContext.read
        .json(path)
        .as[T]
        .asRight[Throwable]

    def writeData(source: Dataset[T], path: String): IdErrorOr[Unit] =
      source.write.format("json").save(path).asRight[Throwable]
  }

  object Repository extends ContextProvider {

    implicit object BandRepository extends Repository[Band]
    implicit object GuitarRepository extends Repository[Guitar]
    implicit object GuitarPlayerRepository extends Repository[GuitarPlayer]

    implicit object GuitarPlayerFinalResult
        extends Repository[GuitarPlayerFinalResult]
  }

  object RepositoryAPI {
    // define API
    def read[T](sourcePath: String)(implicit
        repository: Repository[T],
        encoder: Encoder[T]
    ): DatasetErrorOr[T] =
      repository.readData(sourcePath)

    def write[T](source: Dataset[T], sourcePath: String)(implicit
        repository: Repository[T]
    ): IdErrorOr[Unit] =
      repository.writeData(source, sourcePath)
  }

  object TransformData {

    // filter bands by year and hometown [ > 1965 and hometown != "Liverpool" ]
    def filterBands(year: Long, town: String): TransformDataset[Band] =
      bandDs => bandDs.filter(band => band.year > year && band.homeTown != town)

    // filter players by number of guitars [ >= 2 ]
    def filterGuitarPlayers(
        numberGuitars: Int
    ): TransformDataset[GuitarPlayer] =
      _.filter(_.guitars.length >= numberGuitars)

    // filter guitars by guitar type [ == Electric ]
    def filterGuitars(guitarType: String): TransformDataset[Guitar] =
      _.filter(_.guitarType == guitarType)

    // a function that joins both datasets
    // and applies another function to build final Dataset
    def joinBandsAndBuildResult(
        bands: Dataset[Band],
        players: Dataset[GuitarPlayer],
        guitarsCatalog: List[Guitar]
    )(implicit
        encoder: Encoder[GuitarPlayerFinalResult]
    ): Dataset[GuitarPlayerFinalResult] = {

      players.joinWith(bands, players("band") === bands("id")).map {
        case (guitarPlayer: GuitarPlayer, band: Band) =>
          buildFinalDataset(guitarPlayer, band, guitarsCatalog)
      }

    }

    // a function that receives guitar player, band, and guitar catalogs
    // to build GuitarPlayerFinalResult dataset
    def buildFinalDataset(
        guitarPlayer: GuitarPlayer,
        band: Band,
        guitarsCatalog: List[Guitar]
    ): GuitarPlayerFinalResult = {

      val guitarsMapCatalog: Map[Long, Guitar] =
        guitarsCatalog.map(guitar => guitar.id -> guitar).toMap

      val selectedGuitars: List[Guitar] = guitarPlayer.guitars.flatMap(gId =>
        guitarsMapCatalog.get(gId).fold(List.empty[Guitar])(List(_))
      )

      GuitarPlayerFinalResult(
        guitarPlayer.id,
        guitarPlayer.name,
        selectedGuitars,
        band
      )
    }
  }

  def main(args: Array[String]): Unit = {
    import RepositoryAPI._
    import TransformData._
    import sparkContext.implicits._
    for {
      bands <- read[Band]("src/main/resources/data/bands")
      guitars <- read[Guitar]("src/main/resources/data/guitars")
      players <- read[GuitarPlayer]("src/main/resources/data/guitarPlayers")
      filteredBands = bands.transform(filterBands(1965, "Liverpool"))
      filteredGuitars = guitars.transform(filterGuitars("Electric"))
      guitarsCatalog = filteredGuitars.collect().toList
      filteredPlayers = players.transform(filterGuitarPlayers(2))
      finalResult =
        joinBandsAndBuildResult(filteredBands, filteredPlayers, guitarsCatalog)
      _ = write[GuitarPlayerFinalResult](
        finalResult,
        "src/main/resources/data/guitarPlayersResult"
      )
    } yield ()
  }

}
