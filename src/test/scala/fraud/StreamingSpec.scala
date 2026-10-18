package fraud

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class StreamingSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("streaming-spec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private final case class Dirs(in: Path, out: Path, checkpoint: Path)

  private def dirs(): Dirs = {
    val base = Files.createTempDirectory("fraud-stream")
    Dirs(Files.createDirectory(base.resolve("in")), base.resolve("out"), base.resolve("checkpoint"))
  }

  private def run(d: Dirs): Unit =
    StreamingJob.start(spark, d.in.toString, d.out.toString, d.checkpoint.toString, once = true).awaitTermination()

  private def scoredCount(d: Dirs): Long = spark.read.parquet(d.out.resolve("scored").toString).count()

  test("le writer dépose des fichiers JSON lisibles sans exposer de fichier partiel") {
    val d = dirs()
    StreamSource.writeBatch(d.in, "b1.json", TransactionGenerator.generate(10, seed = 1L, fraudRate = 0.1))
    val names = Files.list(d.in).toArray.map(_.toString.split('/').last).toSeq
    assert(names == Seq("b1.json"))
  }

  test("un micro-batch est traité sans perte ni doublon (critère 6)") {
    val d = dirs()
    val txs = TransactionGenerator.generate(500, seed = 2L, fraudRate = 0.05)
    StreamSource.writeBatch(d.in, "b1.json", txs)
    run(d)
    assert(scoredCount(d) == 500L)
    val ids = spark.read.parquet(d.out.resolve("scored").toString).select("transaction_id").collect().map(_.getString(0))
    assert(ids.toSet == txs.map(_.transactionId).toSet)
  }

  test("un doublon réémis dans un second fichier n'est pas compté deux fois") {
    val d = dirs()
    val txs = TransactionGenerator.generate(200, seed = 3L, fraudRate = 0.05)
    StreamSource.writeBatch(d.in, "b1.json", txs)
    run(d)
    StreamSource.writeBatch(d.in, "b2.json", txs.take(50)) // rejeu des 50 premières
    run(d)
    assert(scoredCount(d) == 200L)
  }

  test("relancer sans nouveau fichier ne retraite rien (checkpoint)") {
    val d = dirs()
    StreamSource.writeBatch(d.in, "b1.json", TransactionGenerator.generate(100, seed = 4L, fraudRate = 0.05))
    run(d)
    run(d)
    assert(scoredCount(d) == 100L)
  }

  test("les nouvelles transactions d'un fichier ultérieur sont ajoutées") {
    val d = dirs()
    StreamSource.writeBatch(d.in, "b1.json", TransactionGenerator.generate(100, seed = 5L, fraudRate = 0.05))
    run(d)
    StreamSource.writeBatch(d.in, "b2.json", TransactionGenerator.generate(60, seed = 6L, fraudRate = 0.05))
    run(d)
    assert(scoredCount(d) == 160L)
  }

  test("les lignes invalides ne sortent pas en scored") {
    val d = dirs()
    val txs = TransactionGenerator.generate(20, seed = 7L, fraudRate = 0.0)
    val bad = txs.head.copy(transactionId = "bad-1", amount = -3.0)
    StreamSource.writeBatch(d.in, "b1.json", txs.tail :+ bad)
    run(d)
    assert(scoredCount(d) == 19L)
  }
}
