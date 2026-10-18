package fraud

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PipelineSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("pipeline-spec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("le pipeline écrit les trois tables gold avec le schéma du contrat (critère 7)") {
    val out = Files.createTempDirectory("fraud-gold").resolve("gold").toString
    Pipeline.run(spark, n = 3000, seed = 42L, fraudRate = 0.02, outDir = out, creditcardPath = None)

    val byHour = spark.read.parquet(s"$out/fraud_by_hour")
    assert(byHour.columns.toSeq == Seq("hour", "country", "category", "tx_count", "fraud_count", "fraud_rate"))
    assert(byHour.count() > 0)
    assert(byHour.agg(org.apache.spark.sql.functions.sum("tx_count")).head().getLong(0) == 3000L)

    val alerts = spark.read.parquet(s"$out/alerts")
    assert(alerts.columns.toSeq == Seq("transaction_id", "card_id", "ts", "amount", "score", "rule"))
    assert(alerts.count() > 0)

    val metrics = spark.read.parquet(s"$out/model_metrics")
    assert(metrics.columns.toSeq == Seq("run_ts", "dataset", "precision", "recall", "threshold"))
    assert(metrics.select("dataset").collect().map(_.getString(0)).toSeq == Seq("synthetic"))
  }

  test("relancer le pipeline écrase les tables sans les dupliquer") {
    val out = Files.createTempDirectory("fraud-gold").resolve("gold").toString
    Pipeline.run(spark, n = 1000, seed = 1L, fraudRate = 0.02, outDir = out, creditcardPath = None)
    Pipeline.run(spark, n = 1000, seed = 1L, fraudRate = 0.02, outDir = out, creditcardPath = None)
    assert(spark.read.parquet(s"$out/model_metrics").count() == 1L)
  }
}
