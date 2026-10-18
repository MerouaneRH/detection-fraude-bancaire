package fraud

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CreditCardModelSpec extends AnyFunSuite with BeforeAndAfterAll {

  private val path = "data/raw/creditcard.csv"

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("creditcard-spec")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("le chargement lit 284 807 lignes et 492 fraudes") {
    assume(Files.exists(Paths.get(path)), s"$path absent (téléchargement requis)")
    val df = CreditCardModel.load(spark, path)
    assert(df.count() == 284807L)
    assert(df.filter("is_fraud").count() == 492L)
  }

  test("rappel ≥ 0,70 et précision ≥ 0,80 sur le jeu de test réel (critère 5)") {
    assume(Files.exists(Paths.get(path)), s"$path absent (téléchargement requis)")
    val m = CreditCardModel.evaluate(spark, path, seed = 42L).collect().head
    info(f"précision=${m.getAs[Double]("precision")}%.3f rappel=${m.getAs[Double]("recall")}%.3f seuil=${m.getAs[Double]("threshold")}%.2f")
    assert(m.getAs[String]("dataset") == "creditcard")
    assert(m.getAs[Double]("recall") >= 0.70)
    assert(m.getAs[Double]("precision") >= 0.80)
  }
}
