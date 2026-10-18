package fraud

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class GoldSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("gold-spec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  // 2025-01-01 12:00:00Z et 2025-01-01 03:00:00Z
  private val noon = Timestamp.valueOf("2025-01-01 12:00:00")
  private val night = Timestamp.valueOf("2025-01-01 03:00:00")

  private def silver(rows: (String, String, String, String, Timestamp, Double, Boolean)*): DataFrame = {
    import spark.implicits._
    rows
      .map { case (id, card, cat, country, ts, amount, fraud) => (id, card, "m1", cat, country, ts, amount, fraud) }
      .toDF("transaction_id", "card_id", "merchant_id", "category", "country", "ts", "amount", "is_fraud")
  }

  test("une transaction étrangère de nuit à montant élevé déclenche une alerte, pas les transactions normales") {
    val normals = (1 to 20).map(i => (s"n$i", "c1", "fuel", "FR", noon, 20.0 + i % 3, false))
    val fraud = ("f1", "c1", "online", "US", night, 400.0, true)
    val df = silver(normals :+ fraud: _*)

    val alerts = Gold.alerts(Gold.score(df), threshold = 0.6).collect()
    assert(alerts.map(_.getAs[String]("transaction_id")).toSet == Set("f1"))
    val rule = alerts.head.getAs[String]("rule")
    assert(rule.contains("foreign_country") && rule.contains("night"))
  }

  test("fraud_by_hour calcule le taux de fraude par heure, pays et catégorie") {
    val df = silver(
      ("a", "c1", "fuel", "FR", noon, 10.0, false),
      ("b", "c1", "fuel", "FR", noon, 12.0, true),
      ("c", "c2", "fuel", "FR", noon, 11.0, false),
      ("d", "c2", "fuel", "FR", noon, 13.0, false)
    )
    val rows = Gold.fraudByHour(df).collect()
    assert(rows.length == 1)
    val r = rows.head
    assert(r.getAs[Long]("tx_count") == 4L)
    assert(r.getAs[Long]("fraud_count") == 1L)
    assert(math.abs(r.getAs[Double]("fraud_rate") - 0.25) < 1e-9)
  }

  test("les métriques de précision et de rappel sont correctes") {
    // seuil 0.5 : alertes sur t1 (fraude), t2 (non fraude) ; t3 fraude manquée
    import spark.implicits._
    val scored = Seq(
      ("t1", 0.9, true),
      ("t2", 0.8, false),
      ("t3", 0.1, true),
      ("t4", 0.0, false)
    ).toDF("transaction_id", "score", "is_fraud")

    val m = Gold.metrics(scored, threshold = 0.5, dataset = "test").collect().head
    assert(math.abs(m.getAs[Double]("precision") - 0.5) < 1e-9)
    assert(math.abs(m.getAs[Double]("recall") - 0.5) < 1e-9)
    assert(m.getAs[String]("dataset") == "test")
  }

  test("précision 0 et rappel 0 quand il n'y a aucune alerte ni fraude") {
    import spark.implicits._
    val scored = Seq(("t1", 0.0, false)).toDF("transaction_id", "score", "is_fraud")
    val m = Gold.metrics(scored, threshold = 0.5, dataset = "vide").collect().head
    assert(m.getAs[Double]("precision") == 0.0)
    assert(m.getAs[Double]("recall") == 0.0)
  }

  // Les légitimes ont aussi des achats de nuit (5 %) ou à l'étranger (4 %) : les règles ne séparent pas parfaitement.
  // Sur 20 000 transactions (graine 42), le compromis mesuré est : seuil 0,5 → précision ≈ 0,64 / rappel ≈ 0,74 ;
  // seuil 0,7 → précision ≈ 0,82 / rappel ≈ 0,54. Aucun seuil n'atteint 0,80 et 0,70 ensemble.
  // Le critère 5 de la spec (précision ≥ 0,80 et rappel ≥ 0,70) porte sur creditcard.csv : voir CreditCardModelSpec.
  test("règles sur synthétique réaliste : seuil par défaut, rappel ≥ 0,70 et précision ≥ 0,50") {
    val txs = TransactionGenerator.generate(n = 20000, seed = 42L, fraudRate = 0.02)
    val (valid, _) = Silver.clean(Bronze.fromTransactions(spark, txs))
    val m = Gold.metrics(Gold.score(valid), Gold.DefaultThreshold, dataset = "synthetic").collect().head
    assert(m.getAs[Double]("recall") >= 0.70)
    assert(m.getAs[Double]("precision") >= 0.50)
  }

  test("un seuil plus haut augmente la précision et baisse le rappel") {
    val txs = TransactionGenerator.generate(n = 20000, seed = 42L, fraudRate = 0.02)
    val (valid, _) = Silver.clean(Bronze.fromTransactions(spark, txs))
    val scored = Gold.score(valid)
    val low = Gold.metrics(scored, 0.5, "synthetic").collect().head
    val high = Gold.metrics(scored, 0.7, "synthetic").collect().head
    assert(high.getAs[Double]("precision") > low.getAs[Double]("precision"))
    assert(high.getAs[Double]("recall") < low.getAs[Double]("recall"))
  }
}
