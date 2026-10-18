package fraud

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SilverSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("silver-spec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val cols = Seq("transaction_id", "card_id", "merchant_id", "category", "country", "ts", "amount", "is_fraud")

  private def raw(rows: (String, String, String, String, String, String, String, String)*): DataFrame = {
    import spark.implicits._
    rows.toDF(cols: _*)
  }

  private val good = ("t1", "c1", "m1", "fuel", "FR", "1735689600", "42.5", "false")

  test("une ligne valide passe en silver avec les bons types") {
    val (valid, rejects) = Silver.clean(raw(good))
    assert(rejects.count() == 0)
    val r = valid.collect().head
    assert(r.getAs[String]("transaction_id") == "t1")
    assert(r.getAs[Double]("amount") == 42.5)
    assert(r.getAs[Boolean]("is_fraud") == false)
    assert(valid.schema("ts").dataType.typeName == "timestamp")
  }

  test("les lignes invalides sont rejetées avec leur raison (critère 2)") {
    val df = raw(
      good,
      ("", "c1", "m1", "fuel", "FR", "1735689600", "10", "false"),
      ("t3", "c1", "m1", "fuel", "FR", "1735689600", "abc", "false"),
      ("t4", "c1", "m1", "fuel", "FR", "1735689600", "-5", "false"),
      ("t5", "c1", "m1", "fuel", "FR", "pas-un-ts", "10", "false"),
      ("t6", "c1", "m1", "fuel", "FR", "1735689600", "10", "peut-etre")
    )
    val (valid, rejects) = Silver.clean(df)
    assert(valid.count() == 1)
    val reasons = rejects.select("reject_reason").collect().map(_.getString(0)).toSet
    assert(reasons == Set("missing_transaction_id", "invalid_amount", "invalid_ts", "invalid_label"))
    assert(rejects.count() == 5)
  }

  test("les doublons de transaction_id sont supprimés") {
    val (valid, _) = Silver.clean(raw(good, good, good.copy(_3 = "m9")))
    assert(valid.count() == 1)
  }

  test("le job silver est idempotent (critère 1)") {
    val df = raw(good, good, ("t2", "c2", "m2", "online", "US", "1735693200", "99.9", "true"))
    val (v1, r1) = Silver.clean(df)
    val (v2, r2) = Silver.clean(df)
    assert(v1.collect().toSet == v2.collect().toSet)
    assert(r1.collect().toSet == r2.collect().toSet)
  }

  test("bronze transforme des transactions générées en texte brut") {
    val txs = TransactionGenerator.generate(n = 50, seed = 1L, fraudRate = 0.1)
    val b = Bronze.fromTransactions(spark, txs)
    assert(b.columns.toSeq == cols)
    assert(b.count() == 50)
  }

  test("pipeline bronze puis silver sur données générées : rejet < 2 % (critère 2)") {
    val txs = TransactionGenerator.generate(n = 2000, seed = 5L, fraudRate = 0.02)
    val (valid, rejects) = Silver.clean(Bronze.fromTransactions(spark, txs))
    assert(rejects.count().toDouble / txs.size < 0.02)
    assert(valid.count() == txs.size)
  }
}
