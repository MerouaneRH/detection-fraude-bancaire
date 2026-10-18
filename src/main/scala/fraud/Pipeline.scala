package fraud

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession

/** Batch complet : génération, bronze, silver, gold. Écrit les tables lues par Streamlit. */
object Pipeline {

  def run(
      spark: SparkSession,
      n: Int,
      seed: Long,
      fraudRate: Double,
      outDir: String,
      threshold: Double = Gold.DefaultThreshold,
      creditcardPath: Option[String] = None
  ): Unit = {
    val txs = TransactionGenerator.generate(n, seed, fraudRate)
    val (silver, _) = Silver.clean(Bronze.fromTransactions(spark, txs))
    val scored = Gold.score(silver).cache()

    Gold.write(Gold.fraudByHour(silver).coalesce(1), s"$outDir/fraud_by_hour")
    Gold.write(Gold.alerts(scored, threshold).coalesce(1), s"$outDir/alerts")

    val synthetic = Gold.metrics(scored, threshold, dataset = "synthetic")
    val metrics = creditcardPath match {
      case Some(p) => synthetic.union(CreditCardModel.evaluate(spark, p, seed))
      case None    => synthetic
    }
    Gold.write(metrics.coalesce(1), s"$outDir/model_metrics")
    scored.unpersist()
  }

  /** Arguments : <outDir> [n=20000] [seed=42] [fraudRate=0.02] [creditcard.csv] [seuilRègles=0.6] */
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "usage: Pipeline <outDir> [n] [seed] [fraudRate] [creditcard.csv]")
    val spark = SparkSession.builder().master("local[*]").appName("fraud-pipeline").config("spark.ui.enabled", "false").getOrCreate()
    try {
      val creditcard = args.lift(4).filter(p => Files.exists(Paths.get(p)))
      run(
        spark,
        n = args.lift(1).map(_.toInt).getOrElse(20000),
        seed = args.lift(2).map(_.toLong).getOrElse(42L),
        fraudRate = args.lift(3).map(_.toDouble).getOrElse(0.02),
        outDir = args(0),
        threshold = args.lift(5).map(_.toDouble).getOrElse(Gold.DefaultThreshold),
        creditcardPath = creditcard
      )
    } finally spark.stop()
  }
}
