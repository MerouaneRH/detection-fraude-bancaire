package fraud

import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Gold {

  val HomeCountry = "FR"
  val DefaultThreshold = 0.6

  /** Ajoute `score` (0 à 1) et `rule` (signaux déclenchés) à la couche silver.
    * Signaux : pays étranger (0,4), heure de nuit (0,3), montant anormal pour la carte (0 à 0,3).
    */
  def score(silver: DataFrame): DataFrame = {
    val perCard = Window.partitionBy("card_id")
    val sd = stddev_pop("amount").over(perCard)
    val z = when(sd.isNull || sd === 0.0, lit(0.0)).otherwise((col("amount") - avg("amount").over(perCard)) / sd)

    val foreign = col("country") =!= HomeCountry
    val night = hour(col("ts")) < 6
    val amountPart = least(greatest(col("_z"), lit(0.0)) / 3.0, lit(1.0))

    silver
      .withColumn("_z", z)
      .withColumn(
        "score",
        when(foreign, 0.4).otherwise(0.0) + when(night, 0.3).otherwise(0.0) + amountPart * 0.3
      )
      .withColumn(
        "rule",
        concat_ws("+", when(foreign, "foreign_country"), when(night, "night"), when(col("_z") >= 2.0, "high_amount"))
      )
      .drop("_z")
  }

  /** Variante sans état pour le streaming : le montant est comparé à un seuil absolu (200) au lieu de l'historique de la carte. */
  def scoreStateless(silver: DataFrame): DataFrame = {
    val foreign = col("country") =!= HomeCountry
    val night = hour(col("ts")) < 6
    val highAmount = col("amount") >= 200.0
    silver
      .withColumn(
        "score",
        when(foreign, 0.4).otherwise(0.0) + when(night, 0.3).otherwise(0.0) +
          least(col("amount") / 200.0, lit(1.0)) * 0.3
      )
      .withColumn("rule", concat_ws("+", when(foreign, "foreign_country"), when(night, "night"), when(highAmount, "high_amount")))
  }

  def alerts(scored: DataFrame, threshold: Double): DataFrame =
    scored
      .filter(col("score") >= threshold)
      .select("transaction_id", "card_id", "ts", "amount", "score", "rule")

  def fraudByHour(silver: DataFrame): DataFrame =
    silver
      .groupBy(date_trunc("hour", col("ts")).as("hour"), col("country"), col("category"))
      .agg(
        count(lit(1)).as("tx_count"),
        sum(when(col("is_fraud"), 1L).otherwise(0L)).as("fraud_count")
      )
      .withColumn("fraud_rate", col("fraud_count").cast("double") / col("tx_count"))

  /** Une ligne : run_ts, dataset, precision, recall, threshold. */
  def metrics(scored: DataFrame, threshold: Double, dataset: String): DataFrame = {
    val alert = col("score") >= threshold
    def n(c: Column): Column = sum(when(c, 1L).otherwise(0L))
    val counts = scored.agg(
      n(alert && col("is_fraud")).as("tp"),
      n(alert && !col("is_fraud")).as("fp"),
      n(!alert && col("is_fraud")).as("fn")
    )
    def ratio(num: String, den: Column): Column =
      when(den === 0L, 0.0).otherwise(col(num).cast("double") / den)

    counts.select(
      current_timestamp().as("run_ts"),
      lit(dataset).as("dataset"),
      ratio("tp", col("tp") + col("fp")).as("precision"),
      ratio("tp", col("tp") + col("fn")).as("recall"),
      lit(threshold).as("threshold")
    )
  }

  def write(df: DataFrame, path: String): Unit =
    df.write.mode(SaveMode.Overwrite).parquet(path)
}
