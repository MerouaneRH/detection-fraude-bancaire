package fraud

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Silver {

  private def blank(c: String): Column = col(c).isNull || trim(col(c)) === ""

  /** Nettoie la couche bronze. Retourne (valides, rejets).
    * Les rejets gardent les colonnes brutes plus `reject_reason`.
    * Les doublons de transaction_id sont supprimés, de façon déterministe.
    */
  /** Typage et validation sans état (utilisable en streaming). Ajoute `_ts`, `_amount`, `_fraud` et `reject_reason`. */
  def flag(raw: DataFrame): DataFrame = {
    val typed = raw
      .withColumn("_ts", col("ts").cast("long"))
      .withColumn("_amount", col("amount").cast("double"))
      .withColumn("_fraud", col("is_fraud").cast("boolean"))

    val reason = when(blank("transaction_id"), "missing_transaction_id")
      .when(blank("card_id"), "missing_card_id")
      .when(col("_ts").isNull, "invalid_ts")
      .when(col("_amount").isNull || col("_amount") <= 0, "invalid_amount")
      .when(col("_fraud").isNull, "invalid_label")

    typed.withColumn("reject_reason", reason)
  }

  /** Colonnes silver à partir de lignes validées par `flag`. */
  def project(validFlagged: DataFrame): DataFrame =
    validFlagged.select(
      col("transaction_id"),
      col("card_id"),
      col("merchant_id"),
      col("category"),
      col("country"),
      col("_ts").cast("timestamp").as("ts"),
      col("_amount").as("amount"),
      col("_fraud").as("is_fraud")
    )

  def clean(raw: DataFrame): (DataFrame, DataFrame) = {
    val flagged = flag(raw)

    val rejects = flagged
      .filter(col("reject_reason").isNotNull)
      .select(Bronze.Columns.map(col) :+ col("reject_reason"): _*)

    val order = Window.partitionBy("transaction_id").orderBy(Bronze.Columns.map(col): _*)
    val valid = project(
      flagged
        .filter(col("reject_reason").isNull)
        .withColumn("_rn", row_number().over(order))
        .filter(col("_rn") === 1)
    )

    (valid, rejects)
  }
}
