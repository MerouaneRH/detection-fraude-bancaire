package fraud

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/** Structured Streaming sur un dossier de fichiers JSON (sans Kafka).
  * Sorties : `<out>/scored` (toutes les transactions valides) et `<out>/alerts`.
  */
object StreamingJob {

  private val rawSchema = StructType(Bronze.Columns.map(StructField(_, StringType)))

  def start(
      spark: SparkSession,
      inDir: String,
      outDir: String,
      checkpointDir: String,
      threshold: Double = Gold.DefaultThreshold,
      once: Boolean = false
  ): StreamingQuery = {
    val raw = spark.readStream.schema(rawSchema).json(inDir)

    // Pas de fenêtre ni de row_number en streaming : validation sans état puis dropDuplicates (état par transaction_id).
    val silver = Silver.project(Silver.flag(raw).filter(col("reject_reason").isNull)).dropDuplicates("transaction_id")
    val scored = Gold.scoreStateless(silver)

    val writer = scored.writeStream
      .option("checkpointLocation", checkpointDir)
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write.mode(SaveMode.Append).parquet(s"$outDir/scored")
        Gold.alerts(batch, threshold).write.mode(SaveMode.Append).parquet(s"$outDir/alerts")
      }

    (if (once) writer.trigger(Trigger.AvailableNow()) else writer.trigger(Trigger.ProcessingTime("5 seconds"))).start()
  }
}
