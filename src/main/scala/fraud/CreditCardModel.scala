package fraud

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Détecteur supervisé pour creditcard.csv (variables V1..V28 anonymisées par PCA). */
object CreditCardModel {

  val FeatureCols: Array[String] = ((1 to 28).map(i => s"V$i") :+ "Amount").toArray
  val DefaultThreshold = 0.5

  def load(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("transaction_id", monotonically_increasing_id().cast("string"))
      .withColumn("is_fraud", col("Class") === 1)

  /** Entraîne sur 80 %, évalue sur 20 %. Retourne une ligne de métriques (schéma de Gold.metrics). */
  def evaluate(spark: SparkSession, path: String, seed: Long, threshold: Double = DefaultThreshold): DataFrame = {
    val data = load(spark, path)
      .withColumn("weight", when(col("is_fraud"), 20.0).otherwise(1.0)) // classes déséquilibrées
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed)

    val pipeline = new Pipeline().setStages(
      Array(
        new VectorAssembler().setInputCols(FeatureCols).setOutputCol("features"),
        new RandomForestClassifier()
          .setLabelCol("Class")
          .setFeaturesCol("features")
          .setWeightCol("weight")
          .setNumTrees(60)
          .setMaxDepth(8)
          .setSeed(seed)
      )
    )

    val scored = pipeline
      .fit(train)
      .transform(test)
      .select(
        col("transaction_id"),
        vector_to_array(col("probability")).getItem(1).as("score"),
        col("is_fraud")
      )

    Gold.metrics(scored, threshold, dataset = "creditcard")
  }
}
