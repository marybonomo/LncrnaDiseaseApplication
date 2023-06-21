package it.unipa.bigdata.dmi.lda.utility

import it.unipa.bigdata.dmi.lda.factory.{LoggerFactory, SparkFactory}
import it.unipa.bigdata.dmi.lda.model.{Prediction, PredictionFDR}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders}

/**
 * Utility function used to compute FDR correction.
 *
 * @param alpha Default value is 0.05.
 * @author Armando La Placa
 */
case class FDRFunction(alpha: Double = 0.05) {
  val sparkSession = SparkFactory.getSparkSession
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
  private val logger: Logger = LoggerFactory.getLogger(classOf[FDRFunction])

  /**
   * Compute FDR over dataframe containing correction of the prediction scores.
   *
   * @param scores A dataframe with a column named "score" containing the prediction scores of the model.
   * @return A copy of the dataframe "scores" with a column named "fdr" containing the score correction and prediction.
   */
  def computeFDR(scores: Dataset[Prediction]): Dataset[PredictionFDR] = {
    logger.info("Computing FDR correction")
    val total_values = scores.count()
    val fdr = scores
      .filter(!col("score").isNaN) // rimuovo gli score nulli, di cui non abbiamo conoscenza
      .sort(Prediction.getScoreCol.desc).coalesce(1).withColumn("rank", monotonically_increasing_id()).repartition(200) // aggiungo un campo rank
      .withColumn("rank", col("rank") + lit(1)) // faccio partire i rank da 1 invece che da 0
      .withColumn(PredictionFDR.getFdrCol.toString(), (col("rank") / lit(total_values)) * lit(alpha)) // aggiungo la colonna FDR
      .sort(col("rank").asc) // ordino per rank crescente
      .cache()
    logger.info(s"Caching FDR corrections ${fdr.count}")
    // Cerco il massimo rank k tale per cui score(k) >= fdr(k)
    var max_rank_centrality = fdr.count.toDouble
    try {
      max_rank_centrality = fdr.filter(Prediction.getScoreCol.geq(PredictionFDR.getFdrCol)).agg(max(col("rank")).as("max")).select("max").first.getLong(0).toDouble
    } catch {
      case _: java.lang.NullPointerException => logger.info("Max value")
    }
    val predictions = fdr.withColumn(PredictionFDR.getPredictionCol.toString(), col("rank").leq(lit(max_rank_centrality))).cache()
    fdr.unpersist()
    logger.info(s"Caching FDR predictions ${predictions.count}")
    predictions.as[PredictionFDR](Encoders.bean(classOf[PredictionFDR]))
  }
}
