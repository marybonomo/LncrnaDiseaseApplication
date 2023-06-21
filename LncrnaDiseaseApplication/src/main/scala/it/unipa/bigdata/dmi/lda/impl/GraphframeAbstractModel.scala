package it.unipa.bigdata.dmi.lda.impl

import it.unipa.bigdata.dmi.lda.config.LDACli
import it.unipa.bigdata.dmi.lda.factory.{LoggerFactory, SparkFactory}
import it.unipa.bigdata.dmi.lda.interfaces.ModelInterface
import it.unipa.bigdata.dmi.lda.model.{Prediction, PredictionFDR}
import it.unipa.bigdata.dmi.lda.utility.{DatasetReader, ROCFunction}
import org.apache.log4j.Logger
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.graphframes.GraphFrame

/**
 * This is a superclass for the models that rely on graphs to be computed. It uses {@link org.graphframes.GraphFrame} to implements graph in Spark.
 *
 * @author Armando La Placa
 */
abstract class GraphframeAbstractModel() extends ModelInterface {
  private val logger: Logger = LoggerFactory.getLogger(classOf[GraphframeAbstractModel])
  protected val sparkSession: SparkSession = SparkFactory.getSparkSession
  protected val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
  protected val rocFunction: ROCFunction = ROCFunction()
  protected var scores: Dataset[Prediction] = _
  protected var predictions: Dataset[PredictionFDR] = _
  protected val datasetReader: DatasetReader = new DatasetReader()
  protected var graphFrame: GraphFrame = _

  /**
   * Utility function used to store dataframe as CSV. It automatically obtain the type of model used to create the input DataFrame.
   * If the argument {@link it.unipa.bigdata.dmi.lda.enums.CliOption#OUTPUT_OPT} is not set, skip the saving.
   * @param ds DataFrame to store.
   */
  def saveResults(ds: DataFrame): Unit = {
    val outputPath = LDACli.getOutputPath
    val model = this.getClass.getSimpleName.replace("Model", "").toLowerCase()
    if (outputPath != null) {
      val outputPartitions = LDACli.getOutputPartitions
      val claz = Thread.currentThread.getStackTrace()(2).getMethodName
      logger.info(s"Saving ${model}_${claz} into '${outputPath}${model}_${claz}' with ${outputPartitions} partitions")
      ds
        .coalesce(outputPartitions)
        .sort(col("lncrna").asc,col("disease").asc)
        .write
        .option("header", "true")
        .csv(s"${outputPath}${model}_${claz}")
    }
  }

  /**
   * Create the GraphFrame using the miRNA-disease, miRNA-lncRNA and lncRNA-disease combinations.
   * @return The Graph as GraphFrame.
   */
  def getGraphFrame(): GraphFrame = {
    if (graphFrame == null) {
      logger.info("Loading GraphFrame - Creating edges")
      val edges = datasetReader.getMirnaDisease().select(col("mirna").as("src"), col("disease").as("dst"), lit("mda").as("relationship"))
        .union(datasetReader.getMirnaLncrna().select(col("mirna").as("src"), col("lncrna").as("dst"), lit("mla").as("relationship")))
        .union(datasetReader.getAllCombination.select(col("lncrna").as("src"), col("disease").as("dst"), lit("lda").as("relationship")))
        .distinct()
        .repartition(360)
        .cache()
      logger.info(s"Cached edges ${edges.count} rows")
      logger.info("Loading GraphFrame - Creating vertices")
      val lncrna = datasetReader.getLncRNA.select(col("lncrna").as("id"))
        .distinct.withColumn("type", lit("LncRNA"))
      val diseases = datasetReader.getDisease.select(col("disease").as("id"))
        .distinct.withColumn("type", lit("Disease"))
      val mirnas = datasetReader.getMiRNA.select(col("mirna").as("id"))
        .distinct.withColumn("type", lit("miRNA"))
      val vertex = lncrna
        .union(diseases)
        .union(mirnas)
        .distinct
        .cache()
      logger.info(s"Cached vertex ${vertex.count} rows")
      // Create a GraphFrame
      graphFrame = GraphFrame(vertex, edges)
    }
    graphFrame
  }

  override def loadPredictions(): Dataset[PredictionFDR] = {
    assert(LDACli.getPredictionPath != null)
    loadPredictions(LDACli.getPredictionPath)
  }

  override def loadScores(): Dataset[Prediction] = {
    assert(LDACli.getScoresPath != null)
    loadScores(LDACli.getScoresPath)
  }

  /**
   * Compute the AUC over the predictions.
   * @see it.unipa.bigdata.dmi.lda.utility.ROCFunction
   * @see it.unipa.bigdata.dmi.lda.impl.GraphframeAbstractModel
   */
  override def auc(): BinaryClassificationMetrics = {
    if (predictions == null)
      predictions = loadPredictions()
    auc(predictions)
  }

  /**
   * Compute the confusion matrix of the predictions, in the format of TP/FP/TN/FN. The result is a DataFrame.
   */
  override def confusionMatrix(): Dataset[Row] = {
    val cm = loadPredictions().select(col("prediction"), col("gs"))
      .groupBy("gs", "prediction").agg(count("gs").as("count"))
      .sort(col("gs").desc, col("prediction").desc)
    logger.info(s"${this.getClass.getSimpleName} Confusion Matrix")
    cm.show(false)
    cm
  }
  /**
   * Load the scores from the given location.
   */
  protected def loadScores(path: String): Dataset[Prediction] = {
    scores = sparkSession.read.parquet(path).as[Prediction](Encoders.bean(classOf[Prediction])).cache()
    logger.info(s"Loaded pValue scores: ${scores.count}")
    scores
  }
  /**
   * Load the predictions from the given location.
   */
  protected def loadPredictions(path: String): Dataset[PredictionFDR] = {
    predictions = sparkSession.read.parquet(path).as[PredictionFDR](Encoders.bean(classOf[PredictionFDR])).cache()
    logger.info(s"Loaded pValue predictions: ${predictions.count}")
    predictions
  }

  /**
   * Compute the AUC over the predictions given as parameter.
   * @see it.unipa.bigdata.dmi.lda.utility.ROCFunction
   * @see it.unipa.bigdata.dmi.lda.impl.GraphframeAbstractModel
   */
  def auc(fdr: Dataset[PredictionFDR]): BinaryClassificationMetrics = {
    logger.info("AUC/PR curve")
    val metrics = rocFunction.roc(fdr)
    metrics
  }
}
