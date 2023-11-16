package it.unipa.bigdata.dmi.lda.impl

import java.io.{FileOutputStream, PrintWriter}

import it.unipa.bigdata.dmi.lda.builder.PredictionBuilder
import it.unipa.bigdata.dmi.lda.config.LDACli
import it.unipa.bigdata.dmi.lda.factory.LoggerFactory
import it.unipa.bigdata.dmi.lda.model.{Prediction, PredictionFDR}
import it.unipa.bigdata.dmi.lda.utility.FDRFunction
import org.apache.commons.lang.NotImplementedException
import org.apache.log4j.Logger
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.WrappedArray

/**
 * This model refers to the <b>ncPred</b> model implemented <a href="http://alpha.dmi.unict.it/ncPred/">here</a>. This class doesn't implement the prediction model, but permits to generate
 * the matrices of miRNA-lncRNA and miRNA-disease associations that are given as input into the implemented model.
 *
 * @author Mariella Bonomo
 */
class CataniaModel() extends GraphframeAbstractModel() {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CataniaModel])

  /**
   * Load the predictions result file into a dataset of {@link it.unipa.bigdata.dmi.lda.model.Prediction}. Save the loaded results as parquet in the given path {@link it.unipa.bigdata.dmi.lda.enums.CliOption#SCORES_PATH_OPT}.
   * @return The dataset of Prediction loaded from the file located at {@link it.unipa.bigdata.dmi.lda.enums.CliOption#SCORES_PATH_OPT}.
   */
  private def loadFromFile(): Dataset[Prediction] = {
    import sparkSession.implicits._
    val predictions_raw = sparkSession.sparkContext.textFile(LDACli.getScoresPath)
    val header = predictions_raw.first.toUpperCase
    val predictions_DF = predictions_raw.filter(line => !line.equals(header)).map(v => v.toUpperCase.split("\t")).toDF()
    val diseases = header.split("\t").map(v => v.replace("\"", ""))

    val extractLncrnaUDF = udf((arr: WrappedArray[String]) => arr(0).replace("\"", ""))
    val assignDiseaseUDF = udf((arr: WrappedArray[String]) => {
      val values = arr.slice(1, arr.size)
      for (i <- 0 to values.size - 1)
        values.update(i, diseases(i) + "=" + values(i))
      values
    }
    )
    val extractDiseaseUDF = udf((combo: String) => combo.split("=")(0))
    val extractValueUDF = udf((combo: String) => combo.split("=")(1))

    val extracted_predictions = predictions_DF
      .withColumn("lncrna", extractLncrnaUDF(col("value")))
      .withColumn("diseaseValues", assignDiseaseUDF(col("value")))
      .drop("value")
      .withColumn("diseaseValue", explode(col("diseaseValues")))
      .drop("diseaseValues")
      .withColumn("disease", extractDiseaseUDF(col("diseaseValue")))
      .withColumn("score", extractValueUDF(col("diseaseValue")))
      .drop("diseaseValue")
      .sort(col("score").desc)
      .coalesce(1)
      .withColumn("rank", monotonically_increasing_id())

    val lncRNA_disease_DF = datasetReader.getLncrnaDisease()
    scores = sparkSession.createDataset(
      extracted_predictions.join(
        broadcast(lncRNA_disease_DF.withColumn("value", lit(1)).na.fill(0, Seq("value"))).as("lncRNA_disease_DF"),
        lncRNA_disease_DF("lncrna").equalTo(extracted_predictions("lncrna")).and(lncRNA_disease_DF("disease").equalTo(extracted_predictions("disease"))),
        "fullOuter"
      )
        .select(col("rank"), extracted_predictions("lncrna"), extracted_predictions("disease"), extracted_predictions("score").as("score").cast(DoubleType), col("value").as("gs").cast(DoubleType))
        .na.fill(0.0, Seq("gs"))
        .na.fill(0.0, Seq("score"))
        .rdd.map(row => {
        val lnc = row.getString(row.fieldIndex("lncrna"))
        val disease = row.getString(row.fieldIndex("disease"))
        val score = row.getDouble(row.fieldIndex("score"))
        val gs = row.getDouble(row.fieldIndex("gs"))
        val rank = row.getLong(row.fieldIndex("rank"))
        new PredictionBuilder()
          .setLncrna(lnc)
          .setDisease(disease)
          .setScore(score)
          .setGs(if (gs == 0.0) false else true)
          .setParameter("rank", rank.toString)
          .build()
      }))(Encoders.bean(classOf[Prediction]))
      .repartition(360)
      .cache()
    logger.info(s"Caching scores ${scores.count()}")
    saveResults(scores
      .select("lncrna", "disease", "score", "gs"))
    scores
  }

  /**
   * This method is called inside the methods {@link it.unipa.bigdata.dmi.lda.impl.CataniaModel#compute()} and saves the matrices used by the ncPred model in the root directory.
   */
  private def createMatrix(): Unit = {
    logger.warn("Creating matrices of mirna-lncrna and mirna-disease ")
    val mirnas = datasetReader.getMiRNA
    val lncrnas = datasetReader.getLncRNA.select(col("lncrna").as("row")).repartition(100)
    val diseases = datasetReader.getDisease.select(col("disease").as("column")).repartition(100)
    val mirna_disease = datasetReader.getMirnaDisease().withColumn("value", lit(1))
    val mirna_lncrna = datasetReader.getMirnaLncrna().withColumn("value", lit(1))

    writeFile(mirnas.select(col("mirna").as("column")).repartition(100), lncrnas, "lncrna", "mirna", mirna_lncrna)
    writeFile(diseases, mirnas.select(col("mirna").as("row")).repartition(100), "mirna", "disease", mirna_disease)
  }

  /**
   * Utility function used to write the dataset, with the header, obtained by combination of two column/row datasets. The file is stored in the root directory.
   * @param columns Dataset of columns data.
   * @param rows Dataset of rows data.
   * @param row Data type as string of the rows. Used to create the header of the file.
   * @param column Data type as string of the columns. Used to create the header of the file.
   * @param source Dataset used to filter out the combinations of {@code columns x rows} that don't belong to it.
   */
  private def writeFile(columns: DataFrame, rows: DataFrame, row: String, column: String, source: DataFrame): Unit = {
    logger.info(s"Writing matrix ${row}-${column} into '${row}-${column}.matrix.txt'")
    val output_file = s"${row}-${column}.matrix.txt"
    var combinations = rows.crossJoin(columns).repartition(1000)
      .cache()
    logger.info(s"Caching combinations ${combinations.count}")
    combinations = combinations
      .join(broadcast(source), combinations("column").equalTo(source(column)).and(combinations("row").equalTo(source(row))), "fullOuter")
      .select(coalesce(source(column), combinations("column")).as("column"), coalesce(source(row), combinations("row")).as("row"), source("value"))
      .na.fill(0, Seq("value"))
      .groupBy("row", "column").agg(max(col("value")).as("val"))
      .select(col("row"), col("column"), col("val").as("value"))
      .repartition(1000).cache()
    logger.info(s"Cleaning all combinations ${combinations.count}")
    // Header
    val orderedColumn = columns.rdd.map(r => r.getString(0)).collect
    scala.util.Sorting.quickSort(orderedColumn)
    val header = orderedColumn.map(v => '"' + v + '"').mkString("\t")
    // Body
    val concatUDF = udf((row: String, value: Long) => s"${row}=${value}")
    logger.info(s"Collecting combinations")
    val collected = combinations.withColumn("rowValue", concatUDF(col("column"), col("value"))).groupBy("row").agg(sort_array(collect_list(col("rowValue"))).as("values"))
      .sort(col("row").asc)
      .rdd
      .map((row: Row) => (row.getString(0), row.getAs[WrappedArray[String]](1))).collect.map { case (column: String, values: WrappedArray[String]) => '"' + column + '"' + "\t" + values.map(v => v.split("=")(1).mkString(",")).array.mkString("\t") }
    combinations.unpersist()
    // Write
    val pw = new PrintWriter(new FileOutputStream(output_file, false))
    pw.write(header + "\n")
    collected.foreach(line => pw.write(line + "\n"))
    pw.close

  }

  /**
   * If not already loaded, load the predictions from the default resource predictions folder, cache it and return.
   */
  override def loadPredictions(): Dataset[PredictionFDR] = {
    if (predictions == null) {
      val tmp = sparkSession.read.parquet(s"resources/predictions/${LDACli.getVersion}/catania_fdr/").withColumn("gs", when(col("gs").equalTo(1.0), true).otherwise(false).as("gs"))
      val names = classOf[PredictionFDR].getDeclaredFields.union(classOf[PredictionFDR].getSuperclass.getDeclaredFields).map(f => f.getName)
      val mapColumn: Column = map(tmp.drop(names: _*).columns.tail.flatMap(name => Seq(lit(name), col(s"$name"))): _*)
      predictions = tmp.withColumn("parameters", mapColumn).select("parameters", names: _*)
        .as[PredictionFDR](Encoders.bean(classOf[PredictionFDR])).cache()
    }
    logger.info(s"Caching Catania predictions: ${predictions.count()}")
    predictions
  }

  /**
   * Compute the AUC over the predictions.
   * @see it.unipa.bigdata.dmi.lda.utility.ROCFunction
   * @see it.unipa.bigdata.dmi.lda.impl.GraphframeAbstractModel
   */
  override def auc(): BinaryClassificationMetrics = {
    if (predictions == null) {
      logger.info("AUC: loading predictions")
      predictions = loadPredictions()
    }
    logger.info("AUC: computing")
    auc(predictions.withColumn("fdr", lit(1) - col("fdr") as "fdr")
      .as[PredictionFDR](Encoders.bean(classOf[PredictionFDR])))
  }

  /**
   * Compute the confusion matrix of the predictions, in the format of TP/FP/TN/FN. The result is a DataFrame.
   */
  override def confusionMatrix(): DataFrame = {
    val scores = loadPredictions()
      .select(col("prediction"), when(col("gs").equalTo(1.0), true).otherwise(false).as("gs"))
      .groupBy("gs", "prediction").agg(count("gs").as("count"))
      .sort(col("gs").desc, col("prediction").desc)
    println("------------\nCatania Confusion Matrix")
    scores.show(false)
    println("------------")
    scores
  }

  /**
   * This method is not implemented. It just recall {@link it.unipa.bigdata.dmi.lda.impl.CataniaModel#createMatrix()} and throws an error.
   */
  override def compute(): Dataset[Prediction] = {
    createMatrix()
    throw new NotImplementedException("This application doesn't implement the prediction functionality of nPred model. It is possible to create the matrix file to be submitted for the execution of the model.")
  }

  /**
   * Apply the FDR correction to the computed scores and return the predictions.
   */
  override def predict(): Dataset[PredictionFDR] = {
    val scores = loadScores()
    predictions = FDRFunction().computeFDR(scores)
    logger.info("catania predictions")
    predictions.show(false)
    saveResults(predictions
      .select("lncrna", "disease", "score", "fdr", "rank", "gs", "prediction"))
    predictions
  }

  /**
   * Return the scores from the default folder located at {@code resources/predictions/<hmdd_version>/catania/} or, if {@link it.unipa.bigdata.dmi.lda.enums.CliOption#SCORES_PATH_OPT} is set,
   * load the scores from the given location.
   */
  override def loadScores(): Dataset[Prediction] = {
    if (LDACli.getScoresPath != null) {
      scores = loadFromFile()
    }
    else if (scores == null)
      scores = sparkSession.createDataset(
        sparkSession.read.parquet(s"resources/predictions/${LDACli.getVersion}/catania/")
          .withColumnRenamed("PValue", "score")
          .rdd.map(row => {
          val lnc = row.getString(row.fieldIndex("lncrna"))
          val disease = row.getString(row.fieldIndex("disease"))
          val score = row.getDouble(row.fieldIndex("score"))
          val gs = row.getDouble(row.fieldIndex("gs"))
          new PredictionBuilder()
            .setLncrna(lnc)
            .setDisease(disease)
            .setScore(score)
            .setGs(if (gs == 0.0) false else true)
            .build()
        }))(Encoders.bean(classOf[Prediction]))
        .cache()
    logger.info(s"Caching Catania scores: ${scores.count()}")
    scores.show(false)
    scores
  }
}
