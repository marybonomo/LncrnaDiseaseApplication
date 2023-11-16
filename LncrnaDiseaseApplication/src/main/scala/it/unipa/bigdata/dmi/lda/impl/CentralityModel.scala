package it.unipa.bigdata.dmi.lda.impl

import it.unipa.bigdata.dmi.lda.builder.PredictionBuilder
import it.unipa.bigdata.dmi.lda.config.LDACli
import it.unipa.bigdata.dmi.lda.factory.LoggerFactory
import it.unipa.bigdata.dmi.lda.model.{Prediction, PredictionFDR}
import it.unipa.bigdata.dmi.lda.utility.FDRFunction
import org.apache.log4j.Logger
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * The centrality model is the implementation of our model. It is based on a tripartite graph that uses miRNA as link between lncRNA and diseases.
 *
 * @author Mariella Bonomo
 */
class CentralityModel() extends GraphframeAbstractModel() {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CentralityModel])

  /**
   * If not already loaded, load the predictions from the default resource predictions folder, cache it and return.
   */
  override def loadPredictions(): Dataset[PredictionFDR] = {
    if (predictions == null) {
      val tmp = sparkSession.read.parquet(s"resources/predictions/${LDACli.getVersion}/centrality_fdr/${LDACli.getAlpha}").withColumnRenamed("PValue", "score")
      val names = classOf[PredictionFDR].getDeclaredFields.union(classOf[PredictionFDR].getSuperclass.getDeclaredFields).map(f => f.getName)
      val mapColumn: Column = map(tmp.drop(names: _*).columns.tail.flatMap(name => Seq(lit(name), col(s"$name"))): _*)
      predictions = tmp.withColumn("parameters", mapColumn).select("parameters", names: _*)
        .as[PredictionFDR](Encoders.bean(classOf[PredictionFDR])).cache()
    }
    logger.info(s"Caching Centrality predictions: ${predictions.count()}")
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
  override def confusionMatrix(): Dataset[Row] = {
    val scores = loadPredictions().select(col("prediction"), col("gs"))
      .groupBy("gs", "prediction").agg(count("gs").as("count"))
      .sort(col("gs").desc, col("prediction").desc)
    logger.info("------------\nCentrality Confusion Matrix")
    scores.show(false)
    logger.info("------------")
    scores
  }

  /**
   * Apply the formula to the lncRNA-disease association, using several neighborhood informations. After computing the first and second term of the formula,
   * apply the alpha value (default: 0.25), then save the results in CSV format.
   */
  override def compute(): Dataset[Prediction] = {
    // Initialize Graphframe
    getGraphFrame()
    // Retrieve neighborhood informations as DataFrame
    val preliminary: DataFrame = preliminaryComputation
    // Compute first and second term of the formula    
    val (firstTerm, secondTerm) = computeScores(preliminary)
    // Join first and second term into one dataframe
    val pre_scores = firstTerm
      .join(secondTerm, firstTerm("lncrna").equalTo(secondTerm("lncrna")).and(firstTerm("disease").equalTo(secondTerm("disease"))))
      .drop(secondTerm("lncrna")).drop(secondTerm("disease"))
      .cache()
    logger.info(s"Scores computed (without alpha) ${pre_scores.count}")
    val alpha_ = LDACli.getAlpha
    val GS_broadcast = sparkSession.sparkContext.broadcast(datasetReader.getLncrnaDisease().rdd.map(r => s"${r.getString(0)};${r.getString(1)}").map(pair => pair.toUpperCase().trim()).collect())
    logger.info("Applying alpha to scores")
    // Apply alpha and return a Dataset of Prediction
    scores = sparkSession.createDataset[Prediction](
      pre_scores
        .withColumn("firstScore", col("firstTerm") * lit(alpha_))
        .withColumn("secondScore", col("secondTerm") * lit(1 - alpha_))
        .withColumn("score", col("firstScore") + col("secondScore"))
        .rdd.map(row => {
        val dis = row.getString(row.fieldIndex("disease"))
        val lnc = row.getString(row.fieldIndex("lncrna"))
        val score = row.getDouble(row.fieldIndex("score"))
        val gs = GS_broadcast.value.contains(lnc + ";" + dis)
        val firstScore = row.get(row.fieldIndex("firstScore"))
        val numeratore = row.get(row.fieldIndex("numeratore"))
        val denominatore = row.get(row.fieldIndex("denominatore"))
        val secondScore = row.get(row.fieldIndex("secondScore"))
        val n = row.get(row.fieldIndex("n"))
        val mld = row.get(row.fieldIndex("mld"))
        new PredictionBuilder()
          .setLncrna(lnc)
          .setDisease(dis)
          .setGs(gs)
          .setScore(score)
          .setParameter("firstScore", firstScore.toString)
          .setParameter("secondScore", secondScore.toString)
          .setParameter("numeratore", numeratore.toString)
          .setParameter("denominatore", denominatore.toString)
          .setParameter("n", n.toString)
          .setParameter("mld", mld.toString)
          .build()
      }))(Encoders.bean(classOf[Prediction])).cache()
    logger.info(s"Scores computed ${scores.count}")
    scores.show(false)
    saveResults(scores.select("lncrna", "disease", "score", "gs"))
    scores
  }

  /**
   * Apply the FDR correction to the computed scores and return the predictions.
   */
  override def predict(): Dataset[PredictionFDR] = {
    scores = compute()
    predictions = FDRFunction().computeFDR(scores)
    logger.info("centrality predictions")
    predictions.show(false)
    saveResults(predictions
      .select("lncrna", "disease", "score", "fdr", "rank", "gs", "prediction"))
    predictions
  }

  /**
   * Return the scores from the default folder located at {@code resources/predictions/<hmdd_version>/centrality/}.
   */
  override def loadScores(): Dataset[Prediction] = {
    if (scores == null)
      scores = sparkSession.read.parquet(s"resources/predictions/${LDACli.getVersion}/centrality/")
        .withColumnRenamed("PValue", "score").as[Prediction](Encoders.bean(classOf[Prediction]))
        .cache()
    logger.info(s"Caching Centrality scores: ${scores.count()}")
    scores
  }

  /**
   * This method is the core of the Centrality model. It includes the formula used to compute the scores for the lncRNA-disease associations.
   * @param preliminary Dataset containing additional informations used for the score computation.
   * @return Two dataframe containing the first and second term of the formula.
   *         These terms are multiplied with an alpha factor specified by the user using the {@link it.unipa.bigdata.dmi.lda.enums.CliOption#ALPHA_OPT} argument.
   */
  private def computeScores(preliminary: DataFrame): (DataFrame, DataFrame) = {
        
    val firstTerm = preliminary
      .select("lncrna", "disease", "mld", "n")
      .distinct
      .withColumn("firstTerm", col("mld") / col("n"))
      //.withColumn("firstTerm", col("mld") / col("n"))   

    val secondTerm = preliminary
      .groupBy("lncrna", "disease")
      .agg(sum(col("mll[i,x]")+col("mld[x,j]")).as("numeratore"), sum(col("mll[x,x]") + col("mld[x,j]")).as("denominatore"))
      //.agg(sum(col("mll[i,x]") * col("mld[x,j]")).as("numeratore"), sum(col("mll[x,x]") * col("nj")).as("denominatore"))
      .withColumn("secondTerm", col("numeratore") / col("denominatore"))

    (firstTerm, secondTerm)
  }

  /**
   * The preliminary computations consist of loading several neighborhood informations from the graphFrame built by the superclass {@link it.unipa.bigdata.dmi.lda.impl.GraphframeAbstractModel}.
   * These informations consist of:
   * <ul>
   *  <li>mld: miRNAs in common between the given lncRNA and disease</li>
   *  <li>mll: miRNAs in common between two neighbors lncRNA</li>
   *  <li>nj: miRNAs associated to a specific disease</li>
   *  <li>mll_xx: miRNAs associated to a specific disease</li>
   *  <li>n: minimum number of miRNAs between nj and mll</li>
   * </ul>
   * @return Dataset containing all the previous fields.
   */
  private def preliminaryComputation = {
    
    /*val mld = graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(disease)").filter("lncrna.type == 'LncRNA' and disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("disease.id").as("disease"))
      .distinct
      .groupBy("lncrna", "disease").agg(count("mirna").as("mld"))
      .repartition(2400)
      .cache()
    logger.info(s"Caching ${mld.count} MLD")
    var mll = graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(lncrna2)").filter("lncrna.type == 'LncRNA' and lncrna2.type == 'LncRNA' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("lncrna2.id").as("lncrna2"))
      .distinct
      .groupBy("lncrna", "lncrna2").agg(count("mirna").as("mll"))
      .repartition(1200)
      .cache()
    logger.info(s"Caching ${mll.count} MLL")
    val nj = graphFrame.find("(mirna)-[]->(disease)").filter("disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("disease.id").as("disease"))
      .distinct
      .groupBy("disease").agg(count("mirna").as("nj"))
      .repartition(18)
      .cache()
    logger.info(s"Caching ${nj.count} nj")
    val mll_xx = mll.filter("lncrna == lncrna2").select(col("lncrna"), col("mll").as("mll_xx")).cache()
    logger.info(s"Caching ${mll_xx.count} MLL[x,x]")
    logger.info("Computing scores")
     
    mll = mll.filter("lncrna!=lncrna2")      
   */
    
    //INTERSEZ. MIRNA L-D
    val mld1 = graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(disease)").filter("lncrna.type == 'LncRNA' and disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("disease.id").as("disease"))
      .distinct
      .groupBy("lncrna", "disease").agg(count("mirna").as("mld1"))
      .repartition(2400)
      .cache()
     //.collectAsList()
    //logger.info(s"Caching ${mld1.count()} MLD1")
   
    //MIRNA D
    val md = graphFrame.find("(mirna)-[]->(disease)").filter("disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("disease.id").as("disease"))
      .distinct
      .groupBy("mirna").agg(count("mirna").as("md"))
      //.repartition(18)
      //.cache()
      //.collectAsList()
    //logger.info(s"Caching ${md.count()} md")
          
    //MIRNA L
    val ml = graphFrame.find("(mirna)-[]->(lncrna)").filter("lncrna.type == 'LncRNA' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("lncrna.id").as("lncrna"))
      .distinct
      .groupBy("mirna").agg(count("mirna").as("ml"))
      //.repartition(18)
      //.cache()    
     // .collectAsList()
    //logger.info(s"Caching ${ml.count()} ml")
    
    // UNIONE MIRNA(L-D)
     val unione = graphFrame.find("(mirna)-[]->(lncrna)").filter("lncrna.type == 'LncRNA' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("lncrna.id").as("lncrna"))
      .distinct()
      .union(graphFrame.find("(mirna)-[]->(disease)").filter("disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("disease.id").as("disease")))
      .distinct()
      .groupBy("mirna").agg(count("mirna").as("unione"))
      //.collectAsList()
      //logger.info(s"Caching ${unione.count()} unione") 
     
    //MIRNA VICINATO 
    var mll1 = graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(lncrna2)").filter("lncrna.type == 'LncRNA' and lncrna2.type == 'LncRNA' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("lncrna2.id").as("lncrna2"))
      .distinct
      .groupBy("lncrna", "lncrna2").agg(count("mirna").as("mll1"))
      .repartition(1200)
      .cache()
      //.collectAsList()
    //logger.info(s"Caching ${mll1.count()} MLL1")
    
    //UNIONE VICINATO (denominatore II termine)
   /* val unione_vic =graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(lncrna2)").filter("lncrna.type == 'LncRNA' and lncrna2.type == 'LncRNA' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("lncrna2.id").as("lncrna2"))
      .distinct
      .union(graphFrame.find("(mirna)-[]->(disease)").filter("disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("disease.id").as("disease")))
      .distinct()
      .groupBy("mirna").agg(count("mirna").as("unione_vic"))
      logger.info(s"Caching ${unione_vic.count()} unione_vic") */
    
    
    //////////////////////////////////////////////////////////
     val mld = graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(disease)").filter("lncrna.type == 'LncRNA' and disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("disease.id").as("disease"))
      .distinct
      .groupBy("lncrna", "disease").agg(count("mirna").as("mld"))
      .repartition(2400)
      .cache()
      //.collectAsList()
    logger.info(s"Caching ${mld.count} MLD")
     
    var mll = graphFrame.find("(mirna)-[]->(lncrna); (mirna)-[]->(lncrna2)").filter("lncrna.type == 'LncRNA' and lncrna2.type == 'LncRNA' and mirna.type == 'miRNA'")
      .select(col("lncrna.id").as("lncrna"), col("mirna.id").as("mirna"), col("lncrna2.id").as("lncrna2"))
      .distinct      
      .groupBy("lncrna", "lncrna2").agg(count("mirna").as("mll"))
      .repartition(1200)
      .cache()
    logger.info(s"Caching ${mll.count} MLL")
        
    val nj = graphFrame.find("(mirna)-[]->(disease)").filter("disease.type == 'Disease' and mirna.type == 'miRNA'")
      .select(col("mirna.id").as("mirna"), col("disease.id").as("disease"))
      .distinct
      .groupBy("disease").agg(count("mirna").as("nj"))
      .repartition(18)
      .cache()
    logger.info(s"Caching ${nj.count} nj")
    
    val mll_xx = mll.filter("lncrna == lncrna2").select(col("lncrna"), col("mll").as("mll_xx")).cache()
    logger.info(s"Caching ${mll_xx.count} MLL[x,x]")
    logger.info("Computing scores")
     
    mll = mll.filter("lncrna!=lncrna2")
        
    val all_combination = datasetReader.getAllCombination
    
    val step0 = all_combination.join(mld, mld("lncrna").equalTo(all_combination("lncrna")).and(mld("disease").equalTo(all_combination("disease"))), "fullOuter")
      .drop(mld("lncrna")).drop(mld("disease")).na.fill(0, Seq("mld")).cache()
    logger.info(s"Caching step0 ${step0.count}")        
    val step1 = step0.join(broadcast(nj), step0("disease").equalTo(nj("disease")), "fullOuter")
      .drop(nj("disease"))
      .na.fill(0, Seq("nj")).cache()
    logger.info(s"Caching step1 ${step1.count}")
    val step2 = step1.join(mll, step1("lncrna").equalTo(mll("lncrna")))
      .drop(mll("lncrna"))
      .withColumnRenamed("mll", "mll[i,x]")
      .join(mll_xx, mll_xx("lncrna").equalTo(step1("lncrna")))
      .drop(mll_xx("lncrna"))
      .withColumnRenamed("mll_xx", "mll[i,i]")      
      .join(mll_xx, mll_xx("lncrna").equalTo(col("lncrna2")))
      .drop(mll_xx("lncrna"))
      .withColumnRenamed("mll_xx", "mll[x,x]").cache()
    logger.info(s"Caching step2 ${step2.count}")
    val step3 = step2.join(mld
      .withColumnRenamed("disease", "disease_")
      .withColumnRenamed("lncrna", "lncrna_")
      .withColumnRenamed("mld", "mld[x,j]"), col("disease").equalTo(col("disease_"))
      .and(col("lncrna2").equalTo(col("lncrna_"))), "fullOuter")
      .drop("disease_", "lncrna_")
      .na.fill(0, Seq("mld[x,j]")).cache()
    logger.info(s"Caching step3 ${step3.count}")
    val step4 = step3.withColumn("n", (col("nj")+ col("mll[i,i]"))).cache()
    //val step4 = step3.withColumn("n", least(col("nj")+ col("mll[i,i]"))).cache()
    logger.info(s"Caching step4 ${step4.count}")
    
      
    step1.unpersist()
    step2.unpersist()
    step3.unpersist()
    step4
    
  }
}
