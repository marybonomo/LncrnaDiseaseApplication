package it.unipa.bigdata.dmi.lda.utility

import it.unipa.bigdata.dmi.lda.config.LDACli
import it.unipa.bigdata.dmi.lda.factory.{LoggerFactory, SparkFactory}
import it.unipa.bigdata.dmi.lda.interfaces.DatasetInterface
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Utility class used to extract data from datasets.
 * @author Mariella Bonomo
 */
class DatasetReader extends DatasetInterface {
  private val sparkSession: SparkSession = SparkFactory.getSparkSession
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
  private val logger: Logger = LoggerFactory.getLogger(classOf[DatasetReader])

  import sqlContext.implicits._

  private var miRNA_disease_DF: Dataset[Row] = null
  private var miRNA_lncRNA_DF: Dataset[Row] = null
  private var lncRNA_disease_DF: Dataset[Row] = null

  private var lncRNA_DF: Dataset[Row] = null
  private var miRNA_DF: Dataset[Row] = null
  private var disease_DF: Dataset[Row] = null

  private var allCombination_DF: Dataset[Row] = null
  private var gsCombination_DF: Dataset[Row] = null


  private def loadDatasetFromCSV(path: String, header: List[String]): Dataset[Row] = {
    assert(header.length == 2)
    val df = sparkSession.sparkContext.textFile(path)
      .map(pair => pair.toUpperCase().trim().split(";"))
      .map(value => (value(0), value(1)))
      .toDF(header.head, header(1))
      .distinct()
      .cache()
    logger.info(s"Total: ${df.count}")
    logger.info(s"${header.head}: ${df.select(header.head).distinct.count}")
    logger.info(s"${header(1)}: ${df.select(header(1)).distinct.count}")
    df
  }

  override def getMirnaLncrna(): Dataset[Row] = {
    assert(LDACli.getMlPath != null)
    if (miRNA_lncRNA_DF == null) {
      logger.info("Loading mirna-lncrna associations")
      miRNA_lncRNA_DF = loadDatasetFromCSV(LDACli.getMlPath, List[String]("mirna", "lncrna"))
    }
    miRNA_lncRNA_DF
  }

  override def getMirnaDisease(): Dataset[Row] = {
    assert(LDACli.getMdPath != null)
    if (miRNA_disease_DF == null) {
      logger.info("Loading mirna-disease associations")
      miRNA_disease_DF = loadDatasetFromCSV(LDACli.getMdPath, List[String]("mirna", "disease"))
    }
    miRNA_disease_DF
  }

  override def getLncrnaDisease(): Dataset[Row] = {
    assert(LDACli.getLdPath != null)
    if (lncRNA_disease_DF == null) {
      logger.info("Loading lncrna-disease associations")
      lncRNA_disease_DF = loadDatasetFromCSV(LDACli.getLdPath, List[String]("lncrna", "disease"))

      val diseases_to_filter = lncRNA_disease_DF.join(getMirnaDisease(), Seq("disease"), "left_anti").select("disease").distinct.rdd.map(r => r.getString(0)).collect.toSet
      // filtro le associazioni con disease che non hanno miRNA in comune con lncRNA
      if (diseases_to_filter.nonEmpty) {
        lncRNA_disease_DF = lncRNA_disease_DF
          .filter(!col("disease").isInCollection(diseases_to_filter))

        logger.info(s"Removing lda without miRNA associated: ${diseases_to_filter.reduce((x, y) => x + ";" + y)}")

        logger.info(s"Disease: ${lncRNA_disease_DF.select("disease").distinct.count}")
        logger.info(s"lncRNA: ${lncRNA_disease_DF.select("lncrna").distinct.count}")
        logger.info(s"Total: ${lncRNA_disease_DF.count}")
      }
    }
    lncRNA_disease_DF
  }

  /**
   * Get all the possible combinations of lncRNA-disease using lncRNA from datasets miRNA-lncRNA, lncRNA-disease and diseases from datasets miRNA-disease, lncRNA-disease.
   */
  override def getAllCombination: Dataset[Row] = {
    val lnc = getLncRNA()
    val dis = getDisease()
    if (allCombination_DF == null && lnc != null && dis != null) {
      logger.info("Loading all lncrna-disease combinations")
      allCombination_DF = lnc.crossJoin(dis).repartition(200).toDF("lncrna", "disease").dropDuplicates().cache()
      logger.info(s"All combination: ${allCombination_DF.count}")
    }
    allCombination_DF
  }

  /**
   * Get all the possible combinations of lncRNA-disease using lncRNA from dataset lncRNA-disease and diseases from dataset lncRNA-disease.
   */
  override def getGSCombination: Dataset[Row] = {
    if (gsCombination_DF == null) {
      gsCombination_DF = getLncrnaDisease().select("lncrna").rdd.map(row => row.getString(0))
        .cartesian(getLncrnaDisease().select("disease").rdd.map(row => row.getString(0)))
        .toDF("lncrna", "disease")
        .dropDuplicates()
        .cache()
      logger.info(s"GS combinations: ${gsCombination_DF.count}")
    }
    gsCombination_DF
  }

  override def getMiRNA: Dataset[Row] = {
    if (miRNA_DF == null) {
      logger.info("Loading all miRNA")
      miRNA_DF = getMirnaDisease().select("mirna").union(getMirnaLncrna().select("mirna")).repartition(10).dropDuplicates().cache()
      logger.info(s"Total: ${miRNA_DF.count}")
    }
    miRNA_DF
  }

  override def getLncRNA: Dataset[Row] = {
    if (lncRNA_DF == null) {
      logger.info("Loading all lncRNA")
      lncRNA_DF = getLncrnaDisease().select("lncrna").union(getMirnaLncrna().select("lncrna")).repartition(10).dropDuplicates().cache()
      logger.info(s"Total: ${lncRNA_DF.count}")
    }
    lncRNA_DF
  }

  override def getDisease: Dataset[Row] = {
    if (disease_DF == null) {
      logger.info("Loading all diseases")
      disease_DF = getMirnaDisease().select("disease").union(getLncrnaDisease().select("disease")).repartition(10).dropDuplicates().cache()
      logger.info(s"Total: ${disease_DF.count}")
    }
    disease_DF
  }
}

