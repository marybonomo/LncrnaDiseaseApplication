package it.unipa.bigdata.dmi.lda.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Armando La Placa
 */
public interface DatasetInterface {
    /**
     * @return The dataset of miRNA-lncRNA associations.
     */
    public Dataset<Row> getMirnaLncrna();
    /**
     * @return The dataset of miRNA-disease associations.
     */
    public Dataset<Row> getMirnaDisease();
    /**
     * @return The dataset of lncRNA-disease associations.
     */
    public Dataset<Row> getLncrnaDisease();
    /**
     * @return The dataset of all the possible lncRNA-disease combinations, extracting the diseases from miRNA-disease and lncRNA-disease, and the lncRNA from miRNA-lncRNA and lncRNA-disease.
     */
    public Dataset<Row> getAllCombination();
    /**
     * @return The dataset of all the possible lncRNA-disease combinations, extracting the diseases lncRNA-disease, and the lncRNA from lncRNA-disease.
     */
    public Dataset<Row> getGSCombination();
    /**
     * @return All the miRNAs from miRNA-disease and miRNA-lncRNA datasets.
     */
    public Dataset<Row> getMiRNA();
    /**
     * @return All the miRNAs from lncRNA-disease and miRNA-lncRNA datasets.
     */
    public Dataset<Row> getLncRNA();
    /**
     * @return All the miRNAs from miRNA-disease and lncRNA-disease datasets.
     */
    public Dataset<Row> getDisease();

}
