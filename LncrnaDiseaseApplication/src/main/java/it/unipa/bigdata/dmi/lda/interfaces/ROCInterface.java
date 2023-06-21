package it.unipa.bigdata.dmi.lda.interfaces;

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Armando La Placa
 */
public interface ROCInterface {

    /**
     * Print the confusion matrix of the prediction, in terms of TP, TN, FP, FN. Dataset must contains two columns named
     * {@code prediction} and {@code gs} in the boolean format.
     * @return The dataset representing the confusion matrix in the format {@code [gs, prediction, count]}
     */
    Dataset<Row> confusionMatrix();

    /**
     * Print the values of AUC and precision-recall curve and save the plot as png file in the root directory, using the format {@code dateTime_roc_plot.png}.
     * @return The metrics obtained.
     */
    BinaryClassificationMetrics auc();
}
