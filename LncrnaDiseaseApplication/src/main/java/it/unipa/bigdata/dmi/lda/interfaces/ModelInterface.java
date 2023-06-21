package it.unipa.bigdata.dmi.lda.interfaces;

import it.unipa.bigdata.dmi.lda.model.Prediction;
import it.unipa.bigdata.dmi.lda.model.PredictionFDR;
import org.apache.spark.sql.Dataset;

/**
 * @author Armando La Placa
 */
public interface ModelInterface extends ROCInterface {

    /**
     * @return The dataset of {@link Prediction} from a specified parquet directory.
     */
    Dataset<Prediction> loadScores();
    /**
     * @return The dataset of {@link PredictionFDR} from a specified parquet directory.
     */
    Dataset<PredictionFDR> loadPredictions();

    /**
     * Compute the prediction score using the model given by the {@link it.unipa.bigdata.dmi.lda.factory.ModelFactory}.
     * @return The dataset of computed {@link Prediction}.
     */
    Dataset<Prediction> compute();
    /**
     * Predict the lncRNA-disease associations using the model given by the {@link it.unipa.bigdata.dmi.lda.factory.ModelFactory}.
     * @return The dataset of predictions {@link PredictionFDR}.
     */
    Dataset<PredictionFDR> predict();
}