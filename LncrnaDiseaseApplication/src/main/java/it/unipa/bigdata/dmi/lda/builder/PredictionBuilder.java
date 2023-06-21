package it.unipa.bigdata.dmi.lda.builder;

import it.unipa.bigdata.dmi.lda.model.Prediction;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Mariella Bonomo
 */
public class PredictionBuilder implements Serializable {
    protected Prediction prediction;

    public PredictionBuilder() {
        prediction = new Prediction();
    }

    public PredictionBuilder setLncrna(String lncrna) {
        prediction.setLncrna(lncrna);
        return this;
    }

    public PredictionBuilder setDisease(String disease) {
        prediction.setDisease(disease);
        return this;
    }

    public PredictionBuilder setScore(Double score) {
        prediction.setScore(score);
        return this;
    }

    public PredictionBuilder setParameters(Map<String, String> parameters) {
        prediction.setParameters(parameters);
        return this;
    }

    public PredictionBuilder setParameter(String key, String obj) {
        prediction.setParameter(key, obj);
        return this;
    }

    public PredictionBuilder setGs(Boolean gs) {
        prediction.setGs(gs);
        return this;
    }

    public Prediction build() {
        return prediction;
    }
}
