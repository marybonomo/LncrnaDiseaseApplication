package it.unipa.bigdata.dmi.lda.builder;

import it.unipa.bigdata.dmi.lda.model.PredictionFDR;

/**
 * @author Mariella Bonomo
 */
public class PredictionFDRBuilder extends PredictionBuilder {
    private Double fdr;
    private PredictionFDR predictionFDR;

    public PredictionFDRBuilder() {
        predictionFDR = new PredictionFDR();
    }

    public PredictionFDRBuilder setFdr(Double score) {
        predictionFDR.setFdr(score);
        return this;
    }

    public PredictionFDR build() {
        return predictionFDR;
    }
}
