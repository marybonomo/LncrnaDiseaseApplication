package it.unipa.bigdata.dmi.lda.factory;

import it.unipa.bigdata.dmi.lda.config.LDACli;
import it.unipa.bigdata.dmi.lda.impl.CataniaModel;
import it.unipa.bigdata.dmi.lda.impl.CentralityModel;
//import it.unipa.bigdata.dmi.lda.impl.GraphframeModel;
//import it.unipa.bigdata.dmi.lda.impl.PValueModel;
//import it.unipa.bigdata.dmi.lda.interfaces.ModelInterface;
import it.unipa.bigdata.dmi.lda.interfaces.ModelInterface;

/**
 * Singleton factory that create the correct model to be used. It uses the parameters set by the user during the execution to provide the correct model.
 *
 * @see LDACli
 * @author Mariella Bonomo
 */
public class ModelFactory {
    private ModelFactory() {
        super();
    }

    /**
     * Return the correct model using the parameters given by the user. If the {@link it.unipa.bigdata.dmi.lda.enums.CliOption#PREDICTION_PATH_OPT} is set, return a generci {@link GraphframeModel}.
     *
     * @return An implementation of the {@link ModelInterface}.
     * @throws IllegalArgumentException If the user specified a model which is not included in {@link it.unipa.bigdata.dmi.lda.enums.Model}.
     */
    public static ModelInterface getModel() throws IllegalArgumentException {
        if (LDACli.getPredictionPath() != null) {
            return new GraphframeModel();
        }
        switch (LDACli.getModel()) {
            case pValue:
                return new PValueModel();
            case Catania:
                return new CataniaModel();
            case Centrality:
                return new CentralityModel();
            default:
                throw new IllegalArgumentException(String.format("Model %s doesn't exists", LDACli.getModel().label));
        }
    }
}
