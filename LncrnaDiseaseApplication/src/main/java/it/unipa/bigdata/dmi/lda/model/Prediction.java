package it.unipa.bigdata.dmi.lda.model;

import org.apache.spark.sql.Column;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Class containing the prediction scores for the lncRNA-disease associations.
 * @author Armando La Placa
 */
public class Prediction implements Serializable {
    protected String lncrna;
    protected String disease;
    protected Double score;
    /**
     * {@code True} if the lncRNA-disease association belong to the Gold Standard dataset, {@code False} otherwise.
     */
    protected Boolean gs;
    /**
     * Additional parameters used for debugging or model analysis.
     */
    protected Map<String, String> parameters = new HashMap<>();

    public String getLncrna() {
        return lncrna;
    }

    public void setLncrna(String lncrna) {
        this.lncrna = lncrna;
    }

    public String getDisease() {
        return disease;
    }

    public void setDisease(String disease) {
        this.disease = disease;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public void setParameter(String key, String obj) {
        parameters.put(key, obj);
    }

    public Boolean getGs() {
        return gs;
    }

    public void setGs(Boolean gs) {
        this.gs = gs;
    }

    public static Column getScoreCol(){
        return new Column("score");
    }
    public static Column getLncrnaCol(){
        return new Column("lncrna");
    }
    public static Column getGsCol(){
        return new Column("gs");
    }
}
