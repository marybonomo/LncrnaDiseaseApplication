package it.unipa.bigdata.dmi.lda.enums;

/**
 * List of the implemented models.
 * @implNote If new models are implemented, add to this list.
 * @author Mariella Bonomo
 */
public enum Model {
    /**
     * @see it.unipa.bigdata.dmi.lda.impl.CentralityModel
     */
    Centrality("centrality"),
    /**
     * @see it.unipa.bigdata.dmi.lda.impl.PValueModel
     */
    pValue("pvalue"),
    /**
     * @see it.unipa.bigdata.dmi.lda.impl.CataniaModel
     */
    Catania("catania");
    public final String label;

    Model(String label) {
        this.label = label;
    }

    public static Model fromString(String text) {
        for (Model b : Model.values()) {
            if (b.label.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new EnumConstantNotPresentException(Model.class, String.format("Valued %s is not present", text));
    }
}