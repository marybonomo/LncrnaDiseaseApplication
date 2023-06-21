package it.unipa.bigdata.dmi.lda.enums;

/**
 * List of possible functions that can be call by the user.
 * @author Mariella Bonomo
 */
public enum Functions {
    COMPUTE("compute"),
    PREDICT("predict"),
    LOAD_PREDICTIONS("loadPredictions"),
    LOAD_SCORES("loadScores"),
    CONFUSION_MATRIX("confusionMatrix"),
    AUC("auc");
    public final String label;
    public static Functions[] order = {LOAD_SCORES, LOAD_PREDICTIONS, COMPUTE, PREDICT, AUC, CONFUSION_MATRIX};
    Functions(String label) {
        this.label = label;
    }

    public static Functions fromString(String text) {
        for (Functions b : Functions.values()) {
            if (b.label.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new EnumConstantNotPresentException(Functions.class, String.format("Valued %s is not present", text));
    }
}