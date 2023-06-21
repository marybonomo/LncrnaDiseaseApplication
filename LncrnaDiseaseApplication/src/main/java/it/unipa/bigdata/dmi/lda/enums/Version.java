package it.unipa.bigdata.dmi.lda.enums;

/**
 * List of HMDD versions.
 * @see <a href="https://www.cuilab.cn/hmdd">CuiLab</a>
 * @author Mariella Bonomo
 */
public enum Version {
    HMDDv2("hmddv2"),
    HMDDv3("hmddv3");
    public final String label;

    Version(String label) {
        this.label = label;
    }

    public static Version fromString(String text) {
        for (Version b : Version.values()) {
            if (b.label.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new EnumConstantNotPresentException(Version.class, String.format("Valued %s is not present", text));
    }

    @Override
    public String toString() {
        return label.toLowerCase();
    }
}

