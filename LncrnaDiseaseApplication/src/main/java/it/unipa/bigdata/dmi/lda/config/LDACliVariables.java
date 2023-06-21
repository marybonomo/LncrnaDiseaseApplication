package it.unipa.bigdata.dmi.lda.config;

import it.unipa.bigdata.dmi.lda.enums.CliOption;
import it.unipa.bigdata.dmi.lda.enums.Functions;
import it.unipa.bigdata.dmi.lda.enums.Model;
import it.unipa.bigdata.dmi.lda.enums.Version;
import org.apache.commons.cli.Option;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class stores the arguments given by the user. It is managed by {@link LDACli} and its values are set during the initialization.
 *
 * @author Mariella Bonomo
 * @see LDACli
 * @see CliOption
 */
public class LDACliVariables {
    /**
     * The path of the prediction to load in parquet format.
     *
     * @see LDACli#getPredictionPath()
     */
    private String predictionPath = null;
    /**
     * The path of the scores to laod in parquet format.
     *
     * @see LDACli#getScoresPath()
     */
    private String scoresPath = null;
    /**
     * The path of the CSV dataset of miRNA-disease associations. See the README of the project for more details.
     *
     * @see LDACli#getMdPath()
     */
    private String mdPath = null;
    /**
     * The path of the CSV dataset of miRNA-lncRNA associations. See the README of the project for more details.
     *
     * @see LDACli#getMlPath()
     */
    private String mlPath = null;
    /**
     * The path of the CSV dataset of lncRNA-disease associations. See the README of the project for more details.
     *
     * @see LDACli#getLdPath() ()
     */
    private String ldPath = null;
    /**
     * The alpha value used by the centrality model for the score computation. See the README of the project for more details.
     *
     * @see LDACli#getAlpha()
     * @see it.unipa.bigdata.dmi.lda.impl.CentralityModel
     */
    private Double alpha = null;
    /**
     * The chosen model used for the prediction and score computations. See the README of the project for more details.
     *
     * @see Model
     * @see LDACli#getModel()
     * @see it.unipa.bigdata.dmi.lda.impl.CataniaModel
     * @see it.unipa.bigdata.dmi.lda.impl.CentralityModel
     * @see it.unipa.bigdata.dmi.lda.impl.PValueModel
     */
    private Model model = null;
    /**
     * The version of the miRNA-disease dataset. See the README of the project for more details.
     *
     * @see Version
     * @see LDACli#getVersion()
     */
    private Version version = null;
    /**
     * Set the log level for the application and Apache Spark workers.
     *
     * @see Level
     * @see LDACli#getLogLevel()
     */
    private Level logLevel = null;
    /**
     * The path where to store the results of the predictions and scores computed by the models.
     *
     * @see LDACli#getOutputPath()
     */
    private String outputPath = null;
    /**
     * The number of partitions used to store the CSV results files.
     *
     * @see LDACli#getOutputPartitions()
     */
    private Integer outputPartitions = null;
    /**
     * The set of functions the user want to execute.
     *
     * @see Functions
     * @see LDACli#getFunction()
     */
    private Set<Functions> function = null;

    /**
     * Construct an object using the options of the user input. It uses the enums {@link CliOption} to determine which option is provided by the user during execution.
     *
     * @param options User inputs given as parameters during the execution.
     */
    public LDACliVariables(Option[] options) {
        Arrays.stream(options).forEach(option -> {
            switch (Objects.requireNonNull(CliOption.fromOption(option))) {
                case FUNCTION_OPT:
                    try {
                        function = Arrays.stream(option.getValues()).map(String::toLowerCase).map(Functions::fromString).collect(Collectors.toSet());
                        break;
                    } catch (IllegalArgumentException e) {
                        System.err.println(String.format("Function must be one between %s",
                                Arrays.stream(Functions.values()).map(m -> String.format("'%s'", m.label)).reduce((x, y) -> String.format("%s,%s", x, y)).get()));
                    }
                case LOG_OPT:
                    logLevel = Level.toLevel(option.getValue().toUpperCase());
                    break;
                case LOG_FILE_OPT:
                    FileAppender fa = new FileAppender();
                    fa.setName("fileAppender");
                    fa.setFile(option.getValue());
                    fa.setLayout(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"));
                    fa.setThreshold(getLogLevel());
                    fa.setAppend(false);
                    fa.activateOptions();

                    Logger.getRootLogger().setAdditivity(false);
                    Logger.getRootLogger().addAppender(fa);
                    break;
                case ALPHA_OPT:
                    try {
                        alpha = Double.parseDouble(option.getValue());
                        break;
                    } catch (NumberFormatException e) {
                        System.err.println("Alpha parameter must be a double!");
                    }
                case PREDICTION_PATH_OPT:
                    predictionPath = option.getValue();
                    break;
                case SCORES_PATH_OPT:
                    scoresPath = option.getValue();
                    break;
                case VERSION_OPT:
                    try {
                        version = Version.fromString(option.getValue().toLowerCase());
                        break;
                    } catch (IllegalArgumentException e) {
                        System.err.println(String.format("Version must be one between %s",
                                Arrays.stream(Version.values()).map(m -> String.format("'%s'", m.label)).reduce((x, y) -> String.format("%s,%s", x, y)).get()));
                    }
                case MODEL_OPT:
                    try {
                        model = Model.fromString(option.getValue().toLowerCase());
                        break;
                    } catch (IllegalArgumentException | NullPointerException e) {
                        System.err.println(String.format("Model must be one between %s",
                                Arrays.stream(Model.values()).map(m -> String.format("'%s'", m.label)).reduce((x, y) -> String.format("%s,%s", x, y)).get()));
                    }
                case LD_OPT:
                    ldPath = option.getValue();
                    break;
                case MD_OPT:
                    mdPath = option.getValue();
                    break;
                case ML_OPT:
                    mlPath = option.getValue();
                    break;
                case OUTPUT_OPT:
                    Arrays.stream(option.getValues()).forEach(v -> {
                        try {
                            outputPartitions = Integer.parseInt(v);
                        } catch (NumberFormatException e) {
                            outputPath = v;
                        }
                    });
                    break;
                default:
                    break;
                //TODO: add HERE further cli options
            }
        });
    }

    public String getPredictionPath() {
        return predictionPath;
    }

    public Double getAlpha() {
        return alpha;
    }

    public Model getModel() {
        return model;
    }

    public Version getVersion() {
        return version;
    }

    public String getMdPath() {
        return mdPath;
    }

    public String getMlPath() {
        return mlPath;
    }

    public String getLdPath() {
        return ldPath;
    }

    public Set<Functions> getFunction() {
        return function;
    }

    public String getScoresPath() {
        return scoresPath;
    }

    public Level getLogLevel() {
        return logLevel;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public Integer getOutputPartitions() {
        return outputPartitions;
    }

    /**
     * Print the variables that are not {@code null} in format: {@code var: 'value'}.
     * @return The string of variables separated by a break line.
     */
    @Override
    public String toString() {
        return Arrays.stream(this.getClass().getDeclaredFields()).filter(field -> {
            try {
                return field.get(this) != null;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                return false;
            }
        }).map(field -> {
            try {
                return String.format("%s: '%s'", field.getName(), field.get(this));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                return "";
            }
        }).reduce((x, y) -> x + "\n" + y).get();
    }
}
