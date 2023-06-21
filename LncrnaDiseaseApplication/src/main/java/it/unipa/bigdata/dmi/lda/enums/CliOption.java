package it.unipa.bigdata.dmi.lda.enums;

import org.apache.commons.cli.Option;

import java.util.Arrays;

/**
 * Options that can be set as arguments for the execution of the application.
 * @author Mariella Bonomo
 */
public enum CliOption {
    /**
     * Select 1 or multiple functions to be executed by the software. This field is <b>required</b>.
     * @see Functions
     */
    FUNCTION_OPT("f", Option.builder("f").longOpt("function")
            .hasArgs()
            .argName(Arrays.stream(Functions.values()).map(functions -> functions.label).reduce((x, y) -> x + "," + y).orElse(""))
            .desc("select a functionality to run")
            .required(true)
            .build()),
    /**
     * Set the log level of the application and Spark actors (master and slaves). Values are:
     * <ul>
     *     <li>INFO</li>
     *     <li>DEBUG</li>
     *     <li>ERROR</li>
     * </ul>
     */
    LOG_OPT("l",Option.builder("l").longOpt("logLevel")
            .hasArgs()
            .argName("INFO, DEBUG, ERROR")
            .desc("select the log level")
            .build()),
    /**
     * This option receive a path and create a file containing the execution logs.
     */
    LOG_FILE_OPT("lf",Option.builder("lf").longOpt("logFile")
            .hasArgs()
            .argName("file path")
            .desc("select where to store the log file")
            .build()),
    /**
     * Specify the HMDD dataset version between versions <i>HMDDv2</i> and <i>HMDDv3</i>. The default one is <i>HMDDv2</i>.
     * @see Version
     */
    VERSION_OPT("v", Option.builder("v")
            .longOpt("version")
            .hasArg()
            .argName("hmddv2, hmddv3")
            .desc("select hmdd dataset version (hmddv2, hmddv3)")
            .required(false)
            .build()),
    /**
     * Configure the model to be used. The default list contains:
     * <ul>
     *     <li>Centrality</li>
     *     <li>pValue</li>
     *     <li>Catania</li>
     * </ul>
     * @see Model
     */
    MODEL_OPT("m", Option.builder("m")
            .longOpt("model")
            .hasArg()
            .argName("centrality, pvalue, catania")
            .desc("select model (centrality, pvalue, catania)")
            .required(false)
            .build()),
    /**
     * This flag overrides the other. If set, will stop the execution and print the helper.
     */
    HELP_OPT("h", Option.builder("h")
            .longOpt("help")
            .hasArg(false)
            .desc("Print all the commands")
            .required(false)
            .build()),
    /**
     * The path of the parquet folder containing the {@link org.apache.spark.sql.Dataset} of predictions to be loaded.
     */
    PREDICTION_PATH_OPT("pp", Option.builder("pp")
            .longOpt("predictionPath")
            .argName("parquet path")
            .hasArg()
            .desc("parquet path for the prediction to load. If set, it will return a generic predictor model")
            .required(false)
            .build()),
    /**
     * The path of the parquet folder containing the {@link org.apache.spark.sql.Dataset} of prediction scores to be loaded.
     */
    SCORES_PATH_OPT("sp", Option.builder("sp")
            .longOpt("scoresPath")
            .argName("parquet path")
            .hasArg()
            .desc("parquet path for the scores to load.")
            .required(false)
            .build()),
    /**
     * The path of the CSV file containing {@code miRNA-lncRNA} associations.
     */
    ML_OPT("ml", Option.builder("ml")
            .longOpt("mirnaLncrnaPath")
            .argName("parquet path")
            .hasArg()
            .desc("csv path for where to load  the dataset of miRNA-lncRNA associations. See README for file format.")
            .required(false)
            .build()),
    /**
     * The path of the CSV file containing {@code miRNA-disease} associations.
     */
    MD_OPT("md", Option.builder("md")
            .longOpt("mirnaDiseasePath")
            .hasArg()
            .argName("parquet path")
            .desc("csv path for where to load the dataset of miRNA-disease associations. See README for file format.")
            .required(false)
            .build()),
    /**
     * The path of the CSV file containing {@code lncRNA-disease} associations.
     */
    LD_OPT("ld", Option.builder("ld")
            .longOpt("lncrnaDiseasePath")
            .hasArg()
            .argName("parquet path")
            .desc("csv path for where to load  the dataset of lncRNA-disease associations. See README for file format.")
            .required(false)
            .build()),
    /**
     * This option accept two parameters (with no specific order):<br>
     * - outputPath: the path where to store the output in CSV format<br>
     * - numOfPartitions: the number of partitions to use for the output file creation
     */
    OUTPUT_OPT("o",Option.builder("o")
            .longOpt("outputPath")
            .hasArgs()
            .argName("output parquet path for the results")
            .desc("csv path for directory in which save the outputs. File will be stored as CSV. It is possible to specify the number of partitions")
            .required(false)
            .build()),
    /**
     * Set the alpha parameter used by {@link it.unipa.bigdata.dmi.lda.impl.CentralityModel}.
     */
    ALPHA_OPT("a", Option.builder("a")
            .longOpt("alpha")
            .hasArg()
            .argName("0.25, 0.50, 0.75")
            .desc("alpha parameter used for Centrality based model")
            .required(false)
            .build());
    public final String label;
    public final Option opt;

    CliOption(String label, Option opt) {
        this.label = label;
        this.opt = opt;
    }

    public static CliOption fromOption(Option opt) {
        for (CliOption b : CliOption.values()) {
            if (b.opt.equals(opt)) {
                return b;
            }
        }
        throw new EnumConstantNotPresentException(CliOption.class, String.format("Valued %s is not present", opt));
    }
}