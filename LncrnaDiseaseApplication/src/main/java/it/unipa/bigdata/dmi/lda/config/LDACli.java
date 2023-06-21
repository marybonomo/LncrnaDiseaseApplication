package it.unipa.bigdata.dmi.lda.config;

import it.unipa.bigdata.dmi.lda.enums.CliOption;
import it.unipa.bigdata.dmi.lda.enums.Functions;
import it.unipa.bigdata.dmi.lda.enums.Model;
import it.unipa.bigdata.dmi.lda.enums.Version;
import it.unipa.bigdata.dmi.lda.factory.LoggerFactory;
import it.unipa.bigdata.dmi.lda.factory.ModelFactory;
import it.unipa.bigdata.dmi.lda.interfaces.ModelInterface;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Set;

/**
 * This class is a singleton that manages the CLI of the application. It is used to:
 * <ul>
 *    <li>store the arguments used by the users during the execution into a variable of type {@link LDACliVariables}</li>
 *    <li>provide the correct model to the main execution thread {@link it.unipa.bigdata.dmi.lda.LncRNADiseaseApplication}</li>
 * </ul>
 * @see it.unipa.bigdata.dmi.lda.LncRNADiseaseApplication
 * @see LDACliVariables
 * @author Mariella Bonomo
 */
public class LDACli {
    private static CommandLine cmd = null;
    private static LDACliVariables variables = null;
    private static Logger logger = null;

    /**
     * This class is a singleton, therefore is not instantiable.
     */
    private LDACli() {
    }

    /**
     * Create the {@link CommandLine} interface using the arguments given by the main application. Instantiate the logger using the {@link LoggerFactory}.
     * If problems occurs, or some mandatory arguments are missing, throw an error and print the helper.
     *
     * @param args Arguments given by the user.
     */
    private static void setupCLI(String[] args) {
        // Define options
        Options options = new Options();
        // Add options
        Arrays.stream(CliOption.values()).forEach(cliOption -> options.addOption(cliOption.opt));
        // Create a parser
        CommandLineParser parser = new DefaultParser();
        try {
            cmd = parser.parse(options, args);
            variables = new LDACliVariables(cmd.getOptions());
            logger = LoggerFactory.getLogger(LDACli.class);
        } catch (ParseException e) {
            e.printStackTrace();
            printHelp();
        }
    }

    /**
     * Print the helper in console.
     */
    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        Options options = new Options();
        Arrays.stream(CliOption.values()).forEach(cliOption -> options.addOption(cliOption.opt));
        formatter.printHelp("java -jar lda.jar", options, true);
    }

    /**
     * Return the model using the factory {@link ModelFactory} accordingly to the user arguments.
     * If {@link CliOption#HELP_OPT} is given, or no arguments are provided, print the helper and return {@code null}
     *
     * @param args User arguments (see {@link CliOption}).
     * @return An implementation of the {@link ModelInterface} obtained by {@link ModelFactory#getModel()}.
     * @see it.unipa.bigdata.dmi.lda.impl.CataniaModel
     * @see it.unipa.bigdata.dmi.lda.impl.CentralityModel
     * @see it.unipa.bigdata.dmi.lda.impl.PValueModel
     * @see it.unipa.bigdata.dmi.lda.impl.GraphframeAbstractModel
     * @see it.unipa.bigdata.dmi.lda.impl.GraphframeModel
     */
    public static ModelInterface getParsedModel(String[] args) {
        setupCLI(args);
        if (cmd.getOptions().length == 0 || cmd.hasOption(CliOption.HELP_OPT.label))
            printHelp();
        else {
            assert variables != null;
            logger.info(variables);
            return ModelFactory.getModel();
        }
        return null;
    }

    /**
     * The version refers to the datasets of mirna-disease associations.
     * @return The requested {@link Version} (if set) or {@link Version#HMDDv2} by default.
     * @see Version
     */
    public static Version getVersion() {
        return variables.getVersion() == null ? Version.HMDDv2 : variables.getVersion();
    }

    /**
     *
     * @return Get the path of the {@code mirna-disease.csv} dataset file or the default one located at {@code resources/datasets/<version>/mirna-disease.csv}
     */
    public static String getMdPath() {
        return variables.getMdPath() == null ? String.format("resources/datasets/%s/mirna-disease.csv", getVersion()) : variables.getMdPath();
    }

    /**
     * @return Get the path of the {@code mirna-lncrna.csv} dataset file or the default one located at {@code resources/datasets/mirna-lncrna.csv}
     */
    public static String getMlPath() {
        return variables.getMlPath() == null ? "resources/datasets/mirna-lncrna.csv" : variables.getMlPath();
    }

    /**
     *
     * @return Get the path of the {@code lncrna-disease.csv} dataset file or the default one located at {@code resources/datasets/lncrna-disease.csv}
     */
    public static String getLdPath() {
        return variables.getLdPath() == null ? "resources/datasets/lncrna-disease.csv" : variables.getLdPath();
    }

    public static String getPredictionPath() {
        return variables.getPredictionPath();
    }

    public static String getScoresPath() {
        return variables.getScoresPath();
    }

    /**
     *
     * @return Get the alpha value or 0.25 by default.
     * @see it.unipa.bigdata.dmi.lda.impl.CentralityModel
     */
    public static Double getAlpha() {
        return variables.getAlpha() == null ? 0.50 : variables.getAlpha();
    }

    /**
     * Check if the model has been correctly created and return it
     * @return {@link Model} created using the factory.
     * @see ModelFactory
     */
    public static Model getModel() {
        assert variables.getModel() != null;
        return variables.getModel();
    }

    public static Set<Functions> getFunction() {
        return variables.getFunction();
    }

    public static Level getLogLevel() {
        return variables.getLogLevel();
    }

    public static String getOutputPath() {
        return variables.getOutputPath();
    }

    /**
     *
     * @return Number of partitions used to store the results (1 by default).
     */
    public static Integer getOutputPartitions() {
        return variables.getOutputPartitions() == null ? 1 : variables.getOutputPartitions();
    }
}