package it.unipa.bigdata.dmi.lda.config;

import it.unipa.bigdata.dmi.lda.factory.LoggerFactory;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configure the Spark environment using a property file located at {@code src/main/resources/spark.properties}. If the file is not available, use standard configurations.
 * See <a href="https://spark.apache.org/docs/2.4.5/configuration.html#application-properties">Spark 2.4.5 Documentation</a> for more configurations.
 *
 * @author Mariella Bonomo
 */
public class SparkConfig {
    private static Logger logger = LoggerFactory.getLogger(SparkConfig.class);
    private String appName = "LDA";

    private String masterUri = "local[*]";

    private Map<String, String> otherProperties = new HashMap<>();

    /**
     * Load the file properties and return the corresponding spark configuration
     *
     * @return The configured Spark Configuration
     */
    public SparkConf conf() {
        try {
            loadProperties();
        } catch (IOException e) {
            logger.warn("Cannot load properties, going with defaults");
        }
        SparkConf sc = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri);
        otherProperties.forEach(sc::set);
        return sc;
    }

    /**
     * Load the properties located at {@code src/main/resources/spark.properties}.
     *
     * @throws IOException If errors occur during the property file opening.
     */
    private void loadProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream("src/main/resources/spark.properties")));
        appName = properties.getProperty("appName", appName);
        masterUri = properties.getProperty("masterUri", masterUri);
        properties.keySet().stream()
                .filter(key -> !"appName".equals(key) && !"masterUri".equals(key))
                .map(Object::toString)
                .forEach(key -> otherProperties.put(key, properties.getProperty(key)));
    }

    public String getAppName() {
        return appName;
    }

    public String getMasterUri() {
        return masterUri;
    }

}
