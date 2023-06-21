package it.unipa.bigdata.dmi.lda.factory;

import it.unipa.bigdata.dmi.lda.config.LDACli;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Mariella Bonomo
 */
public class LoggerFactory {
    public static Logger getLogger(Class clazz) {
        Logger logger = Logger.getLogger(clazz);
        logger.setLevel(LDACli.getLogLevel()==null? Level.INFO:LDACli.getLogLevel());
        return logger;
    }
}
