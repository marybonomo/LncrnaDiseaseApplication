package it.unipa.bigdata.dmi.lda.impl

import it.unipa.bigdata.dmi.lda.model.{Prediction, PredictionFDR}
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.Dataset

/**
 * This is a generic model. It can be used to implement new models that relies on Graphs, using GraphFrame, as the others.
 *
 * @author Mariella Bonomo
 */
class GraphframeModel() extends GraphframeAbstractModel {
  /**
   * To be implemented!
   */
  override def compute(): Dataset[Prediction] = throw new NotImplementedException()
  /**
   * To be implemented!
   */
  override def predict(): Dataset[PredictionFDR] = throw new NotImplementedException()
}
