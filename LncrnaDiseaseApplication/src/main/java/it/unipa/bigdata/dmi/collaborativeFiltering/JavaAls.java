package it.unipa.bigdata.dmi.collaborativeFiltering;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * 
 * @author Mariella Bonomo
 *
 */
public class JavaAls{  
		
    public static class Schema implements Serializable {
    	
    	 //instance variables
        private String lncrna;
        private String disease;
        private String score;
        private String fdr;
        private String gs;
        private String prediction;
    	
       
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
        public String getScore() {
            return score;
        }
        public void setScore(String score) {
            this.score = score;
        }
        public String getFdr() {
            return fdr;
        }
        public void setFdr(String fdr) {
            this.fdr = fdr;
        }
        public String getGs() {
            return gs;
        }
        public void setGs(String gs) {
            this.gs = gs;
        } 
        public String getPrediction() {
            return prediction;
        }
        public void setPrediction(String prediction) {
            this.prediction = prediction;
        } 
        
  
        public Schema() {}
        
        public Schema(String lncrna, String disease, String score, String fdr, String gs, String prediction) {
            this.lncrna = lncrna;
            this.disease = disease;
            this.score = score;
            this.fdr = fdr;
            this.gs = gs;
            this.prediction = prediction;
          }

        
    }
        
    public static Rating parseRating(String str) {
        String[] fields = str.split(",");
        if (fields.length != 6) {
          throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int lncrna = Integer.parseInt(fields[0]);
        int disease = Integer.parseInt(fields[1]);
        float score = Float.parseFloat(fields[2]);
        float fdr = Float.parseFloat(fields[3]);
        int gs = Integer.parseInt(fields[4]);
        int prediction = Integer.parseInt(fields[5]);
        return new Rating(lncrna, disease, fdr);
      }
    
    

    public static void main(String[] args) throws IOException {
    	 SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("ALS");
         JavaSparkContext sc=new JavaSparkContext(sparkConf);
    	
    	SparkSession spark=SparkSession
                .builder()
                .appName("ALSExample")
                .master("local[2]")
                //.config("spark.executor.memory", "8g")
                //.config("spark.executor.cores", 4)
                .getOrCreate();
    	sc.setLogLevel("ERROR");
        SQLContext sqlcontext= new SQLContext(sc);

        String path="C:/Users/Mary/eclipse-workspace/CollaborativeFiltering/src/main/resources/fileDef.csv";
        //fileDef.csv contiene il dataset con gli score
                
        //-------------------------------1.0 Prepare DataFrame---------------- ------------
        //..javaRDD() function converts DataFrame to RDD
        //Then map each row of RDD String->Rating
         
        // mapping of the schema 
        JavaRDD<Schema> schemaRdd = sc.textFile(path).map(
                        new Function<String, Schema>() {
                            public Schema call(String line) throws Exception {
                                String[] tokens=line.split(";");
                                Schema schema = new Schema();
                                schema.setLncrna(tokens[0]);                                                                                               
                                schema.setDisease(tokens[1]);                                                            
                                schema.setScore(tokens[2]);
                                schema.setFdr(tokens[3]);
                                schema.setGs(tokens[4]);
                                schema.setPrediction(tokens[5]);
                                return schema;
                            }
                        });       
        
        
       //dataset originariol
        Dataset<Row> df = sqlcontext.createDataFrame(schemaRdd, Schema.class);
        df.createOrReplaceTempView("rating");        
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM rating");
        //sqlDF.show(10); // prova stampa tabella
        
        
        /*Dataset<Row> col = spark.sql("SELECT prediction FROM rating");      
        col.write().format("text").save("C:/Users/Mary/eclipse-workspace/CollaborativeFiltering/src/main/resources/prova.txt");
       */
        	
        
        //dataset con 3 colonne lncrna, disease 
        Dataset<Row> db = sqlcontext.createDataFrame(schemaRdd, Schema.class);
        db.createOrReplaceTempView("rating");        
        Dataset<Row> sqlDb = spark.sql("SELECT INT(lncrna), INT(disease), FLOAT(fdr) FROM rating");
        //sqlDb.show(10); // prova stampa tabella
        
       /* JavaRDD<Schema> ratingsRDD = spark
        	      .read().textFile("src/main/resources/lncrna-disease.csv").javaRDD()
        	      .map(Schema::parseRating);*/
        	    
        
        //training set and test set
        double[] weights=new double[] {0.8,0.2};
        long seed=1234;
              
        Dataset[] split=sqlDb.randomSplit(weights, seed);
        Dataset training=split[0];
        Dataset test=split[1];         
        List<Double> RMSE=new ArrayList(); //
        //------------------------------2.0 ALS algorithm and training data set to generate recommendation model --------- ----
        for(int rank=1;rank<19;rank++) ////20 cicli
        {
            //algoritmo
            ALS als=new ALS()
                    .setMaxIter(15)////The maximum number of iterations, the setting is too large, java.lang.StackOverflowError occurs
                    .setRegParam(0.05)
                    .setAlpha(1.0)
                    .setImplicitPrefs(false)
                    .setNonnegative(true)
                    .setUserCol("lncrna")               
                    .setRank(rank)
                    .setItemCol("disease")
                    .setRatingCol("fdr");//fdr
                                     
            		
            //Training model
            ALSModel model=als.fit(training);
            //ALSModel model2 =als.fit(test);
           
            
            //method 
            //AbstractJavaRDDLike<Schema, JavaRDD<Schema>> ratings = null;
			//JavaRDD<Tuple2<Object, Object>> predictions_ =
            //		  ratings.map(r -> new Tuple2<>(r.prediction, r.score));
            
            	
                        
            //---------------------------3.0 Model Evaluation: Calculate RMSE, Root Mean Square Error---------------------
           
            Dataset predictions=model.transform(test);
            //Dataset predictions=model2.transform(training);
            //predictions.show();
            
            predictions.createOrReplaceTempView("predictions");
            
            //STAMPARE i valori delle predizioni
            //Dataset<Row> tab_lncrna = spark.sql("SELECT STRING(lncrna) FROM predictions");
            //tab_lncrna.write().format("text").save("C:/Users/Mary/eclipse-workspace/CollaborativeFiltering/src/main/resources/lncrna10.txt");
            //Dataset<Row> tab_disease = spark.sql("SELECT STRING(disease) FROM predictions");
            //tab_disease.write().format("text").save("C:/Users/Mary/eclipse-workspace/CollaborativeFiltering/src/main/resources/disease10.txt");
            //Dataset<Row> tab_score = spark.sql("SELECT STRING(fdr) FROM predictions");
            //tab_score.write().format("text").save("C:/Users/Mary/eclipse-workspace/CollaborativeFiltering/src/main/resources/scoreIni20.txt");
            Dataset<Row> tab_pred = spark.sql("SELECT STRING(prediction) FROM predictions");
            tab_pred.write().format("text").save("C:/Users/Mary/eclipse-workspace/CollaborativeFiltering/src/main/resources/scoregiugno.txt");            
            //model.save("target/tmp/myCollaborativeFilter");
            //MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(),"target/tmp/myCollaborativeFilter");
            //MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(),"C:\\Users\\Mary\\eclipse-workspace\\LDA-LncRNADiseaseAssociation-standalone\\LDA-LncRNADiseaseAssociation-standalone\\resources\\predictions\\hmddv2\\centrality_fdr");
            
            //testing with RMSE
            RegressionEvaluator evaluator=new RegressionEvaluator()
                    .setMetricName("rmse")
                    .setLabelCol("prediction")  
                    .setPredictionCol("fdr"); // "prediction"/"fdr"
               		
            
            Double rmse = evaluator.evaluate(predictions.na().drop());
            RMSE.add(rmse);
            //System.out.println("Rank =" + rank+"  RMSErr = " + rmse+"\n");
            System.out.println("Rank =" + rank);
           	
            
           
           
            ///output all result
            //double cont=(float) 0.0;
            double temp =(float) 0.0;
            float media= (float) 0.0;
            for (int j=0;j<RMSE.size();j++) {
            	Double lambda=(j*5+1)*0.01; 
            	//System.out.println("regParam="+lambda+" RMSE= "+ RMSE.get(j)+"\n");
            	System.out.println("RMSE "+lambda+"\n");
            	temp= lambda+temp;
            	//temp=(double) RMSE.get(j)+temp;
            	int num=j+1;
            	media=(float) (temp/num);
            	
            }System.out.println("Media error:"+media);
       
        }       
    }
}