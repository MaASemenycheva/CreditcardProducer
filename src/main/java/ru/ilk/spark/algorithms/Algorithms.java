package ru.ilk.spark.algorithms;

//import org.apache.spark.ml.classification.RandomForestClassifier;
import com.datastax.driver.core.Row;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Algorithms {
    private static Logger log = Logger.getLogger(Algorithms.class.getName());

    public static RandomForestClassificationModel randomForestClassifier(Dataset<org.apache.spark.sql.Row> df, SparkSession sparkSession) {
        RandomForestClassifier randomForestEstimator = new RandomForestClassifier().setLabelCol("is_fraud").setFeaturesCol("features").setMaxBins(700);
        RandomForestClassificationModel model = randomForestEstimator.fit(df);
        return model;
    }
}
