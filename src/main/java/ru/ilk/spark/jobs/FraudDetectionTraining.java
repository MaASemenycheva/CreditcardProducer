package ru.ilk.spark.jobs;

import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import ru.ilk.spark.DataBalancing;
import ru.ilk.spark.DataReader;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.config.Config;
import ru.ilk.spark.SparkConfig;
import ru.ilk.spark.algorithms.Algorithms;
import ru.ilk.spark.pipline.BuildPipeline;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntBinaryOperator;

import static org.apache.spark.sql.types.DataTypes.DoubleType;


public class FraudDetectionTraining extends SparkJob {

    private static Logger log = Logger.getLogger(FraudDetectionTraining.class.getName());

    public FraudDetectionTraining(String appName) {
        super(appName);
        appName = "Balancing Fraud & Non-Fraud Dataset";
    }

    public static void main( String[] args ) throws IOException {
        Config.parseArgs(args);
        Dataset<Row> fraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable)
                .select("cc_num", "category", "merchant", "distance", "amt", "age", "is_fraud");

        Dataset<Row> nonFraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable)
                .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");

        Dataset<Row> transactionDF = nonFraudTransactionDF.union(fraudTransactionDF);
        transactionDF.cache();

        transactionDF.show(false);

        List<String> coloumnNames = Arrays.asList("category", "merchant", "distance", "amt", "age");

        PipelineStage[] pipelineStages = BuildPipeline.createFeaturePipeline(transactionDF.schema(), coloumnNames);
        Pipeline pipeline = new Pipeline().setStages(pipelineStages);
        PipelineModel preprocessingTransformerModel = pipeline.fit(transactionDF);

        Dataset<Row> featureDF = preprocessingTransformerModel.transform(transactionDF);

        featureDF.show(false);

//        Dataset<Row>[] randomSplit = featureDF.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row>[] randomSplit = featureDF.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = randomSplit[0];
        Dataset<Row> testData = randomSplit[1];

        Dataset<Row> featureLabelDF = trainData.select("features", "is_fraud").cache();

        Dataset<Row> nonFraudDF = featureLabelDF.filter(featureLabelDF.col("is_fraud").contains("0"));

        Dataset<Row> fraudDF = featureLabelDF.filter(featureLabelDF.col("is_fraud").contains("1"));

        Long fraudCount = fraudDF.count();

        System.out.println("fraudCount: " + fraudCount);

        /* There will be very few fraud transaction and more normal transaction. Models created  from such
         * imbalanced data will not have good prediction accuracy. Hence balancing the dataset. K-means is used for balancing
         */

        SparkJob sparkJobVariable = new SparkJob("Balancing Fraud & Non-Fraud Dataset");
        SparkSession sparkSession = sparkJobVariable.sparkSession;

        Dataset<Row> balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.intValue(), sparkSession);

        Dataset<Row> finalfeatureDF = fraudDF.union(balancedNonFraudDF);

        RandomForestClassificationModel randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF, sparkSession);
        Dataset<Row> predictionDF = randomForestModel.transform(testData);
        predictionDF.show(false);



//        RDD<Pair<Double, Double>> predictionAndLabels =
//                predictionDF.select(predictionDF.col("prediction"),
//                                    predictionDF.col("is_fraud").cast(DoubleType))).cache();


//        RDD<Pair<Double, Double>> predictionAndLabels = predictionDF.select(
//                predictionDF.col("prediction"),
//                predictionDF.col("is_fraud").cast(DoubleType))
//                .rdd(new Row(Double prediction, Double label))
//                .cache();


//        Pair<Double, Double> predictionAndLabels =
//                predictionDF.select(
//                        predictionDF.col("prediction"),
//                        predictionDF.col("is_fraud")
//                                .cast(DoubleType)).rdd().map(Pair(Double c, Double f) ->
//         Pair(c, f);
//        );
////                        .rdd();
////                        .map(x instanceof Row(Double prediction, Double label)   -> new Pair(x.getKey(), x.getKey()))
////                        .map {
////            case Row(prediction: Double, label: Double) => (prediction, label)
////        }.cache()


//        Dataset<Row> predictionAndLabels =
//                predictionDF.select("prediction", "is_fraud");

        Dataset<Row> predictionAndLabels =
                predictionDF.select(
                        predictionDF.col("prediction"),
                        predictionDF.col("is_fraud")
                                .cast(DoubleType));

        // confusion matrix
        Float tp = (float) predictionAndLabels.toJavaRDD().filter(row ->
                (Float) row.get(0) == 1.0f && (Float) row.get(1) == 1.0f).count();
        Float fp = (float) predictionAndLabels.toJavaRDD().filter(row ->
                (Float) row.get(0) == 0.0f && (Float) row.get(1) == 1.0f).count();
        Float tn = (float) predictionAndLabels.toJavaRDD().filter(row ->
                (Float) row.get(0) == 0.0f && (Float) row.get(1) == 0.0f).count();
        Float fn = (float) predictionAndLabels.toJavaRDD().filter(row ->
                (Float) row.get(0) == 1.0f && (Float) row.get(1) == 0.0f).count();

        System.out.printf("=================== Confusion matrix ==========================\n" +
                        "#############| %-15s                     %-15s\n" +
                        "-------------+-------------------------------------------------\n" +
                        "Predicted = 1| %-15f                     %-15f\n" +
                        "Predicted = 0| %-15f                     %-15f\n" +
                        "===============================================================",
                "Actual = 1",
                "Actual = 0", tp, fp, fn, tn);

        System.out.println();

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);
        /*True Positive Rate: Out of all fraud transactions, how  much we predicted correctly. It should be high as possible.*/
        System.out.println("True Positive Rate: " + tp/(tp + fn)); // tp/(tp + fn)

        /*Out of all the genuine transactions(not fraud), how much we predicted wrong(predicted as fraud). It should be low as possible*/
        System.out.println("False Positive Rate: " + fp/(fp + tn));

        System.out.println("Precision: " +  tp/(tp + fp));

        /* Save Preprocessing  and Random Forest Model */
        randomForestModel.save(SparkConfig.modelPath);
        preprocessingTransformerModel.save(SparkConfig.preprocessingModelPath);
    }
}
