package ru.ilk.spark.jobs;

import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.config.Config;
import org.apache.spark.sql.Row;
import ru.ilk.spark.DataBalancing;
import ru.ilk.spark.DataReader;
import ru.ilk.spark.algorithms.Algorithms;

import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class FraudDetectionTrainingTest extends SparkJob {
    public FraudDetectionTrainingTest(String appName) {
        super(appName);
        appName = "Balancing Fraud & Non-Fraud Dataset";
    }

    public static void main( String[] args ) {
        Config.parseArgs(args);

        Dataset<Row> fraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable)
                .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");

        Dataset<Row> nonFraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable)
                .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");

        Dataset<Row> transactionDF = nonFraudTransactionDF.union(fraudTransactionDF);
        transactionDF.cache();


        transactionDF.show(false);

        String[] coloumnNames = new String[]{"category", "merchant", "distance", "amt", "age"};

        /*Transform raw numberical columns to vector. Slice the vecotor and Scale the vector.  Scaling is required so that all the column values are at the same level of measurement */
        VectorAssembler numericAssembler = new VectorAssembler().setInputCols(new String[]{"distance", "amt", "age" }).setOutputCol("rawfeature");
        VectorSlicer slicer = new VectorSlicer().setInputCol("rawfeature").setOutputCol("slicedfeatures").setNames(new String[]{"distance", "amt", "age"});
        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures");

        /*String Indexer for category and merchant columns*/
        StringIndexer categoryIndexer = new StringIndexer().setInputCol("category").setOutputCol("category_indexed");
        StringIndexer merchantIndexer = new StringIndexer().setInputCol("merchant").setOutputCol("merchant_indexed");

        /*Transform all the required columns as feature vector*/
        VectorAssembler vectorAssember = new VectorAssembler().setInputCols(new String[]{"category_indexed", "merchant_indexed", "scaledfeatures"}).setOutputCol("features");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{categoryIndexer, merchantIndexer, numericAssembler, slicer, scaler, vectorAssember});

        PipelineModel preprocessingTransformerModel = pipeline.fit(transactionDF);
        Dataset<Row> featureDF = preprocessingTransformerModel.transform(transactionDF);
        featureDF.show(false);

        Dataset<Row>[] randomSplit = featureDF.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = randomSplit[0];
        Dataset<Row> testData = randomSplit[1];

        Dataset<Row> featureLabelDF = trainData.select("features", "is_fraud").cache();

        Dataset<Row> nonFraudDF = featureLabelDF.filter(featureLabelDF.col("is_fraud").contains(0));


        Dataset<Row> fraudDF = featureLabelDF.filter(featureLabelDF.col("is_fraud").contains(1));
        Long fraudCount = fraudDF.count();


        System.out.println("fraudCount: " + fraudCount);

        SparkJob sparkJobVariable = new SparkJob("Balancing Fraud & Non-Fraud Dataset");
        SparkSession sparkSession = sparkJobVariable.sparkSession;

        /* There will be very few fraud transaction and more normal transaction. Models created  from such
         * imbalanced data will not have good prediction accuracy. Hence balancing the dataset. K-means is used for balancing
         */
        Dataset<Row> balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.intValue(), sparkSession);

        Dataset<Row> finalfeatureDF = fraudDF.union(balancedNonFraudDF);

        RandomForestClassificationModel randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF, sparkSession);
        Dataset<Row> predictionDF = randomForestModel.transform(testData);
        predictionDF.show(false);



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

        MulticlassMetrics metrics =new MulticlassMetrics(predictionAndLabels);

        /*True Positive Rate: Out of all fraud transactions, how  much we predicted correctly. It should be high as possible.*/
        System.out.println("True Positive Rate: " + tp/(tp + fn)); // tp/(tp + fn)

        /*Out of all the genuine transactions(not fraud), how much we predicted wrong(predicted as fraud). It should be low as possible*/
        System.out.println("False Positive Rate: " + fp/(fp + tn));

        System.out.println("Precision: " +  tp/(tp + fp));

    }
}
