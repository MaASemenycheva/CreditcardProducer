package ru.ilk.spark.jobs.RealTimeFraudDetection;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
//import org.apache.spark.sql.types.DoubleType;
//import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.DataTypes;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.cassandra.CassandraDriver;
import ru.ilk.config.Config;
import ru.ilk.creditcard.Creditcard;
import ru.ilk.kafka.KafkaSource;
import ru.ilk.spark.DataReader;
import ru.ilk.spark.GracefulShutdown;
import ru.ilk.spark.SparkConfig;
import ru.ilk.spark.jobs.SparkJob;
import ru.ilk.utils.Utils;
//import org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class StructuredStreamingFraudDetection extends SparkJob {

    public StructuredStreamingFraudDetection(String appName) {
        super(appName);
        appName = "Structured Streaming Job to detect fraud transaction";
    }

    public static void main(String[] args ) throws StreamingQueryException {
        SparkJob sparkJobVariable = new SparkJob("Structured Streaming Job to detect fraud transaction");
        SparkSession sparkSession = sparkJobVariable.sparkSession;

        Config.parseArgs(args);

        Dataset<Row> customerDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.customer);
        Dataset<Row> customerAgeDF = customerDF.withColumn("age", (
                org.apache.spark.sql.functions.datediff(
                org.apache.spark.sql.functions.current_date(),
                org.apache.spark.sql.functions.to_date(customerDF.col("dob"))
                        ).divide(365)).cast(IntegerType));
        customerAgeDF.cache();

        /*Offset is read from checkpointing, hence reading offset and saving offset to /from Cassandra is not required*/
        //val (startingOption, partitionsAndOffsets) = CassandraDriver.readOffset(CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable)

//        Dataset<Creditcard.TransactionKafka> rawStream = KafkaSource.readStream(sparkSession);//(startingOption, partitionsAndOffsets)
        Dataset<Row> rawStream = KafkaSource.readStream(sparkSession);//(startingOption, partitionsAndOffsets)

        Dataset<Row> transactionStream = rawStream
                .selectExpr("transaction.*", "partition", "offset")
                .withColumn("amt", functions.lit(rawStream.col("amt")).cast(DoubleType))
                .withColumn("merch_lat", functions.lit(rawStream.col("merch_lat")).cast(DoubleType))
                .withColumn("merch_long", functions.lit(rawStream.col("merch_long")).cast(DoubleType))
                .drop("first")
                .drop("last");

        sparkSession.sqlContext().sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800");

        transactionStream = transactionStream.withColumn(
                "shgd", org.apache.spark.sql.functions.expr("")
        );

        String COLUMN_DOUBLE_UDF_NAME = "distanceUdf";
        sparkSession.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF4<String, String, String, String, Double>)
                (lat1, lon1, lat2, lon2) -> {
                    Double distance = Utils.getDistance(Double.valueOf(lat1), Double.valueOf( lon1), Double.valueOf(lat2), Double.valueOf(lon2));
                    return distance;
                }, DataTypes.DoubleType);


        Dataset<Row> processedTransactionDF = transactionStream.join(
                functions.broadcast(customerAgeDF), transactionStream.col("cc_num"))
                .withColumn("distance",
                        functions.lit(
                                functions.round(
                                        callUDF(COLUMN_DOUBLE_UDF_NAME,
                                                col("lat"), col("long"), col("merch_lat"), col("merch_long"))
                                        , 2)))
                .select(transactionStream.col("cc_num"),
                        transactionStream.col("trans_num"),
                        functions.to_timestamp(
                                transactionStream.col("trans_time"), "yyyy-MM-dd HH:mm:ss").as ("trans_time"),
                        col("category"), col("merchant"), col("amt"), col("merch_lat"), col("merch_long"), col("distance"), col("age"), col("partition"), col("offset"));



        String[] coloumnNames = new String []{"cc_num", "category", "merchant", "distance", "amt", "age"};

        PipelineModel preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath);
        Dataset<Row> featureTransactionDF = preprocessingModel.transform(processedTransactionDF);

        RandomForestClassificationModel randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath);
        Dataset<Row> predictionDF =  randomForestModel.transform(featureTransactionDF).withColumnRenamed("prediction", "is_fraud");
        //predictionDF.cache

        Dataset<Row> fraudPredictionDF = predictionDF.filter(predictionDF.col("is_fraud").contains(1.0));

        Dataset<Row> nonFraudPredictionDF = predictionDF.filter(predictionDF.col("is_fraud").notEqual(1.0));

        /*Save fraud transactions to fraud_transaction table*/
        StreamingQuery fraudQuery = CassandraDriver.saveForeach(fraudPredictionDF, CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable, "fraudQuery", "append");

        /*Save non fraud transactions to non_fraud_transaction table*/
        StreamingQuery nonFraudQuery = CassandraDriver.saveForeach(nonFraudPredictionDF, CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable, "nonFraudQuery", "append");

        /*Offset is read from checkpointing, hence reading offset and saving offset to /from Cassandra is not required*/
        /*val kafkaOffsetDF = predictionDF.select("partition", "offset").groupBy("partition").agg(max("offset") as "offset")
        val offsetQuery = CassandraDriver.saveForeach(kafkaOffsetDF, CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable, "offsetQuery", "update")*/


        GracefulShutdown.handleGracefulShutdown(1000, Arrays.asList(fraudQuery, nonFraudQuery), sparkSession);


    }
}
