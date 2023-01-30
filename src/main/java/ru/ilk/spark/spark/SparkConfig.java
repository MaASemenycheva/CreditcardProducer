package ru.ilk.spark.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import ru.ilk.spark.cassandra.CassandraConfig;

public class SparkConfig {
    private static Logger logger = Logger.getLogger(SparkConfig.class.getName());
    public static SparkConf sparkConf = new SparkConf();

    static String transactionDatasouce;
    static String customerDatasource;
    static String modelPath;
    static String preprocessingModelPath;
    static String shutdownMarker;
    static Integer batchInterval;

    public static void load() {

    }

    public static void defaultSetting() {
        sparkConf.setMaster("local[*]")
                .set("spark.cassandra.connection.host", CassandraConfig.cassandraHost)
                .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint");
        shutdownMarker = "/tmp/shutdownmarker";
        transactionDatasouce = "src/main/resources/data/transactions.csv";
        customerDatasource = "src/main/resources/data/customer.csv";
        modelPath = "src/main/resources/spark/training/RandomForestModel";
        preprocessingModelPath = "src/main/resources/spark/training/PreprocessingModel";
        batchInterval = 5000;
    }
}
