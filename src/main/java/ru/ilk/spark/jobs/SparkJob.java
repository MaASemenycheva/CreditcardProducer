package ru.ilk.spark.jobs;

import org.apache.spark.sql.SparkSession;
import ru.ilk.spark.SparkConfig;

//abstract class SparkJob{
public class SparkJob{
    String appNameSparkJob;
    public SparkJob(String appName) {
        appNameSparkJob = appName;  // Set the initial value for the class attribute x
    }

    public static SparkSession sparkSession = SparkSession.builder()
            .config(SparkConfig.sparkConf)
            .getOrCreate();
}