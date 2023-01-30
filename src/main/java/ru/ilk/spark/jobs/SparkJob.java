package ru.ilk.spark.jobs;
import org.apache.spark.sql.SparkSession;

abstract class SparkJob {

    SparkSession spark = SparkSession
            .builder()
            .appName("Application Name")
            .config("some-config", "some-value")
            .getOrCreate();

}
