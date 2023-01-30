package ru.ilk.spark.spark.jobs;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.ilk.spark.cassandra.CassandraConfig;
import ru.ilk.spark.config.Config;
import ru.ilk.spark.spark.DataReader;
import ru.ilk.spark.spark.pipline.BuildPipeline;

import java.util.Arrays;
import java.util.List;


public class FraudDetectionTraining {

    private static Logger log = Logger.getLogger(FraudDetectionTraining.class.getName());

    public static void main( String[] args ) {
        Config.parseArgs(args);
        DataFrame fraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable)
                .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");


        DataFrame nonFraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable)
                .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");

        DataFrame transactionDF = nonFraudTransactionDF.unionAll(fraudTransactionDF);
                //.union(fraudTransactionDF);
        transactionDF.cache();
        transactionDF.show(false);

        List<String> coloumnNames = Arrays.asList("category", "merchant", "distance", "amt", "age");

        Array<PipelineStage> pipelineStages = BuildPipeline.createFeaturePipeline(transactionDF.schema, coloumnNames);


    }
}
