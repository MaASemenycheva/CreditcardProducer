package ru.ilk.spark.spark.jobs;

import org.apache.orc.DataReader;
import ru.ilk.spark.config.Config;
import org.apache.spark.sql.SparkSession;

public class IntialImportToCassandra {

    static SparkSession sparkSession = null;

    public static void main( String[] args ) {
        Config.parseArgs(args);
//        DataFrame customerDF = DataReader.read(SparkConfig.customerDatasource, Schema.customerSchema)

    }
}
