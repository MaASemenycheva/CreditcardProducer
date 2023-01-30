package ru.ilk.spark.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;


public class DataReader {
    private static Logger logger = Logger.getLogger(DataReader.class.getName());

    public static DataFrame read(String transactionDatasource, StructType schema, SparkSession sparkSession) {
        return sparkSession.read().option("header", "true")
                .schema(schema)
                .csv(transactionDatasource);
    }

    public static DataFrame readFromCassandra(String keySpace, String table) {
        SparkSession sparkSession = null;
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keySpace)
                .option("table", table)
                .option("pushdown", "true")
                .load();
    }
//    }
//
//    public static DataFrame getOffset() {
//        return null;
//    }

//    public static Choice readFromCassandra(String keyspace, String fraudTransactionTable) {
//    }
}
