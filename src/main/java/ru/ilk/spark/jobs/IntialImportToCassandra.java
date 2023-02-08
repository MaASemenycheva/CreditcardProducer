package ru.ilk.spark.jobs;

//import org.apache.orc.DataReader;
import jnr.ffi.annotations.In;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.config.Config;
import org.apache.spark.sql.SparkSession;
import ru.ilk.creditcard.Schema;
import ru.ilk.spark.SparkConfig;
import ru.ilk.utils.Utils;
import ru.ilk.spark.DataReader;
import java.util.HashMap;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class IntialImportToCassandra extends SparkJob {

    public IntialImportToCassandra(String appName) {
        super(appName);
        appName = "Initial Import to Cassandra";
    }

    public static void main( String[] args ) {
        SparkJob sparkJobVariable = new SparkJob("Initial Import to Cassandra");
        SparkSession sparkSession = sparkJobVariable.sparkSession;
        Config.parseArgs(args);

        Dataset<Row> customerDF = DataReader.read(SparkConfig.customerDatasource,
                Schema.customerSchema,
                sparkSession);

        Dataset<Row> customerAgeDF = customerDF.withColumn("age",
                (org.apache.spark.sql.functions.datediff(
                        org.apache.spark.sql.functions.current_date(),
                        org.apache.spark.sql.functions.to_date(customerDF.col("dob"))
                ).divide(365)).cast(IntegerType));

        Dataset<Row> transactionDataframe = DataReader.read(SparkConfig.transactionDatasouce,
                        Schema.fruadCheckedTransactionSchema,
                        sparkSession
                        );
        Dataset<Row> transactionDF = transactionDataframe
                .withColumn("trans_date",
                        org.apache.spark.sql.functions.split(transactionDataframe.col("trans_date"), "T").getItem(0))
                .withColumn("trans_time",
                        org.apache.spark.sql.functions.concat_ws(" ", transactionDataframe.col("trans_date"), transactionDataframe.col("trans_date")))
                        .withColumn("trans_time", org.apache.spark.sql.functions.unix_timestamp(
                                transactionDataframe.col("trans_time"), "YYYY-MM-dd HH:mm:ss").cast(TimestampType));


        String COLUMN_DOUBLE_UDF_NAME = "distanceUdf";
        sparkSession.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF4<String, String, String, String, Double>)
                (lat1, lon1, lat2, lon2) -> {
                    Double distance = Utils.getDistance(Double.valueOf(lat1), Double.valueOf( lon1), Double.valueOf(lat2), Double.valueOf(lon2));
                    return distance;
                }, DataTypes.DoubleType);


        Dataset<Row> processedDF = transactionDF.join(
                org.apache.spark.sql.functions.broadcast(customerAgeDF), transactionDF.col("cc_num"))
                .withColumn("distance",
                        org.apache.spark.sql.functions.lit(
                                org.apache.spark.sql.functions.round(
                                        callUDF(COLUMN_DOUBLE_UDF_NAME,
                                                col("lat"), col("long"), col("merch_lat"), col("merch_long"))
                                        , 2)))
                .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud");

        processedDF.cache();

        Dataset<Row> fraudDF = processedDF.filter(processedDF.col("is_fraud").contains(1));
        Dataset<Row> nonFraudDF = processedDF.filter(processedDF.col("is_fraud").contains(0));


        HashMap<String, String> hmCustomerDF = new HashMap<String, String>();
        hmCustomerDF.put("keyspace", CassandraConfig.keyspace);
        hmCustomerDF.put("table", CassandraConfig.customer);
        /* Save Customer data to cassandra */
        customerDF.write()
                .format("org.apache.spark.sql.cassandra")
                .mode("append")
                .options(hmCustomerDF)
                .save();

        HashMap<String, String> hmFraudDF = new HashMap<String, String>();
        hmFraudDF.put("keyspace", CassandraConfig.keyspace);
        hmFraudDF.put("table", CassandraConfig.fraudTransactionTable);
        /* Save fraud transaction data to fraud_transaction cassandra table*/
        fraudDF.write()
                .format("org.apache.spark.sql.cassandra")
                .mode("append")
                .options(hmFraudDF)
                .save();

        /* Save non fraud transaction data to non_fraud_transaction cassandra table*/
        HashMap<String, String> hmNonFraudDF = new HashMap<String, String>();
        hmNonFraudDF.put("keyspace", CassandraConfig.keyspace);
        hmNonFraudDF.put("table", CassandraConfig.nonFraudTransactionTable);
        nonFraudDF.write()
                .format("org.apache.spark.sql.cassandra")
                .mode("append")
                .options(hmNonFraudDF)
                .save();
    }
}
