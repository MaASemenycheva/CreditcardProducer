package ru.ilk.spark;

import com.datastax.spark.connector.types.DoubleType;
import javafx.util.Pair;
import jnr.ffi.annotations.In;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class DataReader {
    private static Logger logger = Logger.getLogger(DataReader.class.getName());

    public static Dataset<Row> read(String transactionDatasource, StructType schema, SparkSession sparkSession) {
        return sparkSession.read().option("header", "true")
                .schema(schema)
                .csv(transactionDatasource);
    }

    public static Dataset<Row> readFromCassandra(String keySpace, String table) {
        SparkSession sparkSession = null;
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keySpace)
                .option("table", table)
                .option("pushdown", "true")
                .load();
    }
    public static void getOffset (RDD<HasOffsetRanges> rdd, SparkSession sparkSession) {
        HasOffsetRanges hasOffsetRanges = (HasOffsetRanges) rdd.toJavaRDD().rdd();

        StructType featureSchema = new StructType();
        featureSchema.add("partition", DataTypes.StringType, true);
        featureSchema.add("offset", DataTypes.StringType, true);

        List<Pair<Integer, Long>> rowRdd = (List<Pair<Integer, Long>>) Arrays.stream(hasOffsetRanges.offsetRanges()).map(offset ->
                new Pair<Integer, Long>(offset.partition(), offset.untilOffset()));
        sparkSession.createDataFrame((JavaRDD<Row>) rowRdd, featureSchema);
    }
}

