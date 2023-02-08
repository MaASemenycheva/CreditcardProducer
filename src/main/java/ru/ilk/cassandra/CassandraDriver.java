package ru.ilk.cassandra;

import com.datastax.spark.connector.cql.CassandraConnector;
import javafx.util.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import ru.ilk.cassandra.foreachSink.CassandraSinkForeach;
import ru.ilk.spark.SparkConfig;

import java.util.*;
import java.util.function.BiFunction;

public class CassandraDriver {
    private static Logger logger = Logger.getLogger(CassandraDriver.class.getName());

    public static CassandraConnector connector = CassandraConnector.apply(SparkConfig.sparkConf);

    public static StreamingQuery debugStream (Dataset<Row> ds) {
        String mode = "append";
        return ds.writeStream()
                .format("console")
                .option("truncate", "false")
                .option("numRows", "100")
                .outputMode(mode)
                .start();
    }

    public static StreamingQuery saveForeach(Dataset<Row> df,
                                             String db,
                                             String table,
                                             String queryName,
                                             String mode) {
        System.out.println("Calling saveForeach");
        return df.writeStream()
                .queryName(queryName)
                .outputMode(mode)
                .foreach(new CassandraSinkForeach(db, table))
                .start();
    }

    public static Pair<String, String> readOffset(String keyspace,
                                                  String table,
                                                  SparkSession sparkSession) {
        Dataset<Row> df = sparkSession
                .read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keyspace)
                .option("table", table)
                .option("pushdown", "true")
                .load()
                .select("partition", "offset");
        //.filter($"partition".isNotNull)
        //df.show(false)
        if (df.rdd().isEmpty()) {
            return new Pair("startingOffsets", "earliest");
        } else {
      /*
      val offsetDf = df.select("partition", "offset")
        .groupBy("partition").agg(max("offset") as "offset")
      ("startingOffsets", transformKafkaMetadataArrayToJson(offsetDf.collect()))
      */
            return new Pair("startingOffsets", transformKafkaMetadataArrayToJson(df.collect()));
        }
    }

    public static String transformKafkaMetadataArrayToJson(Row[] array) {
//        Integer[] numbers = new Integer[] { 1, 2, 3 };
        List<Row> listOfRow = Arrays.asList(array);
//        listOfRow.stream().map(a -> "\"" + a.<Integer>getAs("partition")+ "\":"
//                + a.<Long>getAs("offset") +", ");
        String partitionOffset = listOfRow.stream().map(a -> "\"" + a.<Integer>getAs("partition")+ "\":"
                + a.<Long>getAs("offset") +", ").toString();



        System.out.println("Offset: " + partitionOffset.substring(0, partitionOffset.length() -2));

        String partitionAndOffset = "{\"creditTransaction\": {"
                + partitionOffset.substring(0, partitionOffset.length() -2)
                + "}}".replaceAll("\n", "").replaceAll(" ", "");
        System.out.println(partitionAndOffset);
        return partitionAndOffset;
    }

    //    Option[Map[TopicPartition, Long]]
    /* Read offsert from Cassandra for Dstream*/
    public static Optional<HashMap<TopicPartition, Long>> readOffset(
            String keySpace,
            String table,
            String topic,
            SparkSession sparkSession) {
        Dataset<Row> df = sparkSession
                .read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keySpace)
                .option("table", table)
                .option("pushdown", "true")
                .load()
                .select("partition", "offset");
        if (df.rdd().isEmpty()) {
            logger.info("No offset. Read from earliest");
            return Optional.empty();
        } else {
            HashMap<TopicPartition, Long> fromOffsets = (HashMap<TopicPartition, Long>) Arrays.stream(df.rdd().collect()).map(o -> {
                        System.out.println(o);
                        return new HashMap(new TopicPartition(topic, o.<Integer>getAs("partition")).partition(), o.<Long>getAs("offset"));
                    }
            );
            return Optional.of(fromOffsets);
        }
    }

    /* Save Offset to Cassandra for Structured Streaming */
    public static void saveOffset(String keySpace, String table, Dataset<Row> df, SparkSession sparkSession) {
        HashMap <String, String> hm = new HashMap<String, String>();
        hm.put("keyspace", keySpace);
        hm.put("table", table);
        df.write()
                .format("org.apache.spark.sql.cassandra")
                .options(hm)
                .save();
    }
    public static String concatenate(List<Character> chars) {
        return foldLeft(chars, new StringBuilder(""), StringBuilder::append).toString();
    }
    static <U, T> U foldLeft(Collection<T> sequence, U identity, BiFunction<U, ? super T, U> accumulator) {
        U result = identity;
        for (T element : sequence)
            result = accumulator.apply(result, element);
        return result;
    }
}