package ru.ilk.testing;

import net.razorvine.pickle.Pair;
import org.apache.spark.sql.*;
import ru.ilk.creditcard.Schema;
import ru.ilk.spark.SparkConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.TimestampType;


public class Streaming {
    static SparkSession sparkSession = SparkSession.builder()
            .config(SparkConfig.sparkConf)
            .master("local")
            .getOrCreate();

    public static Pair<String, String> readOffset(String db, String table) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        // !!!!!!!!!!!!!!!!!!!!!! filter(new Column("%s".formatted("partition")
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace","creditcard")
                .option("table",table)
                .option("pushdown", "true")
                .load()
                .select("partition", "offset")
                .filter(new Column("partition".toString()).isNotNull());

        if (df.rdd().isEmpty()) {
            return new Pair("startingOffsets", "earliest");
        }
        else {
            Dataset<Row> offsetDf = df.select("partition", "offset")
                    .groupBy("partition").agg(org.apache.spark.sql.functions.max("offset").as("offset"));
            return new Pair("startingOffsets", transformKafkaMetadataArrayToJson(offsetDf.collect()));
        }
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    public static String transformKafkaMetadataArrayToJson (Row[] array) {

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


//        String partitionOffset = Arrays.stream(array).reduce("")
//
//        a.toString() + "\""+ i.<Integer>getAs("partition") +"\":" + i.<Long>getAs("offset") + ", "
//
//
////        String partitionOffset = array
////                .foldLeft("")((a, i) => {
////            a + s""""${i.getAs[Int](("partition"))}":${i.getAs[Long](("offset"))}, """
////        })
//
//        System.out.println("Offset: " + partitionOffset.substring(0, partitionOffset.length() -2));
//
//        return "{\"creditTransaction\":\n" +
//                "          {\n" +
//                partitionOffset.substring(0, partitionOffset.length() -2)  +
//                "          }\n" +
//                "         }".replaceAll("\n", "").replaceAll(" ", "");
    }


    public static void main(String[] args)
    {
        Dataset<Row> df = sparkSession.read()
                .option("header", "true")
                .schema(Schema.fruadCheckedTransactionSchema)
                .csv(args[0]);

        df.printSchema();

        df.show(false);

        Dataset<Row> df2 = df.withColumn("trans_date",
                        org.apache.spark.sql.functions.split(df.col("trans_date"), "T").getItem(0))
                .withColumn("trans_time",
                        org.apache.spark.sql.functions.concat_ws(" ", df.col("trans_date"),
                                df.col("trans_time")))
                .withColumn("unix_time", org.apache.spark.sql.functions.unix_timestamp(
                        df.col("trans_time"), "YYYY-MM-dd HH:mm:ss").cast(TimestampType));
        df2.show(false);
    }
    public static <A, B> Collector<A, ?, B> foldLeft(final B init, final BiFunction<? super B, ? super A, ? extends B> f) {
        return Collectors.collectingAndThen(
                Collectors.reducing(Function.<B>identity(), a -> b -> f.apply(b, a), Function::andThen),
                endo -> endo.apply(init)
        );
    }

    <U, T> U foldLeft(Collection<T> sequence, U identity, BiFunction<U, ? super T, U> accumulator) {
        U result = identity;
        for (T element : sequence)
            result = accumulator.apply(result, element);
        return result;
    }
}
