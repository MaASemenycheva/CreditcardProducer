package ru.ilk.kafka;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.StringType;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import ru.ilk.creditcard.Creditcard;
import ru.ilk.creditcard.Schema;
import org.apache.spark.sql.functions.*;

public class KafkaSource {

    private static Logger logger = Logger.getLogger(KafkaSource.class.getName());

    /* Read stream from Kafka using Structured Streaming */
    public static Dataset<Row> readStream(SparkSession sparkSession) {
        String startingOption = "startingOffsets";
        String partitionsAndOffsets = "earliest";
        logger.info("Reading from Kafka");
        //logger.info("partitionsAndOffsets: " + partitionsAndOffsets);
        Dataset<Row> data = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", (String) KafkaConfig.kafkaParams.get("bootstrap.servers"))
                .option("subscribe", (String) KafkaConfig.kafkaParams.get("topic"))
                .option("enable.auto.commit", (Boolean) KafkaConfig.kafkaParams.get("enable.auto.commit")) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
                .option("group.id", (Double) KafkaConfig.kafkaParams.get("group.id"))
                //.option(startingOption, partitionsAndOffsets) //this only applies when a new query is started and that resuming will always pick up from where the query left off
                .load();


        Dataset<Row> data2 = data.withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
                        functions.from_json(data.col("value")
                                ,Schema.kafkaTransactionSchema));
        return data2;
//        Dataset<TransactionKafka> df = data.withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
//                functions.from_json(data.col("value")
//                        ,Schema.kafkaTransactionSchema)).as(TransactionKafka);
    }
}
