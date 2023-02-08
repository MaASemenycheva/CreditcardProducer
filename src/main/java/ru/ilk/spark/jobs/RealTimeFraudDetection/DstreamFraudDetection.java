//package ru.ilk.spark.jobs.RealTimeFraudDetection;
package ru.ilk.spark.jobs.RealTimeFraudDetection;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.spark.connector.cql.CassandraConnector;
import javafx.util.Pair;
import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.javatuples.Tuple;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.cassandra.CassandraDriver;
import ru.ilk.cassandra.dao.CreditcardTransactionRepository;
import ru.ilk.cassandra.dao.KafkaOffsetRepository;
import ru.ilk.config.Config;
import ru.ilk.creditcard.Schema;
import ru.ilk.kafka.KafkaConfig;
import ru.ilk.spark.DataReader;
import ru.ilk.spark.GracefulShutdown;
import ru.ilk.spark.SparkConfig;
import ru.ilk.spark.jobs.SparkJob;
import ru.ilk.utils.Utils;
import org.apache.spark.streaming.dstream.*;
import org.apache.kafka.clients.consumer.*;
import java.util.*;
import org.javatuples.Triplet;
import org.apache.spark.streaming.kafka010.*;
import scala.Some;
import scala.collection.Iterable;
import scala.collection.Map;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.classTag;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;


public class DstreamFraudDetection extends SparkJob {

    private static Logger logger = Logger.getLogger(DstreamFraudDetection.class.getName());

    public DstreamFraudDetection(String appName) {
        super(appName);
        appName = "Fraud Detection using Dstream";
    }

    public static void main( String[] args ) {
        Config.parseArgs(args);

        Dataset<Row> customerDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.customer);

        Dataset<Row> customerAgeDF = customerDF.withColumn("age", (
                org.apache.spark.sql.functions.datediff(
                        org.apache.spark.sql.functions.current_date(),
                        org.apache.spark.sql.functions.to_date(customerDF.col("dob"))
                ).divide(365)
                .cast(IntegerType)));


//        Dataset<Row> customerAgeDF = customerDF.withColumn("age", (
//                org.apache.spark.sql.functions.datediff(
//                        org.apache.spark.sql.functions.current_date(),
//                        org.apache.spark.sql.functions.to_date(customerDF.col("dob"))
//                ).divide(365)
//                ).cast(IntegerType));
        customerAgeDF.cache();
//
        /* Load Preprocessing Model and Random Forest Model saved by Spark ML Job i.e FraudDetectionTraining */
        PipelineModel preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath);
        RandomForestClassificationModel randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath);

    /*
       Connector Object is created in driver. It is serializable.
       So once the executor get it, they establish the real connection
    */
        CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext().getConf());
        HashMap<String, String> mapData = new HashMap<String, String>();
        mapData.put("keyspace", CassandraConfig.keyspace);
        mapData.put("fraudTable", CassandraConfig.fraudTransactionTable);
        mapData.put("nonFraudTable", CassandraConfig.nonFraudTransactionTable);
        mapData.put("kafkaOffsetTable", CassandraConfig.kafkaOffsetTable);

        Broadcast<HashMap<String, String>> brodcastMap = sparkSession.sparkContext().broadcast(
                mapData, classTag(DstreamFraudDetection.class));

        StreamingContext ssc = new StreamingContext(sparkSession.sparkContext(), new Duration(SparkConfig.batchInterval));


        HashSet<String> topics = new HashSet<String>();
        topics.add((String) KafkaConfig.kafkaParams.get("topic"));
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) KafkaConfig.kafkaParams.get("bootstrap.servers"));
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, (String) KafkaConfig.kafkaParams.get("group.id"));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, (String) KafkaConfig.kafkaParams.get("auto.offset.reset"));


//        Optional<HashMap<TopicPartition, Long>> storedOffsets = CassandraDriver.readOffset(
//                CassandraConfig.keyspace,
//                CassandraConfig.kafkaOffsetTable,
//                (String) KafkaConfig.kafkaParams.get("topic"),
//                sparkSession);

        Optional<HashMap<TopicPartition, Long>> storedOffsets = CassandraDriver.readOffset(
                CassandraConfig.keyspace,
                CassandraConfig.kafkaOffsetTable,
                (String) KafkaConfig.kafkaParams.get("topic"),
                sparkSession
        );
//                .get();

//        String securityAnswer = (man.getAge() >= 18) ? "Все в порядке, проходите!" : "Этот фильм не подходит для вашего возраста!";
//        InputDStream<ConsumerRecord<String, String>> stream = (storedOffsets instanceof Optional<HashMap<TopicPartition, Long>>)?
//                KafkaUtils.createDirectStream(ssc,
//                        LocationStrategies.PreferConsistent(),
//                        new Assign<>(storedOffsets.get().keySet(), kafkaParams, storedOffsets.get())):
//                KafkaUtils.createDirectStream(ssc,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.Subscribe((Iterable<String>) topics, (Map<String, Object>) kafkaParams)
//                );

//
//        InputDStream<ConsumerRecord<String, String>> stream =
//        if (storedOffsets instanceof Optional<HashMap<TopicPartition, Long>>) {
//            stream = KafkaUtils.createDirectStream(ssc,
//                    LocationStrategies.PreferConsistent(),
//                    new Assign<>(storedOffsets.get().keySet(), kafkaParams, storedOffsets.get()));
//        } else {
//            stream = KafkaUtils.createDirectStream(ssc,
//                    LocationStrategies.PreferConsistent(),
//                    ConsumerStrategies.Subscribe((Iterable<String>) topics, (Map<String, Object>) kafkaParams)
//            );
//        }

//        Optional<HashMap<TopicPartition, Long>> fromOffsets = storedOffsets;
        InputDStream<ConsumerRecord<String, String>> stream =  KafkaUtils.createDirectStream(ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe((Iterable<String>) topics, (Map<String, Object>) kafkaParams)
                       );
//            case Optional<HashMap<TopicPartition, Long> storedOffsets:
//                KafkaUtils.createDirectStream(ssc,
//                        LocationStrategies.PreferConsistent(),
//                        new Assign<>(storedOffsets.get().keySet(), kafkaParams, storedOffsets.get()));


//         switch (storedOffsets) {
//
//            case  Optional<HashMap<TopicPartition, Long>> fromOffsets  ->
//                KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent(),
//                        new Assign<>(fromOffsets.get().keySet(),kafkaParams,fromOffsets.get()));
////                    String.format("String %s", s);
////            KafkaUtils.createDirectStream[String, String](ssc,
////                    PreferConsistent,
////                    Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
//            default        -> KafkaUtils.createDirectStream(ssc,
//                    LocationStrategies.PreferConsistent(),
//                    ConsumerStrategies.Subscribe((Iterable<String>) topics, (Map<String, Object>) kafkaParams)
//            );
//        };


//                InputDStream<ConsumerRecord<String, String>> stream1 =  switch (storedOffsets) {
//                    default:
//                        KafkaUtils.createDirectStream(ssc,
//                                LocationStrategies.PreferConsistent(),
//                                ConsumerStrategies.Subscribe((Iterable<String>) topics, (Map<String, Object>) kafkaParams)
//                        );
//                    case Optional<HashMap<TopicPartition, Long> storedOffsets:
//                        KafkaUtils.createDirectStream(ssc,
//                                LocationStrategies.PreferConsistent(),
//                                new Assign<>(storedOffsets.get().keySet(), kafkaParams, storedOffsets.get()));
//                InputDStream<ConsumerRecord<String, String>> stream =  switch (storedOffsets) {
//                    default:
//                        KafkaUtils.createDirectStream(ssc,
//                                LocationStrategies.PreferConsistent(),
//                                ConsumerStrategies.Subscribe((Iterable<String>) topics, (Map<String, Object>) kafkaParams)
//                        );
//                    case Optional<HashMap<TopicPartition, Long> storedOffsets:
//                        KafkaUtils.createDirectStream(ssc,
//                                LocationStrategies.PreferConsistent(),
//                                new Assign<>(storedOffsets.get().keySet(), kafkaParams, storedOffsets.get()));

//            case Optional<HashMap<TopicPartition, Long>> fromOffsets:
//                KafkaUtils.createDirectStream(ssc,
//                        LocationStrategies.PreferConsistent(),
//                        new Assign<>(fromOffsets.get().keySet(), kafkaParams, fromOffsets.get()));



//                KafkaUtils.createDirectStream(ssc,
//                        LocationStrategies.PreferConsistent(),
//                        new Assign<String, String>(storedOffsets.keySet(), kafkaParams, storedOffsets));


//            case Optional<HashMap<TopicPartition, Long>> fromOffsets:
//                KafkaUtils.createDirectStream(ssc,
//                        LocationStrategies.PreferConsistent(),
//                        new Assign<>(new ArrayList<>(fromOffsets.get().keySet()), kafkaParams, fromOffsets.get()));

//        };


//        DStream<Triplet<String, Integer, Long>> transactionStream = stream.map(cr ->
//             new Triplet<>(cr.value(), cr.partition(), cr.offset())
//        );

        DStream<Triplet<String, Integer, Long>> transactionStream = stream.map(cr ->
                new Triplet<>(cr.value(), cr.partition(), cr.offset()), classTag(DstreamFraudDetection.class));



        transactionStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                StructType schema = new StructType()
                        .add("transaction", DataTypes.StringType)
                        .add("partition", DataTypes.StringType)
                        .add("offset", DataTypes.StringType);
                Dataset<Row> kafkaTransactionDF = sparkSession.createDataFrame((List<Row>) rdd, schema);
                kafkaTransactionDF.withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
                        org.apache.spark.sql.functions.from_json(
                                kafkaTransactionDF.col("transaction"), Schema.kafkaTransactionSchema))
                        .select("transaction.*", "partition", "offset")
                        .withColumn("amt", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                        .col("amt")).cast(DoubleType))
                        .withColumn("merch_lat", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                        .col("merch_lat")).cast(DoubleType))
                        .withColumn("merch_long", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                        .col("merch_long")).cast (DoubleType))
                        .withColumn("trans_time", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                .col("trans_time")).cast(TimestampType));

                sparkSession.sqlContext().sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800");


                String COLUMN_DOUBLE_UDF_NAME = "distanceUdf";
                sparkSession.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF4<String, String, String, String, Double>)
                        (lat1, lon1, lat2, lon2) -> {
                            Double distance = Utils.getDistance(Double.valueOf(lat1), Double.valueOf( lon1), Double.valueOf(lat2), Double.valueOf(lon2));
                            return distance;
                        }, DataTypes.DoubleType);

                Dataset<Row> processedTransactionDF = kafkaTransactionDF.join(
                        org.apache.spark.sql.functions.broadcast(customerAgeDF), "cc_num")

                        .withColumn("distance",
                                org.apache.spark.sql.functions.lit(
                                        org.apache.spark.sql.functions.round(
                                                callUDF(COLUMN_DOUBLE_UDF_NAME,
                                                        col("lat"), col("long"), col("merch_lat"), col("merch_long"))
                                                , 2)));

                Dataset<Row> featureTransactionDF = preprocessingModel.transform(processedTransactionDF);
                Dataset<Row> predictionDF = randomForestModel.transform(featureTransactionDF)
                        .withColumnRenamed("prediction", "is_fraud");



        /*
         Connector Object is created in driver. It is serializable.
         It is serialized and send to executor. Once the executor get it, they establish the real connection
        */

                predictionDF.foreachPartition(partitionOfRecords -> {
                            /*
                             * dbname and table name are initialzed in the driver. foreachPartition is called in the executor, hence dbname
                             * and table names have to be broadcasted
                             */
                            String keyspace = brodcastMap.getValue().get("keyspace");
                            String fraudTable = brodcastMap.getValue().get("fraudTable");
                            String nonFraudTable = brodcastMap.getValue().get("nonFraudTable");
                            String kafkaOffsetTable = brodcastMap.getValue().get("kafkaOffsetTable");

                             /*
                                      Writing to Fraud, NonFruad and Offset Table in single iteration
                                      Cassandra prepare statement is used because it avoids pasring of the column for every insert and hence efficient
                                      Offset is inserted last to achieve atleast once semantics. it is possible that it may read duplicate creditcard
                                      transactions from kafka while restart.
                                      Even though duplicate creditcard transaction are read from kafka, writing to Cassandra is idempotent. Becasue
                                      cc_num and trans_time is the primary key. So you cannot have duplicate records with same cc_num and trans_time.
                                      As a result we achive exactly once semantics.
                            */
                            connector.withSessionDo(session -> {
                                //Prepare Statement for all three tables
                                PreparedStatement preparedStatementFraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(keyspace, fraudTable));
                                PreparedStatement preparedStatementNonFraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(keyspace, nonFraudTable));
                                PreparedStatement preparedStatementOffset = session.prepare(KafkaOffsetRepository.cqlOffsetPrepare(keyspace, kafkaOffsetTable));

                                HashMap<Integer, Long> partitionOffset = new HashMap<>();
                                partitionOfRecords.forEachRemaining(record -> {
                                    Double isFraud = record.<Double>getAs("is_fraud");
                                    if (isFraud == 1.0) {
                                        // Bind and execute prepared statement for Fraud Table
                                        session.execute(CreditcardTransactionRepository.cqlTransactionBind(preparedStatementFraud, record));
                                    }
                                    else if(isFraud == 0.0) {
                                        // Bind and execute prepared statement for NonFraud Table
                                        session.execute(CreditcardTransactionRepository.cqlTransactionBind(preparedStatementNonFraud, record));
                                    }
                                    //Get max offset in the current match
                                    Integer kafkaPartition = record.<Integer>getAs("partition");
                                    Long offset = record.<Long>getAs("offset");
                                    Long currentMaxOffset = partitionOffset.get(kafkaPartition);
                                    if (offset > currentMaxOffset) {
                                        partitionOffset.put(kafkaPartition, offset);
                                    }
                                    else {
                                        partitionOffset.put(kafkaPartition, offset);
                                    }
                                });
//                                partitionOffset.
                                partitionOffset.forEach( (k,v)-> {
                                    // Bind and execute prepared statement for Offset Table
                                    session.execute(KafkaOffsetRepository
                                            .cqlOffsetBind(preparedStatementOffset, new Pair<Integer, Long>(k,v)));

                                });
                                return partitionOffset;
                            });
                        });
            } else {
                logger.info("Did not receive any data");
            }
            return null; /// !!!!!!!!!!!!!!!!!!!
        });

        ssc.start();
        GracefulShutdown.handleGracefulShutdown(1000, ssc, sparkSession);
    }
}
