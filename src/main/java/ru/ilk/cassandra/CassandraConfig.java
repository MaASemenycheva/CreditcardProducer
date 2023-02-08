package ru.ilk.cassandra;

import org.apache.log4j.Logger;
import com.typesafe.config.*;
import ru.ilk.config.Config;

public class CassandraConfig {
    private static Logger logger = Logger.getLogger(CassandraConfig.class.getName());
    public static String keyspace;
    public static String fraudTransactionTable;
    public static String nonFraudTransactionTable;
    public static String kafkaOffsetTable;
    public static String customer;
    public static String cassandraHost;

    /*Configuration setting are loaded from application.conf when you run Spark Standalone cluster*/
    public static void load() {
        logger.info("Loading Cassandra Setttings");
        keyspace = Config.applicationConf.getString("config.cassandra.keyspace");
        fraudTransactionTable = Config.applicationConf.getString("config.cassandra.table.fraud.transaction");
        nonFraudTransactionTable = Config.applicationConf.getString("config.cassandra.table.non.fraud.transaction");
        kafkaOffsetTable = Config.applicationConf.getString("config.cassandra.table.kafka.offset");
        customer = Config.applicationConf.getString("config.cassandra.table.customer");
        cassandraHost = Config.applicationConf.getString("config.cassandra.host");
    }

    /* Default Settings will be used when you run the project from Intellij */
    public static void defaultSettng(){
        keyspace = "creditcard";
        fraudTransactionTable = "fraud_transaction";
        nonFraudTransactionTable = "non_fraud_transaction";
        kafkaOffsetTable = "kafka_offset";
        customer = "customer";
        cassandraHost = "localhost";
    }
}
