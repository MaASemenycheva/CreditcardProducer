package ru.ilk.creditcard;

import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;


import static org.apache.spark.sql.types.DataTypes.*;

public class Schema {
    static String transactionStructureName = "transaction";
    static StructType transactionSchema = new StructType()
            .add(Creditcard.TransactionKafka.cc_num, StringType,true)
            .add(Creditcard.TransactionKafka.first, StringType, true)
            .add(Creditcard.TransactionKafka.last, StringType, true)
            .add(Creditcard.TransactionKafka.trans_num, StringType, true)
            .add(Creditcard.TransactionKafka.trans_date, StringType, true)
            .add(Creditcard.TransactionKafka.trans_time, StringType, true)
            .add(Creditcard.TransactionKafka.unix_time, LongType, true)
            .add(Creditcard.TransactionKafka.category, StringType, true)
            .add(Creditcard.TransactionKafka.merchant, StringType, true)
            .add(Creditcard.TransactionKafka.amt, DoubleType, true)
            .add(Creditcard.TransactionKafka.merch_lat, DoubleType, true)
            .add(Creditcard.TransactionKafka.merch_long, DoubleType, true);

    /* Transaction  Schema used while importing transaction data to Cassandra*/
    public static StructType fruadCheckedTransactionSchema = transactionSchema
            .add(Creditcard.TransactionKafka.is_fraud, DoubleType, true);

    /* Customer Schema used while importing customer data to Cassandra*/
    String customerStructureName = "customer";
    public static StructType customerSchema = new StructType()
            .add(Creditcard.Customer.cc_num, StringType, true)
            .add(Creditcard.Customer.first, StringType, true)
            .add(Creditcard.Customer.last, StringType, true)
            .add(Creditcard.Customer.gender, StringType, true)
            .add(Creditcard.Customer.street, StringType, true)
            .add(Creditcard.Customer.city, StringType, true)
            .add(Creditcard.Customer.state, StringType, true)
            .add(Creditcard.Customer.zip, StringType, true)
            .add(Creditcard.Customer.lat, DoubleType, true)
            .add(Creditcard.Customer.long_field, DoubleType, true)
            .add(Creditcard.Customer.job, StringType, true)
            .add(Creditcard.Customer.dob, TimestampType, true);


    /* Schema of transaction msgs received from Kafka. Json msg is received from Kafka. Hence evey field is treated as String */
    public static String kafkaTransactionStructureName = transactionStructureName;
    public static StructType kafkaTransactionSchema = new StructType()
            .add(Creditcard.TransactionKafka.cc_num, StringType,true)
            .add(Creditcard.TransactionKafka.first, StringType, true)
            .add(Creditcard.TransactionKafka.last, StringType, true)
            .add(Creditcard.TransactionKafka.trans_num, StringType, true)
            .add(Creditcard.TransactionKafka.trans_time, TimestampType, true)
            .add(Creditcard.TransactionKafka.category, StringType, true)
            .add(Creditcard.TransactionKafka.merchant, StringType, true)
            .add(Creditcard.TransactionKafka.amt, StringType, true)
            .add(Creditcard.TransactionKafka.merch_lat, StringType, true)
            .add(Creditcard.TransactionKafka.merch_long, StringType, true);


}
