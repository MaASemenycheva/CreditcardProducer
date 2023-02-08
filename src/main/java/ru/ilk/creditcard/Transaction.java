package ru.ilk.creditcard;

import org.apache.spark.sql.types.StructType;
import java.sql.Timestamp;
class Transaction {
    private String cc_num;
    private String first;
    private String last;
    private String trans_num;
    private Timestamp trans_time;
    //String unix_time;
    private String category;
    private String merchant;
    private String amt;
    private String merch_lat;
    private String merch_long;

    public Transaction(String cc_num,
                       String first,
                       String last,
                       String trans_num,
                       Timestamp trans_time,
                       //String unix_time,
                       String category,
                       String merchant,
                       String amt,
                       String merch_lat,
                       String merch_long) {
        this.cc_num = cc_num;
        this.first = first;
        this.last = last;
        this.trans_num = trans_num;
        this.trans_time = trans_time;
        // this.unix_time = unix_time;
        this.category = category;
        this.merchant = merchant;
        this.amt = amt;
        this.merch_lat = merch_lat;
        this.merch_long = merch_long;
    }
}


class DstreamTransaction {
    private String cc_num;
    private String first;
    private String last;
    private String trans_num;
    private Timestamp trans_time;
    private String category;
    private String merchant;
    private Double amt;
    private Double merch_lat;
    private Double merch_long;

    public DstreamTransaction(String cc_num,
                              String first,
                              String last,
                              String trans_num,
                              Timestamp trans_time,
                              String category,
                              String merchant,
                              Double amt,
                              Double merch_lat,
                              Double merch_long) {
        this.cc_num = cc_num;
        this.first = first;
        this.last = last;
        this.trans_num = trans_num;
        this.trans_time = trans_time;
        this.category = category;
        this.merchant = merchant;
        this.amt = amt;
        this.merch_lat = merch_lat;
        this.merch_long = merch_long;
    }
}




    /* Spark Dataset case class for mapping messages received from Kafka in Structured Streaming*/

class TransactionKafka {
    private String topic;
    private Integer partition;
    private Long offset;
    private Timestamp timestamp;
    private Integer timestampType;
    private Transaction transaction;

    public TransactionKafka(String topic,
                           Integer partition,
                           Long offset,
                           Timestamp timestamp,
                           Integer timestampType,
                           Transaction transaction) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.transaction = transaction;

    }
}


