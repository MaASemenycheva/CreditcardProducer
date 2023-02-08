package ru.ilk.cbr.producer;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Class to represent the Creditcard data.
 *
 */
public class TransactionKafkaData implements Serializable{

    public static String cc_num = "cc_num";
    public static String first = "first";
    public static String last = "last";
    public static String trans_num = "trans_num";
//    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
//    public static Date trans_date;
//    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
//    public static Date trans_time;
//    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    public static String trans_time = "trans_time";
    public static String trans_date = "trans_date";
    public static String unix_time = "unix_time";
    public static String category = "category";
    public static String merchant = "merchant";
    public static String amt = "amt";
    public static String merch_lat = "merch_lat";
    public static String merch_long = "merch_long";
    public static String distance = "distance";
    public static String age = "age";
    public static String is_fraud = "is_fraud";
    public static String kafka_partition = "kafka_partition";
    public static String kafka_offset = "kafka_offset";


    public TransactionKafkaData(){

    }

    public TransactionKafkaData(String cc_num,
                                String first,
                                String last,
                                String trans_num,
                                String trans_date,
                                String trans_time,
                                String unix_time,
                                String category,
                                String merchant,
                                String amt,
                                String merch_lat,
                                String merch_long,
                                String distance,
                                String age,
                                String is_fraud,
                                String kafka_partition,
                                String kafka_offset) {
        super();
        this.cc_num = cc_num;
        this.first = first;
        this.last = last;
        this.trans_num = trans_num;
        this.trans_date = trans_date;
        this.trans_time = trans_time;
        this.unix_time = unix_time;
        this.category = category;
        this.merchant = merchant;
        this.amt = amt;
        this.merch_lat = merch_lat;
        this.merch_long = merch_long;
        this.distance = distance;
        this.age = age;
        this.is_fraud = is_fraud;
        this.kafka_partition = kafka_partition;
        this.kafka_offset = kafka_offset;
    }

    public String getCc_num() {
        return cc_num;
    }

    public String getFirst() {
        return first;
    }

    public String getLast() {
        return last;
    }

    public String getTrans_num() {
        return trans_num;
    }

    public String getTrans_date() {
        return trans_date;
    }

    public String getTrans_time() {
        return trans_time;
    }

    public String getUnix_time() {
        return unix_time;
    }

    public String getCategory() {
        return category;
    }

    public String getMerchant() {
        return merchant;
    }

    public String getAmt() {
        return amt;
    }

    public String getMerch_lat() {
        return merch_lat;
    }

    public String getMerch_long() {
        return merch_long;
    }

    public String getDistance() {
        return distance;
    }

    public String getAge() {
        return age;
    }

    public String getIs_fraud() {
        return is_fraud;
    }

    public String getKafka_partition() {
        return kafka_partition;
    }

    public String getKafka_offset() {
        return kafka_offset;
    }


}
