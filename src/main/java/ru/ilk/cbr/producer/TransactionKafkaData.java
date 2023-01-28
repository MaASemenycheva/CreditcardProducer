package ru.ilk.cbr.producer;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Class to represent the Creditcard data.
 *
 */
public class TransactionKafkaData implements Serializable{

    public static String cc_num;
    public static String first;
    public static String last;
    public static String trans_num;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    public static Date trans_date;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    public static Date trans_time;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    public static Date unix_time;
    public static String category;
    public static String merchant;
    public static Double amt;
    public static Double merch_lat;
    public static Double merch_long;
    public static Double distance;
    public static Integer age;
    public static String is_fraud;
    public static String kafka_partition;
    public static String kafka_offset;


    public TransactionKafkaData(){

    }

    public TransactionKafkaData(String cc_num,
                                String first,
                                String last,
                                String trans_num,
                                Date trans_date,
                                Date trans_time,
                                Date unix_time,
                                String category,
                                String merchant,
                                Double amt,
                                Double merch_lat,
                                Double merch_long,
                                Double distance,
                                Integer age,
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

    public Date getTrans_date() {
        return trans_date;
    }

    public Date getTrans_time() {
        return trans_time;
    }

    public Date getUnix_time() {
        return unix_time;
    }

    public String getCategory() {

        return category;
    }

    public String getMerchant() {
        return merchant;
    }

    public Double getAmt() {
        return amt;
    }

    public Double getMerch_lat() {
        return merch_lat;
    }

    public Double getMerch_long() {
        return merch_long;
    }

    public Double getDistance() {
        return distance;
    }

    public Integer getAge() {
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
