package ru.ilk.cbr.producer;

import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.*;
import com.google.gson.*;
import com.typesafe.config.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;


/**
 * Class to represent the Creditcard data.
 *
 */

@FunctionalInterface
interface GetCsvIteratorInterface {
    // abstract method
    Iterator<CSVRecord> getCsvIterator(String fileName) throws IOException;
}

@FunctionalInterface
interface PublishJsonMsgInterface {
    // abstract method
    void publishJsonMsg(String fileName) throws IOException, InterruptedException, ExecutionException;
}


public class TransactionProducer {
    static Config applicationConf = ConfigFactory.parseResources("application-local.conf");
    // set producer properties
    static Properties props = new Properties();
    static String topic;
    static void load() {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("kafka.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.key.serializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.value.serializer"));
        props.put(ProducerConfig.ACKS_CONFIG, applicationConf.getString("kafka.acks"));
        props.put(ProducerConfig.RETRIES_CONFIG, applicationConf.getString("kafka.retries"));
        topic = applicationConf.getString("kafka.topic");
    }


    public static class MyProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) System.out.println("AsynchronousProducer failed with an exception" + e);
            else {
                System.out.println("Sent data to partition: " + recordMetadata.partition() + " and offset: " + recordMetadata.offset());
            }
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        String path = args[0];
        Config applicationConf = ConfigFactory.parseFile(new File(path));
        load();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        String file = applicationConf.getString("kafka.producer.file");


        GetCsvIteratorInterface getCsvIterator = (fileName) -> {
            File files = new File(fileName);
            CSVParser csvParser = CSVParser.parse(files, Charset.forName("UTF-8"), CSVFormat.DEFAULT);
            return csvParser.iterator();
        };

        PublishJsonMsgInterface publishJsonMsg = (fileName) -> {
            Gson gson = new Gson();
            Iterator<CSVRecord> csvIterator = getCsvIterator.getCsvIterator(fileName);
            Random rand = new Random();
            Integer count = 0;
            while (csvIterator.hasNext()) {
                CSVRecord record = csvIterator.next();
                JsonObject obj = new JsonObject();
                SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                isoFormat.setTimeZone(TimeZone.getTimeZone("IST"));
                Date d = new Date();
                String timestamp = isoFormat.format(d);
                Long unix_time = d.getTime();
                obj.addProperty(TransactionKafkaData.cc_num, record.get(0));
                obj.addProperty(TransactionKafkaData.first, record.get(1));
                obj.addProperty(TransactionKafkaData.last, record.get(2));
                obj.addProperty(TransactionKafkaData.trans_num, record.get(3));
                obj.addProperty(String.valueOf(TransactionKafkaData.trans_time), timestamp);
                //obj.addProperty(TransactionKafkaEnum.unix_time, unix_time);
                obj.addProperty(TransactionKafkaData.category, record.get(7));
                obj.addProperty(TransactionKafkaData.merchant, record.get(8));
                obj.addProperty(String.valueOf(TransactionKafkaData.amt), record.get(9));
                obj.addProperty(String.valueOf(TransactionKafkaData.merch_lat), record.get(10));
                obj.addProperty(String.valueOf(TransactionKafkaData.merch_long), record.get(11));
                String json = gson.toJson(obj);
                System.out.println("Transaction Record: " + json);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, json); //Round Robin Partitioner


//                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, json.hashCode().toString(), json); //Hash Partitioner
//                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, 1, json.hashCode().toString(), json); //Specific Partition
//                producer.send(producerRecord); //Fire and Forget
//                producer.send(producerRecord).get(); /*Synchronous Producer */
                producer.send(producerRecord, new MyProducerCallback()); /*Asynchrounous Produer */
                Thread.sleep(rand.nextInt(3000 - 1000) + 1000);
            }
        };






        publishJsonMsg.publishJsonMsg(file);
    }
}


