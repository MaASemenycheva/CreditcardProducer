package ru.ilk.config;

import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Logger;
import java.io.File;

import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.kafka.KafkaConfig;
import ru.ilk.spark.SparkConfig;

public class Config {
    private static Logger logger = Logger.getLogger(Config.class.getName());
    public static com.typesafe.config.Config applicationConf = ConfigFactory.parseResources("application-local.conf");
    String runMode = "local";
    static String localProjectDir = "";

    /**
     * Parse a config object from application.conf file in src/main/resources
     */
    public static void parseArgs(String[] args) {
        if(args.length == 0) {
            defaultSetting();
        } else {
            com.typesafe.config.Config applicationConf = ConfigFactory.parseFile(new File(args[0]));
            String runMode = applicationConf.getString("config.mode");
            if(runMode == "local"){
                localProjectDir = "file:///"+ System.getProperty("user.home").toString() + "/frauddetection/";
            }
            loadConfig();
        }
    }

    public static void loadConfig() {
        CassandraConfig.load();
        KafkaConfig.load();
        SparkConfig.load();
    }

    public static void defaultSetting() {
        CassandraConfig.defaultSettng();
        KafkaConfig.defaultSetting();
        SparkConfig.defaultSetting();
    }

}
