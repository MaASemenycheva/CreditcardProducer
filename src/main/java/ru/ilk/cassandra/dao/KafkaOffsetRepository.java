package ru.ilk.cassandra.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import ru.ilk.creditcard.Creditcard;

public class KafkaOffsetRepository {
    private static Logger logger = Logger.getLogger(KafkaOffsetRepository.class.getName());


    public static String cqlOffsetPrepare(String db, String table) {
        return  "insert into "
                + db
                + "."
                + table
                + " ("
                + Creditcard.TransactionCassandra.kafka_partition
                + ", "
                + Creditcard.TransactionCassandra.kafka_offset
                +") values(?, ?)";

    }

    public static BoundStatement cqlOffsetBind(PreparedStatement prepared,
                                               Pair<Integer, Long> record) {
        BoundStatement bound = prepared.bind();
        bound.setInt(Creditcard.TransactionCassandra.kafka_partition,record.getKey());
        bound.setLong(Creditcard.TransactionCassandra.kafka_offset, record.getValue());
        return bound;
    }


    public static String cqlOffset(String db, String table, Row record) {
        return "insert into "
                + db
                + "."
                + table
                + " ("
                + Creditcard.TransactionCassandra.kafka_partition
                + ", "
                + Creditcard.TransactionCassandra.kafka_offset
                +") values("
                + record.<Integer>getAs(Creditcard.TransactionCassandra.kafka_partition)
                + ", "
                + record.<Long>getAs(Creditcard.TransactionCassandra.kafka_offset)
                + ")";
    }

}
