package ru.ilk.cassandra.foreachSink;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.cassandra.CassandraDriver;
import ru.ilk.creditcard.Creditcard;

import java.sql.Timestamp;

public class CassandraSinkForeach extends ForeachWriter<Row> {
    public static String db;
    public static String table;

    public CassandraSinkForeach(String dbName, String tableName) {
        db = dbName;
        table = tableName;
    }

    public static String cqlTransaction(Row record) {
        return "insert into "
                + db
                + "."
                + table
                + " ("
                + Creditcard.TransactionCassandra.cc_num
                + ", "
                + Creditcard.TransactionCassandra.trans_time
                + ", "
                + Creditcard.TransactionCassandra.trans_num
                + ", "
                + Creditcard.TransactionCassandra.category
                + ", "
                + Creditcard.TransactionCassandra.merchant
                + ", "
                + Creditcard.TransactionCassandra.amt
                + ", "
                + Creditcard.TransactionCassandra.merch_lat
                + ", "
                + Creditcard.TransactionCassandra.merch_long
                + ", "
                + Creditcard.TransactionCassandra.distance
                + ", "
                + Creditcard.TransactionCassandra.age
                + ", "
                + Creditcard.TransactionCassandra.is_fraud
                + ") values("
                + record.<String>getAs(Creditcard.TransactionCassandra.cc_num)
                + ", "
                + record.<Timestamp>getAs(Creditcard.TransactionCassandra.trans_time)
                + ", "
                + record.<String>getAs(Creditcard.TransactionCassandra.trans_num)
                + ", "
                + record.<String>getAs(Creditcard.TransactionCassandra.category)
                + ", "
                + record.<String>getAs(Creditcard.TransactionCassandra.merchant)
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.amt)
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.merch_lat)
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.merch_long)
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.distance)
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.age)
                + ", "
                + record.<Double>getAs(Creditcard.TransactionCassandra.is_fraud)
                + ")";
    }

    private String cqlOffset(Row record) {
        return "insert into "
                + db
                + "."
                + table
                + " ("
                + Creditcard.TransactionCassandra.kafka_partition
                + ", "
                + Creditcard.TransactionCassandra.kafka_offset
                + ") values("
                + record.<Integer>getAs(Creditcard.TransactionCassandra.kafka_partition)
                + ", "
                + record.<Long>getAs(Creditcard.TransactionCassandra.kafka_offset)
                + ")";
    }

    @Override
    public boolean open(long partitionId, long version) {
        // open connection
        //@TODO command to check if cassandra cluster is up
        return true;
    }

    @Override
    public void process(Row record) {
        if (table == CassandraConfig.fraudTransactionTable || table == CassandraConfig.nonFraudTransactionTable) {
            System.out.println("Saving record: $record");
            CassandraDriver.connector.withSessionDo(session ->
                    session.execute(cqlTransaction(record))
            );
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
    }
}
