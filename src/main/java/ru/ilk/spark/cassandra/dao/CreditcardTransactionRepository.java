package ru.ilk.spark.cassandra.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

@FunctionalInterface
interface CqlTransactionPrepareInterface {

    // абстрактный метод
    String cqlTransactionPrepare(String db, String table);
}

@FunctionalInterface
interface CqlTransactionBindInterface {
    // абстрактный метод
    BoundStatement cqlTransactionBind(PreparedStatement n);
}

public class CreditcardTransactionRepository {

    private static Logger log = Logger.getLogger(CreditcardTransactionRepository.class.getName());


    public static void main( String[] args ) {

        CqlTransactionPrepareInterface ref = (db, table) -> {

            return     """
     insert into ${db}.${table} (
       ${Enums.TransactionCassandra.cc_num},
       ${Enums.TransactionCassandra.trans_time},
       ${Enums.TransactionCassandra.trans_num},
       ${Enums.TransactionCassandra.category},
       ${Enums.TransactionCassandra.merchant},
       ${Enums.TransactionCassandra.amt},
       ${Enums.TransactionCassandra.merch_lat},
       ${Enums.TransactionCassandra.merch_long},
       ${Enums.TransactionCassandra.distance},
       ${Enums.TransactionCassandra.age},
       ${Enums.TransactionCassandra.is_fraud}
     )
     values(
       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""";
        };
        // вызов метода из интерфейса
        System.out.println("Lambda reversed = " + ref.cqlTransactionPrepare("",""));

        CqlTransactionBindInterface ref1 = (prepared) -> {
            BoundStatement bound = prepared.bind();
//            bound.setString(Enums.TransactionCassandra.cc_num, record.getAs[String](Enums.TransactionCassandra.cc_num))
//            bound.setTimestamp(Enums.TransactionCassandra.trans_time, record.getAs[Timestamp](Enums.TransactionCassandra.trans_time))
//            bound.setString(Enums.TransactionCassandra.trans_num, record.getAs[String](Enums.TransactionCassandra.trans_num))
//            bound.setString(Enums.TransactionCassandra.category, record.getAs[String](Enums.TransactionCassandra.category))
//            bound.setString(Enums.TransactionCassandra.merchant, record.getAs[String](Enums.TransactionCassandra.merchant))
//            bound.setDouble(Enums.TransactionCassandra.amt, record.getAs[Double](Enums.TransactionCassandra.amt))
//            bound.setDouble(Enums.TransactionCassandra.merch_lat, record.getAs[Double](Enums.TransactionCassandra.merch_lat))
//            bound.setDouble(Enums.TransactionCassandra.merch_long, record.getAs[Double](Enums.TransactionCassandra.merch_long))
//            bound.setDouble(Enums.TransactionCassandra.distance, record.getAs[Double](Enums.TransactionCassandra.distance))
//            bound.setInt(Enums.TransactionCassandra.age, record.getAs[Int](Enums.TransactionCassandra.age))
//            bound.setDouble(Enums.TransactionCassandra.is_fraud, record.getAs[Double](Enums.TransactionCassandra.is_fraud))
            return bound;
        };
    }

}
