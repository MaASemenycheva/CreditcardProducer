package ru.ilk.cassandra.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import ru.ilk.creditcard.Creditcard;
import java.sql.Timestamp;

public class CreditcardTransactionRepository {
    private static Logger logger = Logger.getLogger(CreditcardTransactionRepository.class.getName());

    public static String cqlTransactionPrepare(String db,
                                               String table) {
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
                +") values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static BoundStatement cqlTransactionBind(PreparedStatement prepared, Row record) {
        BoundStatement bound = prepared.bind();
        bound.setString(Creditcard.TransactionCassandra.cc_num,
                record.<String>getAs(Creditcard.TransactionCassandra.cc_num));
        bound.setTimestamp(Creditcard.TransactionCassandra.trans_time,
                record.<Timestamp>getAs(Creditcard.TransactionCassandra.trans_time));
        bound.setString(Creditcard.TransactionCassandra.trans_num,
                record.<String>getAs(Creditcard.TransactionCassandra.trans_num));
        bound.setString(Creditcard.TransactionCassandra.category,
                record.<String>getAs(Creditcard.TransactionCassandra.category));
        bound.setString(Creditcard.TransactionCassandra.merchant,
                record.<String>getAs(Creditcard.TransactionCassandra.merchant));
        bound.setDouble(Creditcard.TransactionCassandra.amt,
                record.<Double>getAs(Creditcard.TransactionCassandra.amt));
        bound.setDouble(Creditcard.TransactionCassandra.merch_lat,
                record.<Double>getAs(Creditcard.TransactionCassandra.merch_lat));
        bound.setDouble(Creditcard.TransactionCassandra.merch_long,
                record.<Double>getAs(Creditcard.TransactionCassandra.merch_long));
        bound.setDouble(Creditcard.TransactionCassandra.distance,
                record.<Double>getAs(Creditcard.TransactionCassandra.distance));
        bound.setInt(Creditcard.TransactionCassandra.age,
                record.<Integer>getAs(Creditcard.TransactionCassandra.age));
        bound.setDouble(Creditcard.TransactionCassandra.is_fraud,
                record.<Double>getAs(Creditcard.TransactionCassandra.is_fraud));
        return bound;
    }

    public static String cqlTransaction(String db, String table, Row record) {
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
                +")";

    }

//@FunctionalInterface
//interface CqlTransactionPrepareInterface {
//
//    // абстрактный метод
//    String cqlTransactionPrepare(String db, String table);
//}
//
//@FunctionalInterface
//interface CqlTransactionBindInterface {
//    // абстрактный метод
//    BoundStatement cqlTransactionBind(PreparedStatement n);
//}

//    private static Logger logger = Logger.getLogger(CreditcardTransactionRepository.class.getName());
//
//
//    public static void main( String[] args ) {
//
//        CqlTransactionPrepareInterface ref = (db, table) -> {
//
//            return     """
//     insert into ${db}.${table} (
//       ${Enums.TransactionCassandra.cc_num},
//       ${Enums.TransactionCassandra.trans_time},
//       ${Enums.TransactionCassandra.trans_num},
//       ${Enums.TransactionCassandra.category},
//       ${Enums.TransactionCassandra.merchant},
//       ${Enums.TransactionCassandra.amt},
//       ${Enums.TransactionCassandra.merch_lat},
//       ${Enums.TransactionCassandra.merch_long},
//       ${Enums.TransactionCassandra.distance},
//       ${Enums.TransactionCassandra.age},
//       ${Enums.TransactionCassandra.is_fraud}
//     )
//     values(
//       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
//        )""";
//        };
//        // вызов метода из интерфейса
//        System.out.println("Lambda reversed = " + ref.cqlTransactionPrepare("",""));
//
//        CqlTransactionBindInterface ref1 = (prepared) -> {
//            BoundStatement bound = prepared.bind();
////            bound.setString(Enums.TransactionCassandra.cc_num, record.getAs[String](Enums.TransactionCassandra.cc_num))
////            bound.setTimestamp(Enums.TransactionCassandra.trans_time, record.getAs[Timestamp](Enums.TransactionCassandra.trans_time))
////            bound.setString(Enums.TransactionCassandra.trans_num, record.getAs[String](Enums.TransactionCassandra.trans_num))
////            bound.setString(Enums.TransactionCassandra.category, record.getAs[String](Enums.TransactionCassandra.category))
////            bound.setString(Enums.TransactionCassandra.merchant, record.getAs[String](Enums.TransactionCassandra.merchant))
////            bound.setDouble(Enums.TransactionCassandra.amt, record.getAs[Double](Enums.TransactionCassandra.amt))
////            bound.setDouble(Enums.TransactionCassandra.merch_lat, record.getAs[Double](Enums.TransactionCassandra.merch_lat))
////            bound.setDouble(Enums.TransactionCassandra.merch_long, record.getAs[Double](Enums.TransactionCassandra.merch_long))
////            bound.setDouble(Enums.TransactionCassandra.distance, record.getAs[Double](Enums.TransactionCassandra.distance))
////            bound.setInt(Enums.TransactionCassandra.age, record.getAs[Int](Enums.TransactionCassandra.age))
////            bound.setDouble(Enums.TransactionCassandra.is_fraud, record.getAs[Double](Enums.TransactionCassandra.is_fraud))
//            return bound;
//        };
//    }

}
