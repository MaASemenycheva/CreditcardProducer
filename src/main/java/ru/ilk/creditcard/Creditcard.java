package ru.ilk.creditcard;

import java.util.Enumeration;

public class Creditcard {
    public class TransactionKafka {
        public static String cc_num = "cc_num";
        static String first = "first";
        static String last = "last";
        public static String trans_num = "trans_num";
        static String trans_date = "trans_date";
        public static String trans_time = "trans_time";
        static String unix_time = "unix_time";
        public static String category = "category";
        public static String merchant = "merchant";
        public static String amt = "amt";
        public static String merch_lat = "merch_lat";
        public static String merch_long = "merch_long";
        public static String distance = "distance";
        public static String age = "age";
        public static String is_fraud = "is_fraud";
        public static String kafka_partition = "partition";
        public static String kafka_offset = "offset";
    }

    public class Customer {
        static String cc_num = "cc_num";
        static String first = "first";
        static String last = "last";
        static String gender = "gender";
        static String street = "street";
        static String city = "city";
        static String state = "state";
        static String zip = "zip";
        static String lat = "lat";
        static String long_field = "long";
        static String job = "job";
        static String dob = "dob";
    }

    public class TransactionCassandra extends TransactionKafka {
    }
}
