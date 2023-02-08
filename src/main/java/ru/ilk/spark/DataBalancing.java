package ru.ilk.spark;

import javafx.util.Pair;
import jnr.posix.windows.CommonFileInformation;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.nio.file.Path;

import static java.math.BigDecimal.valueOf;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static org.apache.spark.sql.Encoders.INT;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;

public class DataBalancing {

    private static Logger logger = Logger.getLogger(DataBalancing.class.getName());
  /*
  There will be more non-fruad transaction then fraund transaction. So non-fraud transactions must be balanced
  Kmeans Algorithm is used to balance non fraud transatiion.
  No. of non-fruad transactions  are balanced(reduced) to no. of fraud transaction
   */
    public static  Dataset<Row> createBalancedDataframe( Dataset<Row> df,
                                                         Integer reductionCount,
                                                         SparkSession sparkSession
    ) {
        KMeans kMeans = new KMeans().setK(reductionCount).setMaxIter(30);
        KMeansModel kMeansModel = kMeans.fit(df);
//        List<Pair<org.apache.spark.ml.linalg.Vector, Integer>> sdsd =
//                (List<Pair<org.apache.spark.ml.linalg.Vector, Integer>>)
//                        Arrays.stream(kMeansModel.clusterCenters()).map(v ->
//                                new Pair<org.apache.spark.ml.linalg.Vector, Integer>(v, 0));
        StructType featureSchema = new StructType();
//        featureSchema.add("features", SQLDataTypes.VectorType(), true);
//        featureSchema.add("is_fraud", SQLDataTypes.VectorType(), true);
        featureSchema.add("features", StringType, true);
        featureSchema.add("is_fraud", StringType, true);




        List<Pair<org.apache.spark.ml.linalg.Vector, Integer>> listVariable =
                Arrays.stream(kMeansModel.clusterCenters()).map(
                v -> new Pair<org.apache.spark.ml.linalg.Vector, Integer>(v, 0)
        ).collect(Collectors.toList());
        //      Arrays.stream(kMeansModel.clusterCenters()).map(v -> new Pair<org.apache.spark.ml.linalg.Vector, Integer>(v, 0));
        Dataset<Row> dfData = sparkSession.createDataFrame((JavaRDD<Row>) listVariable, featureSchema);
        return dfData;
    }

}
