package ru.ilk.testing;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.ilk.spark.SparkConfig;
import ru.ilk.spark.jobs.SparkJob;
import org.apache.spark.sql.functions.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.functions.*;
import ru.ilk.utils.Utils;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class SamplePipeline extends SparkJob {
    public SamplePipeline(String appName) {
        super(appName);
        appName = "Sample App";
    }

    private static <T> T[] append(T[] arr, T element) {
        return ArrayUtils.add(arr, element);
    }

    static SparkJob sparkJobVariable = new SparkJob("Sample App");
    static SparkSession sparkSession = sparkJobVariable.sparkSession;

    public static Double getDistance (Double lat1, Double lon1, Double lat2, Double lon2)  {
        Integer r  = 6371; //Earth radius
        Double latDistance = Math.toRadians(lat2 - lat1);
        Double lonDistance = Math.toRadians(lon2 - lon1);
        Double a  = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        Double c  = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        Double distance = r * c;
        return distance;
    }

    public static void main(String[] args)
    {
        HashMap<String,String> readOption = new HashMap();
        readOption.put("inferSchema","true");
        readOption.put("header", "true");

        Dataset<Row> rawTransactionDF = sparkSession.read()
                .options(readOption)
                .csv(SparkConfig.transactionDatasouce);
        rawTransactionDF.printSchema();

        Dataset<Row> rawCustomerDF = sparkSession.read()
                .options(readOption)
                .csv(SparkConfig.customerDatasource);
        rawCustomerDF.printSchema();

        Dataset<Row> customer_age_df = rawCustomerDF
                .withColumn("age", (org.apache.spark.sql.functions.datediff(
                                        org.apache.spark.sql.functions.current_date(),
                                        org.apache.spark.sql.functions.to_date(rawCustomerDF.col("dob"))
                                ).divide(365)).cast(IntegerType))
                .withColumnRenamed("cc_num", "cardNo");


        String COLUMN_DOUBLE_UDF_NAME = "distanceUdf";
        sparkSession.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF4<String, String, String, String, Double>)
                (lat1, lon1, lat2, lon2) -> {
                    Double distance = Utils.getDistance(Double.valueOf(lat1), Double.valueOf( lon1), Double.valueOf(lat2), Double.valueOf(lon2));
                    return distance;
                }, DataTypes.DoubleType);

        Dataset<Row> processedTransactionDF = customer_age_df.join(rawTransactionDF, customer_age_df.col("cardNo")
                        .contains(rawTransactionDF.col("cc_num")))
                .withColumn("distance", org.apache.spark.sql.functions.lit(
                        org.apache.spark.sql.functions.round(
                                callUDF(COLUMN_DOUBLE_UDF_NAME,
                                        col("lat"), col("long"), col("merch_lat"), col("merch_long"))
                                , 2)))
                .selectExpr("cast(cc_num as string) cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud");

        processedTransactionDF.cache();

        Dataset<Row> fraudDF = processedTransactionDF.filter(processedTransactionDF.col("is_fraud").contains(1));
        Dataset<Row> nonFraudDF = processedTransactionDF.filter(processedTransactionDF.col("is_fraud").contains(0));

        Long fraudCount = fraudDF.count();


        StringIndexer ccIndexer = new StringIndexer().setInputCol("cc_num").setOutputCol("cc_numIndex");
        StringIndexer categoryIndexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex");
        StringIndexer merchantIndexer = new StringIndexer().setInputCol("merchant").setOutputCol("merchantIndex");
        StringIndexer[] allIndexer = new StringIndexer[]{ccIndexer, categoryIndexer, merchantIndexer};
        String[] featurecoloumns = new String[]{"cc_numIndex", "categoryIndex", "merchantIndex", "distance", "amt", "age"};
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featurecoloumns).setOutputCol("features");

        append(allIndexer, vectorAssembler);
        Pipeline pipeline = new Pipeline().setStages(allIndexer);
        PipelineModel dummyModel = pipeline.fit(nonFraudDF);
        Dataset<Row> feautureNonFraudDF = dummyModel.transform(nonFraudDF);

        feautureNonFraudDF.show(false);

        KMeans kMeans = new KMeans().setK(fraudCount.intValue()).setMaxIter(30);
        KMeansModel kMeansPredictionModel = kMeans.fit(feautureNonFraudDF);

//        StructField structTypeVariable = new StructField().add("feature_1", SQLDataTypes.VectorType(), true);
//        StructField[] structFieldVariable = {};
//        structFieldVariable.add("feature_1", DataTypes.v, true);
//        StructField featureSchema = new StructField();
//        val featureSchema = StructType(Array(StructField("feature_1", VectorType, true)))

        StructType featureSchema = new StructType();
        featureSchema.add("feature_1", SQLDataTypes.VectorType(), true);


        List<Row> rowList = (List<Row>) Arrays.stream(kMeansPredictionModel.clusterCenters()).map(v -> v);
//        List<Row> rowList = kMeansPredictionModel.clusterCenters().toList.map(v => Row(v))
//        RDD<Row> rowRdd = sparkSession.sparkContext().makeRDD(rowList);
        Dataset<Row> sampledFeatureDF = sparkSession.createDataFrame(rowList, featureSchema);
        sampledFeatureDF.show(false);





    }

}
