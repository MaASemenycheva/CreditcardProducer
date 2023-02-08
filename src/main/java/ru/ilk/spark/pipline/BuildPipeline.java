package ru.ilk.spark.pipline;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.types.*;

public class BuildPipeline {

    private static Logger logger = Logger.getLogger(BuildPipeline.class.getName());

    public static List<StringIndexer> createStringIndexer(List<String> columns)  {
        List<StringIndexer> result = columns.stream().map(column -> {
            StringIndexer stringIndexer = new StringIndexer();
            stringIndexer.setInputCol(column).setOutputCol(column.toString()+"_indexed");
            return stringIndexer;
        }).collect(Collectors.toList());
        return result;
    }

    public static List<StringIndexer> createStringIndexer1(List<String> columns) {
        List<StringIndexer> result = new ArrayList<>();
        for (String column : columns) {
            StringIndexer stringIndexer = new StringIndexer();
            stringIndexer.setInputCol(column).setOutputCol(column.toString() + "_indexed");
            result.add(stringIndexer);
        }
        return result;
    }


    public static List<OneHotEncoder> createOneHotEncoder(List<String> columns)  {
        List<OneHotEncoder> result = columns.stream().map(column -> {
            OneHotEncoder oneHotEncoder = new OneHotEncoder();
            oneHotEncoder.setInputCol(column.toString() + "_indexed")
                    .setOutputCol(column.toString()+ "_encoded");
            return oneHotEncoder;
        }).collect(Collectors.toList());
        return result;
    }

    public static PipelineStage[] createFeaturePipeline(StructType schema, List<String> columns) {
        String[] featureColumns = {};
        String[] scaleFeatureColumns = {};
        PipelineStage[] preprocessingStages = (PipelineStage[]) Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
                field -> {
                    //Empty PipelineStage
                    PipelineStage[] arrPipelineStage = {};
                    // Using Arrays.stream() to convert array into Stream
                    Stream<PipelineStage> streamPipelineStage = Arrays.stream(arrPipelineStage);
                    switch (field.dataType()) {
                        case StringType s:
                            StringIndexer stringIndexer = new StringIndexer();
                            stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
                            append(arrPipelineStage, stringIndexer);
                            return  Arrays.stream(arrPipelineStage);
                        case NumericType n:
                            VectorAssembler numericAssembler = new VectorAssembler();
                            append(scaleFeatureColumns, field.name());
                            return streamPipelineStage;
                        default:
                            // Using Arrays.stream() to convert array into Stream
                            return streamPipelineStage;
                    }
                    }).toArray();

        VectorAssembler numericAssembler = new VectorAssembler();
        numericAssembler.setInputCols(scaleFeatureColumns).setOutputCol("numericRawFeatures");
        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames(scaleFeatureColumns);
        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);

        VectorAssembler vectorAssembler = new VectorAssembler();
        append(featureColumns, "scaledfeatures");
        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");

        append(preprocessingStages, numericAssembler);
        append(preprocessingStages, slicer);
        append(preprocessingStages, scaler);
        append(preprocessingStages, vectorAssembler);
        return preprocessingStages;
    }

    public static PipelineStage[] createStringIndexerPipeline(StructType schema, List<String> columns)
    {
        String[] featureColumns = {};

        PipelineStage[] preprocessingStages = (PipelineStage[]) Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
                field -> {
                    //Empty PipelineStage
                    PipelineStage[] arrPipelineStage = {};
                    // Using Arrays.stream() to convert array into Stream
                    Stream<PipelineStage> streamPipelineStage = Arrays.stream(arrPipelineStage);
                    switch (field.dataType()) {
                        case StringType s:
                            StringIndexer stringIndexer = new StringIndexer();
                            stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
                            append(featureColumns, field.name().toString() +  "_indexed");
                            append(arrPipelineStage, stringIndexer);
                            return  Arrays.stream(arrPipelineStage);
                        case NumericType n:
                            append(featureColumns, field.name());
                            return streamPipelineStage;
                        default:
                            // Using Arrays.stream() to convert array into Stream
                            return streamPipelineStage;
                    }
                }).toArray();


        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");

        append(preprocessingStages, vectorAssembler);

        return preprocessingStages;
    }
        private static <T> T[] append(T[] arr, T element) {
        return ArrayUtils.add(arr, element);
    }

}
