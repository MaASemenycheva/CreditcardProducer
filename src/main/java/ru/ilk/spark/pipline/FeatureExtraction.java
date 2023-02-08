package ru.ilk.spark.pipline;

import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class FeatureExtraction {

    private static Logger logger = Logger.getLogger(FeatureExtraction.class.getName());

    static void getFeatures (PipelineModel pipelineModel) {

    }


}
//    VectorAssembler vectorAssembler = pipelineModel.stages().
//            filter(a -> {a instanceof String}).headOption.
//            getOrElse(throw new IllegalArgumentException("Invalid model")).
//        asInstanceOf[VectorAssembler]
//        String[] featureNames = vectorAssembler.getInputCols();
//
//        Arrays.stream(featureNames).flatMap(featureName -> {
//        Optional<OneHotEncoder> oneHotEncoder = pipelineModel.stages().filter( x -> x instanceOf[OneHotEncoder]).map(_.asInstanceOf[OneHotEncoder]).find(_.getOutputCol == featureName)
//        String oneHotEncoderInputCol = oneHotEncoder.map(a -> a.getInputCol()).getOrElse(featureName);
//
//        Optional<StringIndexerModel> stringIndexer = pipelineModel.stages().filter(_.isInstanceOf[StringIndexerModel]).map(_.asInstanceOf[StringIndexerModel]).find(_.getOutputCol == oneHotEncoderInputCol)
//        String stringIndexerInput = stringIndexer.map(a -> a.getInputCol()).orElse(featureName);
//
//        oneHotEncoder.map(encoder -> {
//        String[] labelValues = stringIndexer.orElseGet(new IllegalArgumentException("Invalid model"))
//
//        .getOrElse(throw new IllegalArgumentException("Invalid model")).labels
//        labelValues.map(label => s"$stringIndexerInput-$label")
//        }).getOrElse(Array(stringIndexerInput))
//        });
