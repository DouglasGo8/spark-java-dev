package com.udemy.spark.java.dev.section05.tuples;

import com.udemy.spark.java.dev.core.BaseSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main extends BaseSpark {


    public static void main(String... args) {

        final List<Integer> inputData = new ArrayList<>();

        //
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);
        //
        final SparkConf conf = new SparkConf().setAppName("helloSpark").setMaster("local[*]");
        //
        try (final JavaSparkContext jSparkContext = new JavaSparkContext(conf)) {
            //
            final JavaRDD<Integer> originalInteger = jSparkContext.parallelize(inputData);   // rdd01
            final JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalInteger.map(v -> new Tuple2<>(v, Math.sqrt(v)));  // rdd02

            final Tuple2<Integer, Double> tuple = new Tuple2<>(9, 3.0d);

            System.out.println(tuple);

            //


        }
    }
}
