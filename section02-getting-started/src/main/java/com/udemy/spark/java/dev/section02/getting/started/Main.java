package com.udemy.spark.java.dev.section02.getting.started;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;

/**
 * @author dbatista
 */
public class Main {

    public static void main(String[] args) {

        final List<Integer> inputData = new ArrayList<>();

        //
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        final SparkConf conf = new SparkConf().setAppName("helloSpark").setMaster("local[*]");

        try (final JavaSparkContext jSparkContext = new JavaSparkContext(conf)) {
            //
            final JavaRDD<Integer> myRdd = jSparkContext.parallelize(inputData);  // stream
            //
            out.println(myRdd.count());
        }
    }

}