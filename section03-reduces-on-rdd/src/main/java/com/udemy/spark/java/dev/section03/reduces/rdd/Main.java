
package com.udemy.spark.java.dev.section03.reduces.rdd;

import com.udemy.spark.java.dev.core.BaseSpark;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;


public class Main extends BaseSpark {


    public static void main(String[] args) {

        final List<Integer> inputData = new ArrayList<>();

        //
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);


        final SparkConf conf = new SparkConf().setAppName("helloSpark").setMaster("local[*]");
        //
        try (final JavaSparkContext jSparkContext = new JavaSparkContext(conf)) {
            //
            final JavaRDD<Integer> myRdd = jSparkContext.parallelize(inputData);  // stream
            final Integer result = myRdd.reduce(Integer::sum);
            final JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
            //
            sqrtRdd.collect().forEach(out::println);
            //
            JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(v -> 1L);
            Long count = singleIntegerRdd.reduce(Long::sum);
            out.println(count);

        }

    }

}