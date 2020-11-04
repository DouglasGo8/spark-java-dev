package com.udemy.spark.java.dev.section08.reading.disk;

import com.udemy.spark.java.dev.core.BaseSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

import static java.lang.System.out;


public class Main extends BaseSpark {


    public static void main(String... args) {

        final SparkConf conf = new SparkConf().setAppName("helloSpark").setMaster("local[*]");

        try (final JavaSparkContext sc = new JavaSparkContext(conf)) {
            // hooray work's with hadoop-aut dependency

            final JavaRDD<String> initialRdd = sc.textFile("./data/input.txt"); // s3://

            initialRdd.map(v -> v.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                    .filter(w -> w.trim().length() > 0)
                    .flatMap(w -> Arrays.asList(w.split(" ")).iterator())
                    .filter(w -> w.trim().length() > 0)
                    .filter(BaseSpark::isNotBoring)
                    .mapToPair(w -> new Tuple2<>(w, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(t -> new Tuple2<>(t._2, t._1))
                    .sortByKey(false)
                    .take(50)
                    .forEach(out::println);

        }
    }
}
