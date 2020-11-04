package com.udemy.spark.java.dev.section07.flatmap.filters;

import com.udemy.spark.java.dev.core.BaseSpark;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.out;

public class Main extends BaseSpark {

    public static void main(String... args) {
        final List<String> inputData = new ArrayList<>();

        //
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");
        inputData.add("WARN: Tuesday 4 September 0406");

        log.setLevel(Level.WARN);

        final SparkConf conf = new SparkConf().setAppName("helloSpark").setMaster("local[*]");

        try (final JavaSparkContext jSparkContext = new JavaSparkContext(conf)) {
            //
            jSparkContext.parallelize(inputData)
                    .flatMap(v -> Arrays.stream(v.split(" ")).iterator())
                    .filter(w -> w.length() > 1).collect()
                    .forEach(out::println);
        }

    }
}
