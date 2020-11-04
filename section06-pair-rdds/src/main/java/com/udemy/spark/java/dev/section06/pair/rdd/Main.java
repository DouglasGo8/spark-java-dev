package com.udemy.spark.java.dev.section06.pair.rdd;

import com.udemy.spark.java.dev.core.BaseSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
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
        //
        final SparkConf conf = new SparkConf().setAppName("helloSpark").setMaster("local[*]");
        //
        try (final JavaSparkContext sc = new JavaSparkContext(conf)) {
            // .groupByKey <String, Iterable<String>>
            sc.parallelize(inputData).mapToPair(raw -> {
                final String[] columns = raw.split(":");
                // return new Tuple2<>(level, 1L);
                return new Tuple2<>(columns[0].trim(), columns[1].trim());
            }).groupByKey().collect().forEach(out::println);

            // recommended approach
            // .reduceByKey(Integer::sum); (v1, v2) -> v1 + v2
            sc.parallelize(inputData).mapToPair(raw -> new Tuple2<>(raw.split(":")[0], 1L))
                    .reduceByKey(Long::sum)
                    .collect()
                    .forEach(out::println);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
