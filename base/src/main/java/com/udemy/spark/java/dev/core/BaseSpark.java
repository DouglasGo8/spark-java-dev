package com.udemy.spark.java.dev.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;


public abstract class BaseSpark {

    private static final Set<String> borings = new HashSet<>();
    protected static final Logger log = Logger.getLogger(BaseSpark.class.getName());

    static {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "C:/hadoop/");
        //
        try (final InputStream is = Files.newInputStream(Paths.get("./data/boringwords.txt"))) {
            final BufferedReader br = new BufferedReader(new InputStreamReader(is));
            br.lines().forEach(borings::add);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static boolean isBoring(final String word) {
        return borings.contains(word);
    }

    protected static boolean isNotBoring(final String word) {
        return !isBoring(word);
    }


}
