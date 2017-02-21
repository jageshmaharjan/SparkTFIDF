package edu.thu.db;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jugs on 12/24/16.
 */
public class SimpleApp
{
    public static void main(String[] args)
    {
        String logFile = "/home/jugs/Downloads/spark-2.0.2-bin-hadoop2.7/README.md";
        SparkConf conf = new SparkConf().setAppName("Simple App").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        JavaRDD<String> tokens = logData.map(line -> test(line));
        System.out.println();

        long numAs = logData.filter(alpha -> alpha.contains("a")).count();

        long numBs = logData.filter(beta -> beta.contains("b")).count();

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        System.out.println("Lines with a : " + numAs + ", lines with b : " + numBs);

        sc.stop();
    }

    private static String test(String line)
    {
        return line+"jugs";
    }
}
