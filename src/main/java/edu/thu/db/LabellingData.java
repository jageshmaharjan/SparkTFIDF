package edu.thu.db;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Created by jugs on 1/9/17.
 */
public class LabellingData
{

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("Read").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        String filepath = "/home/jugs/IdeaProjects/MovieReviewProcessing/ReviewLabelling.txt";

        JavaRDD<String> rdd = jsc.textFile(filepath);

        System.out.println();

    }


}
