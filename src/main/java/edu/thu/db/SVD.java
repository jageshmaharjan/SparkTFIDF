package edu.thu.db;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.LinkedList;
import java.util.Vector;

/**
 * Created by jugs on 12/24/16.
 */
public class SVD
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("Simple App").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        double[][] array = {{1.12, 2.05, 3.12}, {5.56, 6.28, 8.94}, {10.2, 8.0, 20.5}};

        LinkedList<Vector> rowList = new LinkedList<>();
        for (int i=0; i< array.length; i++)
        {
//            Vector currentRow = Vectors.dense(array[i]);
//            rowList.add(currentRow);
        }
    }
}
