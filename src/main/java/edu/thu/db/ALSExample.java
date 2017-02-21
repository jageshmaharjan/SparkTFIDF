package edu.thu.db;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by jugs on 1/17/17.
 */

public class ALSExample
{
    public static class Rating implements Serializable
    {
        private int userId;
        private int movieId;
        private float rating;
        private long timestamp;

        public Rating()
        {
        }

        public Rating(int userId, int movieId, float rating, long timestamp)
        {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        public int getUserId()
        {
            return userId;
        }

        public int getMovieId()
        {
            return movieId;
        }

        public float getRating()
        {
            return rating;
        }

        public long getTimestamp()
        {
            return timestamp;
        }

        public static Rating parseRating(String str)
        {
            String[] fields = str.split("::");
            if (fields.length != 4)
            {
                throw new IllegalArgumentException("Each line must contain 4 fields");
            }
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            float rating = Float.parseFloat(fields[2]);
            long timestamp = Long.parseLong(fields[3]);

            return new Rating(userId, movieId, rating, timestamp);
        }
    }

    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("ALSExample")
                .master("local")
                .getOrCreate();

        JavaRDD<Rating> ratingRDD = spark.read().textFile("/home/jugs/Desktop/sample_movielen_data.txt").javaRDD()
                .map((Function<String, Rating>) str -> Rating.parseRating(str));

        Dataset<Row> ratings = spark.createDataFrame(ratingRDD, Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8,0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setImplicitPrefs(true)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root mean square error= " + rmse);

        spark.stop();

    }

}
