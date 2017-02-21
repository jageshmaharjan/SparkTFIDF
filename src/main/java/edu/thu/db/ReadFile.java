package edu.thu.db;

import edu.stanford.nlp.ling.CoreAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import parquet.org.slf4j.Logger;

import java.util.*;

/**
 * Created by jugs on 12/24/16.
 */
public class ReadFile
{
    private StanfordCoreNLP pipeline;

    public ReadFile()
    {
//        MaxentTagger tagger = new MaxentTagger(
//                "/home/jugs/Documents/libs/stanford-postagger-2015-12-09/models/english-left3words-distsim.tagger");

        Properties properties = new Properties();
        properties.put("annotators", "tokenize, ssplit, pos, lemma");
        this.pipeline = new StanfordCoreNLP(properties);
    }

    public static void main(String[] args)
    {
        ReadFile readFile = new ReadFile();
        readFile.program();
    }

    private void program()
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Read File")
                .master("local")
                .getOrCreate();

        String filePath = "/home/jugs/Desktop/Dataset/Reviewjson.txt";
        JavaRDD<MyMovie> movieRDD = spark.read().textFile(filePath)
                .javaRDD().map((Function<String, MyMovie>) s ->
                {
                    String[] parts = s.split("\\\":\\s\"");
                    MyMovie movie = new MyMovie();
                    movie.setReview(parts[1].substring(0,parts[1].length()-9));
                    movie.setFname(parts[2].substring(0,parts[2].length()-9));
                    movie.setTitle(parts[3].substring(0,parts[3].length()-2));
                    return movie;
                });

        Dataset<Row> movieDF = spark.createDataFrame(movieRDD, MyMovie.class);
        //movieDF.show();

        //-------Creating a view named movie---------
        movieDF.createOrReplaceTempView("movie");
        Dataset<Row> reviewsDF = movieDF.select("review"); //spark.sql("select review from movie");
        //reviewsDF.show();

        //StanfordCoreNLP Tokenizer

        //------------Tokenization Process-----------
        Tokenizer tokenizer = new Tokenizer().setInputCol("review").setOutputCol("words");
        Dataset<Row> wordData = tokenizer.transform(reviewsDF);


        //---------StopWord Removal-------------------
        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered");
        Dataset<Row> filteredData = remover.transform(wordData);

        //--------NGram Transformer-----------------
        NGram nGramTransform = new NGram().setInputCol("filtered").setOutputCol("ngrams");
        Dataset<Row> nGramDF =  nGramTransform.transform(filteredData);
        //nGramDF.select("filtered").write().json("ngrams");


        //-------word2Vec----NOT WORKING------
//        Word2Vec word2Vec = new Word2Vec()
//                .setInputCol("filtered")
//                .setOutputCol("result")
//                .setVectorSize(5)
//                .setMinCount(0);
//        Word2VecModel model = word2Vec.fit(filteredData.select("filtered"));
//        Dataset<Row> result = model.transform(reviewsDF);
//        for (Row r : result.select("result").takeAsList(100))
//        {
//            System.out.println(r);
//        }


        //----------------Generating tfidf--------------
        int numberOfFeatures = 1000;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("ngrams")
                .setOutputCol("rawFeatures");
//                .setNumFeatures(numberOfFeatures);

        Dataset<Row> featuredData = hashingTF.transform(nGramDF);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featuredData);
        Dataset<Row> tfIDFData = idfModel.transform(featuredData);
        tfIDFData.show();


        System.out.println();

//        Dataset<Row> df = spark.read().json("/home/jugs/Desktop/Dataset/Reviewjson.txt");
//        df.select("review").show();

    }

}
