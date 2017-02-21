package edu.thu.db;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jugs on 1/5/17.
 */
public class TokenizeDocument implements Serializable
{


    public TokenizeDocument()
    {


    }
    public static void main(String[] args)
    {
        TokenizeDocument tokenize = new TokenizeDocument();
        tokenize.processTokenization();

    }

    private void processTokenization()
    {
        String filePath = "/home/jugs/Desktop/Dataset/Reviewjson.txt";
        SparkConf conf = new SparkConf().setAppName("Tokenize").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String[]> movieRDD = jsc.textFile(filePath)
                .map(line -> line.split("\\\":\\s\""));

        JavaRDD<String> reviewsRDD =  movieRDD.map(line -> {
            return line[1].substring(0,line[1].length()-9);
        });

        JavaRDD<List<String>> tokensRDD = reviewsRDD.map(line -> getPOS(line));
        System.out.println();

    }

    private static String[] test(String review)
    {
        return review.split(" ");
    }

//    private static List<String> getTokens(String line)
//    {
//        TokenizerFactory ptbt =  PTBTokenizer.factory();
//        return ptbt.getTokenizer(new StringReader(line)).tokenize();
//    }

    private static List<String> getPOS(String line)
    {

        StanfordCoreNLP pipeline = null;

        Properties properties = new Properties();
        properties.put("annotators", "tokenize, ssplit, pos, lemma");
        pipeline = new StanfordCoreNLP(properties);

        List<String> tokens = new ArrayList<>();
        Annotation document = new Annotation(line);

        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences)
        {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class))
            {
                tokens.add(token.getString(CoreAnnotations.LemmaAnnotation.class));
            }
        }
        return tokens;
    }




}
