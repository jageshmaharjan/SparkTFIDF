package edu.thu.db;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jugs on 12/26/16.
 */
public class Ngram
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder()
                .appName("Ngram")
                .master("local")
                .getOrCreate();


        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Arrays.asList("Hi", "I", "heard", "about","spark")),
                RowFactory.create(1.0, Arrays.asList("I", "wish", "java", "could", "use", "case", "classes")),
                RowFactory.create(2.0,Arrays.asList("Logistic","Regression","models","are","neat"))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("words",DataTypes.createArrayType(DataTypes.StringType),false, Metadata.empty())
        });

        Dataset<Row> wordDataFrame = spark.createDataFrame(data, schema);

        NGram ngramTransformer = new NGram().setInputCol("words").setOutputCol("ngrams");

        Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordDataFrame);

        for (Row r : ngramDataFrame.select("ngrams","label").takeAsList(3))
        {
            List<String> ngrams = r.getList(0);
            System.out.println(ngrams);
        }

    }
}
