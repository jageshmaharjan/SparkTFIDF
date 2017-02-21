package edu.thu.db;


//import com.jujutsu.tsne.FastTSne;
//import com.jujutsu.tsne.TSne;
//import com.jujutsu.utils.MatrixOps;
//import com.jujutsu.utils.MatrixUtils;
//
//import java.io.File;
//
///**
// * Created by jugs on 12/28/16.
// */
//public class TNSEExample
//{
//    public static void main(String[] args)
//    {
//        int initial_dims = 55;
//        double perplexity = 20.0;
//        double[][] X = MatrixUtils.simpleRead2DMatrix(new File("/home/jugs/Desktop/Dataset/mnist2500_X.txt"));
//        System.out.println(MatrixOps.doubleArrayToPrintString(X,",",50,10));
//        TSne tSne = new FastTSne();
//        double [][] Y = tSne.tsne(X,2,initial_dims, perplexity);
//
//    }
//}
