package com.lcy.java.demo.ml.dataType;

import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;

public class LocalMatrix {

    public static void main(String[] args) {

        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

        // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
        Matrix sm = Matrices.sparse(3, 2,
                new int[] {0, 1, 3},
                new int[] {0, 2, 1},
                new double[] {9, 6, 8}
                );

        System.out.println(dm);
        System.out.println();
        System.out.println(sm);

    }

}
