package org.apache.sysds.test.component.estim;


import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.sysds.runtime.matrix.data.LibMatrixMult;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.Test;

import java.io.IOException;

public class LibMatrixMultTest {
    public static int n = 10000;
    public static int threads = 1;
    public static MatrixBlock m1 = MatrixBlock.randOperations(n, n, 0.001, 0, 1, "normal", 100);
    public static MatrixBlock m2 = MatrixBlock.randOperations(n, n, 0.001, 0, 1, "normal", 100);


    @Test
    public void benchmark() {
        double[] sp = new double[] {0.001, 0.01, 0.1};

        matrixMultSparseSparseSparseMM();
        matrixMultSparseSparseMM();

        for (int i = 0; i < sp.length; i++) {
            System.out.println("=== " + i + " ===");
            m1 = MatrixBlock.randOperations(n, n, sp[i], 0, 1, "normal", 100);
            m2 = MatrixBlock.randOperations(n, n, sp[i], 0, 1, "normal", 100);
            matrixMultSparseSparseSparseMM();
            matrixMultSparseSparseMM();
        }
    }


    // 0.2 : 44898ms
    // 0.3 : 96242ms
    // 0.03: 2859ms
    @Test
    public void matrixMultSparseSparseSparseMM() {
        MatrixBlock m3 = new MatrixBlock();
        m3.reset(n, n, true);
        m3.allocateBlock();
        calTime(() -> LibMatrixMult.matrixMultSparseSparseSparseMM(
                m1.getSparseBlock(),
                m2.getSparseBlock(),
                m3.getSparseBlock(),
                m2.getNumColumns(),
                0, n
        ), "matrixMultSparseSparseSparseMM");
        System.out.println();
    }

    // 0.2 : 43883ms, 110ms
    // 0.3 : 87200ms, 185ms
    // 0.03: 2060ms , 145ms
    @Test
    public void matrixMultSparseSparseMM() {
        MatrixBlock m3 = new MatrixBlock();
        m3.reset(n, n, false);
        m3.allocateBlock();
        calTime(() -> LibMatrixMult.matrixMultSparseSparseMM(
                m1.getSparseBlock(),
                m2.getSparseBlock(),
                m3.getDenseBlock(),
                m1.getNumRows(),
                m2.getNumColumns(),
                m1.getNonZeros(),
                0, n
        ), "matrixMultSparseSparseMM");
        calTime(m3::examSparsity, "denseToSparse");
        System.out.println();
    }

    public void calTime(Runnable runnable, String desc) {
        long start = System.currentTimeMillis();
        runnable.run();
        long end = System.currentTimeMillis();
        System.out.println(desc + ": " + (end - start) + "ms");
    }

    @Test
    public void test() throws IOException {
        NativeIO.Windows.access("D:\\IdeaProjects\\JavaProjects\\systemds\\systemds\\src\\main\\java\\org\\apache\\sysds\\api\\DMLScript.java", NativeIO.Windows.AccessRight.ACCESS_READ);
    }
}
