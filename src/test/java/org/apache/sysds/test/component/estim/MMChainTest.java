package org.apache.sysds.test.component.estim;

import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.estim.*;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.Test;

public class MMChainTest {
    public static int n = 10000;
    public static int threads = 1;

    public static MatrixBlock m1 = MatrixBlock.randOperations(n, n, 0.001, 0, 1, "normal", 100);
    public static MatrixBlock m2 = MatrixBlock.randOperations(n, n, 0.001, 0, 1, "normal", 100);
    public static MatrixBlock m3 = MatrixBlock.randOperations(n, n, 0.001, 0, 1, "normal", 100);
    public static MatrixBlock m4 = MatrixBlock.randOperations(n, n, 0.001, 0, 1, "normal", 100);
//    public static MatrixBlock m5 = MatrixBlock.randOperations(n, n, 0.005, 0, 1, "normal", 100);

    public static MMNode mmNode1 = new MMNode(m1);
    public static MMNode mmNode2 = new MMNode(mmNode1, new MMNode(m2), SparsityEstimator.OpCode.MM);
    public static MMNode mmNode3 = new MMNode(mmNode2, new MMNode(m3), SparsityEstimator.OpCode.MM);
    public static MMNode mmNode4 = new MMNode(mmNode3, new MMNode(m4), SparsityEstimator.OpCode.MM);
//    public static MMNode mmNode5 = new MMNode(mmNode4, new MMNode(m5), SparsityEstimator.OpCode.MM);

    public static SparsityEstimator estimatorBasicWorst = new EstimatorBasicWorst();
    public static SparsityEstimator estimatorBasicAvg = new EstimatorBasicAvg();
//    public static SparsityEstimator estimatorSample = new EstimatorSample();
    public static SparsityEstimator estimatorDensityMap = new EstimatorDensityMap(1024);
    public static SparsityEstimator estimatorMatrixHistogram = new EstimatorMatrixHistogram(true);
//    public static SparsityEstimator estimatorMatrixHistogram2 = new EstimatorMatrixHistogram(true);
    public static SparsityEstimator estimatorLayeredGraph = new EstimatorLayeredGraph();
    public static SparsityEstimator estimatorBitsetMM = new EstimatorBitsetMM();

    public static void reSetNode() {
        mmNode1 = new MMNode(m1);
        mmNode2 = new MMNode(mmNode1, new MMNode(m2), SparsityEstimator.OpCode.MM);
        mmNode3 = new MMNode(mmNode2, new MMNode(m3), SparsityEstimator.OpCode.MM);
        mmNode4 = new MMNode(mmNode3, new MMNode(m4), SparsityEstimator.OpCode.MM);
//        mmNode5 = new MMNode(mmNode4, new MMNode(m5), SparsityEstimator.OpCode.MM);
    }

    @Test
    public void test() {
        skewRowUp(m1);
        skewColLeft(m2);
        skewRowDown(m3);
        skewColRight(m4);
//
//        skewRowWithZero(m1);
//        skewColWithZero(m2);

//        skewCol(m3);
//        skewCol(m4);
//        skewRow(m5);

        reSetNode();

        final MatrixBlock m12 = estimAndMM(mmNode2, m1,  m2, "=== m1 * m2 ===");
        final MatrixBlock m23 = estimAndMM(mmNode3, m12, m3, "=== m1 * m2 * m3 ===");
        final MatrixBlock m34 = estimAndMM(mmNode4, m23, m4, "=== m1 * m2 * m3 * m4 ===");
//        final MatrixBlock m45 = estimAndMM(mmNode5, m34, m5, "=== m1 * m2 * m3 * m4 * m5 ===");
    }

    private static void skewColLeft(MatrixBlock m) {
        MatrixBlock m1 = MatrixBlock.randOperations(n, n / 10, 0.09, 0, 1, "normal", 100);
        MatrixBlock m2 = MatrixBlock.randOperations(n, n / 10 * 9, 0.0001, 0, 1, "normal", 100);
        m1.append(m2, m, true);
    }

    private static void skewRowUp(MatrixBlock m) {
        MatrixBlock m1 = MatrixBlock.randOperations(n / 10, n, 0.09, 0, 1, "normal", 100);
        MatrixBlock m2 = MatrixBlock.randOperations(n / 10 * 9, n, 0.0001, 0, 1, "normal", 100);
        m1.append(m2, m, false);
    }

    private static void skewColRight(MatrixBlock m) {
        MatrixBlock m2 = MatrixBlock.randOperations(n, n / 10, 0.09, 0, 1, "normal", 100);
        MatrixBlock m1 = MatrixBlock.randOperations(n, n / 10 * 9, 0.001, 0, 1, "normal", 100);
        m1.append(m2, m, true);
    }

    private static void skewRowDown(MatrixBlock m) {
        MatrixBlock m2 = MatrixBlock.randOperations(n / 10, n, 0.09, 0, 1, "normal", 100);
        MatrixBlock m1 = MatrixBlock.randOperations(n / 10 * 9, n, 0.0001, 0, 1, "normal", 100);
        m1.append(m2, m, false);
    }

    private static void skewRowWithZero(MatrixBlock m) {
        MatrixBlock m1 = MatrixBlock.randOperations(1, n, 1 - 1e-20, 0, 1, "normal", 100);
        MatrixBlock m2 = new MatrixBlock(n - 1, n, 0);
//        MatrixBlock m2 = MatrixBlock.randOperations(n / 100 * 99, n, 0.001, 0, 1, "normal", 100);
        m1.append(m2, m, false);
    }

    private static void skewColWithZero(MatrixBlock m) {
        MatrixBlock m1 = MatrixBlock.randOperations(n, 1, 1 - 1e-20, 0, 1, "normal", 100);
        MatrixBlock m2 = new MatrixBlock(n, n - 1, 0);
//        MatrixBlock m2 = MatrixBlock.randOperations(n, n / 100 * 99, 0.001, 0, 1, "normal", 100);
        m1.append(m2, m, true);
    }

    public static MatrixBlock estimAndMM(MMNode mmNode, MatrixBlock m1, MatrixBlock m2, String desc) {
        double[] estim = {0};
        double[] real = {1};

        System.out.println(desc);

        MatrixBlock[] ret = {null};
        calTime(() -> {
            ret[0] = mm(m1, m2);
            real[0] = OptimizerUtils.getSparsity(ret[0].getNumRows(), ret[0].getNumColumns(), ret[0].getNonZeros());
        }, "");
        System.out.println("real: " + real[0]);

        refresh(mmNode);
        calTime(() -> estim[0] = estimatorBasicWorst.estim(mmNode).getSparsity(), "");
        System.out.println("mw:   " + estim[0]);
        System.out.println(error(real[0], estim[0]));

        refresh(mmNode);
        calTime(() -> estim[0] = estimatorBasicAvg.estim(mmNode).getSparsity(), "");
        System.out.println("ma:   " + estim[0]);
        System.out.println(error(real[0], estim[0]));

//        calTime(() -> estim[0] = estimatorSample.estim(m1, m2), "");
//        System.out.println("samp: " + estim[0]);
//        System.out.println(error(real[0], estim[0]));

        refresh(mmNode);
        calTime(() -> estim[0] = estimatorDensityMap.estim(mmNode).getSparsity(), "");
        System.out.println("dm:   " + estim[0]);
        System.out.println(error(real[0], estim[0]));

        refresh(mmNode);
        calTime(() -> estim[0] = estimatorMatrixHistogram.estim(mmNode).getSparsity(), "");
        System.out.println("mnc:  " + estim[0]);
        System.out.println(error(real[0], estim[0]));

//        refresh(mmNode);
//        calTime(() -> estim[0] = estimatorMatrixHistogram2.estim(mmNode).getSparsity(), "");
//        System.out.println("mnc2: " + estim[0]);

        refresh(mmNode);
        calTime(() -> estim[0] = estimatorLayeredGraph.estim(mmNode).getSparsity(), "");
        System.out.println("lg:   " + estim[0]);
        System.out.println(error(real[0], estim[0]));

        refresh(mmNode);
        calTime(() -> estim[0] = estimatorBitsetMM.estim(mmNode).getSparsity(), "");
        System.out.println("bit:  " + estim[0]);
        System.out.println(error(real[0], estim[0]));

        return ret[0];
//        return null;
    }

    public static void calTime(Runnable runnable, String desc) {
        long start = System.currentTimeMillis();
        runnable.run();
        long end = System.currentTimeMillis();
        System.out.println(desc + "- " + (end - start) + "ms");
    }

    public static void refresh(MMNode root) {
        if (root != null) {
            root.setSynopsis(null);
            if (root.getLeft() != null) {
                refresh(root.getLeft());
            }
            if (root.getRight() != null) {
                refresh(root.getRight());
            }
        }
    }

    public static MatrixBlock mm(MatrixBlock m1, MatrixBlock m2) {
        return m1.aggregateBinaryOperations(m1, m2, null, InstructionUtils.getMatMultOperator(threads));
    }

    public static double error(double s1, double s2) {
        return Math.max(s1, s2) / Math.min(s1, s2);
    }

    @Test
    public void testMNC() {
        MatrixBlock m1 = new MatrixBlock(4, 4, new double[]{
                0, 0, 1, 1,
                0, 1, 1, 0,
                1, 1, 0, 0,
                1, 0, 0, 0
        });
        m1.setNonZeros(7);
        MatrixBlock m2 = new MatrixBlock(4, 4, new double[]{
                0, 1, 0, 1,
                0, 0, 1, 1,
                0, 1, 0, 0,
                1, 0, 1, 0
        });
        m2.setNonZeros(7);
        System.out.println(estimatorMatrixHistogram.estim(m1, m2));
    }
}
