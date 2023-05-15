package org.apache.sysds.hops.rewrite;

import org.apache.sysds.hops.*;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.utils.HopUtil;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataExt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RewriteMatrixSparsityEstimPropagation extends HopRewriteRule {
    private static final EstimatorMatrixHistogram estimator = new EstimatorMatrixHistogram();

    @Override
    public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
        if (roots == null) {
            return null;
        }

        for (Hop h : roots) {
            estimPropagation(h);
        }

        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
        if (root == null) {
            return null;
        }

        estimPropagation(root);

        return root;
    }

    protected void estimPropagation(Hop h) {
        if (h.getInput() != null) {
            for (Hop ch : h.getInput()) {
                estimPropagation(ch);
            }
        }

        execute(h);
    }

    protected void execute(Hop h) {
        if (h instanceof LiteralOp) {
            return;
        }
        DataCharacteristics dc = null;
        if (h.isMatrix() && (!h.dimsKnown(true) || MetaDataExt.CACHE.get(h.getName()) == null)) {
            // TODO 当matrix与scalar或vector进行binary计算时，目前认为scalar或vector不会修改matrix稀疏度
            if (h instanceof BinaryOp) {
                Hop mh = null;
                Hop i0 = h.getInput(0);
                Hop i1 = h.getInput(1);
                if ((i0.isScalar() || i0.isVector())
                        && i1.isMatrix()
                        && i1.dimsKnown(true)) {
                    mh = i1;
                } else if (i0.isMatrix()
                        && (i1.isScalar() || i1.isVector())
                        && i0.dimsKnown(true)) {
                    mh = i0;
                }
                if (mh != null) {
                    dc = mh.getDataCharacteristics();
                    MetaDataExt ext = MetaDataExt.CACHE.get(mh.getName());
                    if (ext != null) {
                        MetaDataExt.CACHE.put(h.getName(), ext);
                    }
                }
            }
            // TODO 当按行或列聚合时，目前只是简单地取对应行或列的nnz
            if (h instanceof AggUnaryOp) {
                AggUnaryOp ah = (AggUnaryOp) h;
                switch (ah.getDirection()) {
                    case RowCol: {
                        MetaDataExt.CACHE.put(h.getName(), new MetaDataExt(new int[]{1}, new int[]{1}));
                        dc = ah.getDataCharacteristics();
                        dc.setNonZeros(1);
                        break;
                    }
                    case Col: {
                        MetaDataExt ext = MetaDataExt.CACHE.get(ah.getInput(0).getName());
                        if (ext != null) {
                            int[] c = Arrays.stream(ext.e.getColCounts()).map(o -> o > 0 ? 1 : 0).toArray();
                            int[] r = new int[]{Arrays.stream(c).sum()};
                            MetaDataExt.CACHE.put(h.getName(), new MetaDataExt(r, c));
                            dc = ah.getDataCharacteristics();
                            dc.setNonZeros(r[0]);
                        }
                        break;
                    }
                    case Row: {
                        MetaDataExt ext = MetaDataExt.CACHE.get(ah.getInput(0).getName());
                        if (ext != null) {
                            int[] r = Arrays.stream(ext.e.getRowCounts()).map(o -> o > 0 ? 1 : 0).toArray();
                            int[] c = new int[]{Arrays.stream(r).sum()};
                            MetaDataExt.CACHE.put(h.getName(), new MetaDataExt(r, c));
                            dc = ah.getDataCharacteristics();
                            dc.setNonZeros(c[0]);
                        }
                        break;
                    }
                }
            }
            if (dc == null) {
                SparsityEstimator.OpCode opCode = HopUtil.toOpCode(h);
                if (opCode == null) {
                    dc = h.inferOutputCharacteristics();
                } else {
                    List<EstimatorMatrixHistogram.MatrixHistogram> hList = new ArrayList<>();
                    if (h instanceof AggBinaryOp || h instanceof BinaryOp) {
                        for (int i = 0; i < 2; i++) {
                            Hop ch = h.getInput(i);
                            if (ch.isMatrix() && ch.dimsKnown(true)) {
                                MetaDataExt ext = MetaDataExt.CACHE.get(ch.getName());
                                if (ext == null) {
                                    break;
                                }
                                hList.add(ext.e);
                            }
                        }
                        if (hList.size() == 2) {
                            double sp = estimator.estimIntern(hList.get(0), hList.get(1), opCode, null);
                            dc = EstimatorMatrixHistogram.MatrixHistogram.deriveOutputCharacteristics(hList.get(0), hList.get(1), sp, opCode, null);
                            // MNC存储
                            MetaDataExt.CACHE.put(h.getName(), new MetaDataExt(EstimatorMatrixHistogram.MatrixHistogram.deriveOutputHistogram(hList.get(0), hList.get(1), sp, opCode, null)));
                        } else {
                            dc = h.inferOutputCharacteristics();
                        }
                    } else if (h instanceof ReorgOp) {
                        MetaDataExt ext = null;
                        if (h.getInput(0).isMatrix() && h.getInput(0).dimsKnown(true)) {
                            ext = MetaDataExt.CACHE.get(h.getInput(0).getName());
                        }
                        if (ext != null) {
                            double sp = estimator.estimIntern(ext.e, null, opCode, null);
                            dc = EstimatorMatrixHistogram.MatrixHistogram.deriveOutputCharacteristics(ext.e, null, sp, opCode, null);
                            // MNC存储
                            MetaDataExt.CACHE.put(h.getName(), new MetaDataExt(EstimatorMatrixHistogram.MatrixHistogram.deriveOutputHistogram(ext.e, null, sp, opCode, null)));
                        } else {
                            dc = h.getDataCharacteristics();
                        }
                    } else if (h instanceof NaryOp) {
                        int n = h.getInput().size();
                        for (int i = 0; i < n; i++) {
                            Hop ch = h.getInput(i);
                            if (ch.isMatrix() && ch.dimsKnown(true)) {
                                MetaDataExt ext = MetaDataExt.CACHE.get(ch.getName());
                                if (ext == null) {
                                    break;
                                }
                                hList.add(ext.e);
                            }
                        }
                        if (hList.size() == n) {
                            double sp = estimator.estimIntern(hList.get(0), hList.get(1), opCode, null);
                            dc = EstimatorMatrixHistogram.MatrixHistogram.deriveOutputCharacteristics(hList.get(0), hList.get(1), sp, opCode, null);
                            EstimatorMatrixHistogram.MatrixHistogram ret = EstimatorMatrixHistogram.MatrixHistogram.deriveOutputHistogram(hList.get(0), hList.get(1), sp, opCode, null);
                            for (int i = 1; i < n; i++) {
                                sp = estimator.estimIntern(ret, hList.get(i), opCode, null);
                                dc = EstimatorMatrixHistogram.MatrixHistogram.deriveOutputCharacteristics(ret, hList.get(i), sp, opCode, null);
                                ret = EstimatorMatrixHistogram.MatrixHistogram.deriveOutputHistogram(ret, hList.get(i), sp, opCode, null);
                            }
                            // MNC存储
                            MetaDataExt.CACHE.put(h.getName(), new MetaDataExt(ret));
                        } else {
                            dc = h.inferOutputCharacteristics();
                        }
                    }
                }
            }
        }
        if (dc != null) {
            h.setDataCharacteristics(dc);
        }
        // 稀疏度传播
        HopUtil.propagateDcToParents(h);
    }
}
