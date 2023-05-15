package org.apache.sysds.hops.rewrite;

import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.Hop;

import java.util.ArrayList;

public class RewriteMatrixSparsityEstimPropagationBeforeMMChain extends RewriteMatrixSparsityEstimPropagation {

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

    @Override
    protected void estimPropagation(Hop h) {
        if (h.getInput() != null) {
            for (Hop ch : h.getInput()) {
                estimPropagation(ch);
            }
        }

        // 不是矩阵乘法以及矩阵链
        if (!(h instanceof AggBinaryOp)) {
            execute(h);
        } else {
            boolean flag = true;
            for (Hop ch : h.getInput()) {
                if (ch instanceof AggBinaryOp) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                execute(h);
            }
        }
    }
}
