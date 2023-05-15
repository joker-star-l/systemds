package org.apache.sysds.hops.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.*;
import org.apache.sysds.hops.estim.SparsityEstimator;

import java.util.Objects;
import java.util.function.Consumer;

public class HopUtil {
    private static final Log LOG = LogFactory.getLog(HopUtil.class.getName());

    public static void propagateDcToParents(Hop h) {
        if (h != null && h.dimsKnown(true) && h.getParent() != null) {
            for (Hop ph : h.getParent()) {
                if (ph instanceof DataOp && Objects.equals(ph.getName(), h.getName())) {
                    DataOp dph = (DataOp) ph;
                    if (Types.OpOpData.TRANSIENTREAD.equals(dph.getOp()) || Types.OpOpData.TRANSIENTWRITE.equals(dph.getOp())) {
                        ph.setDataCharacteristics(h.getDataCharacteristics());
                        propagateDcToParents(ph);
                    }
                }
            }
        }
    }

    public static void preWalk(Hop h, Consumer<Hop> consumer) {
        if (h == null) {
            return;
        }
        consumer.accept(h);
        if (h.getInput() != null) {
            for (Hop ch : h.getInput()) {
                preWalk(ch, consumer);
            }
        }
    }

    public static void postWalk(Hop h, Consumer<Hop> consumer) {
        if (h == null) {
            return;
        }
        if (h.getInput() != null) {
            for (Hop ch : h.getInput()) {
                postWalk(ch, consumer);
            }
        }
        consumer.accept(h);
    }

    public static SparsityEstimator.OpCode toOpCode(Hop h) {
        if (h == null || h.getDataType() != Types.DataType.MATRIX) {
            return null;
        }
        if (h instanceof AggBinaryOp) {
            return SparsityEstimator.OpCode.MM;
        }
        if (h instanceof BinaryOp) {
            BinaryOp bh = (BinaryOp) h;
            if (Objects.equals(bh.getOp(), Types.OpOp2.MULT)
                    || Objects.equals(bh.getOp(), Types.OpOp2.DIV)) {
                return SparsityEstimator.OpCode.MULT;
            }
            if (Objects.equals(bh.getOp(), Types.OpOp2.PLUS)
                    || Objects.equals(bh.getOp(), Types.OpOp2.MINUS)) {
                return SparsityEstimator.OpCode.PLUS;
            }
            if (Objects.equals(bh.getOp(), Types.OpOp2.CBIND)) {
                return SparsityEstimator.OpCode.CBIND;
            }
            if (Objects.equals(bh.getOp(), Types.OpOp2.RBIND)) {
                return SparsityEstimator.OpCode.RBIND;
            }
            return null;
        }
        if (h instanceof ReorgOp) {
            ReorgOp rh = (ReorgOp) h;
            if (Objects.equals(rh.getOp(), Types.ReOrgOp.TRANS)) {
                return SparsityEstimator.OpCode.TRANS;
            }
            if (Objects.equals(rh.getOp(), Types.ReOrgOp.DIAG)) {
                return SparsityEstimator.OpCode.DIAG;
            }
            // 暂不支持
//            if (Objects.equals(rh.getOp(), Types.ReOrgOp.RESHAPE)) {
//                return SparsityEstimator.OpCode.RESHAPE;
//            }
            return null;
        }
        if (h instanceof NaryOp) {
            NaryOp nh = (NaryOp) h;
            if (Objects.equals(nh.getOp(), Types.OpOpN.CBIND)) {
                return SparsityEstimator.OpCode.CBIND;
            }
            if (Objects.equals(nh.getOp(), Types.OpOpN.RBIND)) {
                return SparsityEstimator.OpCode.RBIND;
            }
            if (Objects.equals(nh.getOp(), Types.OpOpN.PLUS)) {
                return SparsityEstimator.OpCode.PLUS;
            }
        }
        return null;
    }

    public static void printHop(Hop h) {
        LOG.info("【HOP】");
        printHop(h, "-");
    }

    private static void printHop(Hop h, String s) {
        if (h == null) {
            return;
        }
        LOG.info(s + " " + h + " " + h.getDataCharacteristics());
        if (h.getInput() != null) {
            for (Hop ch : h.getInput()) {
                printHop(ch, s + "-");
            }
        }
    }
}
