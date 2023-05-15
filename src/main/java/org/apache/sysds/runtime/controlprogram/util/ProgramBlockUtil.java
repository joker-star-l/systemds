package org.apache.sysds.runtime.controlprogram.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.recompile.Recompiler;
import org.apache.sysds.hops.utils.HopUtil;
import org.apache.sysds.runtime.controlprogram.BasicProgramBlock;
import org.apache.sysds.runtime.controlprogram.ProgramBlock;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.meta.MetaDataExt;
import org.sparkproject.guava.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProgramBlockUtil {
    private static final Log LOG =  LogFactory.getLog(ProgramBlockUtil.class.getName());

    public static void updateMNCCache(List<ProgramBlock> programBlocks) {
        if (programBlocks == null) {
            return;
        }
        Set<String> variableNames = new HashSet<>();
        for (ProgramBlock block : programBlocks) {
            variableNames.addAll(block.getStatementBlock().variablesRead().getVariableNames());
            variableNames.addAll(block.getStatementBlock().liveIn().getVariableNames());
        }
        Set<String> cacheNames = MetaDataExt.CACHE.keySet();
        for (String name : Sets.difference(cacheNames, variableNames)) {
            MetaDataExt.CACHE.remove(name);
        }
    }

    public static void setReadPropertiesFromMNCCache(ProgramBlock block) {
        if (block == null) {
            return;
        }
        Set<String> variables = block.getStatementBlock().variablesRead().getVariableNames();
        for (Hop hop : block.getStatementBlock().getHops()) {
            HopUtil.postWalk(hop, h -> {
                if (CollectionUtils.isEmpty(h.getInput()) && variables.contains(h.getName())) {
                    MetaDataExt ext = MetaDataExt.CACHE.get(h.getName());
                    if (ext != null) {
                        h.setDim1(ext.e.getRows());
                        h.setDim2(ext.e.getCols());
                        h.setNnz(ext.e.getNonZeros());
                        HopUtil.propagateDcToParents(h);
                    }
                }
            });
        }
    }

    public static void prepareRewriteBasicProgramBlock(List<ProgramBlock> programBlocks, int i, ExecutionContext ec) {
        if (programBlocks == null) {
            return;
        }
        if (programBlocks.get(i) instanceof BasicProgramBlock) {
            long start = System.currentTimeMillis();

            BasicProgramBlock pb = (BasicProgramBlock) programBlocks.get(i);
            // 清除不需要的MNC缓存
            updateMNCCache(programBlocks.subList(i, programBlocks.size()));
            // 根据MNC设置当前block的输入
            setReadPropertiesFromMNCCache(pb);
            // 重写
            ArrayList<Instruction> inst = Recompiler.recompileHopsDag(
                    pb.getStatementBlock(),
                    pb.getStatementBlock().getHops(),
                    ec, null, false, true, pb.getThreadID());
            pb.setInstructions(inst);
            // 关闭重写，防止二次重写
            pb.getStatementBlock().setRecompilationFlag(false);
            // 清除不需要的MNC缓存
            if (i + 1 < programBlocks.size()) {
                updateMNCCache(programBlocks.subList(i + 1, programBlocks.size()));
            }

            long end = System.currentTimeMillis();

            LOG.info("【prepareRewriteBasicProgramBlock】: " + (end - start) + "ms");
        }
    }
}
