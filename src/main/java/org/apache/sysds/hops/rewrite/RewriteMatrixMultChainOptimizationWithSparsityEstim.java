/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.hops.rewrite;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram.MatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator.OpCode;
import org.apache.sysds.hops.utils.HopUtil;
import org.apache.sysds.runtime.meta.MetaDataExt;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Rule: Determine the optimal order of execution for a chain of
 * matrix multiplications 
 * 
 * Solution: Classic Dynamic Programming
 * Approach: Currently, the approach based only on matrix dimensions
 * and sparsity estimates using the MNC sketch
 * Goal: To reduce the number of computations in the run-time
 * (map-reduce) layer
 */
public class RewriteMatrixMultChainOptimizationWithSparsityEstim extends RewriteMatrixMultChainOptimization {
	private static final EstimatorMatrixHistogram estimator = new EstimatorMatrixHistogram();

	@Override
	protected void optimizeMMChain(Hop hop, ArrayList<Hop> mmChain, ArrayList<Hop> mmOperators, ProgramRewriteStatus state) {
		// Step 2: construct dims array and input matrices
		double[] dimsArray = new double[mmChain.size() + 1];
		boolean dimsKnown = getDimsArray( hop, mmChain, dimsArray );
		MMNode[] sketchArray = new MMNode[mmChain.size() + 1];
		boolean inputsAvail = getInputMatrices(mmChain, sketchArray);
		
		if( dimsKnown && inputsAvail ) {
			// Step 3: clear the links among Hops within the identified chain
			clearLinksWithinChain ( hop, mmOperators );
			
			// Step 4: Find the optimal ordering via dynamic programming.
			
			// Invoke Dynamic Programming
			int size = mmChain.size();
			int[][] split = mmChainDPSparse(dimsArray, sketchArray, mmChain.size());

			Hop top = mmOperators.get(0);

			// Step 5: Relink the hops using the optimal ordering (split[][]) found from DP.
			LOG.trace("Sparsity-based Optimal MM Chain: ");
			mmChainRelinkHops(top, 0, size - 1, mmChain, mmOperators, new MutableInt(1), split, 1, sketchArray[size]);

			// 记录结果的MNC
			MetaDataExt.CACHE.put(top.getName(), new MetaDataExt((MatrixHistogram) sketchArray[size].getSynopsis()));

			// 传播
			HopUtil.propagateDcToParents(top);
		}
	}

	/**
	 * mmChainDP(): Core method to perform dynamic programming on a given array
	 * of matrix dimensions.
	 * 
	 * Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest, Clifford Stein
	 * Introduction to Algorithms, Third Edition, MIT Press, page 395.
	 */
	private static int[][] mmChainDPSparse(double[] dimArray, MMNode[] sketchArray, int size) 
	{
		double[][] dpMatrix = new double[size][size]; //min cost table
		MMNode[][] dpMatrixS = new MMNode[size][size]; //min sketch table
		int[][] split = new int[size][size]; //min cost index table

		//init minimum costs for chains of length 1
		for( int i = 0; i < size; i++ ) {
			Arrays.fill(dpMatrix[i], 0);
			Arrays.fill(split[i], -1);
			dpMatrixS[i][i] = sketchArray[i];
		}

		//compute cost-optimal chains for increasing chain sizes 
		for( int l = 2; l <= size; l++ ) { // chain length
			for( int i = 0; i < size - l + 1; i++ ) {
				int j = i + l - 1;
				// find cost of (i,j)
				dpMatrix[i][j] = Double.MAX_VALUE;
				for( int k = i; k <= j - 1; k++ ) {
					//construct estimation nodes (w/ lazy propagation and memoization)
					MMNode tmp = new MMNode(dpMatrixS[i][k], dpMatrixS[k+1][j], OpCode.MM);
					estimator.estim(tmp, false);

					MatrixHistogram lhs = (MatrixHistogram) dpMatrixS[i][k].getSynopsis();
					MatrixHistogram rhs = (MatrixHistogram) dpMatrixS[k+1][j].getSynopsis();

					//recursive cost computation
					double cost = dpMatrix[i][k] + dpMatrix[k + 1][j] 
						+ dotProduct(lhs.getColCounts(), rhs.getRowCounts());

					//prune suboptimal
					if( cost < dpMatrix[i][j] ) {
						dpMatrix[i][j] = cost;
						dpMatrixS[i][j] = tmp;
						split[i][j] = k;
					}
				}

				if( LOG.isTraceEnabled() ){
					LOG.trace("mmchainopt [i="+(i+1)+",j="+(j+1)+"]: costs = "+dpMatrix[i][j]+", split = "+(split[i][j]+1));
				}
			}
		}

		sketchArray[size] = dpMatrixS[0][size - 1];

		return split;
	}

	private static boolean getInputMatrices(ArrayList<Hop> chain, MMNode[] sketchArray) {
		boolean inputsAvail = true;

		for(int i = 0; i < chain.size(); i++) {
			Hop h = chain.get(i);
			MetaDataExt ext = MetaDataExt.CACHE.get(h.getName());
			inputsAvail = ext != null;
			if( inputsAvail ) {
				MMNode node = new MMNode(h.getDataCharacteristics());
				node.setSynopsis(ext.e);
				sketchArray[i] = node;
			}
			else {
				break;
			}
		}

		return inputsAvail;
	}
	
	private static double dotProduct(int[] h1cNnz, int[] h2rNnz) {
		long fp = 0;
		for( int j=0; j<h1cNnz.length; j++ )
			fp += (long)h1cNnz[j] * h2rNnz[j];
		return fp;
	}

	/**
	 * mmChainRelinkHops(): This method gets invoked after finding the optimal
	 * order (split[][]) from dynamic programming. It relinks the Hops that are
	 * part of the mmChain.
	 * @param mmChain : basic operands in the entire matrix multiplication chain.
	 * @param mmOperators : Hops that store the intermediate results in the chain.
	 *                      For example: A = B %*% (C %*% D) there will be three
	 *                      Hops in mmChain (B,C,D), and two Hops in mmOperators
	 *                     (one for each * %*%).
	 * @param h high level operator
	 * @param i array index i
	 * @param j array index j
	 * @param opIndex operator index
	 * @param split optimal order
	 * @param level log level
	 */
	private void mmChainRelinkHops(Hop h, int i, int j, ArrayList<Hop> mmChain,
								   ArrayList<Hop> mmOperators, MutableInt opIndex, int[][] split, int level, MMNode node) {
		//NOTE: the opIndex is a MutableInt in order to get the correct positions
		//in ragged chains like ((((a, b), c), (D, E), f), e) that might be given
		//like that by the original scripts variable assignments

		//single matrix - end of recursion
		if( i == j ) {
			logTraceHop(h, level);
			return;
		}

		if( LOG.isTraceEnabled() ){
			String offset = Explain.getIdentation(level);
			LOG.trace(offset + "(");
		}

		// 设置元数据
		h.setDataCharacteristics(node.getDataCharacteristics());

		// Set Input1 for current Hop h
		if( i == split[i][j] ) {
			h.getInput().add(mmChain.get(i));
			mmChain.get(i).getParent().add(h);
		}
		else {
			int ix = opIndex.getValue();
			opIndex.increment();
			h.getInput().add(mmOperators.get(ix));
			mmOperators.get(ix).getParent().add(h);
		}

		// Set Input2 for current Hop h
		if( split[i][j] + 1 == j ) {
			h.getInput().add(mmChain.get(j));
			mmChain.get(j).getParent().add(h);
		}
		else {
			int ix = opIndex.getValue();
			opIndex.increment();
			h.getInput().add(mmOperators.get(ix));
			mmOperators.get(ix).getParent().add(h);
		}

		// Find children for both the inputs
		mmChainRelinkHops(h.getInput().get(0), i, split[i][j], mmChain, mmOperators, opIndex, split, level+1, node.getLeft());
		mmChainRelinkHops(h.getInput().get(1), split[i][j] + 1, j, mmChain, mmOperators, opIndex, split, level+1, node.getRight());

//		// Propagate properties of input hops to current hop h
//		h.refreshSizeInformation();

		if( LOG.isTraceEnabled() ){
			String offset = Explain.getIdentation(level);
			LOG.trace(offset + ")");
		}
	}

	private static void logTraceHop( Hop hop, int level ) {
		if( LOG.isTraceEnabled() ) {
			String offset = Explain.getIdentation(level);
			LOG.trace(offset+ "Hop " + hop.getName() + "(" + hop.getClass().getSimpleName()
					+ ", " + hop.getHopID() + ")" + " " + hop.getDim1() + "x" + hop.getDim2() + " [" + hop.getNnz() + "]");
		}
	}
}
