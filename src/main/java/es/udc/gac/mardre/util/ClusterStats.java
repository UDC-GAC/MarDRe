/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of MarDRe.
 * 
 * MarDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MarDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

public final class ClusterStats {

	private static long maxInputClusterSize;
	private static long maxOutputClusterSize;
	private static double maxInputClusterTime;
	private static double maxInputWriteTime;
	private static long maxInputClusterTimeSize;
	private static long maxOutputClusterTimeSize;
	private static double maxClusterTime;
	private static double maxClusterWriteTime;
	private static long maxInputWriteTimeSize;
	private static long maxOutputWriteTimeSize;
	private static double maxWriteClusterTime;
	private static double maxWriteTime;
	private static long numInputSequences;
	private static long numOutputSequences;
	private static long numClusters;

	private ClusterStats() {}

	public static void init() {
		maxInputClusterSize = 0;
		maxOutputClusterSize = 0;
		maxInputClusterTime = 0;
		maxInputWriteTime = 0;
		maxInputClusterTimeSize = 0;
		maxOutputClusterTimeSize = 0;
		maxClusterTime = 0;
		maxClusterWriteTime = 0;
		maxInputWriteTimeSize = 0;
		maxOutputWriteTimeSize = 0;
		maxWriteClusterTime = 0;
		maxWriteTime = 0;
		numInputSequences = 0;
		numOutputSequences = 0;
		numClusters = 0;
	}

	public static void reset() {
		init();
	}

	public static void update(long inputClusterSize, long outputClusterSize, double clusterTime, double writeTime) {
		numInputSequences += inputClusterSize;
		numOutputSequences += outputClusterSize;
		numClusters++;

		if (inputClusterSize > maxInputClusterSize) {
			maxInputClusterSize = inputClusterSize;
			maxOutputClusterSize = outputClusterSize;
			maxInputClusterTime = clusterTime;
			maxInputWriteTime = writeTime;
		}

		if (clusterTime > maxClusterTime) {
			maxInputClusterTimeSize = inputClusterSize;
			maxOutputClusterTimeSize = outputClusterSize;
			maxClusterTime = clusterTime;
			maxClusterWriteTime = writeTime;
		}

		if (writeTime > maxWriteTime) {
			maxInputWriteTimeSize = inputClusterSize;
			maxOutputWriteTimeSize = outputClusterSize;
			maxWriteClusterTime = clusterTime;
			maxWriteTime = writeTime;
		}
	}

	public static void report(double reduceTime) {
		if (numInputSequences > 0) {
			System.out.format(java.util.Locale.ENGLISH,"\n%d/%d reads in %d clusters: %.3f sec => "
					+ "largest cluster %d/%d: %.3f sec (%.3f/%.3f), "
					+ "highest cluster time %d/%d: %.3f sec (%.3f/%.3f), "
					+ "highest write time %d/%d: %.3f sec (%.3f/%.3f)\n",
					numOutputSequences, numInputSequences, numClusters, reduceTime,
					maxOutputClusterSize, maxInputClusterSize, maxInputClusterTime + maxInputWriteTime,maxInputClusterTime, maxInputWriteTime,
					maxOutputClusterTimeSize, maxInputClusterTimeSize, maxClusterTime + maxClusterWriteTime, maxClusterTime, maxClusterWriteTime,
					maxOutputWriteTimeSize, maxInputWriteTimeSize, maxWriteClusterTime + maxWriteTime, maxWriteClusterTime, maxWriteTime);
		}
	}

	public static long getNumInputSequences() {
		return numInputSequences;
	}
}