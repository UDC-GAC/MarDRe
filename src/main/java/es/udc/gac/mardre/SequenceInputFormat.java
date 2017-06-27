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
 * along with MarDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public abstract class SequenceInputFormat extends TextInputFormat {

	private static final double SPLIT_SLOP = 1.1; // 10% slop

	public int getNumberOfSplits(JobContext job, Path inputPath, long splitSize) throws IOException {

		int nsplits = 0;
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus contents[] = fs.listStatus(inputPath);

		for (int i = 0; i < contents.length; i++) {
			FileStatus file = contents[i];
			Path path = contents[i].getPath();
			long length = file.getLen();

			if (length != 0) {
				if (isSplitable(job, path)) {
					long bytesRemaining = length;

					while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
						nsplits++;
						bytesRemaining -= splitSize;
					}

					if (bytesRemaining != 0)
						nsplits++;
				} else { // not splitable
					nsplits = 1;
				}
			}
		}

		return nsplits;
	}
}