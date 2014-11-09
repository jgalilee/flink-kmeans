package com.jgalilee.flink.kmeans;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.IterativeDataSet;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * K-Means algorithm implementation.
 *
 * @author jgalilee
 */
public class Driver {

	public static void main(String[] args) throws Exception {
		if (4 > args.length) {
			System.err.println("Usage: points centroids max delta output");
			System.exit(1);
			return;
		}

		// Setup the environment for the algorithm/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Configuration deltaParam = new Configuration();
		deltaParam.setDouble("delta", Double.valueOf(args[3]));

		// Read the points dataset and parse each line.
		DataSet<Tuple1<Double[]>> points = env.
				readTextFile(args[0]).
				map(new ParsePoints());

		// Create iterative dataset with the new set of centroids.
		IterativeDataSet<Tuple2<String, Double[]>> iterator = env.
				readTextFile(args[1]).
				map(new ParseCentroids()).
				iterate(Integer.valueOf(args[2]));

		// Calculate new set of centroids.
		DataSet<Tuple2<String, Double[]>> iteration = points.
				map(new MapCentroidsToPoints()).
				withBroadcastSet(iterator, "CurrentCentroids").
				groupBy(0).
				reduceGroup(new CalculateNewCentroids());

		// Calculate converged dataset using points from each iteration.
		DataSet<Tuple2<String, Double>> converged = iterator.
				join(iteration).
				where(0).
				equalTo(0).
				map(new MapCentroidDistance()).
				filter(new FilterConvergedCentroids()).
				withParameters(deltaParam);

		// Close the iteration operator with the iteration dataset and the converge
		// dataset to be calcualted each iteration.
		DataSet<Tuple2<String, Double[]>> result = iterator.closeWith(iteration, converged);

		// Write the results to the distribtued file system.
		result.map(new RichMapFunction<Tuple2<String,Double[]>, String>() {

				private static final long serialVersionUID = -4025804864153845575L;

				@Override
				public String map(Tuple2<String, Double[]> centroid) throws Exception {
					StringBuilder sb = new StringBuilder();
					sb.append(centroid.f0);
					sb.append("\t");
					Double[] vector = centroid.f1;
					for (int i = 0, j = vector.length; i < j; i++) {
						sb.append(vector[i]);
						if (i < vector.length - 1) {
							sb.append(" ");
						}
					}
					return sb.toString();
				}

			}).
			writeAsText(args[4], WriteMode.OVERWRITE);

		env.execute();
	}

}

