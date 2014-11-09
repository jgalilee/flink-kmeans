package com.jgalilee.flink.kmeans;

import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Parses the string representation of each centroid into the centroid and its
 * array.
 *
 * @author jgalilee
 */
public class ParseCentroids extends RichMapFunction<String, Tuple2<String, Double[]>> {

	private static final long serialVersionUID = 906878130125492263L;

	@Override
	public Tuple2<String, Double[]> map(String in) throws Exception {
		String label = in.substring(0, in.indexOf('\t'));
		Double[] point = Utility.stringToDoubleArray(in.substring(in.indexOf('\t') + 1));
		return new Tuple2<String, Double[]>(label, point);
	}

}
