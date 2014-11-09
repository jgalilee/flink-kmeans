package com.jgalilee.flink.kmeans;

import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Converts each string representation of a point into a double array.
 *
 * @author jgalilee
 */
public class ParsePoints extends RichMapFunction<String, Tuple1<Double[]>> {

	private static final long serialVersionUID = -3996165277056903839L;

	@Override
	public Tuple1<Double[]> map(String in) throws Exception {
		return new Tuple1<Double[]>(Utility.stringToDoubleArray(in));
	}

}
