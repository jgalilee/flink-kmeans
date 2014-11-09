package com.jgalilee.flink.kmeans;

import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Calculates each centroid pairs distance and outputs the id of the centroid
 * and the distance between them. Assumes that the centroids have already been
 * joined on their id.
 *
 * @author jgalilee
 */
public class MapCentroidDistance extends RichMapFunction<Tuple2<Tuple2<String, Double[]>, Tuple2<String, Double[]>>, Tuple2<String, Double>> {

	private static final long serialVersionUID = -142619172268183589L;

	@Override
	public Tuple2<String, Double> map(Tuple2<Tuple2<String, Double[]>, Tuple2<String, Double[]>> in) throws Exception {
		Tuple2<String, Double[]> centroid1 = in.getField(0);
		Tuple2<String, Double[]> centroid2 = in.getField(1);
		String label = centroid1.getField(0);
		Double result = Math.abs(Utility.distance(centroid1.f1, centroid2.f1));
		return new Tuple2<String, Double>(label, result);
	}

}
