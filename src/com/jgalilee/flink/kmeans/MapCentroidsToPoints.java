package com.jgalilee.flink.kmeans;

import java.util.Collection;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * Maps each point to its closest centroids id.
 *
 * @author jgalilee
 */
public class MapCentroidsToPoints extends RichMapFunction<Tuple1<Double[]>, Tuple3<String, Double[], Integer>> {

	private static final long serialVersionUID = 2808216511518130352L;

	private Collection<Tuple2<String, Double[]>> centroids = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		try {
			IterationRuntimeContext context = getIterationRuntimeContext();
			centroids = context.getBroadcastVariable("CurrentCentroids");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Tuple3<String, Double[], Integer> map(Tuple1<Double[]> point) throws Exception {
		String closest = null;
		Double distance = null;
		for (Tuple2<String, Double[]> centroid : centroids) {
			Double dist = Utility.distance(centroid.f1, point.f0);
			if (null == distance || dist < distance) {
				distance = dist;
				closest = centroid.f0;
			}
		}
		return new Tuple3<String, Double[], Integer>(closest, point.f0, 1);
	}

}
