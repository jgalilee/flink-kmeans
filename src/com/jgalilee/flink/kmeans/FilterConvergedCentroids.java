package com.jgalilee.flink.kmeans;

import org.apache.flink.api.java.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Simple function that removes centroids that have been given a distance that is
 * under the delta. This is used for calculating the empty convergence dataset.
 *
 * @author jgalilee
 */
public class FilterConvergedCentroids extends RichFilterFunction<Tuple2<String, Double>> {

	private static final long serialVersionUID = -6900480809333916745L;

	private Double delta = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.delta = parameters.getDouble("delta", 0.5);
	}

	@Override
	public boolean filter(Tuple2<String, Double> in) throws Exception {
		// Filter operator works the other way around for some reason. True means
		// keep, false means discard.
		return in.f1 < delta;
	}

}
