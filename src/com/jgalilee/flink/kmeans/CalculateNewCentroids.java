package com.jgalilee.flink.kmeans;

import java.util.Iterator;

import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.RichGroupReduceFunction.Combinable;

/**
 * Calculates each new centroid given the points that have been assigned to it.
 *
 * @author jgalilee
 */
@Combinable
public class CalculateNewCentroids extends RichGroupReduceFunction<Tuple3<String, Double[], Integer>, Tuple2<String, Double[]>> {

	private static final long serialVersionUID = 5057494042352179055L;

	public void combine(Iterable<Tuple3<String, Double[], Integer>> iterable, Collector<Tuple3<String, Double[], Integer>> out) throws Exception {
		String label = null;
		Double[] result = null;
		Integer length = 0;
		Iterator<Tuple3<String, Double[], Integer>> in = iterable.iterator();
		while(in.hasNext()) {
			Tuple3<String, Double[], Integer> current = in.next();
			length = length + current.f2;
			if (null == label) {
				label = current.f0;
				result = current.f1;
			} else {
				result = Utility.add(result, current.f1);
			}
		}
		Tuple3<String, Double[], Integer> x = new Tuple3<String, Double[], Integer>(label, result, length);
		out.collect(x);
	}

	@Override
	public void reduce(Iterable<Tuple3<String, Double[], Integer>> iterable, Collector<Tuple2<String, Double[]>> out) throws Exception {
		String label = null;
		Double[] result = null;
		Integer length = 0;
		Iterator<Tuple3<String, Double[], Integer>> in = iterable.iterator();
		while(in.hasNext()) {
			Tuple3<String, Double[], Integer> current = in.next();
			length = length + current.f2;
			if (null == label) {
				label = current.f0;
				result = current.f1;
			} else {
				result = Utility.add(result, current.f1);
			}
		}
		if (length > 0) {
			result = Utility.divide(result, length);
			Tuple2<String, Double[]> x = new Tuple2<String, Double[]>(label, result);
			out.collect(x);
		}
	}

}
