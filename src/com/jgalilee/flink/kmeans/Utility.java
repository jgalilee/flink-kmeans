package com.jgalilee.flink.kmeans;

import java.util.Arrays;

/**
 * Utility class for methods to assist with dealing with double arrays for
 * representing point vectors.
 *
 * @author jgalilee
 */
public class Utility {

	/**
	 * Parses the given string input into a double array.
	 *
	 * @return Double array representation of the input string.
	 */
	public static Double[] stringToDoubleArray(String in) {
		String[] coords = in.split(" ");
		Double[] result = new Double[coords.length];
		for (int i = 0, j = result.length; i < j; i++) {
			result[i] = Double.valueOf(coords[i]);
		}
		return result;
	}

	/**
	 * Calculate the distance between two points. Assumes they are the same size.
	 *
	 * @param point1 First point to calculate the distance between.
	 * @param point2 Seconds point to calculate the distance between.
	 * @return Returns this distance between two points.
	 */
	public static Double distance(Double[] point1, Double[] point2) throws DifferentDimensionsException {
		if (point1.length != point2.length) {
			throw new DifferentDimensionsException();
		}
		double sum = 0.0f;
		for (int i = 0, j = point1.length; i < j; i++) {
			Double thisCoordinate = point1[i];
			Double otherCoordinate = point2[i];
			sum += Math.pow(thisCoordinate - otherCoordinate, 2);
		}
		return Math.sqrt(sum);
	}

	/**
	 * Convert an array of doubles into a string seperated by spaces.
	 *
   * @param array Array of doubles to convert into a string.
	 * @return Space seperated string representation of the double array.
	 */
	public static String arrayToString(Double[] array) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0, j = array.length; i < j; i++) {
			sb.append(String.valueOf(array[i]));
			if (i < j - 1) {
				sb.append(" ");
			}
		}
		return sb.toString();
	}

	/**
	 * Divide every item in the given array by a specific number.
	 *
	 * @param array Array to divide each dimensions by the given value.
	 * @param by Amount to divide each dimension by the given value.
	 * @return Divides each item in the array by the given number.
	 */
	public static Double[] divide(Double[] array, Integer by) {
		for (int i = 0, j = array.length; i < j; i++) {
			array[i] /= by;
		}
		return array;
	}

	/**
	 * Add two arrays together. Return the results. Assume they are the same size.
	 *
	 * @param point1 First point to add together.
	 * @param point2 Seconds point to add together.
	 * @return Returns the sum of two double arrays.
	 */
	public static Double[] add(Double[] point1, Double[] point2) throws DifferentDimensionsException {
		if (point1.length != point2.length) {
			throw new DifferentDimensionsException();
		}
		int length = point1.length;
		Double[] result = new Double[length];
		for (int i = 0; i < length; i++) {
			result[i] = point1[i] + point2[i];
		}
		return result;
	}

}
