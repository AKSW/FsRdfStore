package org.aksw.jena_sparql_api.difs.main;

import java.util.Arrays;
import java.util.List;

/**
 * A wrapper for arrays with hash code and equals.
 * The main difference to {@link Arrays#asList(Object...)} is that the underlying array
 * can be obtained from instances of this class directly. Note that {@link List#toArray()}
 * does not support generics.
 * 
 */
public class Array<T> {
	protected T[] array;

	public Array(T[] array) {
		super();
		this.array = array;
	}
	
	public static <T> Array<T> wrap(T[] array) {
		return new Array<>(array);
	}
	
	public T[] getArray() {
		return array;
	}
	
	@Override
	public boolean equals(Object obj) {
		boolean result = false;
		if (obj instanceof Array) {
			Object[] that = ((Array<?>) obj).getArray();
			result = Arrays.equals(array, that);
		}
		return result;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(array);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(array);
	}
}
