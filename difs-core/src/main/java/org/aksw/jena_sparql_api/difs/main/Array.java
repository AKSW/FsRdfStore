package org.aksw.jena_sparql_api.difs.main;

import java.util.Arrays;
import java.util.List;

/**
 * A wrapper for arrays with hash code and equals.
 * The main difference to {@link Arrays#asList(Object...)} is that the underlying array
 * can be accessed directly. Even though the List interface features {@link List#toArray()}
 * this method is not suitable for generics.
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
}
