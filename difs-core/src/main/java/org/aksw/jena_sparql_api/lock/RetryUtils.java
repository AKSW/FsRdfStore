package org.aksw.jena_sparql_api.lock;

import java.util.concurrent.Callable;

public class RetryUtils {
	public static <T> T simpleRetry(
			long retryCount,
			long delayInMs,
			Callable<T> action) {
		
		T result = null;
		long retryAttempt ;
		for (retryAttempt = 0; retryAttempt < retryCount; ++retryAttempt) {
			try {
				result = action.call();
			} catch (Exception e) {
				if (retryAttempt + 1 == retryCount) {
					throw new RuntimeException(e);
				} else {
					try {
						Thread.sleep(delayInMs);
					} catch (Exception e2) {
						throw new RuntimeException(e2);
					}
				}
			}
		}
		return result;
	}
}
