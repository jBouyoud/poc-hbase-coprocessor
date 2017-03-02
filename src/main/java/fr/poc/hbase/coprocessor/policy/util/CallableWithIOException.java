package fr.poc.hbase.coprocessor.policy.util;

import java.io.IOException;

/**
 * A {@link java.util.concurrent.Callable} that could be throws {@link IOException}
 */
@FunctionalInterface
public interface CallableWithIOException<R> {

	/**
	 * Computes a result, or throws an exception if unable to do so.
	 *
	 * @return computed result
	 * @throws IOException if unable to compute a result
	 */
	R call() throws IOException;
}

