package fr.poc.hbase.coprocessor.policy.util;

import lombok.NonNull;

import java.io.IOException;

/**
 * {@link IOException} runtime wrapper
 * <p>
 * Wrap an {@link IOException} to a {@link RuntimeException}
 * </p>
 */
public final class WrappedIOException extends RuntimeException {

	/**
	 * Constructor
	 *
	 * @param message exception message
	 */
	public WrappedIOException(String message) {
		super(message);
	}

	/**
	 * Constructor
	 *
	 * @param cause IOException to wrap
	 */
	public WrappedIOException(@NonNull IOException cause) {
		super(cause);
	}

	/**
	 * Returns the wrapped {@link IOException}
	 *
	 * @return the wrapped {@link IOException}
	 */
	@Override
	public synchronized IOException getCause() {
		return (IOException) super.getCause();
	}
}
