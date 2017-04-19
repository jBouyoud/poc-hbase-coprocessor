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
	 * Serial Version UID
	 */
	private static final long serialVersionUID = 1937393173605326986L;
	
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
	public IOException getCause() {
		return (IOException) super.getCause();
	}
}
