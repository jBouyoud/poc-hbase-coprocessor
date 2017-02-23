package fr.poc.hbase.coprocessor.policy;

import lombok.NonNull;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Execution policy interface, able to implements various execution policy
 */
public interface PolicyHandler {

	/**
	 * Call on method arguments where policies should be applied
	 *
	 * @param arg method argument
	 * @param <T> argument type
	 * @return policy aware argument
	 */
	default <T> T onArgument(T arg) {
		return arg;
	}

	/**
	 * Call before method execution
	 *
	 * @param object  proxied object
	 * @param method  proxied method
	 * @param args    method arguments
	 * @param <T>     proxied object type
	 */
	default <T> void beforeRun(@NonNull T object, @NonNull String method, @NonNull Object[] args) {
		// No operation
	}

	/**
	 * Calle while method as been executed
	 *
	 * @param object proxied object
	 * @param method proxied method
	 * @param args   method arguments
	 * @param future future of current method execution
	 * @param <T>    proxied object type
	 */
	default <T> void running(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Future<?> future) {
		// No Operation
	}

	/**
	 * Call when a method throw an {@link IOException}
	 *
	 * @param object      proxied object
	 * @param method      proxied method
	 * @param args        method arguments
	 * @param ioException method execution error
	 * @param <T>         proxied object type
	 */
	default <T> void onError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull IOException ioException) {
		// No operation
	}

	/**
	 * Call when a method throw an unexpected Error (Timeout, Other error)
	 *
	 * @param object    proxied object
	 * @param method    proxied method
	 * @param args      method arguments
	 * @param throwable method execution error
	 * @param <T>       proxied object type
	 */
	default <T> void onUnexpectedError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Throwable throwable) {
		// No operation
	}

	/**
	 * Call once method is already executed
	 *
	 * @param object        proxied object
	 * @param method        proxied method
	 * @param args          method arguments
	 * @param result        method result
	 * @param executionTime execution time in milliseconds
	 * @param <T>           proxied object type
	 */
	default <T> void afterRun(@NonNull T object, @NonNull String method, @NonNull Object[] args, Object result, long executionTime) {
		// No operation
	}
}
