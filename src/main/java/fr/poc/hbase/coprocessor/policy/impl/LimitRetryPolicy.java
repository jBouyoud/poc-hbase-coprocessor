package fr.poc.hbase.coprocessor.policy.impl;

import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.DoNotRetryIOException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Limit the number of retry for failed requests,
 * the {@link #retryThreshold} interpretation depends on witch {@link #failedExecutionCache} is used
 */
@Slf4j
@RequiredArgsConstructor
public class LimitRetryPolicy implements Policy {

	/**
	 * Retry threshold, when retry
	 */
	private final int retryThreshold;

	/**
	 * Retries cache that store failedExecutionCache runs
	 */
	@NonNull
	private final FailedExecutionCache failedExecutionCache;

	@Override
	public <T> void beforeRun(@NonNull T object, @NonNull String method, @NonNull Object[] args) throws IOException {
		if (failedExecutionCache.get(getHash(object, method, args)) > retryThreshold) {
			throw new ServerSideRetriesExhaustedException("Method [" + method + "] has already failed on [" + object + "]");
		}
	}

	@Override
	public <T> void onUnexpectedError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Throwable throwable) {
		long hash = getHash(object, method, args);
		LOGGER.debug("Add failed execution in cache with identifier [{}]", hash);
		failedExecutionCache.add(hash);
	}

	@Override
	public void close() throws IOException {
		failedExecutionCache.close();
	}

	/**
	 * Compute the cache key from an coprocessor method call
	 *
	 * @param object coprocessor object instance
	 * @param method coprocessor method
	 * @param args   method arguments
	 * @param <T>    coprocessor type
	 * @return the cache key that identifies all input arguments
	 */
	private <T> long getHash(@NonNull T object, @NonNull String method, @NonNull Object[] args) {
		long hash = 0L;
		int idx = 1;
		for (Object arg : args) {
			hash += (arg == null ? 1 : arg.getClass().hashCode()) * idx++;
		}
		return object.getClass().hashCode() * (idx + 2) + method.hashCode() * (idx + 1) + hash;
	}

	/**
	 * Failed Execution cache interface
	 */
	public interface FailedExecutionCache {

		/**
		 * Get the current number of failed execution for a given key
		 *
		 * @param key execution identifier
		 * @return number of failed execution for that key
		 */
		int get(long key);

		/**
		 * Add a new failed execution for the given key
		 *
		 * @param key execution identifier
		 */
		void add(long key);

		/**
		 * Close the cache
		 */
		void close();
	}

	/**
	 * Marker Excpetion for that kind of policy
	 */
	public static final class ServerSideRetriesExhaustedException extends DoNotRetryIOException {


		/**
		 * Default constructor
		 *
		 * @param message error description
		 */
		public ServerSideRetriesExhaustedException(String message) {
			super(message);
		}
	}

	/**
	 * Basic In-memory implementation that store fails in memory and never cleanup
	 */
	public static class InMemoryCache implements FailedExecutionCache {

		/**
		 * failed execution cache
		 */
		private final ConcurrentMap<Long, Integer> failedExecutionCache = new ConcurrentHashMap<>();

		@Override
		public int get(long hash) {
			return failedExecutionCache.getOrDefault(hash, 0);
		}

		@Override
		public void add(long hash) {
			failedExecutionCache.compute(hash, (key, nbFail) -> nbFail == null ? 1 : nbFail + 1);
		}

		@Override
		public void close() {
			failedExecutionCache.clear();
		}
	}

	/**
	 * Simple Rolling in-memory cache execution fails
	 * are stored in memory and cleaned up as specified while building the object
	 */
	public static class RollingInMemoryCache implements FailedExecutionCache {

		/**
		 * Scheduled service able to timeout cleanup outdated fails
		 */
		private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		/**
		 * Row cache
		 */
		private final ConcurrentMap<Long, List<Long>> failedExecutionCache = new ConcurrentHashMap<>();

		/**
		 * Default constructor
		 *
		 * @param retryTimeout failedExecutionCache rolling timeout
		 * @param timeoutUnit  failedExecutionCache rolling timeout unit
		 */
		public RollingInMemoryCache(long retryTimeout, @NonNull final TimeUnit timeoutUnit) {
			executor.scheduleAtFixedRate(() -> {
				long expired = System.nanoTime() - TimeUnit.NANOSECONDS.convert(retryTimeout, timeoutUnit);
				failedExecutionCache.forEach((aLong, listFails) -> listFails.removeIf(ts -> ts <= expired));
			}, retryTimeout, retryTimeout / 4, timeoutUnit);
		}

		@Override
		public int get(long hash) {
			return failedExecutionCache.getOrDefault(hash, Collections.emptyList()).size();
		}

		@Override
		public void add(long hash) {
			failedExecutionCache.compute(hash, (key, nbFail) -> {
				if (nbFail == null) {
					return new CopyOnWriteArrayList<>(Collections.singletonList(System.nanoTime()));
				}
				nbFail.add(System.nanoTime());
				return nbFail;
			});
		}

		@Override
		public void close() {
			executor.shutdownNow();
			failedExecutionCache.clear();
		}

	}
}
