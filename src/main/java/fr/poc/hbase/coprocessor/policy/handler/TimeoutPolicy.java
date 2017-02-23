package fr.poc.hbase.coprocessor.policy.handler;

import fr.poc.hbase.coprocessor.policy.PolicyHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Execution Timeout policy
 */
@Slf4j
@RequiredArgsConstructor
public class TimeoutPolicy implements PolicyHandler {

	/**
	 * the execution timeout
	 */
	private final long timeout;

	/**
	 * the time unit of the timeout
	 */
	@NonNull
	private final TimeUnit timeoutUnit;

	/**
	 * Scheduled service able to timeout too long tasks
	 */
	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

	@Override
	public <T> void running(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Future<?> future) {
		executor.schedule(() -> {
			if (future.cancel(true)) {
				LOGGER.info("Method [{}] on [{}] has been cancelled after [{} {}]", method, object, timeout, timeoutUnit.toString());
			}
		}, timeout, timeoutUnit);
	}
}
