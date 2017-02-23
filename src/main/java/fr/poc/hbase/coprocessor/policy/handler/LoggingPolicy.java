package fr.poc.hbase.coprocessor.policy.handler;

import fr.poc.hbase.coprocessor.policy.PolicyHandler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Simple logging policy
 */
@Slf4j
public class LoggingPolicy implements PolicyHandler {

	@Override
	public <T> T onArgument(T arg) {
		LOGGER.trace("Securised argument [{}]", arg);
		return arg;
	}

	@Override
	public <T> void beforeRun(@NonNull T object, @NonNull String method, @NonNull Object[] args) {
		LOGGER.debug("Method [{}] will be executed on [{}] with [{}] arguments", method, object, args.length);
	}

	@Override
	public <T> void onError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull IOException ioE) {
		LOGGER.warn("Method [{}] has been executed on [{}] with an error [{}]", method, object, ioE);
	}

	@Override
	public <T> void onUnexpectedError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Throwable throwable){
		LOGGER.error("Method [{}] has been executed on [{}] with an unexpected error [{}]", method, object, throwable);
	}

	@Override
	public <T> void afterRun(@NonNull T object, @NonNull String method, @NonNull Object[] args, Object result, long executionTime) {
		LOGGER.info("Method [{}] has been executed on [{}] in [{}]ms with [{}] as result", method, object, executionTime, result);
	}
}
