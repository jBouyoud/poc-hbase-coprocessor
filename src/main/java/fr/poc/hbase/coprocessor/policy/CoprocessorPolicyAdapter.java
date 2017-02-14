package fr.poc.hbase.coprocessor.policy;

import fr.poc.hbase.coprocessor.policy.handler.LoggingPolicy;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * {@link Coprocessor} adapter that wrap all calls to be sure there is "safe".
 * <p>
 * This adapter able to catch all {@link Throwable} that can be thrown be a coprocessor
 * and wrap it into an authorized {@link IOException}
 * </p>
 */
@Slf4j
public class CoprocessorPolicyAdapter<T extends Coprocessor> extends PolicyVerifierAdapter<T> implements Coprocessor {

	/**
	 * Constructor
	 *
	 * @param adaptee coprocessor adaptee
	 */
	public CoprocessorPolicyAdapter(@NonNull T adaptee) {
		super(adaptee);
		LOGGER.info("Setting up coprocessors execution policies");
		setTimeout(TimeUnit.MILLISECONDS.convert(2, TimeUnit.SECONDS));
		setPolicies(Arrays.asList(
				new LoggingPolicy()
				//new JmxMetricsPolicy(),
				//new FailedRetryLimitPolicy(2),
				//new MaxMemoryPolicy(1024 * 1024 * 10L, 500),
				//new NoBypassOrCompletePolicy()
		));
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		runWithPolicies("Coprocessor::start", () -> getAdaptee().start(env), env);
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		runWithPolicies("Coprocessor::stop", () -> getAdaptee().stop(env), env);
	}

}
