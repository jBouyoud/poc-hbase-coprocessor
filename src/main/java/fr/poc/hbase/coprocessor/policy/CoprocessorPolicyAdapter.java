package fr.poc.hbase.coprocessor.policy;

import fr.poc.hbase.coprocessor.policy.handler.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.Time;

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
		setPolicies(Arrays.asList(
				new TimeoutPolicy(2, TimeUnit.SECONDS),
				new LoggingPolicy(),
				new MetricsPolicy(DefaultMetricsSystem.instance(), "Coprocessors"),
				new LimitRetryPolicy(2, new LimitRetryPolicy.RollingInMemoryCache(10, TimeUnit.MINUTES)),
				//new MaxMemoryPolicy(1024 * 1024 * 10L, 500),
				new NoBypassOrCompletePolicy()
		));
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		runWithPolicies("start", () -> getAdaptee().start(env), env);
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		runWithPolicies("stop", () -> getAdaptee().stop(env), env);
		closePolicies();
	}

}
