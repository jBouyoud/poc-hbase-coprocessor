package fr.poc.hbase.coprocessor.policy.config;

import fr.poc.hbase.coprocessor.policy.Policy;
import fr.poc.hbase.coprocessor.policy.impl.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Policies configurer class that allow instanciate policies from hbase configuration
 */
@Slf4j
@RequiredArgsConstructor
public class PoliciesConfigurer {

	/**
	 * Hbase configuration where policies are extracted in
	 */
	@NonNull
	private final Configuration configuration;

	/**
	 * Build policies from the current configuration
	 *
	 * @return configured list of policies
	 */
	public List<Policy> getPolicies() {
		// TODO Implements me !!!
		return Arrays.asList(
				new TimeoutPolicy(2, TimeUnit.SECONDS),
				new LoggingPolicy(),
				new MetricsPolicy(DefaultMetricsSystem.instance(), "Coprocessors"),
				new LimitRetryPolicy(2, new LimitRetryPolicy.RollingInMemoryCache(10, TimeUnit.MINUTES)),
				//new MaxMemoryPolicy(1024 * 1024 * 10L, 500),
				new NoBypassOrCompletePolicy()
		);
	}
}
