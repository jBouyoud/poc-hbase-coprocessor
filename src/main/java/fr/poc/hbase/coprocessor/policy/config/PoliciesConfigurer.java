package fr.poc.hbase.coprocessor.policy.config;

import fr.poc.hbase.coprocessor.policy.Policy;
import fr.poc.hbase.coprocessor.policy.proxy.CoprocessorPolicyProxy;
import fr.poc.hbase.coprocessor.policy.impl.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
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
	 * Coprocessors policies white list
	 */
	public static final String COPROCESSOR_POLICY_WITHE_LIST_CONFIGURATION_NAME = "hbase.coprocessors.policy.white-list";
	/**
	 * Default white listed classes
	 */
	public static final String COPROCESSOR_POLICY_WITHE_LIST_DEFAULT =
		"org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,"+
		"org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint,"+
		"org.apache.hadoop.hbase.backup.master.BackupController";

	/**
	 * Hbase configuration where policies are extracted in
	 */
	@NonNull
	private final Configuration configuration;

	/**
	 * Indicates whenever the given coprocessor should be transformed with policies or not
	 *
	 * @param priority    coprocessor priority
	 * @param coprocessor coprocessor
	 * @return true if policies should be applied, false else
	 */
	public boolean needApplyPolicies(int priority, @NonNull Coprocessor coprocessor) {
		// Do not apply policies when they are already sets
		if (coprocessor instanceof CoprocessorPolicyProxy) {
			return false;
		}
		// Do not apply policies on withe list classes
		if (getCoprocessorsWitheListClasses().stream()
				.anyMatch(c -> c.equals(coprocessor.getClass().getName()))) {
			return false;
		}
		// Else apply
		return true;
	}

	/**
	 * Parse with listed classes from configuration
	 *
	 * @return list of withe listed classes names
	 */
	private List<String> getCoprocessorsWitheListClasses() {
		return Arrays.asList(configuration.get(COPROCESSOR_POLICY_WITHE_LIST_CONFIGURATION_NAME, COPROCESSOR_POLICY_WITHE_LIST_DEFAULT).split(","));
	}

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
				new NoBypassOrCompletePolicy()
		);
	}


}
