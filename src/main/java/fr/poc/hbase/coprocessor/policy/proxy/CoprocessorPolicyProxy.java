package fr.poc.hbase.coprocessor.policy.proxy;

import fr.poc.hbase.coprocessor.policy.Policy;
import fr.poc.hbase.coprocessor.policy.PolicyVerifier;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;

import java.io.IOException;
import java.util.List;

/**
 * {@link Coprocessor} proxy that wrap all calls to be sure there is "safe" according to the given policies
 * <p>
 * This adapter able to catch all {@link Throwable} that can be thrown be a coprocessor
 * and wrap it into an authorized {@link IOException}
 * </p>
 */
@Slf4j
public class CoprocessorPolicyProxy<T extends Coprocessor> extends PolicyVerifier<T> implements Coprocessor {

	/**
	 * Constructor
	 *
	 * @param adaptee  coprocessor adaptee
	 * @param policies default policies to apply
	 */
	public CoprocessorPolicyProxy(@NonNull T adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		runWithPolicies("start", () -> getAdaptee().start(env), env);
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		try {
			runWithPolicies("stop", () -> getAdaptee().stop(env), env);
		} finally {
			close();
		}
	}

}
