package fr.poc.hbase.coprocessor.policy.util;

import fr.poc.hbase.coprocessor.policy.PolicyInvocationHandler;
import fr.poc.hbase.coprocessor.policy.config.PoliciesConfigurer;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;

import java.lang.reflect.Proxy;
import java.util.List;

/**
 * Helper class that help to applies Policies on {@link Coprocessor}
 */
@Slf4j
@UtilityClass
public class PoliciesHelper {


	/**
	 * Adapt a coprocessor that have at least a #PRIORITY_USER priority with polices
	 * witch are build from hbase configuration
	 *
	 * @param coprocessor   Coprocessor where policies should be applied
	 * @param priority      Coprocessor priority
	 * @param configuration Hbase configuration where policies configuration will be fetched in
	 * @return apadated coprocessor with policies
	 */
	@SuppressWarnings("unchecked")
	public static Coprocessor withPolicies(@NonNull Coprocessor coprocessor, int priority,
										   @NonNull Configuration configuration) {

		PoliciesConfigurer configurer = new PoliciesConfigurer(configuration);
		// If the current configuration allows this coprocessor
		if (!configurer.needApplyPolicies(priority, coprocessor)) {
			// DO not modify others coprocessors
			return coprocessor;
		}
		// Fetch interfaces
		List interfaces = ClassUtils.getAllInterfaces(coprocessor.getClass());
		Class[] ifaces = (Class[]) interfaces.toArray(new Class[interfaces.size()]);

		LOGGER.info("Create a PolicyVerifier on Coprocessor : [{}]", coprocessor);
		// Create a proxy for the current coprocessor based on each interfaces
		return (Coprocessor) Proxy.newProxyInstance(
				coprocessor.getClass().getClassLoader(), ifaces,
				new PolicyInvocationHandler<>(coprocessor, ifaces, configurer.getPolicies()));
	}

}
