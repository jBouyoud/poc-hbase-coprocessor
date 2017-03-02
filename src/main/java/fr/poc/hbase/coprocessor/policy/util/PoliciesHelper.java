package fr.poc.hbase.coprocessor.policy.util;

import fr.poc.hbase.coprocessor.policy.Policy;
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
	public static Coprocessor withPolicies(@NonNull Coprocessor coprocessor, int priority,
										   @NonNull Configuration configuration) {

		// If coprocessor is at system level, check if it's an hbase ones
		if (priority <= org.apache.hadoop.hbase.Coprocessor.PRIORITY_SYSTEM
				&& coprocessor.getClass().getName().startsWith(Coprocessor.class.getPackage().getName())) {
			// DO not modify system priority coprocessors
			return coprocessor;
		}
		// build policies from configuration
		return withPolicies(coprocessor, new PoliciesConfigurer(configuration).getPolicies());
	}

	/**
	 * Adapt a coprocessor with given polices
	 *
	 * @param coprocessor Coprocessor where policies should be applied
	 * @param policies    Policies to apply
	 * @return apadated coprocessor with policies
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Coprocessor> T withPolicies(@NonNull T coprocessor, @NonNull List<Policy> policies) {
		List interfaces = ClassUtils.getAllInterfaces(coprocessor.getClass());
		Class[] ifaces = (Class[]) interfaces.toArray(new Class[interfaces.size()]);

		LOGGER.info("Create a PolicyVerifier on Coprocessor : [{}]", coprocessor);
		return (T) Proxy.newProxyInstance(
				coprocessor.getClass().getClassLoader(), ifaces,
				new PolicyInvocationHandler<>(coprocessor, ifaces, policies));
	}

}

