package fr.poc.hbase.coprocessor.policy.agent;

import fr.poc.hbase.coprocessor.policy.agent.transformer.CoprocessorHostWithPoliciesTransformer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.HashMap;
import java.util.Map;

/**
 * Java agent entry point that allow to dynamicaly applies policies on user level coprocessors.
 * Policies are configured in Hbase configuration, see
 * {@link fr.poc.hbase.coprocessor.policy.config.PoliciesConfigurer} for more details
 */
@Slf4j
public class CoprocessorPolicyAgent {

	/**
	 * Agent main (dynamic loading)
	 *
	 * @param args            agent raw arguments
	 * @param instrumentation JVM instrumentation
	 */
	public static void agentmain(String args, Instrumentation instrumentation) {
		premain(args, instrumentation);
	}

	/**
	 * Premain (static loading)
	 *
	 * @param args            agent raw arguments
	 * @param instrumentation JVM instrumentation
	 */
	public static void premain(String args, @NonNull Instrumentation instrumentation) {
		Map<String, String> properties = getPropertiesFromArgs(args);

		LOGGER.info("Loading CoprocessorHost class transformer");
		ClassFileTransformer transformer = new CoprocessorHostWithPoliciesTransformer(properties);
		instrumentation.addTransformer(transformer);

		if (instrumentation.isRetransformClassesSupported()) {
			for (Class<?> loadedClass : instrumentation.getAllLoadedClasses()) {
				if (CoprocessorHost.class.isAssignableFrom(loadedClass)
						&& instrumentation.isModifiableClass(loadedClass)) {

					try {
						instrumentation.retransformClasses(loadedClass);
					} catch (UnmodifiableClassException ex) {
						LOGGER.info("Class {} is not transformable, ignore it", loadedClass.getName(), ex);
					}
				}
			}
		}
	}

	/**
	 * Create properties from arg. properties are comma separated and key,value is separated by '='
	 *
	 * @param args raw arguments
	 * @return parsed properties
	 */
	private static Map<String, String> getPropertiesFromArgs(String args) {
		Map<String, String> properties = new HashMap<>();
		if (args != null) {
			// parse the arguments:
			// param1=val1,param2=val2
			for (String propertyAndValue : args.split(",")) {
				String[] tokens = propertyAndValue.split("=", 2);
				if (tokens.length != 2) {
					continue;
				}
				properties.put(tokens[0], tokens[1]);
			}
		}
		return properties;
	}

}
