package fr.poc.hbase.coprocessor.policy.agent.transformer;

import javassist.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This class transform all class that have org.apache.hadoop.hbase.coprocessor.CoprocessorHost as superclass where
 * there is in configured prefix (default to 'org.apache.hadoop.hase')
 */
@Slf4j
public class CoprocessorHostWithPoliciesTransformer implements ClassFileTransformer {

	/**
	 * CoprocessorHost super class full name
	 */
	public static final String COPROCESSOR_HOST_SUPERCLASS = "org.apache.hadoop.hbase.coprocessor.CoprocessorHost";

	/**
	 * parameter key that allow to specifies class prefix filters separated by comma
	 */
	public static final String COPROCESSOR_HOST_PREFIX_PARAM = "host-prefixes";

	/**
	 * Host implementation classname prefix
	 */
	@NonNull
	private final List<String> hostsClassnamePrefixes;

	/**
	 * Constructor
	 *
	 * @param properties input properties, only fetch {@link #COPROCESSOR_HOST_PREFIX_PARAM} on it
	 */
	public CoprocessorHostWithPoliciesTransformer(@NonNull Map<String, String> properties) {
		hostsClassnamePrefixes = Arrays.asList(properties
				.getOrDefault(COPROCESSOR_HOST_PREFIX_PARAM, "org.apache.hadoop.hbase").split(","));
	}

	@Override
	public byte[] transform(final ClassLoader loader,
							final String fullyQualifiedClassName, final Class<?> classBeingRedefined,
							final ProtectionDomain protectionDomain, final byte[] classfileBuffer) throws IllegalClassFormatException {
		// Filter classes based on their names
		final String className = fullyQualifiedClassName.replaceAll("/", ".");
		if (hostsClassnamePrefixes.stream().filter(className::startsWith).count() != 1) {
			return null;
		}
		// Load class in a temp pool
		final ClassPool classPool = ClassPool.getDefault();
		classPool.appendClassPath(new ByteArrayClassPath(className, classfileBuffer));

		try {
			CtClass currentClass = classPool.get(className);
			CtClass superClass = currentClass.getSuperclass();
			// Check if the current class is a CoprocessorHost
			if (superClass == null || !COPROCESSOR_HOST_SUPERCLASS.equals(superClass.getName())) {
				LOGGER.trace("Class [{}] is not a CoprocessorHost", className);
				return null;
			}
			// Search for createEnvironment that will be the insertion point
			CtMethod createEnvMethod = currentClass.getDeclaredMethod("createEnvironment");
			if (createEnvMethod == null) {
				LOGGER.info("Unable to find createEnvironment method in CoprocessorHost [{}] skip policies adapter on it", className);
				return null;
			}

			// Alter bytecode to insert policies
			createEnvMethod.insertBefore(
					"instance = fr.poc.hbase.coprocessor.policy.util.PoliciesHelper.withPolicies(instance, priority, conf);");

			// Return altered byte code
			return currentClass.toBytecode();

		} catch (NotFoundException e) {
			LOGGER.debug("Unable to load class for manipulation ", e);
		} catch (IOException | CannotCompileException e) {
			LOGGER.info("Unable to obtain ByteCode after modification on CoprocessorHost [{}] skip policies adapter on it", className, e);
		}
		return null;
	}
}
