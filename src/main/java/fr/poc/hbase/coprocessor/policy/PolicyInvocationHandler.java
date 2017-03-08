package fr.poc.hbase.coprocessor.policy;

import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.policy.proxy.ServicePolicyProxy;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Java Proxy invocation handler able to applies policies on {@link Coprocessor} and all derived interfaces.
 * This handler applies policies only on given interfaces
 */
@Slf4j
public class PolicyInvocationHandler<T extends Coprocessor> extends PolicyVerifier<T> implements InvocationHandler {

	/**
	 * List of all Coprocessor friendly interfaces where policies must be applied
	 * <p>
	 * Could not extends Coprocessor because of CoprocessorService & SingletonCoprocessorService
	 * </p>
	 */
	@NonNull
	private final List<Class<?>> ifaces;

	/**
	 * Constructor
	 *
	 * @param adaptee  proxied object
	 * @param ifaces   list of all Coprocessor friendly interfaces where policies must be applied
	 * @param policies policies to check
	 */
	public PolicyInvocationHandler(@NonNull T adaptee, @NonNull Class<?>[] ifaces,
								   @NonNull List<Policy> policies) {
		super(adaptee, policies);
		this.ifaces = new ArrayList<>(Arrays.asList(ifaces));
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		// Detect non interfaces call
		if (!ifaces.stream().anyMatch(c -> c.isAssignableFrom(method.getDeclaringClass()))) {
			return method.invoke(getAdaptee(), args);
		}

		if (method.getDeclaringClass().isAssignableFrom(Coprocessor.class)
				&& "stop".equals(method.getName())) {
			Object res = method.invoke(getAdaptee(), args);
			close();
			return res;
		}

		// Special case for protobuf services
		if ((method.getDeclaringClass().isAssignableFrom(CoprocessorService.class)
				|| method.getDeclaringClass().isAssignableFrom(SingletonCoprocessorService.class))
				&& method.getReturnType().isAssignableFrom(Service.class)) {
			Service service = (Service) method.invoke(getAdaptee(), args);
			LOGGER.debug("Create a ServicePolicyProxy on CoprocessorService [{}]", service);
			return new ServicePolicyProxy(service, getPolicies());
		}

		try {
			return runWithPolicies(method.getName(), () -> {
				try {
					return method.invoke(getAdaptee(), args);
				} catch (InvocationTargetException e) {
					if (e.getTargetException() instanceof IOException) {
						throw (IOException) e.getCause();
					}
					throw new HBaseIOException("An unexpected error occurred while calling method with policies, see root cause for details", e.getTargetException());
				} catch (IllegalAccessException e) {
					throw new HBaseIOException("An unexpected error occurred while calling method with policies, see root cause for details", e);
				}
			}, args == null ? new Object[]{} : args);
		} catch (IOException e) {
			if (method.getExceptionTypes().length > 0) {
				throw e;
			}
			LOGGER.info("An unexpected error occurred in Coprocessor method, see root cause for details", e);
		}
		return null;
	}
}
