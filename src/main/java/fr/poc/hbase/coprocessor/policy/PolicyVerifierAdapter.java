package fr.poc.hbase.coprocessor.policy;

import fr.poc.hbase.coprocessor.policy.util.CallableWithIOException;
import fr.poc.hbase.coprocessor.policy.util.RunnableWithIOException;
import fr.poc.hbase.coprocessor.policy.util.WrappedIOException;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hdfs.util.Holder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * This class is able to checks some policies
 * <ul>
 * <li>on adapted methods via
 * {@link #runWithPolicies(String, CallableWithIOException, Object...)} or
 * {@link #runWithPolicies(String, RunnableWithIOException, Object...)}</li>
 * <li>on paramters via {@link #argumentWithPolicies(Object)}</li>
 * </ul>
 * <br/>
 * This class is generic and can be applied on any kind of objects that wrap methods witch can throws {@link IOException}
 *
 * @param <A> Adaptee object type
 */
@Slf4j
@RequiredArgsConstructor
public class PolicyVerifierAdapter<A> {

	/**
	 * Adaptee object
	 */
	@Getter
	@NonNull
	private final A adaptee;

	/**
	 * Executor that able to execute method in a separate thread
	 */
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	/**
	 * policies to check
	 */
	private List<PolicyHandler> policies = new ArrayList<>();

	/**
	 * Call a wrapped method with current policies
	 *
	 * @param method   name of the method that will be executed
	 * @param args     all row arguments passed from original method call (before any transformation via {@link #argumentWithPolicies(Object)}
	 * @param callable the method to execute
	 * @param <R>      Method return type
	 * @return the method return type
	 * @throws IOException Returns only IOException
	 */
	private <R> R runMethodWithPolicies(@NonNull String method,
										@NonNull Object[] args,
										@NonNull Callable<R> callable) throws IOException {
		final long start = System.nanoTime();
		Future<R> future = null;
		Holder<R> resultHolder = new Holder<>(null);

		try {
			//Execute before handlers
			for (PolicyHandler handler : policies) {
				handler.beforeRun(adaptee, method, args);
			}

			// Start the execution
			future = executor.submit(callable);

			// Execute 'execution' handlers
			final Future<R> theFuture = future;
			policies.forEach(a -> a.running(adaptee, method, args, theFuture));

			// Fetch result
			resultHolder.held = future.get();
		} catch (IOException ioe) {
			throw ioe;
		} catch (CancellationException cancel){
			throw new DoNotRetryIOException("coprocessor method has spend to much time to execute, see root cause for details", cancel);
		} catch (ExecutionException executionEx) {
			future.cancel(true);
			LOGGER.trace("An error occurred while trying to execute " + method + " on " + adaptee, executionEx);

			Throwable th = executionEx.getCause();
			if (th instanceof WrappedIOException) {
				IOException ioEx = ((WrappedIOException) th).getCause();
				policies.forEach(a -> a.onError(adaptee, method, args, ioEx));
				throw ioEx;
			}
			if (th instanceof IOException) {
				IOException ioEx = (IOException) th;
				policies.forEach(a -> a.onError(adaptee, method, args, ioEx));
				throw ioEx;
			}

			policies.forEach(a -> a.onUnexpectedError(adaptee, method, args, th));
			throw new HBaseIOException("An unexpected error occurred in Coprocessor method, see root cause for details", th);
		} catch (Throwable th) {
			if (future != null) {
				future.cancel(true);
			}
			policies.forEach(a -> a.onUnexpectedError(adaptee, method, args, th));
			throw new HBaseIOException("An unexpected error occurred in Coprocessor method, see root cause for details", th);
		} finally {
			long executionTime = System.nanoTime() - start;
			policies.forEach(a -> a.afterRun(adaptee, method, args, resultHolder.held, executionTime));
		}
		return resultHolder.held;
	}

	/**
	 * Applies polices on argument
	 *
	 * @param arg argument
	 * @param <T> argument type
	 * @return mutated argument
	 */
	protected final <T> T argumentWithPolicies(T arg) {
		T argWithPolicies = arg;
		for (PolicyHandler handler : policies) {
			argWithPolicies = handler.onArgument(argWithPolicies);
		}
		return argWithPolicies;
	}

	/**
	 * Run a method runnable (without result) with the current policies
	 *
	 * @param method   name of the method that will be executed
	 * @param runnable runnable like to execute
	 * @param args     all row arguments passed from original method call (before any transformation via {@link #argumentWithPolicies(Object)}
	 * @throws IOException throws for any proxied method issue
	 */
	protected final void runWithPolicies(@NonNull String method, @NonNull RunnableWithIOException runnable, Object... args) throws IOException {
		runMethodWithPolicies(method, args, () -> {
			try {
				runnable.run();
			} catch (IOException ioEx) {
				throw new WrappedIOException(ioEx);
			}
			return null;
		});
	}

	/**
	 * Run a method callable (with result) with the current policies
	 *
	 * @param method   name of the method that will be executed
	 * @param callable collable like to execute
	 * @param args     all row arguments passed from original method call (before any transformation via {@link #argumentWithPolicies(Object)}
	 * @param <R>      Method return type
	 * @return the method return type
	 * @throws IOException throws for any proxied method issue
	 */
	protected final <R> R runWithPolicies(@NonNull String method, @NonNull CallableWithIOException<R> callable, Object... args) throws IOException {
		return runMethodWithPolicies(method, args, () -> {
			try {
				return callable.call();
			} catch (IOException ioEx) {
				throw new WrappedIOException(ioEx);
			}
		});
	}

	/**
	 * Close policies
	 */
	protected void closePolicies() throws IOException {
		Holder<Throwable> errorHolder = new Holder<>(null);
		policies.forEach(policyHandler -> {
			try {
				policyHandler.close();
			} catch (Throwable th) {
				LOGGER.error("An unexpected error occurred while closing coprocessor policy", th);
				errorHolder.held = th;
			}
		});
		if (errorHolder.held != null) {
			if (errorHolder.held instanceof IOException) {
				throw (IOException) errorHolder.held;
			}
			throw new HBaseIOException("An unexpected error occurred while closing coprocessor policies, see root cause for details", errorHolder.held);
		}
	}


	/**
	 * Return the current policies
	 *
	 * @return the current policies
	 */
	public List<PolicyHandler> getPolicies() {
		return Collections.unmodifiableList(policies);
	}

	/**
	 * Set new policies
	 *
	 * @param policies new policies
	 * @return self
	 */
	public PolicyVerifierAdapter setPolicies(List<PolicyHandler> policies) {
		this.policies = new ArrayList<>(policies);
		return this;
	}

	/**
	 * Add a new policy to the policy set (in latest position)
	 *
	 * @param policy the new policy
	 * @return self
	 */
	public PolicyVerifierAdapter addPolicy(PolicyHandler policy) {
		this.policies.add(policy);
		return this;
	}

}
