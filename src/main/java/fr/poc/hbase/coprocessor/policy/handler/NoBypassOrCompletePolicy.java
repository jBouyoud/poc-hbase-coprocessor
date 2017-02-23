package fr.poc.hbase.coprocessor.policy.handler;

import fr.poc.hbase.coprocessor.policy.PolicyHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

/**
 * A coprocessor must not call ObserverContext#bypass or ObserverContext#complete.<br>
 * That could be break the coprocessor chain (including security checks)
 */
public class NoBypassOrCompletePolicy implements PolicyHandler {

	@Override
	@SuppressWarnings("unchecked")
	public <T> T onArgument(T arg) {
		if (arg instanceof ObserverContext<?>) {
			return (T) new NoBypassOrCompleteObserverContextAdapter<>((ObserverContext) arg);
		}
		return arg;
	}

	/**
	 * {@link ObserverContext} adapter that throws an Exception when calling bypass or complete methods
	 *
	 * @param <E> Type of coprocessor environement
	 */
	@RequiredArgsConstructor
	private static final class NoBypassOrCompleteObserverContextAdapter<E extends CoprocessorEnvironment> extends ObserverContext<E> {

		@NonNull
		private final ObserverContext<E> adaptee;

		@Override
		public E getEnvironment() {
			return adaptee.getEnvironment();
		}

		@Override
		public void prepare(E env) {
			adaptee.prepare(env);
		}

		@Override
		public void bypass() {
			throw new IllegalStateException("bypass() on coprocessor operation is disallowed");
		}

		@Override
		public void complete() {
			throw new IllegalStateException("complete() on coprocessor operation is disallowed");
		}

	}
}
