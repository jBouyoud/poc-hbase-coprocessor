package fr.poc.hbase.coprocessor.policy;

import com.google.protobuf.*;
import lombok.NonNull;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import java.io.IOException;
import java.util.List;

/**
 * {@link CoprocessorService} adapter that wrap all calls to be sure there is "safe".
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
public class CoprocessorServicePolicyAdapter<T extends CoprocessorService & Coprocessor>
		extends CoprocessorPolicyAdapter<T> implements CoprocessorService, Coprocessor {

	/**
	 * Constructor
	 *
	 * @param adaptee coprocessor and coprocessor service adaptee
	 */
	public CoprocessorServicePolicyAdapter(@NonNull T adaptee) {
		super(adaptee);
	}

	@Override
	public Service getService() {
		return new CoprocessorServicePolicyAdapter.ServiceAdapter(getAdaptee().getService(), getPolicies());
	}

	/**
	 * Safer Service adapter
	 */
	private static class ServiceAdapter extends PolicyVerifierAdapter<Service> implements Service {

		/**
		 * Constructor
		 *
		 * @param adaptee service adaptee
		 * @param policies policies to check
		 */
		public ServiceAdapter(Service adaptee, @NonNull List<PolicyHandler> policies) {
			super(adaptee);
			setPolicies(policies);
		}

		@Override
		public Descriptors.ServiceDescriptor getDescriptorForType() {
			// No needs to secure it because of code generation and it's final
			return getAdaptee().getDescriptorForType();
		}

		@Override
		public Message getRequestPrototype(Descriptors.MethodDescriptor method) {
			// No needs to secure it because of code generation and it's final
			return getAdaptee().getRequestPrototype(method);
		}

		@Override
		public Message getResponsePrototype(Descriptors.MethodDescriptor method) {
			// No needs to secure it because of code generation and it's final
			return getAdaptee().getResponsePrototype(method);
		}

		@Override
		public void callMethod(Descriptors.MethodDescriptor method, RpcController controller, Message request, RpcCallback<Message> done) {
			try {
				runWithPolicies(method.getFullName(),
						() -> getAdaptee().callMethod(method, controller, request, done),
						method, controller, request, done);
			} catch (IOException ioe) {
				ResponseConverter.setControllerException(controller, ioe);
				done.run(null);
			}
		}
	}
}
