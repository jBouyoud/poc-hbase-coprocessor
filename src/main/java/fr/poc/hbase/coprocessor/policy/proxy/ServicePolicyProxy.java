package fr.poc.hbase.coprocessor.policy.proxy;

import com.google.protobuf.*;
import fr.poc.hbase.coprocessor.policy.Policy;
import fr.poc.hbase.coprocessor.policy.PolicyVerifier;
import lombok.NonNull;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import java.io.IOException;
import java.util.List;

/**
 * Protobuf {@link Service} proxy that wrap all calls to be sure there is "safe" according to the given policies
 * <br>
 * See {@link PolicyVerifier} for more details.
 */
public class ServicePolicyProxy extends PolicyVerifier<Service> implements Service {

	/**
	 * Constructor
	 *
	 * @param adaptee  service adaptee
	 * @param policies policies to check
	 */
	public ServicePolicyProxy(@NonNull Service adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
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
	public void callMethod(Descriptors.MethodDescriptor method, RpcController controller,
						   Message request, RpcCallback<Message> done) {
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
