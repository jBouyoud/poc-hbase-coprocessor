package fr.poc.hbase.coprocessor.policy.proxy;

import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import java.util.List;

/**
 * {@link CoprocessorService} proxy that wrap all calls to be sure there is "safe" according to the given policies
 * <br>
 * See {@link CoprocessorPolicyProxy} for more details.
 */
public class CoprocessorServicePolicyProxy<T extends CoprocessorService & Coprocessor>
		extends CoprocessorPolicyProxy<T> implements CoprocessorService, Coprocessor {

	/**
	 * Constructor
	 *
	 * @param adaptee  coprocessor adaptee
	 * @param policies default policies to apply
	 */
	public CoprocessorServicePolicyProxy(@NonNull T adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}


	@Override
	public Service getService() {
		return new ServicePolicyProxy(getAdaptee().getService(), getPolicies());
	}


}
