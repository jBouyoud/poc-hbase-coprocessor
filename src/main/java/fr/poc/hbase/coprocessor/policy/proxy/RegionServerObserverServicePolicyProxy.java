package fr.poc.hbase.coprocessor.policy.proxy;

import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;

import java.util.List;

/**
 * {@link RegionServerObserver} and {@link SingletonCoprocessorService} proxy that wrap all calls to be sure there is "safe" according to the given policies
 * <br>
 * See {@link CoprocessorPolicyProxy} for more details.
 */
public class RegionServerObserverServicePolicyProxy<T extends SingletonCoprocessorService & RegionServerObserver>
		extends RegionServerObserverPolicyProxy<T> implements SingletonCoprocessorService {

	/**
	 * Constructor
	 *
	 * @param adaptee  egion observer adaptee
	 * @param policies default policies to apply
	 */
	public RegionServerObserverServicePolicyProxy(@NonNull T adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}

	@Override
	public Service getService() {
		return new ServicePolicyProxy(getAdaptee().getService(), getPolicies());
	}

}
