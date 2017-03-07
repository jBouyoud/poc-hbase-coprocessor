package fr.poc.hbase.coprocessor.policy.proxy;

import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

import java.util.List;

/**
 * {@link RegionObserver} and {@link CoprocessorService} proxy that wrap all calls to be sure there is
 * "safe" according to the given policies
 * <br>
 * See {@link CoprocessorPolicyProxy} for more details.
 */
public class RegionObserverServicePolicyProxy<T extends CoprocessorService & RegionObserver>
		extends RegionObserverPolicyProxy<T> implements CoprocessorService {

	/**
	 * Constructor
	 *
	 * @param adaptee  egion observer adaptee
	 * @param policies default policies to apply
	 */
	public RegionObserverServicePolicyProxy(@NonNull T adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}

	@Override
	public Service getService() {
		return new ServicePolicyProxy(getAdaptee().getService(), getPolicies());
	}

}
