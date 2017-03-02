package fr.poc.hbase.coprocessor.policy.adapter;

import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

import java.util.List;

/**
 * {@link RegionObserver} and {@link CoprocessorService} adapter that wrap all calls to be sure there is
 * "safe" according to the given policies
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
public class RegionObserverServicePolicyAdapter<T extends CoprocessorService & RegionObserver>
		extends RegionObserverPolicyAdapter<T> implements CoprocessorService {

	/**
	 * Constructor
	 *
	 * @param adaptee  egion observer adaptee
	 * @param policies default policies to apply
	 */
	public RegionObserverServicePolicyAdapter(@NonNull T adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}

	@Override
	public Service getService() {
		return new ServicePolicyAdapter(getAdaptee().getService(), getPolicies());
	}

}
