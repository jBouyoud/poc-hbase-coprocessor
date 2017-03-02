package fr.poc.hbase.coprocessor.policy.adapter;

import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import java.util.List;

/**
 * {@link CoprocessorService} adapter that wrap all calls to be sure there is "safe" according to the given policies
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
public class CoprocessorServicePolicyAdapter<T extends CoprocessorService & Coprocessor>
		extends CoprocessorPolicyAdapter<T> implements CoprocessorService, Coprocessor {

	/**
	 * Constructor
	 *
	 * @param adaptee  coprocessor adaptee
	 * @param policies default policies to apply
	 */
	public CoprocessorServicePolicyAdapter(@NonNull T adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}


	@Override
	public Service getService() {
		return new ServicePolicyAdapter(getAdaptee().getService(), getPolicies());
	}


}
