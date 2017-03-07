package fr.poc.hbase.coprocessor.policy.proxy;

import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.NonNull;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

import java.io.IOException;
import java.util.List;

/**
 * {@link WALObserver} proxy that wrap all calls to be sure there is "safe" according to the given policies
 * <br>
 * See {@link CoprocessorPolicyProxy} for more details.
 */
public class WALObserverPolicyProxy extends CoprocessorPolicyProxy<WALObserver> implements WALObserver {

	/**
	 * Constructor
	 *
	 * @param adaptee  coprocessor adaptee
	 * @param policies default policies to apply
	 */
	public WALObserverPolicyProxy(@NonNull WALObserver adaptee, @NonNull List<Policy> policies) {
		super(adaptee, policies);
	}

	@Override
	public boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
							   HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
		return runWithPolicies("WALObserver:preWALWrite",
				() -> getAdaptee().preWALWrite(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo info,
							   HLogKey logKey, WALEdit logEdit) throws IOException {
		return runWithPolicies("WALObserver:preWALWrite(deprecated)",
				() -> getAdaptee().preWALWrite(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
							 HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
		runWithPolicies("WALObserver:postWALWrite",
				() -> getAdaptee().postWALWrite(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
							 HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
		runWithPolicies("WALObserver:postWALWrite(deprecated)",
				() -> getAdaptee().postWALWrite(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}
}
