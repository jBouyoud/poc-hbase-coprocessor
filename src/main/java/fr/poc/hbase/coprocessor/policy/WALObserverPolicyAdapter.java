package fr.poc.hbase.coprocessor.policy;

import lombok.NonNull;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

import java.io.IOException;

/**
 * {@link WALObserver} adapter that wrap all calls to be sure there is "safe".
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
public class WALObserverPolicyAdapter extends CoprocessorPolicyAdapter<WALObserver> implements WALObserver {

	/**
	 * Constructor
	 *
	 * @param adaptee WAL observer adaptee
	 */
	public WALObserverPolicyAdapter(@NonNull WALObserver adaptee) {
		super(adaptee);
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
