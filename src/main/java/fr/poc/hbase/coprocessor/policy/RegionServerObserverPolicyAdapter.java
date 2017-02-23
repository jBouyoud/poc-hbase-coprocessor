package fr.poc.hbase.coprocessor.policy;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;

import java.io.IOException;
import java.util.List;

/**
 * {@link RegionServerObserver} runWithPolicieser that wrap all calls to be sure there is "safe".
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
@Slf4j
public class RegionServerObserverPolicyAdapter extends CoprocessorPolicyAdapter<RegionServerObserver> implements RegionServerObserver {

	/**
	 * Constructor
	 *
	 * @param runWithPoliciesee region server observer runWithPoliciesee
	 */
	public RegionServerObserverPolicyAdapter(@NonNull RegionServerObserver runWithPoliciesee) {
		super(runWithPoliciesee);
	}


	@Override
	public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
		runWithPolicies("RegionServerObserver:preStopRegionServer",
				() -> getAdaptee().preStopRegionServer(argumentWithPolicies(env)), env);
	}

	@Override
	public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
						 Region regionA, Region regionB) throws IOException {
		runWithPolicies("RegionServerObserver:preMerge",
				() -> getAdaptee().preMerge(argumentWithPolicies(ctx), regionA, regionB),
				ctx, regionA, regionB);
	}

	@Override
	public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c,
						  Region regionA, Region regionB, Region mergedRegion) throws IOException {
		runWithPolicies("RegionServerObserver:postMerge",
				() -> getAdaptee().postMerge(argumentWithPolicies(c), regionA, regionB, mergedRegion),
				c, regionA, regionB, mergedRegion);
	}

	@Override
	public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
							   Region regionA, Region regionB,
							   @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException {
		runWithPolicies("RegionServerObserver:preMergeCommit",
				() -> getAdaptee().preMergeCommit(argumentWithPolicies(ctx), regionA, regionB, metaEntries),
				ctx, regionA, regionB, metaEntries);
	}

	@Override
	public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
								Region regionA, Region regionB, Region mergedRegion) throws IOException {
		runWithPolicies("RegionServerObserver:postMergeCommit",
				() -> getAdaptee().postMergeCommit(argumentWithPolicies(ctx), regionA, regionB, mergedRegion),
				ctx, regionA, regionB, mergedRegion);
	}

	@Override
	public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
								 Region regionA, Region regionB) throws IOException {
		runWithPolicies("RegionServerObserver:preRollBackMerge",
				() -> getAdaptee().preRollBackMerge(argumentWithPolicies(ctx), regionA, regionB),
				ctx, regionA, regionB);
	}

	@Override
	public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
								  Region regionA, Region regionB) throws IOException {
		runWithPolicies("RegionServerObserver:postRollBackMerge",
				() -> getAdaptee().postRollBackMerge(argumentWithPolicies(ctx), regionA, regionB),
				ctx, regionA, regionB);
	}

	@Override
	public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("RegionServerObserver:preRollWALWriterRequest",
				() -> getAdaptee().preRollWALWriterRequest(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("RegionServerObserver:postRollWALWriterRequest",
				() -> getAdaptee().postRollWALWriterRequest(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
															 ReplicationEndpoint endpoint) {
		try {
			return runWithPolicies("RegionServerObserver:postCreateReplicationEndPoint",
					() -> getAdaptee().postCreateReplicationEndPoint(argumentWithPolicies(ctx), endpoint),
					ctx, endpoint);
		} catch (IOException ioEx) {
			LOGGER.info("An unexpected error occurred in Coprocessor method, see root cause for details", ioEx);
			return null;
		}
	}

	@Override
	public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
		runWithPolicies("RegionServerObserver:preReplicateLogEntries",
				() -> getAdaptee().preReplicateLogEntries(argumentWithPolicies(ctx), entries, cells),
				ctx, entries, cells);
	}

	@Override
	public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
		runWithPolicies("RegionServerObserver:postReplicateLogEntries",
				() -> getAdaptee().postReplicateLogEntries(argumentWithPolicies(ctx), entries, cells),
				ctx, entries, cells);
	}
}
