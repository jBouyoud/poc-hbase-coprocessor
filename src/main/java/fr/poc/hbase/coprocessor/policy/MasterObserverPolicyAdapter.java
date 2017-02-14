package fr.poc.hbase.coprocessor.policy;

import lombok.NonNull;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;

import java.io.IOException;
import java.util.List;


/**
 * {@link MasterObserver} runWithPolicieser that wrap all calls to be sure there is "safe".
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
public class MasterObserverPolicyAdapter extends CoprocessorPolicyAdapter<MasterObserver> implements MasterObserver {

	/**
	 * Constructor
	 *
	 * @param runWithPoliciesee master observer runWithPoliciesee
	 */
	public MasterObserverPolicyAdapter(@NonNull MasterObserver runWithPoliciesee) {
		super(runWithPoliciesee);
	}

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		runWithPolicies("MasterObserver::preCreateTable",
				() -> getAdaptee().preCreateTable(argumentWithPolicies(ctx), desc, regions),
				ctx, desc, regions);
	}

	@Override
	public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		runWithPolicies("MasterObserver::postCreateTable",
				() -> getAdaptee().postCreateTable(argumentWithPolicies(ctx), desc, regions),
				ctx, desc, regions);
	}

	@Override
	public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		runWithPolicies("MasterObserver::preCreateTableHandler",
				() -> getAdaptee().preCreateTableHandler(argumentWithPolicies(ctx), desc, regions),
				ctx, desc, regions);
	}

	@Override
	public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		runWithPolicies("MasterObserver::postCreateTableHandler",
				() -> getAdaptee().postCreateTableHandler(argumentWithPolicies(ctx), desc, regions),
				ctx, desc, regions);
	}

	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preDeleteTable",
				() -> getAdaptee().preDeleteTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postDeleteTable",
				() -> getAdaptee().postDeleteTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preDeleteTableHandler",
				() -> getAdaptee().preDeleteTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
									   TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postDeleteTableHandler",
				() -> getAdaptee().postDeleteTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
								 TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preTruncateTable",
				() -> getAdaptee().preTruncateTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
								  TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postTruncateTable",
				() -> getAdaptee().postTruncateTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
										TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preTruncateTableHandler",
				() -> getAdaptee().preTruncateTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
										 TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postTruncateTableHandler",
				() -> getAdaptee().postTruncateTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
							   TableName tableName, HTableDescriptor htd) throws IOException {
		runWithPolicies("MasterObserver::preModifyTable",
				() -> getAdaptee().preModifyTable(argumentWithPolicies(ctx), tableName, htd),
				ctx, tableName, htd);
	}

	@Override
	public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
								TableName tableName, HTableDescriptor htd) throws IOException {
		runWithPolicies("MasterObserver::postModifyTable",
				() -> getAdaptee().postModifyTable(argumentWithPolicies(ctx), tableName, htd),
				ctx, tableName, htd);
	}

	@Override
	public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
									  TableName tableName, HTableDescriptor htd) throws IOException {
		runWithPolicies("MasterObserver::preModifyTableHandler",
				() -> getAdaptee().preModifyTableHandler(argumentWithPolicies(ctx), tableName, htd),
				ctx, tableName, htd);
	}

	@Override
	public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
									   TableName tableName, HTableDescriptor htd) throws IOException {
		runWithPolicies("MasterObserver::postModifyTableHandler",
				() -> getAdaptee().postModifyTableHandler(argumentWithPolicies(ctx), tableName, htd),
				ctx, tableName, htd);
	}

	@Override
	public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
							 TableName tableName, HColumnDescriptor column) throws IOException {
		runWithPolicies("MasterObserver::preAddColumn",
				() -> getAdaptee().preAddColumn(argumentWithPolicies(ctx), tableName, column),
				ctx, tableName, column);
	}

	@Override
	public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
							  TableName tableName, HColumnDescriptor column) throws IOException {
		runWithPolicies("MasterObserver::postAddColumn",
				() -> getAdaptee().postAddColumn(argumentWithPolicies(ctx), tableName, column),
				ctx, tableName, column);
	}

	@Override
	public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
									TableName tableName, HColumnDescriptor column) throws IOException {
		runWithPolicies("MasterObserver::preAddColumnHandler",
				() -> getAdaptee().preAddColumnHandler(argumentWithPolicies(ctx), tableName, column),
				ctx, tableName, column);
	}

	@Override
	public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
									 TableName tableName, HColumnDescriptor column) throws IOException {
		runWithPolicies("MasterObserver::postAddColumnHandler",
				() -> getAdaptee().postAddColumnHandler(argumentWithPolicies(ctx), tableName, column),
				ctx, tableName, column);
	}

	@Override
	public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
								TableName tableName, HColumnDescriptor descriptor) throws IOException {
		runWithPolicies("MasterObserver::preModifyColumn",
				() -> getAdaptee().preModifyColumn(argumentWithPolicies(ctx), tableName, descriptor),
				ctx, tableName, descriptor);
	}

	@Override
	public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
								 TableName tableName, HColumnDescriptor descriptor) throws IOException {
		runWithPolicies("MasterObserver::postModifyColumn",
				() -> getAdaptee().postModifyColumn(argumentWithPolicies(ctx), tableName, descriptor),
				ctx, tableName, descriptor);
	}

	@Override
	public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
									   TableName tableName, HColumnDescriptor descriptor) throws IOException {
		runWithPolicies("MasterObserver::preModifyColumnHandler",
				() -> getAdaptee().preModifyColumnHandler(argumentWithPolicies(ctx), tableName, descriptor),
				ctx, tableName, descriptor);
	}

	@Override
	public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
										TableName tableName, HColumnDescriptor descriptor) throws IOException {
		runWithPolicies("MasterObserver::postModifyColumnHandler",
				() -> getAdaptee().postModifyColumnHandler(argumentWithPolicies(ctx), tableName, descriptor),
				ctx, tableName, descriptor);
	}

	@Override
	public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] c) throws IOException {
		runWithPolicies("MasterObserver::preDeleteColumn",
				() -> getAdaptee().preDeleteColumn(argumentWithPolicies(ctx), tableName, c),
				ctx, tableName, c);
	}

	@Override
	public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] c) throws IOException {
		runWithPolicies("MasterObserver::postDeleteColumn",
				() -> getAdaptee().postDeleteColumn(argumentWithPolicies(ctx), tableName, c),
				ctx, tableName, c);
	}

	@Override
	public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] c) throws IOException {
		runWithPolicies("MasterObserver::preDeleteColumnHandler",
				() -> getAdaptee().preDeleteColumnHandler(argumentWithPolicies(ctx), tableName, c),
				ctx, tableName, c);
	}

	@Override
	public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] c) throws IOException {
		runWithPolicies("MasterObserver::postDeleteColumnHandler",
				() -> getAdaptee().postDeleteColumnHandler(argumentWithPolicies(ctx), tableName, c),
				ctx, tableName, c);
	}

	@Override
	public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preEnableTable",
				() -> getAdaptee().preEnableTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postEnableTable",
				() -> getAdaptee().postEnableTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preEnableTableHandler",
				() -> getAdaptee().preEnableTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postEnableTableHandler",
				() -> getAdaptee().postEnableTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preDisableTable",
				() -> getAdaptee().preDisableTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postDisableTable",
				() -> getAdaptee().postDisableTable(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preDisableTableHandler",
				() -> getAdaptee().preDisableTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
										TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postDisableTableHandler",
				() -> getAdaptee().postDisableTableHandler(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
						ServerName srcServer, ServerName destServer) throws IOException {
		runWithPolicies("MasterObserver::preMove",
				() -> getAdaptee().preMove(argumentWithPolicies(ctx), region, srcServer, destServer),
				ctx, region, srcServer, destServer);
	}

	@Override
	public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
						 ServerName srcServer, ServerName destServer) throws IOException {
		runWithPolicies("MasterObserver::postMove",
				() -> getAdaptee().postMove(argumentWithPolicies(ctx), region, srcServer, destServer),
				ctx, region, srcServer, destServer);
	}

	@Override
	public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx,
								  ProcedureExecutor<MasterProcedureEnv> procEnv, long procId) throws IOException {
		runWithPolicies("MasterObserver::preAbortProcedure",
				() -> getAdaptee().preAbortProcedure(argumentWithPolicies(ctx), procEnv, procId),
				ctx, procEnv, procId);
	}

	@Override
	public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::postAbortProcedure",
				() -> getAdaptee().postAbortProcedure(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::preListProcedures",
				() -> getAdaptee().preListProcedures(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void postListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx, List<ProcedureInfo> procInfoList) throws IOException {
		runWithPolicies("MasterObserver::postListProcedures",
				() -> getAdaptee().postListProcedures(argumentWithPolicies(ctx), procInfoList),
				ctx, procInfoList);
	}

	@Override
	public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
		runWithPolicies("MasterObserver::preAssign",
				() -> getAdaptee().preAssign(argumentWithPolicies(ctx), regionInfo),
				ctx, regionInfo);
	}

	@Override
	public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
		runWithPolicies("MasterObserver::postAssign",
				() -> getAdaptee().postAssign(argumentWithPolicies(ctx), regionInfo),
				ctx, regionInfo);
	}

	@Override
	public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo, boolean force) throws IOException {
		runWithPolicies("MasterObserver::preUnassign",
				() -> getAdaptee().preUnassign(argumentWithPolicies(ctx), regionInfo, force),
				ctx, regionInfo, force);
	}

	@Override
	public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo, boolean force) throws IOException {
		runWithPolicies("MasterObserver::postUnassign",
				() -> getAdaptee().postUnassign(argumentWithPolicies(ctx), regionInfo, force),
				ctx, regionInfo, force);
	}

	@Override
	public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
		runWithPolicies("MasterObserver::preRegionOffline",
				() -> getAdaptee().preRegionOffline(argumentWithPolicies(ctx), regionInfo),
				ctx, regionInfo);
	}

	@Override
	public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
		runWithPolicies("MasterObserver::postRegionOffline",
				() -> getAdaptee().postRegionOffline(argumentWithPolicies(ctx), regionInfo),
				ctx, regionInfo);
	}

	@Override
	public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::preBalance",
				() -> getAdaptee().preBalance(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans) throws IOException {
		runWithPolicies("MasterObserver::postBalance",
				() -> getAdaptee().postBalance(argumentWithPolicies(ctx), plans),
				ctx, plans);
	}

	@Override
	public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean newValue) throws IOException {
		return runWithPolicies("MasterObserver::preBalanceSwitch",
				() -> getAdaptee().preBalanceSwitch(argumentWithPolicies(ctx), newValue),
				ctx, newValue);
	}

	@Override
	public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean oldValue, boolean newValue) throws IOException {
		runWithPolicies("MasterObserver::postBalanceSwitch",
				() -> getAdaptee().postBalanceSwitch(argumentWithPolicies(ctx), oldValue, newValue),
				ctx, oldValue, newValue);
	}

	@Override
	public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::preShutdown",
				() -> getAdaptee().preShutdown(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::preStopMaster",
				() -> getAdaptee().preStopMaster(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::postStartMaster",
				() -> getAdaptee().postStartMaster(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("MasterObserver::preMasterInitialization",
				() -> getAdaptee().preMasterInitialization(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
							HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		runWithPolicies("MasterObserver::preSnapshot",
				() -> getAdaptee().preSnapshot(argumentWithPolicies(ctx), snapshot, hTableDescriptor),
				ctx, snapshot, hTableDescriptor);
	}

	@Override
	public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
							 HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		runWithPolicies("MasterObserver::postSnapshot",
				() -> getAdaptee().postSnapshot(argumentWithPolicies(ctx), snapshot, hTableDescriptor),
				ctx, snapshot, hTableDescriptor);
	}

	@Override
	public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								HBaseProtos.SnapshotDescription snapshot) throws IOException {
		runWithPolicies("MasterObserver::preListSnapshot",
				() -> getAdaptee().preListSnapshot(argumentWithPolicies(ctx), snapshot),
				ctx, snapshot);
	}

	@Override
	public void postListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								 HBaseProtos.SnapshotDescription snapshot) throws IOException {
		runWithPolicies("MasterObserver::postListSnapshot",
				() -> getAdaptee().postListSnapshot(argumentWithPolicies(ctx), snapshot),
				ctx, snapshot);
	}

	@Override
	public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								 HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		runWithPolicies("MasterObserver::preCloneSnapshot",
				() -> getAdaptee().preCloneSnapshot(argumentWithPolicies(ctx), snapshot, hTableDescriptor),
				ctx, snapshot, hTableDescriptor);
	}

	@Override
	public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								  HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		runWithPolicies("MasterObserver::postCloneSnapshot",
				() -> getAdaptee().postCloneSnapshot(argumentWithPolicies(ctx), snapshot, hTableDescriptor),
				ctx, snapshot, hTableDescriptor);
	}

	@Override
	public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								   HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		runWithPolicies("MasterObserver::preRestoreSnapshot",
				() -> getAdaptee().preRestoreSnapshot(argumentWithPolicies(ctx), snapshot, hTableDescriptor),
				ctx, snapshot, hTableDescriptor);
	}

	@Override
	public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
									HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		runWithPolicies("MasterObserver::postRestoreSnapshot",
				() -> getAdaptee().postRestoreSnapshot(argumentWithPolicies(ctx), snapshot, hTableDescriptor),
				ctx, snapshot, hTableDescriptor);
	}

	@Override
	public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								  HBaseProtos.SnapshotDescription snapshot) throws IOException {
		runWithPolicies("MasterObserver::preDeleteSnapshot",
				() -> getAdaptee().preDeleteSnapshot(argumentWithPolicies(ctx), snapshot),
				ctx, snapshot);
	}

	@Override
	public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
								   HBaseProtos.SnapshotDescription snapshot) throws IOException {
		runWithPolicies("MasterObserver::postDeleteSnapshot",
				() -> getAdaptee().postDeleteSnapshot(argumentWithPolicies(ctx), snapshot),
				ctx, snapshot);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
									   List<TableName> tableNamesList, List<HTableDescriptor> descriptors) throws IOException {
		runWithPolicies("MasterObserver::preGetTableDescriptors(deprecated)",
				() -> getAdaptee().preGetTableDescriptors(argumentWithPolicies(ctx), tableNamesList, descriptors),
				ctx, tableNamesList, descriptors);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
										List<HTableDescriptor> descriptors) throws IOException {
		runWithPolicies("MasterObserver::postGetTableDescriptors(deprecated)",
				() -> getAdaptee().postGetTableDescriptors(argumentWithPolicies(ctx), descriptors),
				ctx, descriptors);
	}

	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
									   List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex) throws IOException {
		runWithPolicies("MasterObserver::preGetTableDescriptors",
				() -> getAdaptee().preGetTableDescriptors(argumentWithPolicies(ctx), tableNamesList, descriptors, regex),
				ctx, tableNamesList, descriptors, regex);
	}

	@Override
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
										List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex) throws IOException {
		runWithPolicies("MasterObserver::postGetTableDescriptors",
				() -> getAdaptee().postGetTableDescriptors(argumentWithPolicies(ctx), tableNamesList, descriptors, regex),
				ctx, tableNamesList, descriptors, regex);
	}

	@Override
	public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
								 List<HTableDescriptor> descriptors, String regex) throws IOException {
		runWithPolicies("MasterObserver::preGetTableNames",
				() -> getAdaptee().preGetTableNames(argumentWithPolicies(ctx), descriptors, regex),
				ctx, descriptors, regex);
	}

	@Override
	public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
								  List<HTableDescriptor> descriptors, String regex) throws IOException {
		runWithPolicies("MasterObserver::postGetTableNames",
				() -> getAdaptee().postGetTableNames(argumentWithPolicies(ctx), descriptors, regex),
				ctx, descriptors, regex);
	}

	@Override
	public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
								   NamespaceDescriptor ns) throws IOException {
		runWithPolicies("MasterObserver::preCreateNamespace",
				() -> getAdaptee().preCreateNamespace(argumentWithPolicies(ctx), ns),
				ctx, ns);
	}

	@Override
	public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
									NamespaceDescriptor ns) throws IOException {
		runWithPolicies("MasterObserver::postCreateNamespace",
				() -> getAdaptee().postCreateNamespace(argumentWithPolicies(ctx), ns),
				ctx, ns);
	}

	@Override
	public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
								   String namespace) throws IOException {
		runWithPolicies("MasterObserver::preDeleteNamespace",
				() -> getAdaptee().preDeleteNamespace(argumentWithPolicies(ctx), namespace),
				ctx, namespace);
	}

	@Override
	public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
									String namespace) throws IOException {
		runWithPolicies("MasterObserver::postDeleteNamespace",
				() -> getAdaptee().postDeleteNamespace(argumentWithPolicies(ctx), namespace),
				ctx, namespace);
	}

	@Override
	public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
								   NamespaceDescriptor ns) throws IOException {
		runWithPolicies("MasterObserver::preModifyNamespace",
				() -> getAdaptee().preModifyNamespace(argumentWithPolicies(ctx), ns),
				ctx, ns);
	}

	@Override
	public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
									NamespaceDescriptor ns) throws IOException {
		runWithPolicies("MasterObserver::postModifyNamespace",
				() -> getAdaptee().postModifyNamespace(argumentWithPolicies(ctx), ns),
				ctx, ns);
	}

	@Override
	public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
										  String namespace) throws IOException {
		runWithPolicies("MasterObserver::preGetNamespaceDescriptor",
				() -> getAdaptee().preGetNamespaceDescriptor(argumentWithPolicies(ctx), namespace),
				ctx, namespace);
	}

	@Override
	public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
										   NamespaceDescriptor ns) throws IOException {
		runWithPolicies("MasterObserver::postGetNamespaceDescriptor",
				() -> getAdaptee().postGetNamespaceDescriptor(argumentWithPolicies(ctx), ns),
				ctx, ns);
	}

	@Override
	public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
											List<NamespaceDescriptor> descriptors) throws IOException {
		runWithPolicies("MasterObserver::preListNamespaceDescriptors",
				() -> getAdaptee().preListNamespaceDescriptors(argumentWithPolicies(ctx), descriptors),
				ctx, descriptors);
	}

	@Override
	public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
											 List<NamespaceDescriptor> descriptors) throws IOException {
		runWithPolicies("MasterObserver::postListNamespaceDescriptors",
				() -> getAdaptee().postListNamespaceDescriptors(argumentWithPolicies(ctx), descriptors),
				ctx, descriptors);
	}

	@Override
	public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::preTableFlush",
				() -> getAdaptee().preTableFlush(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		runWithPolicies("MasterObserver::postTableFlush",
				() -> getAdaptee().postTableFlush(argumentWithPolicies(ctx), tableName),
				ctx, tableName);
	}

	@Override
	public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
								QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::preSetUserQuota",
				() -> getAdaptee().preSetUserQuota(argumentWithPolicies(ctx), userName, quotas),
				ctx, userName, quotas);
	}

	@Override
	public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
								 QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::postSetUserQuota",
				() -> getAdaptee().postSetUserQuota(argumentWithPolicies(ctx), userName, quotas),
				ctx, userName, quotas);
	}

	@Override
	public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, TableName tableName,
								QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::preSetUserQuota(Table)",
				() -> getAdaptee().preSetUserQuota(argumentWithPolicies(ctx), userName, tableName, quotas),
				ctx, userName, tableName, quotas);
	}

	@Override
	public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, TableName tableName,
								 QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::postSetUserQuota(Table)",
				() -> getAdaptee().postSetUserQuota(argumentWithPolicies(ctx), userName, tableName, quotas),
				ctx, userName, tableName, quotas);
	}

	@Override
	public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, String namespace,
								QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::preSetUserQuota(namespace)",
				() -> getAdaptee().preSetUserQuota(argumentWithPolicies(ctx), userName, namespace, quotas),
				ctx, userName, namespace, quotas);
	}

	@Override
	public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, String namespace,
								 QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::postSetUserQuota(namespace)",
				() -> getAdaptee().postSetUserQuota(argumentWithPolicies(ctx), userName, namespace, quotas),
				ctx, userName, namespace, quotas);
	}

	@Override
	public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
								 QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::preSetTableQuota",
				() -> getAdaptee().preSetTableQuota(argumentWithPolicies(ctx), tableName, quotas),
				ctx, tableName, quotas);
	}

	@Override
	public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
								  QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::postSetTableQuota",
				() -> getAdaptee().postSetTableQuota(argumentWithPolicies(ctx), tableName, quotas),
				ctx, tableName, quotas);
	}

	@Override
	public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace, QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::preSetNamespaceQuota",
				() -> getAdaptee().preSetNamespaceQuota(argumentWithPolicies(ctx), namespace, quotas),
				ctx, namespace, quotas);
	}

	@Override
	public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace, QuotaProtos.Quotas quotas) throws IOException {
		runWithPolicies("MasterObserver::postSetNamespaceQuota",
				() -> getAdaptee().postSetNamespaceQuota(argumentWithPolicies(ctx), namespace, quotas),
				ctx, namespace, quotas);
	}
}
