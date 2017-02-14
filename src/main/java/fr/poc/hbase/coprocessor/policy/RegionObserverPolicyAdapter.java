package fr.poc.hbase.coprocessor.policy;

import com.google.common.collect.ImmutableList;
import fr.poc.hbase.coprocessor.policy.util.RunnableWithIOException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

/**
 * {@link RegionObserver} runWithPolicieser that wrap all calls to be sure there is "safe".
 * <br>
 * See {@link CoprocessorPolicyAdapter} for more details.
 */
@Slf4j
public class RegionObserverPolicyAdapter extends CoprocessorPolicyAdapter<RegionObserver> implements RegionObserver {

	/**
	 * Constructor
	 *
	 * @param runWithPoliciesee region observer runWithPoliciesee
	 */
	public RegionObserverPolicyAdapter(@NonNull RegionObserver runWithPoliciesee) {
		super(runWithPoliciesee);
	}

	private void catchIOException(@NonNull RunnableWithIOException runnable) {
		try {
			runnable.run();
		} catch (IOException ioEx) {
			LOGGER.info("An unexpected error occurred in Coprocessor method, see root cause for details", ioEx);
		}
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
		runWithPolicies("RegionObserver::preOpen",
				() -> getAdaptee().preOpen(argumentWithPolicies(c)), c);
	}

	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
		catchIOException(
				() -> runWithPolicies("RegionObserver::postOpen",
						() -> getAdaptee().postOpen(argumentWithPolicies(c)), c));
	}

	@Override
	public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> c) {
		catchIOException(
				() -> runWithPolicies("RegionObserver::postLogReplay",
						() -> getAdaptee().postLogReplay(argumentWithPolicies(c)), c));
	}

	@Override
	public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
											   Store store, KeyValueScanner memstoreScanner,
											   InternalScanner s) throws IOException {
		return runWithPolicies("RegionObserver::preFlushScannerOpen",
				() -> getAdaptee().preFlushScannerOpen(argumentWithPolicies(c), store, memstoreScanner, s),
				c, store, memstoreScanner, s);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
		runWithPolicies("RegionObserver::preFlush(deprecated)",
				() -> getAdaptee().preFlush(argumentWithPolicies(c)), c);
	}

	@Override
	public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c,
									Store store, InternalScanner scanner) throws IOException {
		return runWithPolicies("RegionObserver::preFlush",
				() -> getAdaptee().preFlush(argumentWithPolicies(c), store, scanner),
				c, store, scanner);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
		runWithPolicies("RegionObserver::postFlush(deprecated)",
				() -> getAdaptee().postFlush(argumentWithPolicies(c)), c);
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile) throws IOException {
		runWithPolicies("RegionObserver::postFlush",
				() -> getAdaptee().postFlush(argumentWithPolicies(c), store, resultFile),
				c, store, resultFile);
	}

	@Override
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c,
									Store store, List<StoreFile> candidates, CompactionRequest request) throws IOException {
		runWithPolicies("RegionObserver::preCompactSelection",
				() -> getAdaptee().preCompactSelection(argumentWithPolicies(c), store, candidates, request),
				c, store, candidates, request);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c,
									Store store, List<StoreFile> candidates) throws IOException {
		runWithPolicies("RegionObserver::preCompactSelection(deprecated)",
				() -> getAdaptee().preCompactSelection(argumentWithPolicies(c), store, candidates),
				c, store, candidates);
	}

	@Override
	public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c,
									 Store store, ImmutableList<StoreFile> selected, CompactionRequest request) {
		catchIOException(
				() -> runWithPolicies("RegionObserver::postCompactSelection",
						() -> getAdaptee().postCompactSelection(argumentWithPolicies(c), store, selected, request),
						c, store, selected, request));
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c,
									 Store store, ImmutableList<StoreFile> selected) {
		catchIOException(
				() -> runWithPolicies("RegionObserver::postCompactSelection(deprecated)",
						() -> getAdaptee().postCompactSelection(argumentWithPolicies(c), store, selected),
						c, store, selected));
	}

	@Override
	public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c,
									  Store store, InternalScanner scanner, ScanType scanType,
									  CompactionRequest request) throws IOException {
		return runWithPolicies("RegionObserver::preCompact",
				() -> getAdaptee().preCompact(argumentWithPolicies(c), store, scanner, scanType, request),
				c, store, scanner, scanType, request);
	}

	@Override
	@SuppressWarnings("deprecation")
	public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c,
									  Store store, InternalScanner scanner, ScanType scanType) throws IOException {
		return runWithPolicies("RegionObserver::preCompact(deprecated)",
				() -> getAdaptee().preCompact(argumentWithPolicies(c), store, scanner, scanType),
				c, store, scanner, scanType);
	}

	@Override
	public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
												 Store store, List<? extends KeyValueScanner> scanners,
												 ScanType scanType, long earliestPutTs, InternalScanner s,
												 CompactionRequest request) throws IOException {
		return runWithPolicies("RegionObserver::preCompactScannerOpen",
				() -> getAdaptee().preCompactScannerOpen(argumentWithPolicies(c), store, scanners, scanType, earliestPutTs, s, request),
				c, store, scanners, scanType, earliestPutTs, s, request);
	}

	@Override
	@SuppressWarnings("deprecation")
	public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
												 Store store, List<? extends KeyValueScanner> scanners,
												 ScanType scanType, long earliestPutTs, InternalScanner s) throws IOException {
		return runWithPolicies("RegionObserver::preCompactScannerOpen(deprecated)",
				() -> getAdaptee().preCompactScannerOpen(argumentWithPolicies(c), store, scanners, scanType, earliestPutTs, s),
				c, store, scanners, scanType, earliestPutTs, s);
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
							StoreFile resultFile, CompactionRequest request) throws IOException {
		runWithPolicies("RegionObserver::postCompact",
				() -> getAdaptee().postCompact(argumentWithPolicies(c), store, resultFile, request),
				c, store, resultFile, request);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile) throws IOException {
		runWithPolicies("RegionObserver::postCompact(deprecated)",
				() -> getAdaptee().postCompact(argumentWithPolicies(c), store, resultFile),
				c, store, resultFile);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
		runWithPolicies("RegionObserver::preSplit(deprecated)",
				() -> getAdaptee().preSplit(argumentWithPolicies(c)), c);
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow) throws IOException {
		runWithPolicies("RegionObserver::preSplit",
				() -> getAdaptee().preSplit(argumentWithPolicies(c), splitRow),
				c, splitRow);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> c, Region l, Region r) throws IOException {
		runWithPolicies("RegionObserver::postSplit(deprecated)",
				() -> getAdaptee().postSplit(argumentWithPolicies(c), l, r), c, l, r);
	}

	@Override
	public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] splitKey, List<Mutation> metaEntries) throws IOException {
		runWithPolicies("RegionObserver::preSplitBeforePONR",
				() -> getAdaptee().preSplitBeforePONR(argumentWithPolicies(ctx), splitKey, metaEntries),
				ctx, splitKey, metaEntries);
	}

	@Override
	public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("RegionObserver::preSplitAfterPONR",
				() -> getAdaptee().preSplitAfterPONR(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("RegionObserver::preRollBackSplit",
				() -> getAdaptee().preRollBackSplit(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("RegionObserver::postRollBackSplit",
				() -> getAdaptee().postRollBackSplit(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		runWithPolicies("RegionObserver::postCompleteSplit",
				() -> getAdaptee().postCompleteSplit(argumentWithPolicies(ctx)), ctx);
	}

	@Override
	public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) throws IOException {
		runWithPolicies("RegionObserver::preClose",
				() -> getAdaptee().preClose(argumentWithPolicies(c), abortRequested),
				c, abortRequested);
	}

	@Override
	public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
		catchIOException(
				() -> runWithPolicies("RegionObserver::postClose",
						() -> getAdaptee().postClose(argumentWithPolicies(c), abortRequested),
						c, abortRequested));
	}

	@Override
	public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, Result result) throws IOException {
		runWithPolicies("RegionObserver::preGetClosestRowBefore",
				() -> getAdaptee().preGetClosestRowBefore(argumentWithPolicies(c), row, family, result),
				c, row, family, result);
	}

	@Override
	public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, Result result) throws IOException {
		runWithPolicies("RegionObserver::postGetClosestRowBefore",
				() -> getAdaptee().postGetClosestRowBefore(argumentWithPolicies(c), row, family, result),
				c, row, family, result);
	}

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) throws IOException {
		runWithPolicies("RegionObserver::preGetOp",
				() -> getAdaptee().preGetOp(argumentWithPolicies(c), get, result),
				c, get, result);
	}

	@Override
	public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) throws IOException {
		runWithPolicies("RegionObserver::postGetOp",
				() -> getAdaptee().postGetOp(argumentWithPolicies(c), get, result),
				c, get, result);
	}

	@Override
	public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
		return runWithPolicies("RegionObserver::preExists",
				() -> getAdaptee().preExists(argumentWithPolicies(c), get, exists),
				c, get, exists);
	}

	@Override
	public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
		return runWithPolicies("RegionObserver::postExists",
				() -> getAdaptee().postExists(argumentWithPolicies(c), get, exists),
				c, get, exists);
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
		runWithPolicies("RegionObserver::prePut",
				() -> getAdaptee().prePut(argumentWithPolicies(c), put, edit, durability),
				c, put, edit, durability);
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
		runWithPolicies("RegionObserver::postPut",
				() -> getAdaptee().postPut(argumentWithPolicies(c), put, edit, durability),
				c, put, edit, durability);
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
		runWithPolicies("RegionObserver::preDelete",
				() -> getAdaptee().preDelete(argumentWithPolicies(c), delete, edit, durability),
				c, delete, edit, durability);
	}

	@Override
	public void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> c,
													Mutation mutation, Cell cell, byte[] byteNow, Get get) throws IOException {
		runWithPolicies("RegionObserver::prePrepareTimeStampForDeleteVersion",
				() -> getAdaptee().prePrepareTimeStampForDeleteVersion(argumentWithPolicies(c), mutation, cell, byteNow, get),
				c, mutation, cell, byteNow, get);
	}

	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
						   WALEdit edit, Durability durability) throws IOException {
		runWithPolicies("RegionObserver::postDelete",
				() -> getAdaptee().postDelete(argumentWithPolicies(c), delete, edit, durability),
				c, delete, edit, durability);
	}

	@Override
	public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
							   MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
		runWithPolicies("RegionObserver::preBatchMutate",
				() -> getAdaptee().preBatchMutate(argumentWithPolicies(c), miniBatchOp),
				c, miniBatchOp);
	}

	@Override
	public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
								MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
		runWithPolicies("RegionObserver::postBatchMutate",
				() -> getAdaptee().postBatchMutate(argumentWithPolicies(c), miniBatchOp),
				c, miniBatchOp);
	}

	@Override
	public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
										 Region.Operation operation) throws IOException {
		runWithPolicies("RegionObserver::postStartRegionOperation",
				() -> getAdaptee().postStartRegionOperation(argumentWithPolicies(ctx), operation),
				ctx, operation);
	}

	@Override
	public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
										 Region.Operation operation) throws IOException {
		runWithPolicies("RegionObserver::postCloseRegionOperation",
				() -> getAdaptee().postCloseRegionOperation(argumentWithPolicies(ctx), operation),
				ctx, operation);
	}

	@Override
	public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx,
											 MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
		runWithPolicies("RegionObserver::postBatchMutateIndispensably",
				() -> getAdaptee().postBatchMutateIndispensably(argumentWithPolicies(ctx), miniBatchOp, success),
				ctx, miniBatchOp, success);
	}

	@Override
	public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c,
								  byte[] row, byte[] family, byte[] qualifier,
								  CompareFilter.CompareOp compareOp,
								  ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		return runWithPolicies("RegionObserver::preCheckAndPut",
				() -> getAdaptee().preCheckAndPut(argumentWithPolicies(c), row, family, qualifier, compareOp, comparator, put, result),
				c, row, family, qualifier, compareOp, comparator, put, result);
	}

	@Override
	public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
											  byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
											  ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		return runWithPolicies("RegionObserver::preCheckAndPutAfterRowLock",
				() -> getAdaptee().preCheckAndPutAfterRowLock(argumentWithPolicies(c), row, family, qualifier, compareOp, comparator, put, result),
				c, row, family, qualifier, compareOp, comparator, put, result);
	}

	@Override
	public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c,
								   byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
								   ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		return runWithPolicies("RegionObserver::postCheckAndPut",
				() -> getAdaptee().postCheckAndPut(argumentWithPolicies(c), row, family, qualifier, compareOp, comparator, put, result),
				c, row, family, qualifier, compareOp, comparator, put, result);
	}

	@Override
	public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c,
									 byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
									 ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		return runWithPolicies("RegionObserver::preCheckAndDelete",
				() -> getAdaptee().preCheckAndDelete(argumentWithPolicies(c), row, family, qualifier, compareOp, comparator, delete, result),
				c, row, family, qualifier, compareOp, comparator, delete, result);
	}

	@Override
	public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
												 byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
												 ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		return runWithPolicies("RegionObserver::preCheckAndDeleteAfterRowLock",
				() -> getAdaptee().preCheckAndDeleteAfterRowLock(argumentWithPolicies(c), row, family, qualifier, compareOp, comparator, delete, result),
				c, row, family, qualifier, compareOp, comparator, delete, result);
	}

	@Override
	public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c,
									  byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
									  ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		return runWithPolicies("RegionObserver::postCheckAndDelete",
				() -> getAdaptee().postCheckAndDelete(argumentWithPolicies(c), row, family, qualifier, compareOp, comparator, delete, result),
				c, row, family, qualifier, compareOp, comparator, delete, result);
	}

	@Override
	@SuppressWarnings("deprecation")
	public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c,
										byte[] row, byte[] family, byte[] qualifier, long amount,
										boolean writeToWAL) throws IOException {
		return runWithPolicies("RegionObserver::preIncrementColumnValue(deprecated)",
				() -> getAdaptee().preIncrementColumnValue(argumentWithPolicies(c), row, family, qualifier, amount, writeToWAL),
				c, row, family, qualifier, amount, writeToWAL);
	}

	@Override
	@SuppressWarnings("deprecation")
	public long postIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c,
										 byte[] row, byte[] family, byte[] qualifier, long amount,
										 boolean writeToWAL, long result) throws IOException {
		return runWithPolicies("RegionObserver::postIncrementColumnValue(deprecated)",
				() -> getAdaptee().postIncrementColumnValue(argumentWithPolicies(c), row, family, qualifier, amount, writeToWAL, result),
				c, row, family, qualifier, amount, writeToWAL, result);
	}

	@Override
	public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
		return runWithPolicies("RegionObserver::preAppend",
				() -> getAdaptee().preAppend(argumentWithPolicies(c), append),
				c, append);
	}

	@Override
	public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
		return runWithPolicies("RegionObserver::preAppendAfterRowLock",
				() -> getAdaptee().preAppendAfterRowLock(argumentWithPolicies(c), append),
				c, append);
	}

	@Override
	public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append, Result result) throws IOException {
		return runWithPolicies("RegionObserver::postAppend",
				() -> getAdaptee().postAppend(argumentWithPolicies(c), append, result),
				c, append, result);
	}

	@Override
	public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
		return runWithPolicies("RegionObserver::preIncrement",
				() -> getAdaptee().preIncrement(argumentWithPolicies(c), increment),
				c, increment);
	}

	@Override
	public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
		return runWithPolicies("RegionObserver::preIncrementAfterRowLock",
				() -> getAdaptee().preIncrementAfterRowLock(argumentWithPolicies(c), increment),
				c, increment);
	}

	@Override
	public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment, Result result) throws IOException {
		return runWithPolicies("RegionObserver::postIncrement",
				() -> getAdaptee().postIncrement(argumentWithPolicies(c), increment, result),
				c, increment, result);
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
		return runWithPolicies("RegionObserver::preScannerOpen",
				() -> getAdaptee().preScannerOpen(argumentWithPolicies(c), scan, s),
				c, scan, s);
	}

	@Override
	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
											   Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {
		return runWithPolicies("RegionObserver::preStoreScannerOpen",
				() -> getAdaptee().preStoreScannerOpen(argumentWithPolicies(c), store, scan, targetCols, s),
				c, store, scan, targetCols, s);
	}

	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
										 Scan scan, RegionScanner s) throws IOException {
		return runWithPolicies("RegionObserver::postScannerOpen",
				() -> getAdaptee().postScannerOpen(argumentWithPolicies(c), scan, s),
				c, scan, s);
	}

	@Override
	public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
								  InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
		return runWithPolicies("RegionObserver::preScannerNext",
				() -> getAdaptee().preScannerNext(argumentWithPolicies(c), s, result, limit, hasNext),
				c, s, result, limit, hasNext);
	}

	@Override
	public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
								   InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
		return runWithPolicies("RegionObserver::postScannerNext",
				() -> getAdaptee().postScannerNext(argumentWithPolicies(c), s, result, limit, hasNext),
				c, s, result, limit, hasNext);
	}

	@Override
	public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> c,
										InternalScanner s, byte[] currentRow, int offset,
										short length, boolean hasMore) throws IOException {
		return runWithPolicies("RegionObserver::postScannerFilterRow",
				() -> getAdaptee().postScannerFilterRow(argumentWithPolicies(c), s, currentRow, offset, length, hasMore),
				c, s, currentRow, offset, length, hasMore);
	}

	@Override
	public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		runWithPolicies("RegionObserver::preScannerClose",
				() -> getAdaptee().preScannerClose(argumentWithPolicies(c), s),
				c, s);
	}

	@Override
	public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		runWithPolicies("RegionObserver::postScannerClose",
				() -> getAdaptee().postScannerClose(argumentWithPolicies(c), s),
				c, s);
	}

	@Override
	public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
							  HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
		runWithPolicies("RegionObserver::preWALRestore",
				() -> getAdaptee().preWALRestore(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> ctx,
							  HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
		runWithPolicies("RegionObserver::preWALRestore(deprecated)",
				() -> getAdaptee().preWALRestore(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
							   HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
		runWithPolicies("RegionObserver::postWALRestore",
				() -> getAdaptee().postWALRestore(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> ctx,
							   HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
		runWithPolicies("RegionObserver::postWALRestore(deprectated)",
				() -> getAdaptee().postWALRestore(argumentWithPolicies(ctx), info, logKey, logEdit),
				ctx, info, logKey, logEdit);
	}

	@Override
	public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
		runWithPolicies("RegionObserver::preBulkLoadHFile",
				() -> getAdaptee().preBulkLoadHFile(argumentWithPolicies(ctx), familyPaths),
				ctx, familyPaths);
	}

	@Override
	public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[],
			String>> familyPaths, boolean hasLoaded) throws IOException {
		return runWithPolicies("RegionObserver::postBulkLoadHFile",
				() -> getAdaptee().postBulkLoadHFile(argumentWithPolicies(ctx), familyPaths, hasLoaded),
				ctx, familyPaths, hasLoaded);
	}

	@Override
	public StoreFile.Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
												   FileSystem fs, Path p, FSDataInputStreamWrapper in, long size,
												   CacheConfig cacheConf, Reference r, StoreFile.Reader reader) throws IOException {
		return runWithPolicies("RegionObserver::preStoreFileReaderOpen",
				() -> getAdaptee().preStoreFileReaderOpen(argumentWithPolicies(ctx), fs, p, in, size, cacheConf, r, reader),
				ctx, fs, p, in, size, cacheConf, r, reader);
	}

	@Override
	public StoreFile.Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
													FileSystem fs, Path p, FSDataInputStreamWrapper in,
													long size, CacheConfig cacheConf, Reference r,
													StoreFile.Reader reader) throws IOException {
		return runWithPolicies("RegionObserver::postStoreFileReaderOpen",
				() -> getAdaptee().postStoreFileReaderOpen(argumentWithPolicies(ctx), fs, p, in, size, cacheConf, r, reader),
				ctx, fs, p, in, size, cacheConf, r, reader);
	}

	@Override
	public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
									  MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
		return runWithPolicies("RegionObserver::postMutationBeforeWAL",
				() -> getAdaptee().postMutationBeforeWAL(argumentWithPolicies(ctx), opType, mutation, oldCell, newCell),
				ctx, opType, mutation, oldCell, newCell);
	}

	@Override
	public DeleteTracker postInstantiateDeleteTracker(ObserverContext<RegionCoprocessorEnvironment> ctx,
													  DeleteTracker delTracker) throws IOException {
		return runWithPolicies("RegionObserver::postInstantiateDeleteTracker",
				() -> getAdaptee().postInstantiateDeleteTracker(argumentWithPolicies(ctx), delTracker),
				ctx, delTracker);
	}
}
