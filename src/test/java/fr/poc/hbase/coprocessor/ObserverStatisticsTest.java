package fr.poc.hbase.coprocessor;

import fr.poc.hbase.HBaseHelper;
import fr.poc.hbase.coprocessor.exemple.ObserverStatisticsEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import fr.poc.hbase.coprocessor.generated.ObserverStatisticsProtos;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Use an endpoint to query observer statistics
 */
@Slf4j
public class ObserverStatisticsTest {

	private static final String TABLE_NAME_STRING = "testtable";
	private static final TableName TABLE_NAME = TableName.valueOf("testtable");

	private static HBaseHelper helper;

	private Table table;

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		helper = HBaseHelper.getHelper(null);
	}

	@AfterClass
	public static void teardownAfterClass() throws Exception {
		helper.close();
	}

	/**
	 * Init test by preparing user and previews mocks
	 */
	@Before
	public void initTest() throws Exception {
		helper.dropTable(TABLE_NAME_STRING);
		helper.createTable(TABLE_NAME_STRING, 3, "colfam1", "colfam2");
		helper.alterTable(TABLE_NAME_STRING,
				htd -> {
					try {
						htd.addCoprocessor(ObserverStatisticsEndpoint.class.getName(), null, Coprocessor.PRIORITY_USER, null);
					} catch (IOException e) {
						throw new IllegalStateException("Uncatched IO",e);
					}
				});
		helper.put(TABLE_NAME_STRING,
				new String[]{"row1", "row2", "row3", "row4", "row5"},
				new String[]{"colfam1", "colfam2"}, new String[]{"qual1", "qual1"},
				new long[]{1, 2}, new String[]{"val1", "val2"});

		table = helper.getConnection().getTable(TABLE_NAME);
	}

	/**
	 * observerStats Test
	 */
	@Test
	public void observerStatsTest() throws Throwable {
		printStatistics(false, true);

		LOGGER.info("Apply single put...");
		Put put = new Put(Bytes.toBytes("row10"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
				Bytes.toBytes("val10"));
		table.put(put);
		printStatistics(true, true);

		LOGGER.info("Do single get...");
		Get get = new Get(Bytes.toBytes("row10"));
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"));
		table.get(get);
		printStatistics(true, true);


		LOGGER.info("Send batch with put and get...");
		List<Row> batch = new ArrayList<>();
		Object[] results = new Object[2];
		batch.add(put);
		batch.add(get);
		table.batch(batch, results);
		printStatistics(true, true);

		LOGGER.info("Scan single row...");
		Scan scan = new Scan()
				.setStartRow(Bytes.toBytes("row10"))
				.setStopRow(Bytes.toBytes("row11"));
		ResultScanner scanner = table.getScanner(scan);
		LOGGER.info("  -> after getScanner()...");
		printStatistics(true, true);
		scanner.next();
		LOGGER.info("  -> after next()...");
		printStatistics(true, true);
		scanner.close();
		LOGGER.info("  -> after close()...");
		printStatistics(true, true);

		LOGGER.info("Scan multiple rows...");
		scan = new Scan();
		scanner = table.getScanner(scan);
		LOGGER.info("  -> after getScanner()...");
		printStatistics(true, true);
		scanner.next();
		LOGGER.info("  -> after next()...");
		printStatistics(true, true);
		scanner.next();
		printStatistics(false, true);
		scanner.close();
		LOGGER.info("  -> after close()...");
		printStatistics(true, true);

		LOGGER.info("Apply single put with mutateRow()...");
		RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
		put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
				Bytes.toBytes("val10"));
		mutations.add(put);
		table.mutateRow(mutations);
		printStatistics(true, true);

		LOGGER.info("Apply single column increment...");
		Increment increment = new Increment(Bytes.toBytes("row10"));
		increment.addColumn(Bytes.toBytes("colfam1"),
				Bytes.toBytes("qual11"), 1);
		table.increment(increment);
		printStatistics(true, true);

		LOGGER.info("Apply multi column increment...");
		increment = new Increment(Bytes.toBytes("row10"));
		increment.addColumn(Bytes.toBytes("colfam1"),
				Bytes.toBytes("qual12"), 1);
		increment.addColumn(Bytes.toBytes("colfam1"),
				Bytes.toBytes("qual13"), 1);
		table.increment(increment);
		printStatistics(true, true);

		LOGGER.info("Apply single incrementColumnValue...");
		table.incrementColumnValue(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual12"), 1);
		printStatistics(true, true);

		LOGGER.info("Call single exists()...");
		table.exists(get);
		printStatistics(true, true);

		LOGGER.info("Apply single delete...");
		Delete delete = new Delete(Bytes.toBytes("row10"));
		delete.addColumn(Bytes.toBytes("colfam1"),
				Bytes.toBytes("qual10"));
		table.delete(delete);
		printStatistics(true, true);

		LOGGER.info("Apply single append...");
		Append append = new Append(Bytes.toBytes("row10"));
		append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual15"),
				Bytes.toBytes("-valnew"));
		table.append(append);
		printStatistics(true, true);

		LOGGER.info("Apply checkAndPut (failing)...");
		put = new Put(Bytes.toBytes("row10"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual17"),
				Bytes.toBytes("val17"));
		boolean cap = table.checkAndPut(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual15"), null, put);
		LOGGER.info("  -> success: " + cap);
		printStatistics(true, true);

		LOGGER.info("Apply checkAndPut (succeeding)...");
		cap = table.checkAndPut(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual16"), null, put);
		LOGGER.info("  -> success: " + cap);
		printStatistics(true, true);

		LOGGER.info("Apply checkAndDelete (failing)...");
		delete = new Delete(Bytes.toBytes("row10"));
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual17"));
		cap = table.checkAndDelete(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual15"), null, delete);
		LOGGER.info("  -> success: " + cap);
		printStatistics(true, true);

		LOGGER.info("Apply checkAndDelete (succeeding)...");
		cap = table.checkAndDelete(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual18"), null, delete);
		LOGGER.info("  -> success: " + cap);
		printStatistics(true, true);

		LOGGER.info("Apply checkAndMutate (failing)...");
		mutations = new RowMutations(Bytes.toBytes("row10"));
		put = new Put(Bytes.toBytes("row10"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual20"),
				Bytes.toBytes("val20"));
		delete = new Delete(Bytes.toBytes("row10"));
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual17"));
		mutations.add(put);
		mutations.add(delete);
		cap = table.checkAndMutate(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
				CompareFilter.CompareOp.GREATER, Bytes.toBytes("val10"), mutations);
		LOGGER.info("  -> success: " + cap);
		printStatistics(true, true);

		LOGGER.info("Apply checkAndMutate (succeeding)...");
		cap = table.checkAndMutate(Bytes.toBytes("row10"),
				Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val10"), mutations);
		LOGGER.info("  -> success: " + cap);
		printStatistics(true, true);
	}


	private void printStatistics(boolean print, boolean clear) throws Throwable {
		final ObserverStatisticsProtos.StatisticsRequest request = ObserverStatisticsProtos.StatisticsRequest
				.newBuilder().setClear(clear).build();
		Map<byte[], Map<String, Integer>> results = table.coprocessorService(
				ObserverStatisticsProtos.ObserverStatisticsService.class,
				null, null,
				statistics -> {
					BlockingRpcCallback<ObserverStatisticsProtos.StatisticsResponse> rpcCallback =
							new BlockingRpcCallback<>();
					statistics.getStatistics(null, request, rpcCallback);
					ObserverStatisticsProtos.StatisticsResponse response = rpcCallback.get();
					Map<String, Integer> stats = new LinkedHashMap<>();
					for (ObserverStatisticsProtos.NameInt32Pair pair : response.getAttributeList()) {
						stats.put(pair.getName(), pair.getValue());
					}
					return stats;
				}
		);
		if (print) {
			for (Map.Entry<byte[], Map<String, Integer>> entry : results.entrySet()) {
				LOGGER.info("Region: {}", Bytes.toString(entry.getKey()));
				for (Map.Entry<String, Integer> call : entry.getValue().entrySet()) {
					LOGGER.info("  {}: {}", call.getKey(), call.getValue());
				}
			}
			LOGGER.info("");
		}
	}
}
