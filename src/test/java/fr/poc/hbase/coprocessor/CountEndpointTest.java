package fr.poc.hbase.coprocessor;


import fr.poc.hbase.HBaseHelper;
import fr.poc.hbase.coprocessor.exemple.RowCountEndpoint;
import fr.poc.hbase.coprocessor.exemple.RowCountEndpointClient;
import fr.poc.hbase.coprocessor.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CountEndpointTest {

	private static final String TABLE_NAME_STRING = "testtable";

	private static HBaseHelper helper;
	private static Table table;
	@Rule
	public final RepeatRule repeatRule = new RepeatRule();
	@Rule
	public final WithExecutionTimeRule executionTimeTime = new WithExecutionTimeRule();

	@BeforeClass
	public static void setupBeforeClass() throws Throwable {
		helper = HBaseHelper.getHelper(null);
		CountTestUtil.buildCountTestTable(helper, TABLE_NAME_STRING);

		long start = System.nanoTime();
		helper.alterTable(TABLE_NAME_STRING,
				htd -> {
					try {
						htd.addCoprocessor(RowCountEndpoint.class.getName(), null, Coprocessor.PRIORITY_USER, null);
					} catch (IOException e) {
						throw new IllegalStateException("Uncatched IO", e);
					}
				});
		long end = System.nanoTime();
		LOGGER.info(MarkerFactory.getMarker("TEST_EXECUTION_TIME"), "Test [{}.loadingTime] executed in [{}]ms",
				CountEndpointTest.class.getName(), TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));

		table = helper.getConnection().getTable(TableName.valueOf(TABLE_NAME_STRING));
		//Warmup
		new RowCountEndpointClient(table).getRowCount();
	}

	@AfterClass
	public static void teardownAfterClass() throws Exception {
		helper.close();
	}

	/**
	 * Simple use of endpoint
	 *
	 * @throws Throwable
	 */
	@Test
	@Repeat(CountTestUtil.REPEAT_COUNT)
	@WithExecutionTime
	public void testEndpoint() throws Throwable {
		assertThat(new RowCountEndpointClient(table).getRowCount())
				.as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
	}

	/**
	 * Extending the batch call to execute multiple endpoint calls
	 *
	 * @throws Throwable
	 */
	@Test
	@Repeat(CountTestUtil.REPEAT_COUNT)
	@WithExecutionTime
	public void testEndpointCombined() throws Throwable {
		RowCountEndpointClient client = new RowCountEndpointClient(table);
		Pair<Long, Long> combinedCount = client.getRowAndCellsCount();
		assertThat(combinedCount.getFirst())
				.as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
		assertThat(combinedCount.getSecond())
				.as("Total cell count")
				.isEqualTo(CountTestUtil.ROW_COUNT * CountTestUtil.COL_COUNT_PER_FAMILIES * CountTestUtil.FAMILIES.length);
	}

	/**
	 * Using the custom row-count endpoint in batch mode
	 *
	 * @throws Throwable
	 */
	@Test
	@Repeat(CountTestUtil.REPEAT_COUNT)
	@WithExecutionTime
	public void testEndpointBatch() throws Throwable {
		assertThat(new RowCountEndpointClient(table).getRowCountWithBatch())
				.as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
	}
}
