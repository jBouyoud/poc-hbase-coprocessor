package fr.poc.hbase.coprocessor;


import fr.poc.hbase.HBaseHelper;
import fr.poc.hbase.coprocessor.exemple.GroupByEndpoint;
import fr.poc.hbase.coprocessor.exemple.GroupByEndpointClient;
import fr.poc.hbase.coprocessor.generated.GroupByProtos;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class GroupByEndpointTest {

	private static final String TABLE_NAME = "groupby_table";

	private static final int ROW_COUNT = 25_000;
	private static final int COL_COUNT_PER_FAMILIES = 5;
	private static final String[] FAMILIES = new String[]{
			"colfam0", "colfam1", "colfam2", "colfam3"
	};

	private static HBaseHelper helper;

	private Table table;

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		helper = HBaseHelper.getHelper(null);
		helper.dropTable(TABLE_NAME);
		helper.createTable(TABLE_NAME, new byte[][]{
				Bytes.toBytes("row-8333"), Bytes.toBytes("row-16666")
		}, FAMILIES);
		helper.alterTable(TABLE_NAME,
				htd -> {
					try {
						htd.addCoprocessor(GroupByEndpoint.class.getName(), null, Coprocessor.PRIORITY_USER, null);
					} catch (IOException e) {
						throw new IllegalStateException("Uncatched IO", e);
					}
				}
		);
		helper.fillTable(TABLE_NAME, 1, ROW_COUNT, COL_COUNT_PER_FAMILIES, -1, false, true, FAMILIES);

		Admin admin = helper.getConnection().getAdmin();
		// wait for the split to be done
		while (admin.getTableRegions(TableName.valueOf(TABLE_NAME)).size() < 3) {
			Thread.sleep(1000);
		}
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
		table = helper.getConnection().getTable(TableName.valueOf(TABLE_NAME));
	}

	/**
	 * Simple use of endpoint
	 *
	 * @throws Throwable
	 */
	@Test
	public void testGroupByColumn() throws Throwable {
		long start = System.currentTimeMillis();
		List<GroupByProtos.Value> values = new GroupByEndpointClient(table).groupByColumn(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes("col-1"), 6, null, null, null);
		assertThat(values.size()).as("Groups count").isEqualTo(100);
		assertThat(values.stream().map(GroupByProtos.Value::getKey).distinct().count()).as("Distinct groups count").isEqualTo(100);
		assertThat(values.stream().mapToLong(GroupByProtos.Value::getCount).sum()).as("Total records").isEqualTo(ROW_COUNT);
		LOGGER.info("GroupByEndpointTest:testEndpoint executed in [{}]ms", System.currentTimeMillis() - start);
	}

	/**
	 * Using the custom row-count endpoint in batch mode
	 *
	 * @throws Throwable
	 */
	@Test
	public void testGroupByColumnBatch() throws Throwable {
		long start = System.currentTimeMillis();
		List<GroupByProtos.Value> values = new GroupByEndpointClient(table).groupByColumnWithBatch(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes("col-1"), 6, null, null, null);
		assertThat(values.size()).as("Groups count").isEqualTo(100);
		assertThat(values.stream().map(GroupByProtos.Value::getKey).distinct().count()).as("Distinct groups count").isEqualTo(100);
		assertThat(values.stream().mapToLong(GroupByProtos.Value::getCount).sum()).as("Total records").isEqualTo(ROW_COUNT);
		LOGGER.info("GroupByEndpointTest:testEndpoint executed in [{}]ms", System.currentTimeMillis() - start);
	}

	/**
	 * Simple use of endpoint, with filters
	 *
	 * @throws Throwable
	 */
	@Test
	public void testGroupByColumnWithFilters() throws Throwable {
		long start = System.currentTimeMillis();
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^val-[02468]\\d*$"));
		List<GroupByProtos.Value> values = new GroupByEndpointClient(table).groupByColumn(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes("col-1"), 6, null, null, filter);
		assertThat(values.size()).as("Groups count").isEqualTo(50);
		assertThat(values.stream().map(GroupByProtos.Value::getKey).distinct().count()).as("Distinct groups count").isEqualTo(50);
		assertThat(values.stream().mapToLong(GroupByProtos.Value::getCount).sum()).as("Total records").isLessThan(ROW_COUNT);
		LOGGER.info("GroupByEndpointTest:testEndpoint executed in [{}]ms", System.currentTimeMillis() - start);
	}

	/**
	 * Simple use of endpoint, with filters
	 *
	 * @throws Throwable
	 */
	@Test
	public void testGroupByColumnBatchWithFilters() throws Throwable {
		long start = System.currentTimeMillis();
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^val-[02468]\\d*$"));
		List<GroupByProtos.Value> values = new GroupByEndpointClient(table).groupByColumnWithBatch(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes("col-1"), 6, null, null, filter);
		assertThat(values.size()).as("Groups count").isEqualTo(50);
		assertThat(values.stream().map(GroupByProtos.Value::getKey).distinct().count()).as("Distinct groups count").isEqualTo(50);
		assertThat(values.stream().mapToLong(GroupByProtos.Value::getCount).sum()).as("Total records").isLessThan(ROW_COUNT);
		LOGGER.info("GroupByEndpointTest:testEndpoint executed in [{}]ms", System.currentTimeMillis() - start);
	}
}
