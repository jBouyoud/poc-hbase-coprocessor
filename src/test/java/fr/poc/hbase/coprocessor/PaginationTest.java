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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@Slf4j
public class PaginationTest {

	private static final String TABLE_NAME = "pager_test";

	private static final int ROW_COUNT = 350;
	private static final int COL_COUNT_PER_FAMILIES = 1;
	private static final String[] FAMILIES = new String[]{"random", "sorted"};

	private static HBaseHelper helper;

	private Table table;

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		helper = HBaseHelper.getHelper(null);
		helper.dropTable(TABLE_NAME);
		helper.createTable(TABLE_NAME, new byte[][]{
				Bytes.toBytes("row-1"), Bytes.toBytes("row-2"), Bytes.toBytes("row-3")
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
		helper.fillTable(TABLE_NAME, 1, ROW_COUNT, COL_COUNT_PER_FAMILIES, -1, false, true, Arrays.copyOfRange(FAMILIES, 0, 1));
		helper.fillTable(TABLE_NAME, 1, ROW_COUNT, COL_COUNT_PER_FAMILIES, -1, false, false, Arrays.copyOfRange(FAMILIES, 1, 2));

		Admin admin = helper.getConnection().getAdmin();
		// wait for the split to be done
		while (admin.getTableRegions(TableName.valueOf(TABLE_NAME)).size() < 4) {
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
	 * TODO
	 *
	 * @throws Throwable
	 */
	@Test
	public void testSorted() throws Throwable {
		long start = System.currentTimeMillis();
		List<GroupByProtos.Value> values = new GroupByEndpointClient(table).groupByRow(6, null, null, null);

		// Input
		int pageNumber = 0;
		int pageSize = 10;

		// tmp
		int count = 0;

		// Generated output
		byte[] minRowkey;
		byte[] maxRowKey;
		Filter filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		long offset = 0;

		values.stream()
				.sorted(Comparator.comparing(value -> value.getKey().toByteArray(), new Bytes.ByteArrayComparator()))
				.forEachOrdered(value -> {
					// TODO
				});

		LOGGER.info("PaginationTest executed in [{}]ms", System.currentTimeMillis() - start);
	}
}
