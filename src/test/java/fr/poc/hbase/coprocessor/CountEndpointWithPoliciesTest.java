package fr.poc.hbase.coprocessor;

import fr.poc.hbase.HBaseHelper;
import fr.poc.hbase.coprocessor.exemple.RowCountEndpoint;
import fr.poc.hbase.coprocessor.exemple.RowCountEndpointClient;
import fr.poc.hbase.coprocessor.policy.CoprocessorServicePolicyAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by JBD on 23/02/2017.
 */
@Slf4j
public class CountEndpointWithPoliciesTest {

	private static final String TABLE_NAME_STRING = "testtable";
	private static HBaseHelper helper;
	private Table table;

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		helper = HBaseHelper.getHelper(null);
		CountTestUtil.buildCountTestTable(helper, TABLE_NAME_STRING,
				htd -> {
					try {
						htd.addCoprocessor(RowCountEndpointWithPolicies.class.getName(), null, Coprocessor.PRIORITY_USER, null);
					} catch (IOException e) {
						throw new IllegalStateException("Uncatched IO", e);
					}
				});
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
		table = helper.getConnection().getTable(TableName.valueOf(TABLE_NAME_STRING));
	}

	/**
	 * Simple use of endpoint
	 *
	 * @throws Throwable
	 */
	@Test
	public void testEndpoint() throws Throwable {
		long start = System.currentTimeMillis();
		assertThat(new RowCountEndpointClient(table).getRowCount()).as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
		LOGGER.info("CountEndpointWithPoliciesTest:testEndpoint executed in [{}]ms", System.currentTimeMillis() - start);
	}

	/**
	 * Extending the batch call to execute multiple endpoint calls
	 *
	 * @throws Throwable
	 */
	@Test
	public void testEndpointCombined() throws Throwable {
		long start = System.currentTimeMillis();
		RowCountEndpointClient client = new RowCountEndpointClient(table);
		Pair<Long, Long> combinedCount = client.getRowAndCellsCount();
		assertThat(combinedCount.getFirst()).as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
		assertThat(combinedCount.getSecond()).as("Total cell count").isEqualTo(-3L);
		LOGGER.info("CountEndpointWithPoliciesTest:testEndpointCombined executed in [{}]ms", System.currentTimeMillis() - start);
	}

	/**
	 * Using the custom row-count endpoint in batch mode
	 *
	 * @throws Throwable
	 */
	@Test
	public void testEndpointBatch() throws Throwable {
		long start = System.currentTimeMillis();
		assertThat(new RowCountEndpointClient(table).getRowCountWithBatch()).as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
		LOGGER.info("CountEndpointWithPoliciesTest:testEndpointBatch executed in [{}]ms", System.currentTimeMillis() - start);
	}

	public static final class RowCountEndpointWithPolicies extends CoprocessorServicePolicyAdapter<RowCountEndpoint> {
		public RowCountEndpointWithPolicies() {
			super(new RowCountEndpoint());
		}
	}
}
