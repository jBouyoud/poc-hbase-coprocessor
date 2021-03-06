package fr.poc.hbase.coprocessor;

import com.ea.agentloader.AgentLoader;
import fr.poc.hbase.HBaseHelper;
import fr.poc.hbase.coprocessor.exemple.*;
import fr.poc.hbase.coprocessor.policy.agent.CoprocessorPolicyAgent;
import fr.poc.hbase.coprocessor.util.CountTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.*;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link CoprocessorPolicyAgent}
 */
@Slf4j
public class CoprocessorPolicyAgentTest {

	private static final String TABLE_NAME_STRING = "testtable";

	private static HBaseHelper helper;
	private static Table table;

	private ObserverStatisticsEndpointClient stats;

	@BeforeClass
	public static void setupBeforeClass() throws Throwable {
		// Load agent at runtime
		AgentLoader.loadAgentClass(CoprocessorPolicyAgent.class.getName(), "");

		//
		//Static coprocessor loading
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.coprocessor.region.classes",
				RegionObserverWithBypassExample.class.getName() + "," +
						RegionObserverWithCompleteExample.class.getName() + "," +
						RegionObserverExample.class.getName()
		);

		helper = HBaseHelper.getHelper(conf);
		CountTestUtil.buildCountTestTable(helper, TABLE_NAME_STRING);

		long start = System.nanoTime();
		helper.alterTable(TABLE_NAME_STRING, htd -> {
			try {
				// Dynamic loading
				htd.addCoprocessor(RowCountEndpoint.class.getName(), null, Coprocessor.PRIORITY_USER, null);
				htd.addCoprocessor(ObserverStatisticsEndpoint.class.getName(), null, Coprocessor.PRIORITY_USER, null);
			} catch (IOException e) {
				throw new IllegalStateException("Uncatched IO", e);
			}
		});
		long end = System.nanoTime();
		LOGGER.info(MarkerFactory.getMarker("TEST_EXECUTION_TIME"), "Test [{}.loadingTime] executed in [{}]ms",
				CoprocessorPolicyAgentTest.class.getName(), TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));

		table = helper.getConnection().getTable(TableName.valueOf(TABLE_NAME_STRING));
		// Warmup
		new RowCountEndpointClient(table).getRowCount();
	}

	@AfterClass
	public static void teardownAfterClass() throws Exception {
		helper.close();
	}

	/**
	 * Init test by preparing user and previews mocks
	 */
	@Before
	public void initTest() throws Throwable {
		stats = new ObserverStatisticsEndpointClient(table);
	}

	/**
	 * Remove coprocessor after test
	 */
	@After
	public void afterTest() throws Throwable {
		stats.printStatistics(true, true);
	}

	/**
	 * Simple use of endpoint
	 *
	 * @throws Throwable
	 */
	@Test
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
	public void testEndpointCombined() throws Throwable {
		RowCountEndpointClient client = new RowCountEndpointClient(table);
		Pair<Long, Long> combinedCount = client.getRowAndCellsCount();
		assertThat(combinedCount.getFirst())
				.as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
		assertThat(combinedCount.getSecond()).as("Total cell count").isEqualTo(-3L);
	}

	/**
	 * Using the custom row-count endpoint in batch mode
	 *
	 * @throws Throwable
	 */
	@Test
	public void testEndpointBatch() throws Throwable {
		assertThat(new RowCountEndpointClient(table).getRowCountWithBatch())
				.as("Total row count")
				.isEqualTo(CountTestUtil.ROW_COUNT);
	}
}
