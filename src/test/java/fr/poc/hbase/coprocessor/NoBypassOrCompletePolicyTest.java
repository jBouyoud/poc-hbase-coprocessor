package fr.poc.hbase.coprocessor;

import fr.poc.hbase.HBaseHelper;
import fr.poc.hbase.coprocessor.exemple.RegionObserverWithBypassExample;
import fr.poc.hbase.coprocessor.exemple.RegionObserverWithCompleteExample;
import fr.poc.hbase.coprocessor.policy.proxy.RegionObserverPolicyProxy;
import fr.poc.hbase.coprocessor.policy.impl.LimitRetryPolicy;
import fr.poc.hbase.coprocessor.policy.impl.NoBypassOrCompletePolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test {@link fr.poc.hbase.coprocessor.policy.impl.NoBypassOrCompletePolicy}
 */
@Slf4j
public class NoBypassOrCompletePolicyTest {

	private static final String TABLE_NAME_STRING = "testtable";

	/**
	 * Test that a coprocessor can bypass their chain
	 *
	 * @throws Throwable
	 */
	@Test
	public void testBypassAllowed() throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.coprocessor.region.classes", RegionObserverWithBypassExample.class.getName());
		getByPassFixedRow(conf);
	}

	/**
	 * Test that a coprocessor can't bypass their chain
	 *
	 * @throws Throwable
	 */
	@Test(expected = LimitRetryPolicy.ServerSideRetriesExhaustedException.class)
	public void testBypassPolicy() throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.coprocessor.region.classes", SafeBypass.class.getName());
		getByPassFixedRow(conf);
	}

	private void getByPassFixedRow(Configuration conf) throws Exception {
		try (HBaseHelper helper = HBaseHelper.getHelper(conf)) {
			helper.dropTable(TABLE_NAME_STRING);
			helper.createTable(TABLE_NAME_STRING, "test");
			helper.put(TABLE_NAME_STRING, new String(RegionObserverWithBypassExample.FIXED_ROW), "test", "q", "1");
			Table table = helper.getConnection().getTable(TableName.valueOf(TABLE_NAME_STRING));
			table.get(new Get(RegionObserverWithBypassExample.FIXED_ROW));
		}
	}

	/**
	 * Test that a coprocessor can complete their chain
	 *
	 * @throws Throwable
	 */
	@Test
	public void tesCompleteAllowed() throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.coprocessor.region.classes", RegionObserverWithCompleteExample.class.getName());
		getCompleteFixedRow(conf);
	}

	/**
	 * Test that a coprocessor can't complete their chain
	 *
	 * @throws Throwable
	 */
	@Test(expected = LimitRetryPolicy.ServerSideRetriesExhaustedException.class)
	public void testCompletePolicy() throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.coprocessor.region.classes", SafeComplete.class.getName());
		getByPassFixedRow(conf);
	}

	private void getCompleteFixedRow(Configuration conf) throws Exception {
		try (HBaseHelper helper = HBaseHelper.getHelper(conf)) {
			helper.dropTable(TABLE_NAME_STRING);
			helper.createTable(TABLE_NAME_STRING, "test");
			helper.put(TABLE_NAME_STRING, new String(RegionObserverWithCompleteExample.FIXED_ROW), "test", "q", "1");
			Table table = helper.getConnection().getTable(TableName.valueOf(TABLE_NAME_STRING));
			table.get(new Get(RegionObserverWithBypassExample.FIXED_ROW));
		}
	}

	public static final class SafeBypass extends RegionObserverPolicyProxy {

		public SafeBypass() {
			super(new RegionObserverWithBypassExample(), Arrays.asList(
					new NoBypassOrCompletePolicy(),
					new LimitRetryPolicy(2, new LimitRetryPolicy.InMemoryCache())
			));
		}
	}

	public static final class SafeComplete extends RegionObserverPolicyProxy {

		public SafeComplete() {
			super(new RegionObserverWithCompleteExample(), Arrays.asList(
					new NoBypassOrCompletePolicy(),
					new LimitRetryPolicy(2, new LimitRetryPolicy.InMemoryCache())
			));
		}
	}

}
