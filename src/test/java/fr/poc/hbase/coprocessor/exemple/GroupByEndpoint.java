package fr.poc.hbase.coprocessor.exemple;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import fr.poc.hbase.coprocessor.generated.GroupByProtos;
import fr.poc.hbase.coprocessor.generated.GroupByProtos.GroupByResponse.Builder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.*;

/**
 * Example endpoint implementation, to group results by column
 */
public final class GroupByEndpoint extends GroupByProtos.GroupByService implements Coprocessor, CoprocessorService {

	private RegionCoprocessorEnvironment env;

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// nothing to do when coprocessor is shutting down
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void call(RpcController controller,
					 GroupByProtos.GroupByRequest request,
					 RpcCallback<GroupByProtos.GroupByResponse> done) {

		GroupByProtos.GroupByResponse response = null;
		try {
			response = scanGroupByCount(request.getFamily().toByteArray(), request.getColumn().toByteArray(), request.getMatchLength(), request.hasFilter() ? request.getFilter() : null);
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		}
		done.run(response);
	}

	/**
	 * Group-by scan, aggregate by counting result
	 *
	 * @param family column family to group
	 * @param column column to group
	 * @param length length of value prefix to match
	 * @param filter filters to add to scan
	 * @return GroupBy response
	 */
	private GroupByProtos.GroupByResponse scanGroupByCount(byte[] family, byte[] column, int length, FilterProtos.Filter filter) throws IOException {
		long count = 0;

		// Create scan
		final Scan scan = new Scan();
		if (filter != null) {
			scan.setFilter(ProtobufUtil.toFilter(filter));
		}

		// Execute scan
		try (
				InternalScanner scanner = env.getRegion().getScanner(scan)
		) {
			Map<GroupKey, GroupByProtos.Value.Builder> groupByResults = new HashMap<>();
			List<Cell> row = new ArrayList<>();
			boolean hasMore;
			do {
				hasMore = scanner.next(row);
				for (Cell cell : row) {
					// Select column to match
					if (Arrays.equals(CellUtil.cloneFamily(cell), family) && Arrays.equals(CellUtil.cloneQualifier(cell), column)) {
						// Extract the key from the beginning of the value
						GroupKey key = new GroupKey(CellUtil.cloneValue(cell), length);

						// Get aggregation (or create a new one)
						// Rowkey start is set only once (the first one is always the minimum rowkey)
						GroupByProtos.Value.Builder value = groupByResults.get(key);
						if (value == null) {
							value = GroupByProtos.Value.newBuilder()
									.setRowkeyStart(ByteString.copyFrom(CellUtil.cloneRow(cell)))
									.setKey(ByteString.copyFrom(key.getKey()));
							groupByResults.put(key, value);
						}

						// Update aggregation
						// Rowkey end is always overwritten (the last one is always the maximum rowkey)
						value.setCount(value.getCount() + 1).setRowkeyEnd(ByteString.copyFrom(CellUtil.cloneRow(cell)));
					}
					count++;
				}
				row.clear();
			} while (hasMore);

			// Long delay to test interruption
			if (count > 20_000) {
				Thread.sleep(10_000);
			}

			// Build response
			final Builder response = GroupByProtos.GroupByResponse.newBuilder();
			groupByResults.forEach((key, value) -> response.addValues(value));
			return response.build();
		} catch (InterruptedException e) {
			throw new IllegalStateException("Random timer interrupted", e);
		}
	}

	/**
	 * Key wrapper to use a byte[] as a map key, implements both equals and hashCode (from Arrays)
	 */
	private static class GroupKey {
		private final byte[] key;

		/**
		 * @param value  value used to extract group key
		 * @param length maximum length of group key
		 */
		GroupKey(byte[] value, int length) {
			this.key = (length <= 0) ? value : Arrays.copyOfRange(value, 0, Math.min(value.length, length));
		}

		/**
		 * @return the group key
		 */
		byte[] getKey() {
			return key;
		}

		@Override
		public boolean equals(Object o) {
			return (o == this) || (o instanceof GroupKey) && Arrays.equals(key, ((GroupKey) o).getKey());
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(key);
		}
	}
}
