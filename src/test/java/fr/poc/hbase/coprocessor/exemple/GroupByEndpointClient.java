package fr.poc.hbase.coprocessor.exemple;

import com.google.protobuf.ByteString;
import fr.poc.hbase.coprocessor.generated.GroupByProtos;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Example endpoint implementation, to execute a coprocessor groupBy request and merge results from region servers
 */
@Slf4j
@RequiredArgsConstructor
public class GroupByEndpointClient {

	@NonNull
	private final Table table;


	/**
	 * Group by row
	 *
	 * @param length      length of value prefix to match
	 * @param rowkeyStart rowkey start (or null)
	 * @param rowkeyEnd   rowkey end (or null)
	 * @param filter      filter to apply to scan
	 * @return list of values
	 */
	public List<GroupByProtos.Value> groupByRow(int length, byte[] rowkeyStart, byte[] rowkeyEnd, Filter filter) throws Throwable {
		GroupByProtos.GroupByRowRequest.Builder request = GroupByProtos.GroupByRowRequest.newBuilder()
				.setMatchLength(length);

		if (filter != null) {
			request.setFilter(ProtobufUtil.toFilter(filter));
		}

		Map<byte[], GroupByProtos.GroupByResponse> results = table.coprocessorService(
				// Define the protocol interface being invoked.
				GroupByProtos.GroupByService.class,
				rowkeyStart, rowkeyEnd,
				groupBy -> {
					BlockingRpcCallback<GroupByProtos.GroupByResponse> rpcCallback = new BlockingRpcCallback<>();
					// The groupByColumn() method is executing the endpoint functions.
					groupBy.groupByRow(null, request.build(), rpcCallback);
					return rpcCallback.get();
				}
		);

		//  Iterate over the returned map, containing the result for each region separately.
		return aggregateResults(results);
	}

	/**
	 * Group by column
	 *
	 * @param family      column family to group
	 * @param column      column to group
	 * @param length      length of value prefix to match
	 * @param rowkeyStart rowkey start (or null)
	 * @param rowkeyEnd   rowkey end (or null)
	 * @param filter      filter to apply to scan
	 * @return list of values
	 */
	public List<GroupByProtos.Value> groupByColumn(@NonNull byte[] family, @NonNull byte[] column, int length, byte[] rowkeyStart, byte[] rowkeyEnd, Filter filter) throws Throwable {
		GroupByProtos.GroupByColumnRequest.Builder request = GroupByProtos.GroupByColumnRequest.newBuilder()
				.setFamily(ByteString.copyFrom(family))
				.setColumn(ByteString.copyFrom(column))
				.setMatchLength(length);

		if (filter != null) {
			request.setFilter(ProtobufUtil.toFilter(filter));
		}

		Map<byte[], GroupByProtos.GroupByResponse> results = table.coprocessorService(
				// Define the protocol interface being invoked.
				GroupByProtos.GroupByService.class,
				rowkeyStart, rowkeyEnd,
				groupBy -> {
					BlockingRpcCallback<GroupByProtos.GroupByResponse> rpcCallback = new BlockingRpcCallback<>();
					// The groupByColumn() method is executing the endpoint functions.
					groupBy.groupByColumn(null, request.build(), rpcCallback);
					return rpcCallback.get();
				}
		);

		//  Iterate over the returned map, containing the result for each region separately.
		return aggregateResults(results);
	}

	/**
	 * GroupBy
	 *
	 * @param length      length of value prefix to match
	 * @param rowkeyStart rowkey start (or null)
	 * @param rowkeyEnd   rowkey end (or null)
	 * @param filter      filter to apply to scan
	 * @return list of values
	 */
	public List<GroupByProtos.Value> groupByRowWithBatch(int length, byte[] rowkeyStart, byte[] rowkeyEnd, Filter filter) throws Throwable {
		GroupByProtos.GroupByRowRequest.Builder request = GroupByProtos.GroupByRowRequest.newBuilder()
				.setMatchLength(length);

		if (filter != null) {
			request.setFilter(ProtobufUtil.toFilter(filter));
		}

		Map<byte[], GroupByProtos.GroupByResponse> results = table.batchCoprocessorService(
				GroupByProtos.GroupByService.getDescriptor().findMethodByName("groupByRow"),
				request.build(), rowkeyStart, rowkeyEnd,
				GroupByProtos.GroupByResponse.getDefaultInstance()
		);
		return aggregateResults(results);
	}

	/**
	 * GroupBy
	 *
	 * @param family      column family to group
	 * @param column      column to group
	 * @param length      length of value prefix to match
	 * @param rowkeyStart rowkey start (or null)
	 * @param rowkeyEnd   rowkey end (or null)
	 * @param filter      filter to apply to scan
	 * @return list of values
	 */
	public List<GroupByProtos.Value> groupByColumnWithBatch(byte[] family, byte[] column, int length, byte[] rowkeyStart, byte[] rowkeyEnd, Filter filter) throws Throwable {
		GroupByProtos.GroupByColumnRequest.Builder request = GroupByProtos.GroupByColumnRequest.newBuilder()
				.setFamily(ByteString.copyFrom(family))
				.setColumn(ByteString.copyFrom(column))
				.setMatchLength(length);

		if (filter != null) {
			request.setFilter(ProtobufUtil.toFilter(filter));
		}

		Map<byte[], GroupByProtos.GroupByResponse> results = table.batchCoprocessorService(
				GroupByProtos.GroupByService.getDescriptor().findMethodByName("groupByColumn"),
				request.build(), rowkeyStart, rowkeyEnd,
				GroupByProtos.GroupByResponse.getDefaultInstance()
		);
		return aggregateResults(results);
	}

	/**
	 * GroupBy aggregation of results
	 *
	 * @param results results to aggregate
	 * @return list of values
	 */
	private List<GroupByProtos.Value> aggregateResults(Map<byte[], GroupByProtos.GroupByResponse> results) {
		//  Iterate over the returned map, containing the result for each region separately.
		final HashMap<ByteString, GroupByProtos.Value.Builder> aggregatedValues = new HashMap<>();
		for (Map.Entry<byte[], GroupByProtos.GroupByResponse> entry : results.entrySet()) {
			LOGGER.debug("Region: {}", Bytes.toString(entry.getKey()));
			entry.getValue().getValuesList().forEach(value -> {
				// Extract response value data
				ByteString aggKey = value.getKey();
				long aggCount = value.getCount();
				ByteString aggRowkeyStart = value.getRowkeyStart();
				ByteString aggRowkeyEnd = value.getRowkeyEnd();

				// Update/Set aggregated value
				GroupByProtos.Value.Builder aggregatedValue = aggregatedValues.get(aggKey);
				if (aggregatedValue == null) {
					aggregatedValue = GroupByProtos.Value.newBuilder()
							.setKey(aggKey)
							.setCount(aggCount)
							.setRowkeyStart(aggRowkeyStart)
							.setRowkeyEnd(aggRowkeyEnd);
					aggregatedValues.put(aggKey, aggregatedValue);
				} else {
					// Sum result
					aggregatedValue.setCount(aggregatedValue.getCount() + aggCount);

					// Min rowkey start
					if (Bytes.compareTo(aggRowkeyStart.toByteArray(), aggregatedValue.getRowkeyStart().toByteArray()) < 0) {
						aggregatedValue.setRowkeyStart(aggRowkeyStart);
					}

					// Max rowkey end
					if (Bytes.compareTo(aggRowkeyEnd.toByteArray(), aggregatedValue.getRowkeyEnd().toByteArray()) > 0) {
						aggregatedValue.setRowkeyEnd(aggRowkeyEnd);
					}
				}
			});
		}

		return aggregatedValues.values().stream().map(GroupByProtos.Value.Builder::build).collect(Collectors.toList());
	}
}
