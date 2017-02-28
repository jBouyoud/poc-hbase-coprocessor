package fr.poc.hbase.coprocessor.exemple;

import com.google.protobuf.ByteString;
import fr.poc.hbase.coprocessor.generated.GroupByProtos;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
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
	 * GroupBy
	 *
	 * @param family      column family to group
	 * @param column      column to group
	 * @param length      length of value prefix to match
	 * @param rowkeyStart rowkey start (or null)
	 * @param rowkeyEnd   rowkey end (or null)
	 * @return list of values
	 */
	public List<GroupByProtos.Value> groupBy(byte[] family, byte[] column, int length, byte[] rowkeyStart, byte[] rowkeyEnd) throws Throwable {
		final GroupByProtos.GroupByRequest request = GroupByProtos.GroupByRequest.newBuilder()
				.setFamily(ByteString.copyFrom(family))
				.setColumn(ByteString.copyFrom(column))
				.setMatchLength(length)
				.build();

		Map<byte[], GroupByProtos.GroupByResponse> results = table.coprocessorService(
				// Define the protocol interface being invoked.
				GroupByProtos.GroupByService.class,
				rowkeyStart, rowkeyEnd,
				groupBy -> {
					BlockingRpcCallback<GroupByProtos.GroupByResponse> rpcCallback = new BlockingRpcCallback<>();
					// The call() method is executing the endpoint functions.
					groupBy.call(null, request, rpcCallback);
					return rpcCallback.get();
				}
		);

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
