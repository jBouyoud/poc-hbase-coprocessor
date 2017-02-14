package fr.poc.hbase.coprocessor.exemple;

import fr.poc.hbase.coprocessor.generated.RowCounterProtos;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;

/**
 * Created by JBD on 23/02/2017.
 */
@Slf4j
@RequiredArgsConstructor
public class RowCountEndpointClient {

	@NonNull
	private final Table table;


	public long getRowCount() throws Throwable {
		final RowCounterProtos.CountRequest request =
				RowCounterProtos.CountRequest.getDefaultInstance();
		Map<byte[], Long> results = table.coprocessorService(
				// Define the protocol interface being invoked.
				RowCounterProtos.RowCountService.class,
				// Set start and end row key to "null" to count all rows.
				null, null,
				counter -> {
					BlockingRpcCallback<RowCounterProtos.CountResponse> rpcCallback = new BlockingRpcCallback<>();
					// The call() method is executing the endpoint functions.
					counter.getRowCount(null, request, rpcCallback);
					RowCounterProtos.CountResponse response = rpcCallback.get();
					return response == null ? -1 : response.hasCount() ? response.getCount() : 0;
				}
		);

		long total = 0;
		//  Iterate over the returned map, containing the result for each region separately.
		for (Map.Entry<byte[], Long> entry : results.entrySet()) {
			total += entry.getValue();
			LOGGER.debug("Region: {}, Count: {}", Bytes.toString(entry.getKey()), entry.getValue());
		}
		return total;
	}

	public long getRowCountWithBatch() throws Throwable {
		final RowCounterProtos.CountRequest request = RowCounterProtos.CountRequest.getDefaultInstance();
		Map<byte[], RowCounterProtos.CountResponse> results = table.batchCoprocessorService(
				RowCounterProtos.RowCountService.getDescriptor().findMethodByName("getRowCount"),
				request, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				RowCounterProtos.CountResponse.getDefaultInstance());

		long total = 0;
		for (Map.Entry<byte[], RowCounterProtos.CountResponse> entry : results.entrySet()) {
			RowCounterProtos.CountResponse response = entry.getValue();
			total += response == null ? -1 : response.hasCount() ? response.getCount() : 0;
			LOGGER.debug("Region: {}, Count: {}", Bytes.toString(entry.getKey()), entry.getValue());
		}
		return total;
	}


	public Pair<Long, Long> getRowAndCellsCount() throws Throwable {
		final RowCounterProtos.CountRequest request =
				RowCounterProtos.CountRequest.getDefaultInstance();
		Map<byte[], Pair<Long, Long>> results = table.coprocessorService(
				RowCounterProtos.RowCountService.class,
				null, null,
				counter -> {
					BlockingRpcCallback<RowCounterProtos.CountResponse> rowCallback = new BlockingRpcCallback<>();
					counter.getRowCount(null, request, rowCallback);
					RowCounterProtos.CountResponse rowResponse = rowCallback.get();
					Long rowCount = rowResponse == null ? -1 : rowResponse.hasCount() ? rowResponse.getCount() : 0;

					BlockingRpcCallback<RowCounterProtos.CountResponse> cellCallback = new BlockingRpcCallback<>();
					counter.getCellCount(null, request, cellCallback);
					RowCounterProtos.CountResponse cellResponse = cellCallback.get();
					Long cellCount = cellResponse == null ? -1 : cellResponse.hasCount() ? cellResponse.getCount() : 0;

					return new Pair<>(rowCount, cellCount);
				}
		);

		long totalRows = 0;
		long totalKeyValues = 0;
		for (Map.Entry<byte[], Pair<Long, Long>> entry : results.entrySet()) {
			totalRows += entry.getValue().getFirst();
			totalKeyValues += entry.getValue().getSecond();
			LOGGER.debug("Region: {}, Count: {}", Bytes.toString(entry.getKey()), entry.getValue());
		}
		return Pair.newPair(totalRows, totalKeyValues);
	}
}
