package fr.poc.hbase.coprocessor.exemple;

import fr.poc.hbase.coprocessor.generated.ObserverStatisticsProtos;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link ObserverStatisticsEndpoint} simple client
 */
@Slf4j
@RequiredArgsConstructor
public class ObserverStatisticsEndpointClient {

	@NonNull
	private final Table table;

	public void printStatistics(boolean print, boolean clear) throws Throwable {
		final ObserverStatisticsProtos.StatisticsRequest request = ObserverStatisticsProtos.StatisticsRequest
				.newBuilder().setClear(clear).build();
		Map<byte[], Map<String, Integer>> results = table.coprocessorService(
				ObserverStatisticsProtos.ObserverStatisticsService.class,
				null, null,
				statistics -> {
					BlockingRpcCallback<ObserverStatisticsProtos.StatisticsResponse> rpcCallback =
							new BlockingRpcCallback<>();
					statistics.getStatistics(null, request, rpcCallback);
					ObserverStatisticsProtos.StatisticsResponse response = rpcCallback.get();
					Map<String, Integer> stats = new LinkedHashMap<>();
					for (ObserverStatisticsProtos.NameInt32Pair pair : response.getAttributeList()) {
						stats.put(pair.getName(), pair.getValue());
					}
					return stats;
				}
		);
		if (print) {
			for (Map.Entry<byte[], Map<String, Integer>> entry : results.entrySet()) {
				LOGGER.info("Region: {}", Bytes.toString(entry.getKey()));
				for (Map.Entry<String, Integer> call : entry.getValue().entrySet()) {
					LOGGER.info("  {}: {}", call.getKey(), call.getValue());
				}
			}
			LOGGER.info("");
		}
	}
}
