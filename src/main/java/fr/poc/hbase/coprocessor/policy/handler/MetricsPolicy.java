package fr.poc.hbase.coprocessor.policy.handler;

import fr.poc.hbase.coprocessor.policy.PolicyHandler;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Hadoop Metrics2 instrumentation policy
 */
@RequiredArgsConstructor
public class MetricsPolicy implements PolicyHandler {

	/**
	 * Metric system where metrics will be registered
	 */
	@NonNull
	private final MetricsSystem metricsSystem;

	/**
	 * Metric context name
	 */
	@NonNull
	private final String metricsContext;


	/**
	 * Metrics tmp store
	 */
	private final ConcurrentMap<String, MetricInfo> metrics = new ConcurrentHashMap<>();

	@Override
	public <T> void beforeRun(@NonNull T object, @NonNull String method, @NonNull Object[] args) throws IOException {
		String metricName = getMetricName(object, method);

		if (!metrics.containsKey(metricName)) {
			metrics.put(metricName, metricsSystem.register(metricName, "", new MetricInfo(metricName)));
		}
	}

	@Override
	public <T> void onError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull IOException ioException) {
		metrics.get(getMetricName(object, method)).addError();
	}

	@Override
	public <T> void onUnexpectedError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Throwable throwable) {
		metrics.get(getMetricName(object, method)).addUnexpectedError();
	}

	@Override
	public <T> void afterRun(@NonNull T object, @NonNull String method, @NonNull Object[] args, Object result, long executionTime) {
		metrics.get(getMetricName(object, method)).getExecutionStats().addValue(TimeUnit.NANOSECONDS.toMillis(executionTime));
	}

	/**
	 * Compute metric name
	 *
	 * @param object proxied object
	 * @param method currently executed method
	 * @param <T>    type of object
	 * @return the metric name
	 */
	private <T> String getMetricName(@NonNull T object, @NonNull String method) {
		return method.replaceAll("[:()]", "-") + ",sub=" + metricsContext + ",coprocessor=" + object.getClass().getSimpleName();
	}

	/**
	 * Metric informations
	 */
	@Getter
	@RequiredArgsConstructor
	private static final class MetricInfo implements MetricsSource {

		/**
		 * Metric name
		 */
		private final String name;

		/**
		 * Execution statistics
		 */
		private final SummaryStatistics executionStats = new SummaryStatistics();

		/**
		 * error count
		 */
		private long error = 0L;

		/**
		 * unexpected error count
		 */
		private long unexpectedError = 0L;

		/**
		 * Add an unexpected error
		 */
		public void addUnexpectedError() {
			++unexpectedError;
		}

		/**
		 * Add an error
		 */
		public void addError() {
			++error;
		}

		@Override
		public void getMetrics(@NonNull MetricsCollector collector, boolean all) {
			// Fetch stats
			StatisticalSummary stats = executionStats.getSummary();

			// Report metric
			collector.addRecord(name)
					.addCounter(info("ErrorCount", "Number of coprocessor method that end with an error"), error)
					.addCounter(info("UncatchedErrorsCount", "Number of coprocessor method that end with an uncatched error"), unexpectedError)
					.addCounter(info("Count", "Number of coprocessor method count"), stats.getN())
					.addGauge(info("ErrorRatio", "Ration of execution error"), stats.getN() == 0 ? 0 : (error + unexpectedError) / stats.getN())
					.addGauge(info("Min", "Minimum execution time of coprocessor method"), stats.getMin())
					.addGauge(info("Max", "Maximum execution time of coprocessor method"), stats.getMax())
					.addGauge(info("Avg", "Average execution time of coprocessor method"), stats.getMean())
					.addGauge(info("Std", "Standard deviation in coprocessor execution time"), stats.getStandardDeviation());
		}
	}

}
