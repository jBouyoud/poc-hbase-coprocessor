package fr.poc.hbase.coprocessor.policy.impl;

import fr.poc.hbase.coprocessor.policy.Policy;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
public class MetricsPolicy implements Policy {

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
		String metricName = getMetricName(object, method, args.length >= 1 ? args[0] : null);

		if (!metrics.containsKey(metricName)) {
			MetricsSource source = metricsSystem.getSource(metricName);
			if (source != null && source instanceof MetricInfo) {
				metrics.put(metricName, (MetricInfo) source);
			} else {
				metrics.put(metricName, new MetricInfo(metricName));
				metricsSystem.register(metricName, "", metrics.get(metricName));
			}
		}
	}

	@Override
	public <T> void onError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull IOException ioException) {
		metrics.get(getMetricName(object, method, args.length >= 1 ? args[0] : null)).addError();
	}

	@Override
	public <T> void onUnexpectedError(@NonNull T object, @NonNull String method, @NonNull Object[] args, @NonNull Throwable throwable) {
		metrics.get(getMetricName(object, method, args.length >= 1 ? args[0] : null)).addUnexpectedError();
	}

	@Override
	public <T> void afterRun(@NonNull T object, @NonNull String method, @NonNull Object[] args, Object result, long executionTime) {
		metrics.get(getMetricName(object, method, args.length >= 1 ? args[0] : null)).getExecutionStats().addValue(TimeUnit.NANOSECONDS.toMillis(executionTime));
	}

	/**
	 * Compute metric name
	 *
	 * @param object proxied object
	 * @param method currently executed method
	 * @param <T>    type of object
	 * @return the metric name
	 */
	private <T> String getMetricName(@NonNull T object, @NonNull String method, Object arg) {
		String on = null;
		if (arg != null) {
			CoprocessorEnvironment env = null;
			if (arg instanceof CoprocessorEnvironment) {
				env = (CoprocessorEnvironment) arg;
			} else if (arg instanceof ObserverContext) {
				env = ((ObserverContext) arg).getEnvironment();
			}
			if (env != null && env instanceof RegionCoprocessorEnvironment) {
				on = ((RegionCoprocessorEnvironment) env).getRegionInfo().getRegionNameAsString();
			}
		}
		String name = method.replaceAll("[:()]", "-") + ",sub=" + metricsContext + ",coprocessor=" + object.getClass().getSimpleName();
		if (on != null) {
			name += ",on=" + on;
		}
		return name;
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
		@NonNull
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
