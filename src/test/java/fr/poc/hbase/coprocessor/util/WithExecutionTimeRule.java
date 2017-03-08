package fr.poc.hbase.coprocessor.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.MarkerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Simple Junit rule to log test execution time
 */
@Slf4j
public class WithExecutionTimeRule implements TestRule {

	@Override
	public Statement apply(Statement statement, Description description) {
		Statement result = statement;
		WithExecutionTime withExecutionTime = description.getAnnotation(WithExecutionTime.class);
		if (withExecutionTime != null) {
			String key = description.getClassName() + "." + description.getMethodName();
			result = new WithExecutionTimeRule.WithExecutionTimeStatement(statement, description);
		}
		return result;
	}

	@RequiredArgsConstructor
	private static class WithExecutionTimeStatement extends Statement {

		@NonNull
		private final Statement statement;

		@NonNull
		private final Description description;


		@Override
		public void evaluate() throws Throwable {
			long start = System.nanoTime();
			statement.evaluate();
			long end = System.nanoTime();
			LOGGER.info(MarkerFactory.getMarker("TEST_EXECUTION_TIME"), "Test [{}.{}] executed in [{}]ms",
					description.getClassName(), description.getMethodName(),
					TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));
		}
	}
}
