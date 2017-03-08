package fr.poc.hbase.coprocessor.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RepeatRule implements TestRule {

	@Override
	public Statement apply(Statement statement, Description description) {
		Statement result = statement;
		Repeat repeat = description.getAnnotation(Repeat.class);
		if (repeat != null) {
			int times = repeat.value();
			result = new RepeatStatement(times, statement);
		}
		return result;
	}

	@RequiredArgsConstructor
	private static class RepeatStatement extends Statement {

		private final int times;

		@NonNull
		private final Statement statement;

		@Override
		public void evaluate() throws Throwable {
			for (int i = 0; i < times; i++) {
				statement.evaluate();
			}
		}
	}
}
