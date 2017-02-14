package fr.poc.hbase.coprocessor.policy.util;

import java.io.IOException;

/**
 * A {@link Runnable} that could be throws {@link IOException}
 */
@FunctionalInterface
public interface RunnableWithIOException {

	/**
	 * When an object implementing interface <code>Runnable</code> is used
	 * to create a thread, starting the thread causes the object's
	 * <code>run</code> method to be called in that separately executing
	 * thread.
	 * <p>
	 * The general contract of the method <code>run</code> is that it may
	 * take any action whatsoever.
	 *
	 * @throws IOException could be thrown
	 * @see java.lang.Runnable#run()
	 */
	void run() throws IOException;
}
