package org.dia.benchmark.kafka;

import org.dia.benchmark.kafka.Configuration;

/**
 * Interface used to support aggregation of measured data.
 *
 * @starchmd
 */
public interface Aggregator {
	/**
	 * Setup function to supply configuration
	 * @param config - configuration object
	 */
	public void setup(Configuration config);
    /**
     * Start the aggregator and its internal aggregation.
     */
    public void start();

    /**
     * Stop the aggregator and report the message count.
     * @return - number of messages counted.
     */
    public long stop();
}
