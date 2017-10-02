package com.vmturbo.proactivesupport;

import java.util.Collection;
import javax.annotation.Nonnull;

/**
 * The AggregatorBridge implements the bridge from LDCF to the Aggregator.
 * In some cases, it will utilize TCP/IP, in some others, it will utilize
 * local method calls.
 */
public interface AggregatorBridge {
    /**
     * Sends the urgent collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    void sendUrgent(final @Nonnull Collection<DataMetric> messages);

    /**
     * Sends the offline collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    void sendOffline(final @Nonnull Collection<DataMetric> messages);
}
