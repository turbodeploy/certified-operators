package com.vmturbo.clustermgr;

import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nonnull;

import com.vmturbo.clustermgr.aggregator.DataAggregator;
import com.vmturbo.proactivesupport.AggregatorBridge;
import com.vmturbo.proactivesupport.DataMetric;
import org.springframework.stereotype.Component;

/**
 * The LocalAggregatorBridge implements in-memory aggregator bridge.
 * We implement it here because of the LDCF inside the Cluster Manager.
 */
public class LocalAggregatorBridge implements AggregatorBridge {
    /**
     * The data aggregator.
     *
     * @param dataAggregator The data aggregator.
     */
    private final DataAggregator dataAggregator_;

    /**
     * Constructs the local aggregator bridge.
     *
     * @param dataAggregator The data aggregator.
     */
    public LocalAggregatorBridge(final @Nonnull DataAggregator dataAggregator) {
        dataAggregator_ = Objects.requireNonNull(dataAggregator);
    }

    /**
     * Sends the urgent collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    @Override
    public void sendUrgent(@Nonnull Collection<DataMetric> messages) {
    }

    /**
     * Sends the offline collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    @Override
    public void sendOffline(final @Nonnull Collection<DataMetric> messages) {
        dataAggregator_.receiveLocalOffline(messages);
    }
}
