package com.vmturbo.clustermgr;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.vmturbo.clustermgr.aggregator.DataAggregator;
import com.vmturbo.proactivesupport.DataMetric;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the LocalAggregatorBridge.
 */
public class LocalAggregatorBridgeTest {
    @Test
    public void testSendOffline() {
        DataAggregator aggregator = Mockito.mock(DataAggregator.class);
        LocalAggregatorBridge bridge = new LocalAggregatorBridge(aggregator);
        Collection<DataMetric> messages = ImmutableList.of(Mockito.mock(DataMetric.class));
        bridge.sendOffline(messages);
        Mockito.verify(aggregator, Mockito.times(1)).receiveLocalOffline(messages);
    }
}
