package com.vmturbo.ml.datastore.topology;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.ml.datastore.influx.*;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory.InfluxUnavailableException;
import com.vmturbo.ml.datastore.influx.Obfuscator.HashingObfuscator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyEntitiesListenerTest {

    private final InfluxMetricsWriterFactory connectionFactory = mock(InfluxMetricsWriterFactory.class);
    private final MetricsStoreWhitelist whitelist = mock(MetricsStoreWhitelist.class);
    private final MetricJitter metricJitter = new MetricJitter(false, 0);
    private final InfluxTopologyMetricsWriter metricsWriter = mock(InfluxTopologyMetricsWriter.class);
    private final Obfuscator obfuscator = new HashingObfuscator();

    private final TopologyEntitiesListener listener = new TopologyEntitiesListener(connectionFactory,
        whitelist, metricJitter, obfuscator);
    private static final long TIME = 99999L;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(12345L)
        .setTopologyId(333333L)
        .setCreationTime(TIME)
        .build();

    private final TopologyEntityDTO dtoOneE = TopologyEntityDTO.newBuilder()
        .setOid(1L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .build();

    private final TopologyDTO.Topology.DataSegment
        dtoOne = TopologyDTO.Topology.DataSegment.newBuilder()
                                                       .setEntity(dtoOneE)
                                                       .build();

    private final TopologyEntityDTO dtoTwoE = TopologyEntityDTO.newBuilder()
        .setOid(2L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .build();

    private final TopologyDTO.Topology.DataSegment
        dtoTwo = TopologyDTO.Topology.DataSegment.newBuilder()
                                                 .setEntity(dtoTwoE)
                                                 .build();
    @SuppressWarnings("unchecked")
    private final RemoteIterator<TopologyDTO.Topology.DataSegment> iterator =
        (RemoteIterator<TopologyDTO.Topology.DataSegment>)mock(RemoteIterator.class);

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        when(connectionFactory.getDatabase()).thenReturn("db");
        when(connectionFactory.getRetentionPolicyName()).thenReturn("rp");

        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.nextChunk()).thenReturn(Collections.singletonList(dtoOne), Collections.singletonList(dtoTwo));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMetricsWritten() {
        when(connectionFactory.createTopologyMetricsWriter(eq(whitelist), eq(metricJitter), eq(obfuscator)))
            .thenReturn(metricsWriter);

        listener.onTopologyNotification(topologyInfo, iterator);
        verify(metricsWriter).writeMetrics(eq(Collections.singletonList(dtoOneE)), eq(TIME),
            anyMap(), anyMap(), anyMap());
        verify(metricsWriter).writeMetrics(eq(Collections.singletonList(dtoTwoE)), eq(TIME),
            anyMap(), anyMap(), anyMap());
    }

    @Test
    public void testWritesFlushed() {
        when(connectionFactory.createTopologyMetricsWriter(eq(whitelist), eq(metricJitter), eq(obfuscator)))
            .thenReturn(metricsWriter);

        listener.onTopologyNotification(topologyInfo, iterator);
        verify(metricsWriter).flush();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInfluxConnectionRecreatedOnError() {
        // First make influx unavailable
        doThrow(InfluxUnavailableException.class)
            .when(connectionFactory)
            .createTopologyMetricsWriter(eq(whitelist), eq(metricJitter), eq(obfuscator));
        listener.onTopologyNotification(topologyInfo, iterator);

        // Then influx is available.
        when(connectionFactory.createTopologyMetricsWriter(eq(whitelist), eq(metricJitter), eq(obfuscator)))
            .thenReturn(metricsWriter);
        listener.onTopologyNotification(topologyInfo, iterator);
        verify(metricsWriter).writeMetrics(eq(Collections.singletonList(dtoOneE)), eq(TIME),
            anyMap(), anyMap(), anyMap());
        verify(metricsWriter).writeMetrics(eq(Collections.singletonList(dtoTwoE)), eq(TIME),
            anyMap(), anyMap(), anyMap());
    }
}