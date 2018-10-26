package com.vmturbo.ml.datastore.topology;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriter;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory.InfluxUnavailableException;
import com.vmturbo.ml.datastore.influx.MetricJitter;
import com.vmturbo.ml.datastore.influx.MetricsStoreWhitelist;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyEntitiesListenerTest {

    private final InfluxMetricsWriterFactory connectionFactory = mock(InfluxMetricsWriterFactory.class);
    private final MetricsStoreWhitelist whitelist = mock(MetricsStoreWhitelist.class);
    private final MetricJitter metricJitter = new MetricJitter(false, 0);
    private final InfluxMetricsWriter metricsWriter = mock(InfluxMetricsWriter.class);

    private final TopologyEntitiesListener listener = new TopologyEntitiesListener(connectionFactory,
        whitelist, metricJitter);
    private static final long TIME = 99999L;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(12345L)
        .setTopologyId(333333L)
        .setCreationTime(TIME)
        .build();

    private final TopologyEntityDTO dtoOne = TopologyEntityDTO.newBuilder()
        .setOid(1L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .build();

    private final TopologyEntityDTO dtoTwo = TopologyEntityDTO.newBuilder()
        .setOid(2L)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .build();

    @SuppressWarnings("unchecked")
    private final RemoteIterator<TopologyEntityDTO> iterator =
        (RemoteIterator<TopologyEntityDTO>)mock(RemoteIterator.class);

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
        when(connectionFactory.createMetricsWriter(eq(whitelist), eq(metricJitter)))
            .thenReturn(metricsWriter);

        listener.onTopologyNotification(topologyInfo, iterator);
        verify(metricsWriter).writeTopologyMetrics(eq(Collections.singletonList(dtoOne)), eq(TIME), anyMap(), anyMap());
        verify(metricsWriter).writeTopologyMetrics(eq(Collections.singletonList(dtoTwo)), eq(TIME), anyMap(), anyMap());
    }

    @Test
    public void testWritesFlushed() {
        when(connectionFactory.createMetricsWriter(eq(whitelist), eq(metricJitter)))
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
            .createMetricsWriter(eq(whitelist), eq(metricJitter));
        listener.onTopologyNotification(topologyInfo, iterator);

        // Then influx is available.
        when(connectionFactory.createMetricsWriter(eq(whitelist), eq(metricJitter)))
            .thenReturn(metricsWriter);
        listener.onTopologyNotification(topologyInfo, iterator);
        verify(metricsWriter).writeTopologyMetrics(eq(Collections.singletonList(dtoOne)), eq(TIME), anyMap(), anyMap());
        verify(metricsWriter).writeTopologyMetrics(eq(Collections.singletonList(dtoTwo)), eq(TIME), anyMap(), anyMap());
    }
}