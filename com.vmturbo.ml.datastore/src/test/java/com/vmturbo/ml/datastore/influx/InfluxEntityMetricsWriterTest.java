package com.vmturbo.ml.datastore.influx;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist.MetricType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.ml.datastore.influx.Obfuscator.HashingObfuscator;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class InfluxEntityMetricsWriterTest {
    private final MetricsStoreWhitelist metricStoreWhitelist = mock(MetricsStoreWhitelist.class);
    private final MetricJitter metricJitter = new MetricJitter(false, 0);
    private final Obfuscator obfuscator = mock(Obfuscator.class);
    private final InfluxDB influx = mock(InfluxDB.class);

    private InfluxTopologyMetricsWriter metricsWriter;

    private static final long TOPOLOGY_TIME = 999999L;
    private static final long PROVIDER_ID = 12345L;
    private static final long ENTITY_ID = 11111L;
    private final CommodityBoughtDTO.Builder boughtDTO = CommodityBoughtDTO.newBuilder()
        .setActive(true)
        .setCommodityType(CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.CPU.getNumber()))
        .setPeak(1.0)
        .setUsed(2.0);

    private final CommoditiesBoughtFromProvider.Builder boughtFromProvider = CommoditiesBoughtFromProvider.newBuilder()
        .setProviderEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
        .setProviderId(PROVIDER_ID)
        .addCommodityBought(boughtDTO);

    private final CommoditySoldDTO.Builder soldDTO = CommoditySoldDTO.newBuilder()
        .setActive(true)
        .setCommodityType(CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.VMEM.getNumber()))
        .setUsed(4.0)
        .setCapacity(5.0);

    private final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
        .setOid(ENTITY_ID);

    private final Map<String, Long> boughtStatistics = new HashMap<>();
    private final Map<String, Long> soldStatistics = new HashMap<>();
    private final Map<String, Long> clusterStatistics = new HashMap<>();

    private static final String DATABASE = "database";
    private static final String RETENTION_POLICY = "rp";

    @Captor
    private ArgumentCaptor<Point> pointCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        metricsWriter = new InfluxTopologyMetricsWriter(influx, DATABASE, RETENTION_POLICY,
            metricStoreWhitelist, metricJitter, obfuscator);
        when(metricStoreWhitelist.getClusterSupport()).thenReturn(false);
    }

    /**
     * Test that commodities bought can be written in the expected schema.
     */
    @Test
    public void testWriteCommodityBought() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.USED, MetricType.PEAK));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.singleton(CommonDTO.CommodityDTO.CommodityType.CPU.getNumber()));

        entity.addCommoditiesBoughtFromProviders(boughtFromProvider);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Both used and peak stats should be written.
        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), containsString("CPU_PEAK"));
        assertThat(point.toString(), containsString("CPU_USED"));
    }

    @Test
    public void foo() {
        Obfuscator o = new HashingObfuscator();
        System.out.println(o.obfuscate("foobar"));
    }

    /**
     * Test that commodities sold can be written in the expected schema.
     */
    @Test
    public void testWriteCommoditySold() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.USED, MetricType.PEAK, MetricType.CAPACITY));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(ImmutableSet.of(CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber(),
                CommodityDTO.CommodityType.VMEM.getNumber()));

        entity.addCommoditySoldList(soldDTO);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // VMEM USED and CAPACITY should be written. PEAK should not because it is not on the
        // commodity.
        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), containsString("VMEM_USED"));
        assertThat(point.toString(), containsString("VMEM_CAPACITY"));
        assertThat(point.toString(), not(containsString("VMEM_PEAK")));
    }

    /**
     * Test that commodities bought not on the commodity whitelist are not written.
     */
    @Test
    public void testBoughtNotOnCommodityTypeWhitelist() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.USED, MetricType.PEAK, MetricType.CAPACITY));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.singleton(CommonDTO.CommodityDTO.CommodityType.MEM.getNumber()));

        entity.addCommoditiesBoughtFromProviders(boughtFromProvider);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // No available metrics are on the whitelist so nothing should be written.
        verifyNoMoreInteractions(influx);
    }

    /**
     * Test that commodities bought not on the metric whitelist are not written.
     */
    @Test
    public void testBoughtNotOnMetricTypeWhitelist() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(Collections.singleton(MetricType.PEAK));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.singleton(CommonDTO.CommodityDTO.CommodityType.CPU.getNumber()));

        entity.addCommoditiesBoughtFromProviders(boughtFromProvider);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Only peak should be written.
        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), containsString("CPU_PEAK"));
        assertThat(point.toString(), not(containsString("CPU_USED")));
    }

    /**
     * Test that commodities sold not on the commodity whitelist are not written.
     */
    @Test
    public void testSoldNotOnCommodityTypeWhitelist() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.USED, MetricType.PEAK, MetricType.CAPACITY));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.singleton(CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber()));

        entity.addCommoditySoldList(soldDTO);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        verifyNoMoreInteractions(influx);
    }

    /**
     * Test that commodities sold not on the metric type whitelist are not written.
     */
    @Test
    public void testSoldNotOnMetricTypeWhitelist() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.CAPACITY));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(ImmutableSet.of(CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber(),
                CommodityDTO.CommodityType.VMEM.getNumber()));

        entity.addCommoditySoldList(soldDTO);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // VMEM USED and CAPACITY should be written. PEAK should not because it is not on the
        // commodity.
        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), not(containsString("VMEM_USED")));
        assertThat(point.toString(), containsString("VMEM_CAPACITY"));
        assertThat(point.toString(), not(containsString("VMEM_PEAK")));
    }

    /**
     * Test that inactive commodities bought are are not written.
     */
    @Test
    public void testBoughtNotActive() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.USED, MetricType.PEAK));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.singleton(CommonDTO.CommodityDTO.CommodityType.CPU.getNumber()));

        boughtFromProvider.getCommodityBoughtBuilder(0).setActive(false);
        entity.addCommoditiesBoughtFromProviders(boughtFromProvider);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Inactive metrics should not be written.
        verifyNoMoreInteractions(influx);
    }

    /**
     * Test that inactive commodities sold are are not written.
     */
    @Test
    public void testSoldNotActive() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(ImmutableSet.of(MetricType.USED, MetricType.PEAK, MetricType.CAPACITY));
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(ImmutableSet.of(CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber(),
                CommodityDTO.CommodityType.VMEM.getNumber()));

        soldDTO.setActive(false);
        entity.addCommoditySoldList(soldDTO);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Inactive metrics should not be written.
        verifyNoMoreInteractions(influx);
    }

    @Test
    public void testWriteComputeClusterSold() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(Collections.emptySet());
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.emptySet());
        when(metricStoreWhitelist.getClusterSupport()).thenReturn(true);
        when(obfuscator.obfuscate("foo")).thenReturn("bar");

        final CommoditySoldDTO.Builder soldDTO = CommoditySoldDTO.newBuilder()
            .setActive(true)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CLUSTER.getNumber())
                .setKey("foo"));

        entity.addCommoditySoldList(soldDTO);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Inactive metrics should not be written.
        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), containsString("cluster_membership"));
        assertThat(point.toString(), containsString("VIRTUAL_MACHINE"));
        assertThat(point.toString(), containsString("11111"));
        assertThat(point.toString(), containsString("COMPUTE_CLUSTER_SOLD=bar"));
    }

    @Test
    public void testWriteStorageClusterBought() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(Collections.emptySet());
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.emptySet());
        when(metricStoreWhitelist.getClusterSupport()).thenReturn(true);
        when(obfuscator.obfuscate("foo")).thenReturn("bar");

        boughtFromProvider.getCommodityBoughtBuilder(0)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_CLUSTER.getNumber())
                .setKey("foo"));

        entity.addCommoditiesBoughtFromProviders(boughtFromProvider);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Inactive metrics should not be written.
        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), containsString("cluster_membership"));
        assertThat(point.toString(), containsString("VIRTUAL_MACHINE"));
        assertThat(point.toString(), containsString("11111"));
        assertThat(point.toString(), containsString("STORAGE_CLUSTER_BOUGHT=bar"));
    }

    @Test
    public void testClusterNotWrittenIfNotEnabled() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE))
            .thenReturn(Collections.emptySet());
        when(metricStoreWhitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE))
            .thenReturn(Collections.emptySet());
        when(metricStoreWhitelist.getClusterSupport()).thenReturn(false);
        when(obfuscator.obfuscate("foo")).thenReturn("bar");

        final CommoditySoldDTO.Builder soldDTO = CommoditySoldDTO.newBuilder()
            .setActive(true)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CLUSTER.getNumber())
                .setKey("foo"));

        entity.addCommoditySoldList(soldDTO);
        metricsWriter.writeMetrics(Collections.singleton(entity.build()),
            TOPOLOGY_TIME, boughtStatistics, soldStatistics, clusterStatistics);

        // Inactive metrics should not be written.
        verifyNoMoreInteractions(influx);
    }

    /**
     * Test that flushing the metrics writer flushes the underlying influx connection.
     */
    @Test
    public void testFlush() {
        metricsWriter.flush();
        verify(influx).flush();
    }

    /**
     * Test that closing the metrics writer closes the underlying influx connection.
     */
    @Test
    public void testClose() throws Exception {
        metricsWriter.close();
        verify(influx).close();
    }
}