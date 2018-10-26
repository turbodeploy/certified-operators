package com.vmturbo.ml.datastore.influx;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist.MetricType;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class MetricsStoreWhitelistTest {

    private final Set<MetricType> defaultMetricTypes = ImmutableSet
        .of(MetricType.USED, MetricType.CAPACITY);
    private final Set<CommodityType> defaultCommodityTypes = ImmutableSet
        .of(CommodityType.BALLOONING, CommodityType.VCPU);
    private final KeyValueStore kvStore = spy(new MapKeyValueStore());

    private final Gson gson = new Gson();

    @Test
    public void testSetWhitelistCommodityTypes() {
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        whitelist.setWhitelistCommodityTypes(Collections.singleton(CommodityType.VMEM));

        assertEquals(Collections.singleton(CommodityType.VMEM), whitelist.getWhitelistCommodityTypes());
        verify(kvStore).put(anyString(), eq("[\"VMEM\"]"));
    }

    @Test
    public void testSetWhitelistMetricTypes() {
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        whitelist.setWhitelistMetricTypes(Collections.singleton(MetricType.PEAK));

        assertEquals(Collections.singleton(MetricType.PEAK), whitelist.getWhitelistMetricTypes());
        verify(kvStore).put(anyString(), eq("[\"PEAK\"]"));
    }

    @Test
    public void testLoadCommodityTypeOverride() {
        final Set<CommodityType> overrides = ImmutableSet.of(CommodityType.BALLOONING, CommodityType.SWAPPING);
        kvStore.put(MetricsStoreWhitelist.keyPath(MetricsStoreWhitelist.COMMODITY_TYPES_KEY),
            gson.toJson(overrides));

        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        assertEquals(overrides, whitelist.getWhitelistCommodityTypes());
    }

    @Test
    public void testLoadMetricTypeOverride() {
        final Set<MetricType> overrides = ImmutableSet.of(MetricType.PEAK, MetricType.SCALING_FACTOR);
        kvStore.put(MetricsStoreWhitelist.keyPath(MetricsStoreWhitelist.METRIC_TYPES_KEY),
            gson.toJson(overrides));

        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        assertEquals(overrides, whitelist.getWhitelistMetricTypes());
    }

    @Test
    public void testGetDefaultWhitelistCommodityTypeNumbers() {
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        assertEquals(ImmutableSet.of(CommodityType.BALLOONING.getNumber(),
            CommodityType.VCPU.getNumber()), whitelist.getWhitelistCommodityTypeNumbers());

        whitelist.setWhitelistCommodityTypes(Collections.singleton(CommodityType.VMEM));
        assertEquals(Collections.singleton(CommodityType.VMEM.getNumber()),
            whitelist.getWhitelistCommodityTypeNumbers());
    }

    @Test
    public void testGetDefaultWhitelistCommodityTypes() {
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        assertEquals(defaultCommodityTypes, whitelist.getWhitelistCommodityTypes());

        whitelist.setWhitelistCommodityTypes(Collections.singleton(CommodityType.VMEM));
        assertEquals(Collections.singleton(CommodityType.VMEM), whitelist.getWhitelistCommodityTypes());
    }

    @Test
    public void testGetDefaultWhitelistMetricTypes() {
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultCommodityTypes, defaultMetricTypes, kvStore);
        assertEquals(defaultMetricTypes, whitelist.getWhitelistMetricTypes());

        whitelist.setWhitelistMetricTypes(Collections.singleton(MetricType.PEAK));
        assertEquals(Collections.singleton(MetricType.PEAK), whitelist.getWhitelistMetricTypes());
    }
}