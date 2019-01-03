package com.vmturbo.ml.datastore.influx;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist.ActionState;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionTypeWhitelist.ActionType;
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
    private final Set<ActionType> defaultActionTypes = ImmutableSet
            .of(ActionType.ACTIVATE, ActionType.PROVISION);
    private final Set<ActionState> defaultActionStates = ImmutableSet
            .of(ActionState.RECOMMENDED_ACTIONS);
    private final boolean defaultClustersSupported = true;
    Map<MetricsStoreWhitelist.WhitelistType<?>, Set<? extends Enum<?>>> defaultWhitelists = new HashMap<>();

    private final KeyValueStore kvStore = spy(new MapKeyValueStore());

    private final Gson gson = new Gson();

    @Test
    public void testSetWhitelistCommodityTypes() {
        defaultWhitelists.put(MetricsStoreWhitelist.COMMODITY_TYPE, defaultCommodityTypes);
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists, defaultClustersSupported, kvStore);
        whitelist.storeWhitelist(Collections.singleton(CommodityType.VMEM), MetricsStoreWhitelist.COMMODITY_TYPE);

        assertEquals(Collections.singleton(CommodityType.VMEM), whitelist.getWhitelist(MetricsStoreWhitelist.COMMODITY_TYPE));
        verify(kvStore).put(anyString(), eq("[\"VMEM\"]"));
    }

    @Test
    public void testSetWhitelistMetricTypes() {
        defaultWhitelists.put(MetricsStoreWhitelist.METRIC_TYPE, defaultMetricTypes);
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists,
                defaultClustersSupported, kvStore);
        whitelist.storeWhitelist(Collections.singleton(MetricType.PEAK), MetricsStoreWhitelist.METRIC_TYPE);

        assertEquals(Collections.singleton(MetricType.PEAK), whitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE));
        verify(kvStore).put(anyString(), eq("[\"PEAK\"]"));
    }

    @Test
    public void testSetWhitelistActionTypes() {
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_TYPE, defaultActionTypes);
        final MetricsStoreWhitelist whitelist =
                new MetricsStoreWhitelist(defaultWhitelists,
                        defaultClustersSupported, kvStore);
        whitelist.storeWhitelist(Collections.singleton(ActionType.MOVE), MetricsStoreWhitelist.ACTION_TYPE);

        assertEquals(Collections.singleton(ActionType.MOVE), whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE));
        verify(kvStore).put(anyString(), eq("[\"MOVE\"]"));
    }

    @Test
    public void testSetWhitelistActionStates() {
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_STATE, defaultActionStates);
        final MetricsStoreWhitelist whitelist =
                new MetricsStoreWhitelist(defaultWhitelists,
                        defaultClustersSupported, kvStore);
        whitelist.storeWhitelist(Collections.singleton(ActionState.COMPLETED_ACTIONS), MetricsStoreWhitelist.ACTION_STATE);

        assertEquals(Collections.singleton(ActionState.COMPLETED_ACTIONS), whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE));
        verify(kvStore).put(anyString(), eq("[\"COMPLETED_ACTIONS\"]"));
    }

    @Test
    public void testLoadCommodityTypeOverride() {
        defaultWhitelists.put(MetricsStoreWhitelist.COMMODITY_TYPE, defaultCommodityTypes);
        final Set<CommodityType> overrides = ImmutableSet.of(CommodityType.BALLOONING, CommodityType.SWAPPING);
        kvStore.put(MetricsStoreWhitelist.keyPath(MetricsStoreWhitelist.COMMODITY_TYPES_KEY),
            gson.toJson(overrides));

        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists,
                defaultClustersSupported, kvStore);
        assertEquals(overrides, whitelist.getWhitelist(MetricsStoreWhitelist.COMMODITY_TYPE));
    }

    @Test
    public void testLoadMetricTypeOverride() {
        defaultWhitelists.put(MetricsStoreWhitelist.METRIC_TYPE, defaultMetricTypes);
        final Set<MetricType> overrides = ImmutableSet.of(MetricType.PEAK, MetricType.SCALING_FACTOR);
        kvStore.put(MetricsStoreWhitelist.keyPath(MetricsStoreWhitelist.METRIC_TYPES_KEY),
            gson.toJson(overrides));

        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists,
                defaultClustersSupported, kvStore);
        assertEquals(overrides, whitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE));
    }

    @Test
    public void testLoadActionTypeOverride() {
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_TYPE, defaultMetricTypes);
        final Set<ActionType> overrides = ImmutableSet.of(ActionType.DEACTIVATE);
        kvStore.put(MetricsStoreWhitelist.keyPath(MetricsStoreWhitelist.ACTION_TYPES_KEY),
                gson.toJson(overrides));

        final MetricsStoreWhitelist whitelist =
                new MetricsStoreWhitelist(defaultWhitelists,
                        defaultClustersSupported, kvStore);
        assertEquals(overrides, whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE));
    }

    @Test
    public void testLoadActionStateOverride() {
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_STATE, defaultMetricTypes);
        final Set<ActionState> overrides = ImmutableSet.of(ActionState.COMPLETED_ACTIONS);
        kvStore.put(MetricsStoreWhitelist.keyPath(MetricsStoreWhitelist.ACTION_STATE_KEY),
                gson.toJson(overrides));

        final MetricsStoreWhitelist whitelist =
                new MetricsStoreWhitelist(defaultWhitelists,
                        defaultClustersSupported, kvStore);
        assertEquals(overrides, whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE));
    }

    @Test
    public void testGetDefaultWhitelistCommodityTypeNumbers() {
        defaultWhitelists.put(MetricsStoreWhitelist.COMMODITY_TYPE, defaultCommodityTypes);
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists,
                defaultClustersSupported, kvStore);
        assertEquals(ImmutableSet.of(CommodityType.BALLOONING.getNumber(),
            CommodityType.VCPU.getNumber()), whitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE));

        whitelist.storeWhitelist(Collections.singleton(CommodityType.VMEM), MetricsStoreWhitelist.COMMODITY_TYPE);
        assertEquals(Collections.singleton(CommodityType.VMEM.getNumber()),
            whitelist.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE));
    }

    @Test
    public void testGetDefaultWhitelistCommodityTypes() {
        defaultWhitelists.put(MetricsStoreWhitelist.COMMODITY_TYPE, defaultCommodityTypes);
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists,
                defaultClustersSupported, kvStore);
        assertEquals(defaultCommodityTypes, whitelist.getWhitelist(MetricsStoreWhitelist.COMMODITY_TYPE));

        whitelist.storeWhitelist(Collections.singleton(CommodityType.VMEM), MetricsStoreWhitelist.COMMODITY_TYPE);
        assertEquals(Collections.singleton(CommodityType.VMEM), whitelist.getWhitelist(MetricsStoreWhitelist.COMMODITY_TYPE));
    }

    @Test
    public void testGetDefaultWhitelistMetricTypes() {
        defaultWhitelists.put(MetricsStoreWhitelist.METRIC_TYPE, defaultMetricTypes);
        final MetricsStoreWhitelist whitelist =
            new MetricsStoreWhitelist(defaultWhitelists,
                defaultClustersSupported, kvStore);
        assertEquals(defaultMetricTypes, whitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE));

        whitelist.storeWhitelist(Collections.singleton(MetricType.PEAK), MetricsStoreWhitelist.METRIC_TYPE);
        assertEquals(Collections.singleton(MetricType.PEAK), whitelist.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE));
    }

    @Test
    public void testGetDefaultWhitelistActionTypes() {
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_TYPE, defaultActionTypes);
        final MetricsStoreWhitelist whitelist =
                new MetricsStoreWhitelist(defaultWhitelists,
                        defaultClustersSupported, kvStore);
        assertEquals(defaultActionTypes, whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE));

        whitelist.storeWhitelist(Collections.singleton(ActionType.MOVE), MetricsStoreWhitelist.ACTION_TYPE);
        assertEquals(Collections.singleton(ActionType.MOVE), whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE));
    }

    @Test
    public void testGetDefaultWhitelistActionStates() {
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_STATE, defaultActionStates);
        final MetricsStoreWhitelist whitelist =
                new MetricsStoreWhitelist(defaultWhitelists,
                        defaultClustersSupported, kvStore);
        assertEquals(defaultActionStates, whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE));

        whitelist.storeWhitelist(Collections.singleton(ActionState.RECOMMENDED_ACTIONS), MetricsStoreWhitelist.ACTION_STATE);
        assertEquals(Collections.singleton(ActionState.RECOMMENDED_ACTIONS), whitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE));
    }
}