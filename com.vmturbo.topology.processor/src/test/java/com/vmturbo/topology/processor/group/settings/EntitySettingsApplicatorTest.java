package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionMode;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Unit test for {@link EntitySettingsApplicator}.
 */
public class EntitySettingsApplicatorTest {

    private static final long PARENT_ID = 23345;
    private static final long DEFAULT_SETTING_ID = 23346;

    private static final Setting MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
            .build();

    private static final Setting MOVE_MANUAL_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))
                    .build();

    private static final Setting STORAGE_MOVE_MANUAL_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.StorageMove.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))
                    .build();

    private static final Setting MOVE_AUTOMATIC_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()))
                    .build();

    private static final Setting STORAGE_MOVE_AUTOMATIC_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.StorageMove.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()))
                    .build();

    private static final Setting STORAGE_MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.StorageMove.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("DISABLED"))
            .build();

    private static final Setting.Builder REZISE_SETTING_BUILDER = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());

    private static final TopologyEntityDTO PARENT_OBJECT =
            TopologyEntityDTO.newBuilder().setOid(PARENT_ID).setEntityType(100001).build();

    private EntitySettingsApplicator applicator;

    private final TopologyInfo TOPOLOGY_INFO = TopologyInfo.getDefaultInstance();

    private final TopologyInfo CLUSTER_HEADROOM_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
            .setTopologyType(TopologyType.PLAN)
            .build();

    @java.lang.SuppressWarnings("unchecked")
    private final StitchingJournal<TopologyEntity> stitchingJournal = Mockito.mock(StitchingJournal.class);

    @Before
    public void init() {
        applicator = new EntitySettingsApplicator();
    }

    /**
     * Tests application of resize setting with MANUAL value, commodities shouldn't be changed.
     */
    @Test
    public void testResizeSettingEnabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false));
        REZISE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.MANUAL.name()));
        applySettings(TOPOLOGY_INFO, entity, REZISE_SETTING_BUILDER.build());
        Assert.assertEquals(true, entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertEquals(false, entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Tests application of resize setting with DISABLED value. Commodities which have isResizeable
     * true should be shange, commodities with isResizeable=false shouldn't be changed.
     */
    @Test
    public void testResizeSettingDisabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false));
        REZISE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.DISABLED.name()));
        applySettings(TOPOLOGY_INFO, entity, REZISE_SETTING_BUILDER.build());
        Assert.assertEquals(false, entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertEquals(false, entity.getCommoditySoldList(1).getIsResizeable());
    }

    private TopologyEntityDTO.Builder getEntityForResizeableTest(final List<Boolean> commoditiesResizeable) {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder();
        for (boolean isResizeable : commoditiesResizeable) {
            entity.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1).build())
                    .setIsResizeable(isResizeable).build());
        }
        return entity;
    }

    /**
     * Tests move setting application. The result topology entity has to be marked not movable.
     */
    @Test
    public void testMoveApplicatorForVM() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    @Test
    public void testShopTogetherApplicator() {
        final TopologyEntityDTO.Builder pmSNMNotSupport = TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(false))
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(PARENT_ID)
                                .setProviderEntityType(EntityType.DATACENTER_VALUE)
                                .setMovable(true));
        applySettings(TOPOLOGY_INFO, pmSNMNotSupport, MOVE_MANUAL_SETTING);
        assertThat(pmSNMNotSupport.getAnalysisSettings().getShopTogether(), is(false));

        final TopologyEntityDTO.Builder pmSNMSupport = TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(true))
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(PARENT_ID)
                                .setProviderEntityType(EntityType.DATACENTER_VALUE)
                                .setMovable(true));
        applySettings(TOPOLOGY_INFO, pmSNMSupport, MOVE_MANUAL_SETTING);
        assertThat(pmSNMSupport.getAnalysisSettings().getShopTogether(), is(true));

        final TopologyEntityDTO.Builder vmSNMNotSupported = TopologyEntityDTO.newBuilder()
                .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(false))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, vmSNMNotSupported, MOVE_MANUAL_SETTING, STORAGE_MOVE_MANUAL_SETTING);
        assertThat(vmSNMNotSupported.getAnalysisSettings().getShopTogether(), is(false));

        final TopologyEntityDTO.Builder vmSNMSupported = TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(true))
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(PARENT_ID)
                                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setMovable(true));
        applySettings(TOPOLOGY_INFO, vmSNMSupported, MOVE_AUTOMATIC_SETTING, STORAGE_MOVE_AUTOMATIC_SETTING);
        assertThat(vmSNMSupported.getAnalysisSettings().getShopTogether(), is(true));

        final TopologyEntityDTO.Builder vmSNMSupported2 = TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(true))
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(PARENT_ID)
                                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setMovable(true));
        applySettings(TOPOLOGY_INFO, vmSNMSupported2, MOVE_MANUAL_SETTING, STORAGE_MOVE_AUTOMATIC_SETTING);
        assertThat(vmSNMSupported2.getAnalysisSettings().getShopTogether(), is(false));

        final TopologyEntityDTO.Builder vmSNMSupported3 = TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(true))
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(PARENT_ID)
                                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setMovable(true));
                applySettings(TOPOLOGY_INFO, vmSNMSupported3, MOVE_MANUAL_SETTING, STORAGE_MOVE_MANUAL_SETTING);
                assertThat(vmSNMSupported3.getAnalysisSettings().getShopTogether(), is(true));
    }

    /**
     * Tests move setting application without setting provider for the bought commodity.
     * The result topology entity has to remain marked as movable.
     */
    @Test
    public void testMoveApplicatorNoProviderForVM() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder().setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
    }

    @Test
    public void testMovaApplicatorForStorage() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.DISK_ARRAY_VALUE)
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    @Test
    public void testMovaApplicatorNoProviderForStorage() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
    }

    @Test
    public void testStorageMoveApplicator() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.STORAGE.getNumber())
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, STORAGE_MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    @Test
    public void testStorageMoveApplicatorNoProvider() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder().setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, STORAGE_MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
    }

    /**
     * Tests application of utilization threshold entity settings.
     */
    @Test
    public void testUtilizationThresholdSettings() {
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.CPU,
                EntitySettingSpecs.CpuUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.MEM,
                EntitySettingSpecs.MemoryUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.IO_THROUGHPUT,
                EntitySettingSpecs.IoThroughput);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.NET_THROUGHPUT,
                EntitySettingSpecs.NetThroughput);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.SWAPPING,
                EntitySettingSpecs.SwappingUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.QN_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);

        testUtilizationSettings(EntityType.SWITCH, CommodityType.NET_THROUGHPUT,
                EntitySettingSpecs.NetThroughput);

        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_AMOUNT,
                EntitySettingSpecs.StorageAmountUtilization);
        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_ACCESS,
                EntitySettingSpecs.IopsUtilization);
        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_LATENCY,
                EntitySettingSpecs.LatencyUtilization);

        testUtilizationSettings(EntityType.DISK_ARRAY, CommodityType.STORAGE_AMOUNT,
            EntitySettingSpecs.StorageAmountUtilization);

        testUtilizationSettings(EntityType.STORAGE_CONTROLLER, CommodityType.STORAGE_AMOUNT,
                EntitySettingSpecs.StorageAmountUtilization);
        testUtilizationSettings(EntityType.STORAGE_CONTROLLER, CommodityType.CPU,
                EntitySettingSpecs.CpuUtilization);
    }

    private TopologyEntityDTO.Builder createEntityWithCommodity(@Nonnull EntityType entityType,
            @Nonnull CommodityType commodityType, float initialEffectiveCapacity) {
        final TopologyEntityDTO.Builder builder =
                TopologyEntityDTO.newBuilder().setEntityType(entityType.getNumber()).setOid(1);

        builder.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(commodityType.getNumber())
                        .build())
                .setCapacity(1000)
                .setEffectiveCapacityPercentage(initialEffectiveCapacity));
        return builder;
    }

    private TopologyEntityDTO.Builder createEntityWithCommodity(@Nonnull EntityType entityType,
            @Nonnull CommodityType commodityType) {
        final TopologyEntityDTO.Builder builder =
                TopologyEntityDTO.newBuilder().setEntityType(entityType.getNumber()).setOid(1);

        builder.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(commodityType.getNumber())
                        .build())
                .setCapacity(1000));
        return builder;
    }

    private void testUtilizationSettings(EntityType entityType, CommodityType commodityType,
            EntitySettingSpecs setting) {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(entityType, commodityType, 100f);
        final Setting settingObj = Setting.newBuilder()
                .setSettingSpecName(setting.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(57f).build())
                .build();
        applySettings(TOPOLOGY_INFO, builder, settingObj);
        Assert.assertEquals(57, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.00001f);
    }

    /**
     * Tests not settings applied. Effective capacity is expected not to be changed.
     */
    @Test
    public void testNoSettings() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 123f);
        applySettings(TOPOLOGY_INFO, builder);
        Assert.assertEquals(123, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.00001f);
    }

    /**
     * Tests effective capacity of the commodity when ignoring HA and Utilization Threshold setting.
     * It is expected that value is sourced from setting.
     */
    @Test
    public void testHaUtilOverrride() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.CpuUtilization, 22f),
                createSetting(EntitySettingSpecs.IgnoreHA, true));
        Assert.assertEquals(22f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Tests effective capacity of the commodity when not ignoring HA and having Utilization
     * Threshold setting.
     * It is expected that value is sourced from probe.
     */
    @Test
    public void testNoHaNoUtilOverride() {
        final TopologyEntityDTO.Builder entity =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        final Setting setting = createSetting(EntitySettingSpecs.IgnoreHA, false);
        applySettings(TOPOLOGY_INFO, entity, setting);
        Assert.assertEquals(11f, entity.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Tests effective capacity of the commodity when ignoring HA and no Utilization Threshold
     * setting present.
     * It is expected that value is sourced from oribe.
     */
    @Test
    public void testHaNoUtilOverride() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.IgnoreHA, true));
        Assert.assertFalse(builder.getCommoditySoldList(0).hasEffectiveCapacityPercentage());
    }

    /**
     * Tests effective capacity of the commodity when not ignoring HA and having Utilization
     * Threshold setting.
     * It is expected that value is sourced from setting.
     */
    @Test
    public void testNoHaUtilOverride() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.CpuUtilization, 22f),
                createSetting(EntitySettingSpecs.IgnoreHA, false));
        Assert.assertEquals(22f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Tests setting of effective capacity when it was not set by probe.
     * It is expected that value is sourced from setting.
     */
    @Test
    public void testNoHaUtilOverrideNoInitial() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.CpuUtilization, 22f),
                createSetting(EntitySettingSpecs.IgnoreHA, false));
        Assert.assertEquals(22f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    @Test
    public void testHeadroomDesiredCapacityOverrideCpu() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU);
        // Suppose the effective capacity is 80 - i.e. 20% is reserved for HA
        builder.getCommoditySoldListBuilder(0).setEffectiveCapacityPercentage(80.0f);
        // Suppose the maximum desired utilization is 75%
        applySettings(CLUSTER_HEADROOM_TOPOLOGY_INFO, builder,
            createSetting(EntitySettingSpecs.TargetBand, 10f),
            createSetting(EntitySettingSpecs.UtilTarget, 70f));
        // The effective capacity for headroom should be 80% * 75% = 60%
        Assert.assertEquals(60f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    @Test
    public void testHeadroomDesiredCapacityOverrideMem() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.MEM);
        // Suppose the effective capacity is 80 - i.e. 20% is reserved for HA
        builder.getCommoditySoldListBuilder(0).setEffectiveCapacityPercentage(80.0f);
        // Suppose the maximum desired utilization is 75%
        applySettings(CLUSTER_HEADROOM_TOPOLOGY_INFO, builder,
                createSetting(EntitySettingSpecs.TargetBand, 10f),
                createSetting(EntitySettingSpecs.UtilTarget, 70f));
        // The effective capacity for headroom should be 80% * 75% = 60%
        Assert.assertEquals(60f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    @Test
    public void testHeadroomDesiredCapacityOverrideNoDesiredState() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.MEM);
        // Suppose the effective capacity is 80 - i.e. 20% is reserved for HA
        builder.getCommoditySoldListBuilder(0).setEffectiveCapacityPercentage(80.0f);
        // Suppose no maximum desired utilization is set.
        applySettings(CLUSTER_HEADROOM_TOPOLOGY_INFO, builder);
        // Since there is no desired state settings set, leave effective capacity as is.
        Assert.assertEquals(80.0f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Creates float value setting.
     *
     * @param setting setting ID to create a setting with.
     * @param value value of the setting
     * @return setting object
     */
    private Setting createSetting(EntitySettingSpecs setting, float value) {
        return Setting.newBuilder()
                .setSettingSpecName(setting.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value).build())
                .build();
    }

    /**
     * Creates boolean value setting.
     *
     * @param setting setting ID to create a setting with
     * @param value value of the setting
     * @return setting object
     */
    private Setting createSetting(EntitySettingSpecs setting, boolean value) {
        return Setting.newBuilder()
                .setSettingSpecName(setting.getSettingName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(value).build())
                .build();
    }

    /**
     * Applies the specified settings to the specified topology builder. Internally it constructs
     * topology graph with one additional parent entity to be able to buy from it.
     *
     * @param entity entity to apply settings to
     * @param settings settings to be applied
     */
    private void applySettings(@Nonnull final TopologyInfo topologyInfo,
                               @Nonnull TopologyEntityDTO.Builder entity,
                               @Nonnull Setting... settings) {
        final long entityId = entity.getOid();
        final TopologyGraph graph = TopologyGraph.newGraph(ImmutableMap.of(
            entityId, topologyEntityBuilder(entity),
            PARENT_ID, topologyEntityBuilder(PARENT_OBJECT.toBuilder())));
        final EntitySettings.Builder settingsBuilder = EntitySettings.newBuilder()
                .setEntityOid(entityId)
                .setDefaultSettingPolicyId(DEFAULT_SETTING_ID);
        for (Setting setting : settings) {
            settingsBuilder.addUserSettings(setting);
        }
        final SettingPolicy policy = SettingPolicy.newBuilder().setId(DEFAULT_SETTING_ID).build();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph,
                Collections.singletonMap(entityId, settingsBuilder.build()),
                Collections.singletonMap(DEFAULT_SETTING_ID, policy));
        applicator.applySettings(topologyInfo, graphWithSettings);
    }
}
