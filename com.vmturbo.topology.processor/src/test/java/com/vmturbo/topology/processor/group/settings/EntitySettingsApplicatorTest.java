package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionMode;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit test for {@link EntitySettingsApplicator}.
 */
public class EntitySettingsApplicatorTest {

    private static final long PARENT_ID = 23345;
    private static final long DEFAULT_SETTING_ID = 23346;

    private static final Setting INSTANCE_STORE_AWARE_SCALING_SETTING =
                    createInstanceStoreAwareScalingSetting(true);
    private static final Setting MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
            .build();

    private static final Setting MOVE_RECOMMEND_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()))
        .build();

    private static final Setting DELETE_MANUAL_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.Delete.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))
        .build();

    private static final Setting DELETE_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.Delete.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_DISABLED_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
                    .build();

    private static final Setting SUSPEND_DISABLED_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.Suspend.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
                    .build();

    private static final Setting PROVISION_DISABLED_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.Provision.getSettingName())
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

    private static final Setting BUSINESS_USER_MOVE_RECOMMEND_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.BusinessUserMove.getSettingName())
            .setEnumSettingValue(
                    EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()))
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

    private static final Setting VM_VSTORAGE_INCREMENT_DEFAULT = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.VstorageIncrement.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(
                                    EntitySettingSpecs.VstorageIncrement
                                        .getSettingSpec()
                                        .getNumericSettingValueType()
                                        .getDefault()))
                    .build();

    private static final Setting VM_VSTORAGE_INCREMENT_NOT_DEFAULT = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.VstorageIncrement.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(5000))
                    .build();

    private static final Setting BU_IMAGE_STORAGE_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeTargetUtilizationImageStorage.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(80))
            .build();

    private static final Setting BU_IMAGE_MEM_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeTargetUtilizationImageMem.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(70))
            .build();

    private static final Setting BU_IMAGE_CPU_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeTargetUtilizationImageCPU.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(60))
            .build();

    private static final Setting VM_IO_THROUGHPUT_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(
                    EntitySettingSpecs.ResizeTargetUtilizationIoThroughput.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(50))
            .build();

    private static final Setting VM_NET_THROUGHPUT_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(
                    EntitySettingSpecs.ResizeTargetUtilizationNetThroughput.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(40))
            .build();

    private static final Setting VM_VMEM_THROUGHPUT_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeTargetUtilizationVmem.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(30))
            .build();

    private static final Setting VM_VCPU_THROUGHPUT_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeTargetUtilizationVcpu.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(20))
            .build();

    private static final Setting.Builder RESIZE_SETTING_BUILDER = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());

    private static final TopologyEntityDTO PARENT_OBJECT =
            TopologyEntityDTO.newBuilder().setOid(PARENT_ID).setEntityType(100001).build();
    private static final double DELTA = 0.001;
    private static final int INSTANCE_DISK_SIZE_GB = 10;
    private static final double NUM_DISKS = 3D;

    private EntitySettingsApplicator applicator;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.getDefaultInstance();

    private static final TopologyInfo CLUSTER_HEADROOM_TOPOLOGY_INFO = TopologyInfo.newBuilder()
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
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.MANUAL.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
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
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.DISABLED.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
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

    /**
     * Test Move Volume should apply movable to its connected VM's associated ST.
     */
    @Test
    public void testMoveApplicatorForVolume() {
        final long vvId = 123456L;
        final long vmId = 234567L;
        final long stId = 345678L;
        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(vmId)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PARENT_ID)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setVolumeId(vvId)
                .setMovable(false))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PARENT_ID + 1)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setVolumeId(stId)
                .setMovable(false))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(vvId)
                .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                .build());

        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(vmId)
                .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                .build())
            .setOid(vvId);

        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, applicator, MOVE_RECOMMEND_SETTING);
        assertThat(vmEntity.getCommoditiesBoughtFromProvidersList().size(), is(2));
        assertThat(vmEntity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
        assertThat(vmEntity.getCommoditiesBoughtFromProviders(1).getMovable(), is(false));
    }

    /**
     * Test Move Unattached Volume.
     */
    @Test
    public void testMoveApplicatorForUnattachedVolume() {
        final long vvId = 123456L;
        final long stId = 345678L;
        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PARENT_ID)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setVolumeId(stId)
                .setMovable(false));

        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setOid(vvId);

        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, applicator, MOVE_RECOMMEND_SETTING);
        assertThat(vmEntity.getCommoditiesBoughtFromProvidersList().size(), is(1));
        assertThat(vmEntity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    /**
     * Test Delete Applicator for manual setting with entity's original AnalysisSetting.deletable True.
     */
    @Test
    public void testDeleteApplicatorForManualSettingWithEntityConsumerPolicyDeletableTrue() {
        final long vvId = 123456L;
        final long stId = 345678L;
        final boolean isVvDeletable = true;
        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PARENT_ID)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setVolumeId(stId)
                .setMovable(false));
        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setDeletable(isVvDeletable)
                .build())
            .setOid(vvId);
        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, applicator, DELETE_MANUAL_SETTING);

        assertThat(vvEntity.getAnalysisSettings().getDeletable(), is(true));
    }

    /**
     * Test Delete Applicator for manual setting with entity's original AnalysisSetting.deletable false.
     * Ensure the applicator do not remove the original setting.
     */
    @Test
    public void testDeleteApplicatorForManualSettingWithEntityConsumerPolicyDeletableFalse() {
        final long vvId = 123456L;
        final long stId = 345678L;
        final boolean isVvDeletable = false;
        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PARENT_ID)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setVolumeId(stId)
                .setMovable(false));
        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setDeletable(isVvDeletable)
                .build())
            .setOid(vvId);
        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, applicator, DELETE_MANUAL_SETTING);

        assertThat(vvEntity.getAnalysisSettings().getDeletable(), is(false));
    }

    /**
     * Test Delete Applicator for disabled setting.
     */
    @Test
    public void testDeleteApplicatorForDisabledSetting() {
        final long vvId = 123456L;
        final long stId = 345678L;
        final boolean isVvDeletable = true;
        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PARENT_ID)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setVolumeId(stId)
                .setMovable(true));
        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setOid(vvId)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setDeletable(isVvDeletable)
                .build());
        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, applicator, DELETE_DISABLED_SETTING);

        assertThat(vvEntity.getAnalysisSettings().getDeletable(), is(false));
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

    /**
     * Test application of {@link EntitySettingSpecs#BusinessUserMove}.
     */
    @Test
    public void testMoveBusinessUserSettingApplication() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_USER_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.DESKTOP_POOL_VALUE)
                        .setMovable(false));
        applySettings(TOPOLOGY_INFO, entity, BUSINESS_USER_MOVE_RECOMMEND_SETTING);
        Assert.assertEquals(1, entity.getCommoditiesBoughtFromProvidersCount());
        Assert.assertTrue(entity.getCommoditiesBoughtFromProviders(0).getMovable());
    }

    @Test
    public void testApplicatorsForDiskArray() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.DISK_ARRAY_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.STORAGE_CONTROLLER_VALUE)
                        .setMovable(true))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setIsResizeable(true)
                        .setCommodityType(com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder().setType(1000)))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                                .setCloneable(true)
                                .setSuspendable(true));

        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertFalse(entity.getCommoditiesBoughtFromProviders(0).getMovable());

        applySettings(TOPOLOGY_INFO, entity, RESIZE_DISABLED_SETTING);
        assertFalse(entity.getCommoditySoldList(0).getIsResizeable());

        applySettings(TOPOLOGY_INFO, entity, SUSPEND_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getSuspendable());

        applySettings(TOPOLOGY_INFO, entity, PROVISION_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getCloneable());
    }

    /**
     * Checks all cases when instance store aware scaling setting will not cause addition of
     * commodities to target entity.
     */
    @Test
    public void checkSettingHasNoEffect() {
        final Builder entity = createComputeTier(0L);
        final Setting inactiveSetting = Setting.newBuilder()
                        .setSettingSpecName(
                                        EntitySettingSpecs.InstanceStoreAwareScaling.getSettingName())
                        .build();
        verifySettingHasNoEffect(entity, inactiveSetting);
        final Setting disabledSetting = createInstanceStoreAwareScalingSetting(false);
        verifySettingHasNoEffect(entity, disabledSetting);
        verifySettingHasNoEffect(TopologyEntityDTO.newBuilder().setOid(0L)
                                        .setEntityType(EntityType.COMPUTE_TIER_VALUE),
                        INSTANCE_STORE_AWARE_SCALING_SETTING);
        verifySettingHasNoEffect(TopologyEntityDTO.newBuilder().setOid(0L)
                                        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().build()),
                        INSTANCE_STORE_AWARE_SCALING_SETTING);
    }

    private static Setting createInstanceStoreAwareScalingSetting(final boolean value) {
        return Setting.newBuilder().setSettingSpecName(
                        EntitySettingSpecs.InstanceStoreAwareScaling.getSettingName())
                        .setBooleanSettingValue(
                                        BooleanSettingValue.newBuilder().setValue(value).build())
                        .build();
    }

    private static Builder createComputeTier(long oid) {
        return TopologyEntityDTO.newBuilder().setOid(oid)
                        .setEntityType(EntityType.COMPUTE_TIER_VALUE).setTypeSpecificInfo(
                                        TypeSpecificInfo.newBuilder()
                                                        .setComputeTier(ComputeTierInfo.newBuilder()
                                                                        .setInstanceDiskSizeGb(
                                                                                        INSTANCE_DISK_SIZE_GB)
                                                                        .setInstanceDiskType(
                                                                                        InstanceDiskType.HDD)
                                                                        .setNumInstanceDisks(3)
                                                                        .build()).build());
    }

    private void verifySettingHasNoEffect(Builder entity, Setting disabledSetting) {
        applySettings(TOPOLOGY_INFO, entity, disabledSetting);
        final List<CommoditySoldDTO> soldCommodities = entity.getCommoditySoldListList();
        Assert.assertThat(soldCommodities.size(), CoreMatchers.is(0));
    }

    /**
     * Checks that when instance store aware scaling setting is enabled than compute tier instance
     * which has instance store disks, will have 3 additional commodities after setting
     * application.
     */
    @Test
    public void checkComputeTierInstanceStoreSettings() {
        final Builder entity = createComputeTier(0L);
        final long entityOid = entity.getOid();
        final CommoditiesBoughtFromProvider computeTierBoughtProvider =
                        CommoditiesBoughtFromProvider.newBuilder().setProviderId(entityOid)
                                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                        .build();
        final TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder().setOid(777_778L)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(computeTierBoughtProvider);
        final long vmOid = vm.getOid();
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator
                        .newGraph(ImmutableMap.of(vmOid, topologyEntityBuilder(vm), entity.getOid(),
                                        topologyEntityBuilder(entity)));
        final Map<Long, EntitySettings> entitySettings = ImmutableMap.of(vmOid,
                        createSettings(vmOid, INSTANCE_STORE_AWARE_SCALING_SETTING), entityOid,
                        createSettings(entityOid, INSTANCE_STORE_AWARE_SCALING_SETTING));
        applySettings(graph, TOPOLOGY_INFO, entitySettings);
        final List<CommoditySoldDTO.Builder> soldCommodities = entity.getCommoditySoldListBuilderList();
        final Set<CommodityType> commodityTypes =
                        soldCommodities.stream().map(CommoditySoldDTO.Builder::getCommodityType)
                                        .map(TopologyDTO.CommodityType::getType)
                                        .map(CommodityType::forNumber).collect(Collectors.toSet());
        Assert.assertThat(soldCommodities.size(), CoreMatchers.is(3));
        Assert.assertThat(commodityTypes, Matchers.containsInAnyOrder(CommodityType.NUM_DISK,
                        CommodityType.INSTANCE_DISK_TYPE, CommodityType.INSTANCE_DISK_SIZE));
        Assert.assertThat(soldCommodities.stream().map(CommoditySoldDTO.Builder::getIsResizeable)
                                        .collect(Collectors.toSet()),
                        CoreMatchers.is(Collections.singleton(false)));
        Assert.assertThat(soldCommodities.stream().map(CommoditySoldDTO.Builder::getActive)
                                        .collect(Collectors.toSet()),
                        CoreMatchers.is(Collections.singleton(true)));
        Assert.assertThat(getCommodityCapacity(soldCommodities, CommodityType.INSTANCE_DISK_SIZE),
                        CoreMatchers.is(INSTANCE_DISK_SIZE_GB * 1024D));
        Assert.assertThat(getCommodityCapacity(soldCommodities, CommodityType.NUM_DISK),
                        CoreMatchers.is(NUM_DISKS));
        final String instanceTypeKey = findCommodity(soldCommodities,
                        c -> c.getCommodityType().getType()
                                        == CommodityType.INSTANCE_DISK_TYPE_VALUE)
                        .getCommodityType().getKey();
        Assert.assertThat(instanceTypeKey, CoreMatchers.is(InstanceDiskType.HDD.name()));
    }

    private void applySettings(TopologyGraph<TopologyEntity> graph, TopologyInfo topologyInfo,
                    final Map<Long, EntitySettings> entitySettings) {
        final SettingPolicy policy = SettingPolicy.newBuilder().setId(DEFAULT_SETTING_ID).build();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph, entitySettings,
                        Collections.singletonMap(DEFAULT_SETTING_ID, policy));
        applicator.applySettings(topologyInfo, graphWithSettings);
    }

    private EntitySettings createSettings(final long entityId, Setting... settings) {
        final EntitySettings.Builder settingsBuilder = EntitySettings.newBuilder()
                        .setEntityOid(entityId)
                        .setDefaultSettingPolicyId(DEFAULT_SETTING_ID);
        for (Setting setting : settings) {
            settingsBuilder.addUserSettings(SettingToPolicyId.newBuilder()
                            .setSetting(setting)
                            .addSettingPolicyId(1L)
                            .build());
        }
        return settingsBuilder.build();
    }

    /**
     * Checks the case when there is no {@link EntityType#COMPUTE_TIER} provider for a VM, than
     * despite that setting is active and enabled we will not add instance store commodities.
     */
    @Test
    public void checkSettingHasNoEffectForVm() {
        final long providerOid = 777_777L;
        final int providerEntityType = EntityType.PHYSICAL_MACHINE_VALUE;
        final Builder provider = TopologyEntityDTO.newBuilder().setOid(providerOid)
                        .setEntityType(providerEntityType);
        final CommoditiesBoughtFromProvider computeTierBoughtProvider =
                        CommoditiesBoughtFromProvider.newBuilder().setProviderId(providerOid)
                                        .setProviderEntityType(providerEntityType)
                                        .build();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(computeTierBoughtProvider);
        applySettings(TOPOLOGY_INFO, entity, provider, applicator,
                        INSTANCE_STORE_AWARE_SCALING_SETTING);
        final CommoditiesBoughtFromProvider computeTierUpdatedProvider =
                        entity.getCommoditiesBoughtFromProvidersList().stream()
                                        .filter(p -> p.getProviderId() == providerOid)
                                        .findFirst().get();
        final List<CommodityBoughtDTO> boughtCommodities =
                        computeTierUpdatedProvider.getCommodityBoughtList();
        Assert.assertThat(boughtCommodities.size(), CoreMatchers.is(0));
    }

    /**
     * Checks that in case VM has at least one Instance Store commodity type bought from compute
     * tier provider, then instance store settings applicators will not add more commodities.
     */
    @Test
    public void checkVmAlreadyHasInstanceStoreCommodities() {
        final long providerOid = 777_777L;
        final Builder provider = createComputeTier(providerOid);
        final CommoditiesBoughtFromProvider computeTierBoughtProvider =
                        CommoditiesBoughtFromProvider.newBuilder().setProviderId(providerOid)
                                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                                        .setCommodityType(TopologyDTO.CommodityType
                                                                        .newBuilder()
                                                                        .setType(CommodityType.INSTANCE_DISK_SIZE_VALUE)
                                                                        .build())
                                                        .setUsed(INSTANCE_DISK_SIZE_GB).build())
                                        .build();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(computeTierBoughtProvider);
        applySettings(TOPOLOGY_INFO, entity, provider, applicator,
                        INSTANCE_STORE_AWARE_SCALING_SETTING);
        final CommoditiesBoughtFromProvider computeTierUpdatedProvider =
                        entity.getCommoditiesBoughtFromProvidersList().stream()
                                        .filter(p -> p.getProviderId() == providerOid)
                                        .findFirst().get();
        final List<CommodityBoughtDTO> boughtCommodities =
                        computeTierUpdatedProvider.getCommodityBoughtList();
        Assert.assertThat(boughtCommodities.size(), CoreMatchers.is(1));
    }

    /**
     * Checks that when instance store aware scaling setting is enabled than VM instance which has
     * instance store disks, will have 3 additional commodities after setting application.
     */
    @Test
    public void checkVmInstanceStoreSettings() {
        final long computeTierOid = 777_777L;
        final long secondTierOid = 777_778L;
        final Builder computeTierFirst = createComputeTier(computeTierOid);
        final Builder computeTierSecond = createComputeTier(secondTierOid);
        final CommoditiesBoughtFromProvider computeTierBoughtProvider =
                        CommoditiesBoughtFromProvider.newBuilder().setProviderId(computeTierOid)
                                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                        .build();
        final CommoditiesBoughtFromProvider computeTierBoughtProviderSecond =
                        CommoditiesBoughtFromProvider.newBuilder().setProviderId(secondTierOid)
                                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                                        .build();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(computeTierBoughtProvider)
                        .addCommoditiesBoughtFromProviders(computeTierBoughtProviderSecond);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
                        entity.getOid(), topologyEntityBuilder(entity),
                        computeTierOid, topologyEntityBuilder(computeTierFirst),
                        secondTierOid, topologyEntityBuilder(computeTierSecond)));
        applySettings(TOPOLOGY_INFO, applicator, graph, entity.getOid(),
                        INSTANCE_STORE_AWARE_SCALING_SETTING);
        /*
          Any of compute tier provider can have new instance store commodities. It depends on
          the order in entity.getCommoditiesBoughtFromProvidersList().
         */
        final Collection<CommodityBoughtDTO> boughtCommodities =
                        entity.getCommoditiesBoughtFromProvidersList().stream()
                                        .filter(p -> p.getProviderEntityType()
                                                        == EntityType.COMPUTE_TIER_VALUE)
                                        .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                                        .flatMap(Collection::stream).collect(Collectors.toSet());
        Assert.assertThat(boughtCommodities.size(), CoreMatchers.is(3));
        Assert.assertThat(getCommodityUsage(boughtCommodities, CommodityType.INSTANCE_DISK_SIZE),
                        CoreMatchers.is(INSTANCE_DISK_SIZE_GB * 1024D));
        Assert.assertThat(getCommodityUsage(boughtCommodities, CommodityType.NUM_DISK),
                        CoreMatchers.is(NUM_DISKS));
        final String instanceTypeKey = findCommodity(boughtCommodities,
                        c -> c.getCommodityType().getType()
                                        == CommodityType.INSTANCE_DISK_TYPE_VALUE)
                        .getCommodityType().getKey();
        Assert.assertThat(instanceTypeKey, CoreMatchers.is(InstanceDiskType.HDD.name()));
    }

    private static double getCommodityCapacity(Collection<CommoditySoldDTO.Builder> commodities,
                    CommodityType commodityType) {
        return findCommodity(commodities,
                        c -> c.getCommodityType().getType() == commodityType.getNumber())
                        .getCapacity();

    }

    @Nullable
    private static <T> T findCommodity(@Nonnull Collection<T> commodities,
                    @Nonnull Predicate<T> filter) {
        return commodities.stream().filter(filter).findFirst().orElse(null);
    }

    private static double getCommodityUsage(Collection<CommodityBoughtDTO> commodities,
                    CommodityType commodityType) {
        return findCommodity(commodities,
                        c -> c.getCommodityType().getType() == commodityType.getNumber()).getUsed();
    }

    @Test
    public void testApplicatorsForLogicalPool() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.LOGICAL_POOL_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.STORAGE_CONTROLLER_VALUE)
                        .setMovable(true))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setIsResizeable(true)
                        .setCommodityType(com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder().setType(1000)))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                                .setCloneable(true)
                                .setSuspendable(true));

        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertFalse(entity.getCommoditiesBoughtFromProviders(0).getMovable());

        applySettings(TOPOLOGY_INFO, entity, RESIZE_DISABLED_SETTING);
        assertFalse(entity.getCommoditySoldList(0).getIsResizeable());

        applySettings(TOPOLOGY_INFO, entity, SUSPEND_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getSuspendable());

        applySettings(TOPOLOGY_INFO, entity, PROVISION_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getCloneable());
    }

    @Test
    public void testApplicatorsForStorageController() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_CONTROLLER_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setIsResizeable(true)
                        .setCommodityType(com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder().setType(1000)))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                                .setCloneable(true));

        applySettings(TOPOLOGY_INFO, entity, PROVISION_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getCloneable());
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
        testUtilizationSettings(EntityType.VIRTUAL_MACHINE, CommodityType.VCPU_REQUEST,
                EntitySettingSpecs.VCPURequestUtilization);
        testUtilizationSettings(EntityType.CONTAINER_POD, CommodityType.VCPU_REQUEST,
                EntitySettingSpecs.VCPURequestUtilization);
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
     * Tests effective capacity of the commodity.
     * It is expected that value is sourced from setting.
     */
    @Test
    public void testUtilOverrride() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.CpuUtilization, 22f));
        Assert.assertEquals(22f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Tests effective capacity of the commodity without settings.
     */
    @Test
    public void testNoUtilOverride() {
        final TopologyEntityDTO.Builder entity =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        Assert.assertEquals(11f, entity.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Tests effective capacity of the commodity when having Utilization
     * Threshold setting.
     * It is expected that value is sourced from setting.
     */
    @Test
    public void testUtilOverride() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 11f);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.CpuUtilization, 22f));
        Assert.assertEquals(22f, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.0001);
    }

    /**
     * Tests setting of effective capacity when it was not set by probe.
     * It is expected that value is sourced from setting.
     */
    @Test
    public void testUtilOverrideNoInitial() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU);
        applySettings(TOPOLOGY_INFO, builder, createSetting(EntitySettingSpecs.CpuUtilization, 22f));
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

    @Test
    public void testResizeIncrementApplicatorForVStorageForDefault() {
        final TopologyEntityDTO.Builder builder =
                        createEntityWithCommodity(EntityType.VIRTUAL_MACHINE, CommodityType.VSTORAGE);
        builder.getCommoditySoldListBuilder(0).setIsResizeable(true);

        applySettings(TOPOLOGY_INFO, builder, VM_VSTORAGE_INCREMENT_DEFAULT);

        // Resizeable false for default setting of vStorage increment.
        assertFalse(builder.getCommoditySoldListBuilder(0).getIsResizeable());
    }

    @Test
    public void testResizeIncrementApplicatorForVStorageForNotDefault() {
        final TopologyEntityDTO.Builder builder =
                        createEntityWithCommodity(EntityType.VIRTUAL_MACHINE, CommodityType.VSTORAGE);
        builder.getCommoditySoldListBuilder(0).setIsResizeable(true);

        applySettings(TOPOLOGY_INFO, builder, VM_VSTORAGE_INCREMENT_NOT_DEFAULT);

        // Resizeable unchanged for non default setting of vStorage increment.
        assertTrue(builder.getCommoditySoldListBuilder(0).getIsResizeable());
    }

    /**
     * Test resize target utilization commodity sold ang bought applicators.
     */
    @Test
    public void testResizeTargetUtilizationCommodityApplicator() {
        Stream.of(new Object[][]{
                {CommodityType.VCPU, VM_VCPU_THROUGHPUT_RESIZE_TARGET_UTILIZATION},
                {CommodityType.VMEM, VM_VMEM_THROUGHPUT_RESIZE_TARGET_UTILIZATION}})
                .forEach(data -> testResizeTargetUtilizationCommoditySoldApplicator(
                        EntityType.VIRTUAL_MACHINE, (CommodityType)data[0], (Setting)data[1]));

        Stream.of(new Object[][]{
                {EntityType.VIRTUAL_MACHINE, CommodityType.IO_THROUGHPUT,
                        VM_IO_THROUGHPUT_RESIZE_TARGET_UTILIZATION},
                {EntityType.VIRTUAL_MACHINE, CommodityType.NET_THROUGHPUT,
                        VM_NET_THROUGHPUT_RESIZE_TARGET_UTILIZATION},
                {EntityType.BUSINESS_USER, CommodityType.IMAGE_CPU,
                        BU_IMAGE_CPU_RESIZE_TARGET_UTILIZATION},
                {EntityType.BUSINESS_USER, CommodityType.IMAGE_MEM,
                        BU_IMAGE_MEM_RESIZE_TARGET_UTILIZATION},
                {EntityType.BUSINESS_USER, CommodityType.IMAGE_STORAGE,
                        BU_IMAGE_STORAGE_RESIZE_TARGET_UTILIZATION}})
                .forEach(data -> testResizeTargetUtilizationCommodityBoughtApplicator(
                        (EntityType)data[0], (CommodityType)data[1], (Setting)data[2]));
    }

    /**
     * Test resize target utilization commodity bought applicator.
     *
     * @param entityType the {@link EntityType}
     * @param commodityType the {@link CommodityType}
     * @param resizeTargetUtilizationSetting the resize target utilization {@link Setting}
     */
    private void testResizeTargetUtilizationCommodityBoughtApplicator(EntityType entityType,
            CommodityType commodityType, Setting resizeTargetUtilizationSetting) {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(entityType.getNumber())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(commodityType.getNumber()))));
        applySettings(TOPOLOGY_INFO, entity, resizeTargetUtilizationSetting);
        assertEquals(resizeTargetUtilizationSetting.getNumericSettingValue().getValue() / 100,
                entity.getCommoditiesBoughtFromProviders(0)
                        .getCommodityBoughtList()
                        .get(0)
                        .getResizeTargetUtilization(), DELTA);
    }

    /**
     * Test resize target utilization commodity sold applicator.
     *
     * @param entityType the {@link EntityType}
     * @param commodityType the {@link CommodityType}
     * @param resizeTargetUtilizationSetting the resize target utilization {@link Setting}
     */
    private void testResizeTargetUtilizationCommoditySoldApplicator(EntityType entityType,
            CommodityType commodityType, Setting resizeTargetUtilizationSetting) {
        final TopologyEntityDTO.Builder entity =
                createEntityWithCommodity(entityType, commodityType);
        applySettings(TOPOLOGY_INFO, entity, resizeTargetUtilizationSetting);
        assertEquals(entity.getCommoditySoldList(0).getResizeTargetUtilization(),
                resizeTargetUtilizationSetting.getNumericSettingValue().getValue() / 100, DELTA);
    }

    @Test
    public void testResizeManualForAppServer() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.APPLICATION_SERVER, CommodityType.HEAP);
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.MANUAL.name()));

        applySettings(TOPOLOGY_INFO, builder, RESIZE_SETTING_BUILDER.build());

        // Resizeable false for default setting of vStorage increment.
        assertTrue(builder.getCommoditySoldListBuilder(0).getIsResizeable());
    }

    @Test
    public void testResizeDisabledForAppServer() {
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.APPLICATION_SERVER, CommodityType.HEAP);
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.DISABLED.name()));

        applySettings(TOPOLOGY_INFO, builder, RESIZE_SETTING_BUILDER.build());

        // Resizeable false for default setting of vStorage increment.
        assertFalse(builder.getCommoditySoldListBuilder(0).getIsResizeable());
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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
            entityId, topologyEntityBuilder(entity),
            PARENT_ID, topologyEntityBuilder(PARENT_OBJECT.toBuilder())));
        applySettings(topologyInfo, applicator, graph, entityId, settings);
    }

    /**
     * Applies the specified settings to the specified topology builder. Add additional entity
     * to construct topology graph.
     *
     * @param topologyInfo {@link TopologyInfo}
     * @param entity {@link TopologyEntityDTO} the entity to apply the setting
     * @param otherEntity Other entity in TopologyGraph
     * @param applicator the applicator to apply the setting
     * @param settings setting to be applied
     */
    private static void applySettings(@Nonnull final TopologyInfo topologyInfo,
                               @Nonnull TopologyEntityDTO.Builder entity,
                               @Nonnull TopologyEntityDTO.Builder otherEntity,
                               @Nonnull EntitySettingsApplicator applicator,
                               @Nonnull Setting... settings) {
        final long entityId = entity.getOid();
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
            entityId, topologyEntityBuilder(entity),
            PARENT_ID, topologyEntityBuilder(PARENT_OBJECT.toBuilder()),
            otherEntity.getOid(), topologyEntityBuilder(otherEntity)));
        applySettings(topologyInfo, applicator, graph, entityId, settings);
    }

    /**
     * Applies the specified settings to the specified topology builder. Add additional entity
     * to construct topology graph.
     *
     * @param topologyInfo {@link TopologyInfo}
     * @param entityId identifier of the entity to apply the setting
     * @param graph with other entities that will be processed.
     * @param applicator the applicator to apply the setting
     * @param settings setting to be applied
     */
    private static void applySettings(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull EntitySettingsApplicator applicator,
                    @Nonnull TopologyGraph<TopologyEntity> graph, long entityId,
                    @Nonnull Setting... settings) {
        final EntitySettings.Builder settingsBuilder = EntitySettings.newBuilder()
                        .setEntityOid(entityId)
                        .setDefaultSettingPolicyId(DEFAULT_SETTING_ID);
        for (Setting setting : settings) {
            settingsBuilder.addUserSettings(SettingToPolicyId.newBuilder()
                            .setSetting(setting)
                            .addSettingPolicyId(1L)
                            .build());
        }
        final SettingPolicy policy = SettingPolicy.newBuilder().setId(DEFAULT_SETTING_ID).build();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph,
                        Collections.singletonMap(entityId, settingsBuilder.build()),
                        Collections.singletonMap(DEFAULT_SETTING_ID, policy));
        applicator.applySettings(topologyInfo, graphWithSettings);
    }
}
