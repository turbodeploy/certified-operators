package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionMode;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityMoles.CpuCapacityServiceMole;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.ScalingPolicyEnum;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterTestUtil;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor.TemplateActionType;
import com.vmturbo.topology.processor.template.VirtualMachineEntityConstructor;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit test for {@link EntitySettingsApplicator}.
 */
@RunWith(JUnitParamsRunner.class)
public class EntitySettingsApplicatorTest {

    private static final long PARENT_ID = 23345;
    private static final long DEFAULT_SETTING_ID = 23346;

    private static final Template VM_TEMPLATE = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)
            .build();

    private static final Setting INSTANCE_STORE_AWARE_SCALING_SETTING =
                    createInstanceStoreAwareScalingSetting(true);
    private static final Setting MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
            .build();

    private static final Setting MOVE_RECOMMEND_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()))
        .build();

    private static final Setting DELETE_MANUAL_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.Delete.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))
        .build();

    private static final Setting DELETE_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.Delete.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_DISABLED_SETTING = Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
                    .build();

    private static final Setting RESIZE_VCPU_DOWN_ENABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()))
        .build();

    private static final Setting RESIZE_VMEM_DOWN_ENABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()))
        .build();

    private static final Setting RESIZE_VCPU_DOWN_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
            .build();

    private static final Setting RESIZE_VCPU_UP_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_VCPU_BELOW_MIN_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_VCPU_ABOVE_MAX_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_VMEM_DOWN_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
            .build();

    private static final Setting RESIZE_VMEM_UP_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_VMEM_BELOW_MIN_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVmemBelowMinThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting RESIZE_VMEM_ABOVE_MAX_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVmemAboveMaxThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting SUSPEND_DISABLED_SETTING = Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.Suspend.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
                    .build();

    private static final Setting PROVISION_DISABLED_SETTING = Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.Provision.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
                    .build();

    private static final Setting SUSPEND_RECOMMEND_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.Suspend.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()))
            .build();

    private static final Setting PROVISION_RECOMMEND_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.Provision.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()))
            .build();


    private static final Setting MOVE_MANUAL_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))
            .build();

    private static final Setting SHOP_TOGETHER_ENABLED = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ShopTogether.getSettingName())
            .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true).build())
            .build();

    private static final Setting SHOP_TOGETHER_DISABLED = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ShopTogether.getSettingName())
            .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false).build())
            .build();

    private static final Setting BUSINESS_USER_MOVE_RECOMMEND_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
            .setEnumSettingValue(
                    EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()))
            .build();

    private static final Setting MOVE_AUTOMATIC_SETTING = Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()))
                    .build();

    private static final Setting STORAGE_MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.StorageMove.getSettingName())
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

    private static final Setting IOPS_RESIZE_TARGET_UTILIZATION = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeTargetUtilizationIops.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10))
            .build();

    private static final EnumSettingValue AUTOMATIC = EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.AUTOMATIC.name()).build();

    /**
     * VMem min mode setting.
     */
    private static final Setting VMEM_MIN_MODE_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVmemBelowMinThreshold.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionDTO.ActionMode.MANUAL.name()).build())
            .build();

    /**
     * VMem min setting.
     */
    private static final Setting VMEM_MIN_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeVmemMinThreshold.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1024))
            .build();


    /**
     * VMem Max mode setting.
     */
    private static final Setting VMEM_MAX_MODE_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVmemAboveMaxThreshold.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionDTO.ActionMode.MANUAL.name()).build())
            .build();

    /**
     * VMem Max setting.
     */
    private static final Setting VMEM_MAX_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeVmemMaxThreshold.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10240))
            .build();

    /**
     * VMem inbetween down settings.
     */
    private static final Setting VMEM_INBETWEEN_DOWN_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds.getSettingName())
            .setEnumSettingValue(AUTOMATIC).build();

    /**
     * VMem inbetween up settings.
     */
    private static final Setting VMEM_INBETWEEN_UP_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds.getSettingName())
            .setEnumSettingValue(AUTOMATIC).build();

    /**
     * VCPU min mode setting.
     */
    private static final Setting VCPU_MIN_MODE_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionDTO.ActionMode.MANUAL.name()).build())
            .build();

    /**
     * VCPU min setting.
     */
    private static final Setting VCPU_MIN_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10))
            .build();

    /**
     * VCPU Max mode setting.
     */
    private static final Setting VCPU_MAX_MODE_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionDTO.ActionMode.MANUAL.name()).build())
            .build();

    /**
     * VCPU Max setting.
     */
    private static final Setting VCPU_MAX_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(100))
            .build();

    /**
     * VMem inbetween down settings.
     */
    private static final Setting VCPU_INBETWEEN_DOWN_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
            .setEnumSettingValue(AUTOMATIC).build();

    /**
     * VMem inbetween up settings.
     */
    private static final Setting VCPU_INBETWEEN_UP_SETTING = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds.getSettingName())
            .setEnumSettingValue(AUTOMATIC).build();

    private static final Setting VCPU_LIMIT_ABOVE_MAX_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuLimitAboveMaxThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.DISABLED.name())
            .build())
        .build();
    /**
     * VCPU Limit Resize Max Threshold 10,000 millicores.
     */
    private static final Setting VCPU_LIMIT_MAX_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.ResizeVcpuLimitMaxThreshold.getSettingName())
        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10_000))
        .build();
    private static final Setting VCPU_LIMIT_BELOW_MIN_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuLimitBelowMinThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.DISABLED.name())
            .build()).build();
    /**
     * VCPU Limit Resize Min Threshold 100 millicores.
     */
    private static final Setting VCPU_LIMIT_MIN_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.ResizeVcpuLimitMinThreshold.getSettingName())
        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(100))
        .build();
    private static final Setting VCPU_REQUEST_BELOW_MIN_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuRequestBelowMinThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.RECOMMEND.name())
            .build()).build();
    /**
     * VCPU Request Resize Min Threshold 20 millicores.
     */
    private static final Setting VCPU_REQUEST_MIN_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.ResizeVcpuRequestMinThreshold.getSettingName())
        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(50))
        .build();
    private static final Setting VMEM_LIMIT_ABOVE_MAX_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVmemLimitAboveMaxThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.DISABLED.name())
            .build())
        .build();
    /**
     * VMEM Limit Resize Max Threshold 300 MB.
     */
    private static final Setting VMEM_LIMIT_MAX_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.ResizeVmemLimitMaxThreshold.getSettingName())
        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(300))
        .build();
    private static final Setting VMEM_LIMIT_BELOW_MIN_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVmemLimitBelowMinThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.DISABLED.name())
            .build()).build();
    /**
     * VMEM Limit Resize Min Threshold 100 MB.
     */
    private static final Setting VMEM_LIMIT_MIN_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.ResizeVmemLimitMinThreshold.getSettingName())
        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(100))
        .build();
    private static final Setting VMEM_REQUEST_BELOW_MIN_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.ResizeVmemRequestBelowMinThreshold.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder()
            .setValue(ActionDTO.ActionMode.RECOMMEND.name())
            .build())
        .build();
    /**
     * VMEM Request Resize Min Threshold 50 MB.
     */
    private static final Setting VMEM_REQUEST_MIN_SETTING = Setting.newBuilder()
        .setSettingSpecName(EntitySettingSpecs.ResizeVmemRequestMinThreshold.getSettingName())
        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(50))
        .build();
    private static final Setting CONTAINER_RESIZE_MODE_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionDTO.ActionMode.MANUAL.name()).build())
        .build();

    private static final TopologyEntityDTO.Builder testVM = TopologyEntityDTO.newBuilder().setOid(111)
        .setEnvironmentType(EnvironmentType.ON_PREM)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE))
            .setCapacity(8));
    private static final TopologyEntityDTO.Builder testPod = TopologyEntityDTO.newBuilder().setOid(222)
        .setEntityType(EntityType.CONTAINER_POD_VALUE)
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(testVM.getOid())
            .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE));
    private static final TopologyEntityDTO.Builder testContainer = TopologyEntityDTO.newBuilder().setOid(333)
        .setEntityType(EntityType.CONTAINER_VALUE)
        // VMem capacity 204800 KB (200 MB)
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VMEM_VALUE))
            .setCapacity(204800))
        // VMemRequest capacity 102400 KB (100 MB)
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VMEM_REQUEST_VALUE))
            .setCapacity(102400))
        // VCPU capacity 10 MHz
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE))
            .setCapacity(10))
        // VCPURequest capacity 10 MHz
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_REQUEST_VALUE))
            .setCapacity(10))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(testPod.getOid())
            .setProviderEntityType(EntityType.CONTAINER_POD_VALUE));

    private static final Setting.Builder RESIZE_SETTING_BUILDER = Setting.newBuilder()
            .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName());

    private static final Setting.Builder SCALING_POLICY_SETTING_BUILDER = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ScalingPolicy.getSettingName());

    private static final Setting.Builder MIN_POLICY_SETTING_BUILDER = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.MinReplicas.getSettingName());

    private static final Setting.Builder MAX_POLICY_SETTING_BUILDER = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.MaxReplicas.getSettingName());

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

    private final CpuCapacityServiceMole cpuCapacityServiceMole =  spy(new CpuCapacityServiceMole());

    /**
     * Must be public, otherwise when unit test starts up, JUnit throws ValidationError.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(cpuCapacityServiceMole);

    /**
     * Cannot be initialized in the field, otherwise a IllegalStateException: GrpcTestServer has not
     * been started yet. Please call start() before is thrown.
     */
    private CpuCapacityServiceBlockingStub cpuCapacityService;

    /**
     * Setup the mocked services that cannot be initialized as fields.
     */
    @Before
    public void init() {
        applicator = new EntitySettingsApplicator();
        cpuCapacityService = CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Tests application of resize setting with MANUAL value, commodities shouldn't be changed.
     */
    @Test
    public void testResizeSettingEnabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false), Optional.ofNullable(null));
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.MANUAL.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        Assert.assertTrue(entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Tests application of resize setting with DISABLED value. Commodities which have isResizeable
     * true should be change, commodities with isResizeable=false shouldn't be changed.
     */
    @Test
    public void testResizeSettingDisabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false), Optional.ofNullable(null));
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.DISABLED.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        Assert.assertFalse(entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Tests application of resize setting with DISABLED value for VMEM, besides one setting.
     * Commodities should not change
     */
    @Test
    public void testResizeVmemSettingEnabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false), Optional.ofNullable(CommodityType.VMEM));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_VMEM_DOWN_ENABLED_SETTING,
            RESIZE_VMEM_UP_DISABLED_SETTING, RESIZE_VMEM_ABOVE_MAX_DISABLED_SETTING,
            RESIZE_VMEM_BELOW_MIN_DISABLED_SETTING);
        Assert.assertTrue(entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Tests application of resize setting with DISABLED value for VCPU, besides one setting.
     * Commodities should not change
     */
    @Test
    public void testResizeVcpuSettingEnabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false), Optional.ofNullable(CommodityType.VCPU));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_VCPU_DOWN_ENABLED_SETTING,
            RESIZE_VCPU_UP_DISABLED_SETTING, RESIZE_VCPU_ABOVE_MAX_DISABLED_SETTING,
            RESIZE_VCPU_BELOW_MIN_DISABLED_SETTING);
        Assert.assertTrue(entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Tests application of resize setting with DISABLED value for VCPU. Commodities which have
     * isResizeable true should be changed, commodities with isResizeable=false shouldn't be
     * changed.
     */
    @Test
    public void testResizeVmemSettingDisabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(true, false), Optional.ofNullable(CommodityType.VMEM));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_VMEM_DOWN_DISABLED_SETTING,
                RESIZE_VMEM_UP_DISABLED_SETTING, RESIZE_VMEM_ABOVE_MAX_DISABLED_SETTING,
                RESIZE_VMEM_BELOW_MIN_DISABLED_SETTING);
        Assert.assertFalse(entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Tests application of resize setting with DISABLED value for VCPU. Commodities which have
     * isResizeable true should be changed, commodities with isResizeable=false shouldn't be
     * changed.
     */
    @Test
    public void testResizeVcpuSettingDisabled() {
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(
                ImmutableList.of(true, false), Optional.ofNullable(CommodityType.VCPU));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_VCPU_DOWN_DISABLED_SETTING,
                RESIZE_VCPU_UP_DISABLED_SETTING, RESIZE_VCPU_ABOVE_MAX_DISABLED_SETTING,
                RESIZE_VCPU_BELOW_MIN_DISABLED_SETTING);
        Assert.assertFalse(entity.getCommoditySoldList(0).getIsResizeable());
        Assert.assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    private TopologyEntityDTO.Builder getEntityForResizeableTest(final List<Boolean> commoditiesResizeable,
                                                                 Optional<CommodityType> commodityType) {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder();
        for (boolean isResizeable : commoditiesResizeable) {
            entity.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType
                        .newBuilder()
                        .setType(commodityType.map(CommodityType::getNumber).orElse(1)).build())
                    .setIsResizeable(isResizeable).build());
        }
        return entity;
    }

    /**
     * Test that if the resizeable is disabled at the entity level,
     * it is not enabled by the user setting.
     */
    @Test
    public void testResizeSettingDisabledAtEntityLevel() {
        // entity has resizable disabled at entity level
        final TopologyEntityDTO.Builder entity = getEntityForResizeableTest(ImmutableList.of(false, false),
                                                             Optional.ofNullable(CommodityType.VCPU));
        // user setting is enabled
        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.MANUAL.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        assertFalse(entity.getCommoditySoldList(0).getIsResizeable());
        assertFalse(entity.getCommoditySoldList(1).getIsResizeable());

        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.AUTOMATIC.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        assertFalse(entity.getCommoditySoldList(0).getIsResizeable());
        assertFalse(entity.getCommoditySoldList(1).getIsResizeable());

        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.RECOMMEND.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        assertFalse(entity.getCommoditySoldList(0).getIsResizeable());
        assertFalse(entity.getCommoditySoldList(1).getIsResizeable());
    }

    /**
     * Test that the resizeable that is unset at the entity level, is set to the user setting.
     */
    @Test
    public void testResizeSettingUnsetAtEntityLevel() {
        TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder();
        entity.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1).build()));

        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.DISABLED.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        Assert.assertFalse(entity.getCommoditySoldList(0).getIsResizeable());

        entity = TopologyEntityDTO.newBuilder();
        entity.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1).build()));

        RESIZE_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionMode.AUTOMATIC.name()));
        applySettings(TOPOLOGY_INFO, entity, RESIZE_SETTING_BUILDER.build());
        Assert.assertTrue(entity.getCommoditySoldList(0).getIsResizeable());
    }


    /**
     * Tests move setting application.
     * The result is that the topology entity has to be marked not movable
     * if the user setting disables move for that entity type
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
     * Tests applying of VMem/VCPU min/Max applicators for a VM.
     */
    @Test
    public void testThresholdApplicatorForVM() {
        long vmId = 7;
        long pmId = 77;
        double mbToKb = 1024;
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder().setCpuCoreMhz(1000)).build();
        final TopologyEntityDTO.Builder pm =
                TopologyEntityDTO.newBuilder().setOid(pmId)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setTypeSpecificInfo(typeSpecificInfo);
        TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder().setOid(vmId)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VMEM_VALUE)))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE)))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(pmId)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE));
        final long entityId = entity.getOid();
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
                entityId, topologyEntityBuilder(entity),
                pmId, topologyEntityBuilder(pm)));
        applySettings(TOPOLOGY_INFO, applicator, graph, entityId, VMEM_MIN_SETTING, VMEM_MIN_MODE_SETTING,
                VMEM_MAX_SETTING, VMEM_MAX_MODE_SETTING, VMEM_INBETWEEN_DOWN_SETTING, VMEM_INBETWEEN_UP_SETTING,
                VCPU_MIN_SETTING, VCPU_MIN_MODE_SETTING, VCPU_MAX_SETTING, VCPU_MAX_MODE_SETTING,
                VCPU_INBETWEEN_DOWN_SETTING, VCPU_INBETWEEN_UP_SETTING);
        final Thresholds vMemCommoditySoldThresholds = entity.getCommoditySoldList(0).getThresholds();
        final Thresholds vCPUCommoditySoldThresholds = entity.getCommoditySoldList(1).getThresholds();
        // VMem min is 1GB
        Assert.assertEquals(0, vMemCommoditySoldThresholds.getMin(), DELTA);
        // VMem Max is 10GB
        Assert.assertEquals(10240 * mbToKb, vMemCommoditySoldThresholds.getMax(), DELTA);
        // VCPU min is 10 cores X 1000 (host CPU speed)
        Assert.assertEquals(0, vCPUCommoditySoldThresholds.getMin(), DELTA);
        // VCPU min is 100 cores X 1000 (host CPU speed)
        Assert.assertEquals(100_000, vCPUCommoditySoldThresholds.getMax(), DELTA);
    }

    /**
     * Tests applying of VCPU min/Max applicators for a VM from Template.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testThresholdApplicatorForTemplateVM() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(888L);

        final TopologyEntityDTO.Builder entity = new VirtualMachineEntityConstructor(
            cpuCapacityService)
                .createTopologyEntityFromTemplate(VM_TEMPLATE, Collections.emptyMap(), null,
                        TemplateActionType.CLONE, identityProvider, null);
        entity.setEnvironmentType(EnvironmentType.ON_PREM);
        final long entityId = entity.getOid();
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator
            .newGraph(ImmutableMap.of(entityId, topologyEntityBuilder(entity)));
        applySettings(TOPOLOGY_INFO, applicator, graph, entityId, VMEM_MIN_SETTING, VMEM_MIN_MODE_SETTING,
                VMEM_MAX_SETTING, VMEM_MAX_MODE_SETTING, VMEM_INBETWEEN_DOWN_SETTING, VMEM_INBETWEEN_UP_SETTING,
                VCPU_MIN_SETTING, VCPU_MIN_MODE_SETTING, VCPU_MAX_SETTING, VCPU_MAX_MODE_SETTING,
                VCPU_INBETWEEN_DOWN_SETTING, VCPU_INBETWEEN_UP_SETTING);
        final Thresholds vCPUCommoditySoldThresholds = entity.getCommoditySoldList(0).getThresholds();
        // VCPU minThreshold 0 because the capacity < minThreshold from policy and the action is not disabled
        Assert.assertEquals(0, vCPUCommoditySoldThresholds.getMin(), DELTA);
        // VCPU min is 100 cores X 200 (vm capacity / num cpu)
        Assert.assertEquals(20000, vCPUCommoditySoldThresholds.getMax(), DELTA);
    }

    /**
     * Tests applying of VCPU, VCPURequest, VMem and VMemRequest min and max applicators for a container
     * with 10 MHz VCPU, 10 MHz VCPURequest, 200 MB VMem and 100 MB VMemRequest with following settings:
     * <p/>
     * VCPU Limit Resize Above Max: DISABLED;
     * VCPU Limit Resize Max Threshold: 10,000 millicores;
     * VCPU Limit Resize Below Min: DISABLED;
     * VCPU Limit Resize Min Threshold: 100 millicores;
     * VCPU Request Resize Below Min: RECOMMEND;
     * VCPU Request Resize Min Threshold 20 millicores;
     * VMem Limit Resize Above Max: DISABLED;
     * VMEM Limit Resize Max Threshold 300 MB;
     * VMem Limit Resize Below Min: DISABLED;
     * VMEM Limit Resize Min Threshold 100 MB;
     * VMem Request Resize Below Min: RECOMMEND;
     * VMEM Request Resize Min Threshold 50 MB.
     */
    @Test
    public void testEntityThresholdApplicatorForContainer() {
        double mbToKb = 1024;
        // Set numCPUs to VirtualMachineInfo.
        testVM.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus(4))
            .build());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
            testContainer.getOid(), topologyEntityBuilder(testContainer),
            testPod.getOid(), topologyEntityBuilder(testPod),
            testVM.getOid(), topologyEntityBuilder(testVM)));
        applySettings(TOPOLOGY_INFO, applicator, graph, testContainer.getOid(), VMEM_LIMIT_ABOVE_MAX_MODE_SETTING,
            VMEM_LIMIT_MAX_SETTING, VMEM_LIMIT_BELOW_MIN_MODE_SETTING, VMEM_LIMIT_MIN_SETTING,
            VMEM_REQUEST_BELOW_MIN_MODE_SETTING, VMEM_REQUEST_MIN_SETTING, VCPU_LIMIT_ABOVE_MAX_MODE_SETTING,
            VCPU_LIMIT_MAX_SETTING, VCPU_LIMIT_BELOW_MIN_MODE_SETTING, VCPU_LIMIT_MIN_SETTING,
            VCPU_REQUEST_BELOW_MIN_MODE_SETTING, VCPU_REQUEST_MIN_SETTING, CONTAINER_RESIZE_MODE_SETTING);
        final Thresholds vMemCommoditySoldThresholds = testContainer.getCommoditySoldList(0).getThresholds();
        final Thresholds vMemRequestCommoditySoldThresholds = testContainer.getCommoditySoldList(1).getThresholds();
        final Thresholds vCPUCommoditySoldThresholds = testContainer.getCommoditySoldList(2).getThresholds();
        final Thresholds vCPURequestCommoditySoldThresholds = testContainer.getCommoditySoldList(3).getThresholds();

        assertEquals(300 * mbToKb, vMemCommoditySoldThresholds.getMax(), DELTA);
        assertEquals(100 * mbToKb, vMemCommoditySoldThresholds.getMin(), DELTA);
        assertEquals(50 * mbToKb, vMemRequestCommoditySoldThresholds.getMin(), DELTA);
        assertEquals(20, vCPUCommoditySoldThresholds.getMax(), DELTA);
        assertEquals(0.2, vCPUCommoditySoldThresholds.getMin(), DELTA);
        assertEquals(0.1, vCPURequestCommoditySoldThresholds.getMin(), DELTA);
    }

    /**
     * Tests applying of VCPU, VCPURequest, VMem and VMemRequest min and max applicators for a container,
     * where corresponding provider VM has no numCPUs. In this case, VCPU and VCPURequest commodities
     * won't have thresholds set.
     */
    @Test
    public void testEntityThresholdApplicatorForContainerWithoutNumCPUs() {
        double mbToKb = 1024;
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
            testContainer.getOid(), topologyEntityBuilder(testContainer),
            testPod.getOid(), topologyEntityBuilder(testPod),
            testVM.getOid(), topologyEntityBuilder(testVM)));
        applySettings(TOPOLOGY_INFO, applicator, graph, testContainer.getOid(), VMEM_LIMIT_ABOVE_MAX_MODE_SETTING,
            VMEM_LIMIT_MAX_SETTING, VMEM_LIMIT_BELOW_MIN_MODE_SETTING, VMEM_LIMIT_MIN_SETTING,
            VMEM_REQUEST_BELOW_MIN_MODE_SETTING, VMEM_REQUEST_MIN_SETTING, VCPU_LIMIT_ABOVE_MAX_MODE_SETTING,
            VCPU_LIMIT_MAX_SETTING, VCPU_LIMIT_BELOW_MIN_MODE_SETTING, VCPU_LIMIT_MIN_SETTING,
            VCPU_REQUEST_BELOW_MIN_MODE_SETTING, VCPU_REQUEST_MIN_SETTING, CONTAINER_RESIZE_MODE_SETTING);

        Thresholds vMemCommoditySoldThresholds = testContainer.getCommoditySoldList(0).getThresholds();
        Thresholds vMemRequestCommoditySoldThresholds = testContainer.getCommoditySoldList(1).getThresholds();
        assertEquals(300 * mbToKb, vMemCommoditySoldThresholds.getMax(), DELTA);
        assertEquals(100 * mbToKb, vMemCommoditySoldThresholds.getMin(), DELTA);
        assertEquals(50 * mbToKb, vMemRequestCommoditySoldThresholds.getMin(), DELTA);

        CommoditySoldDTO vCPUCommoditySold = testContainer.getCommoditySoldList(2);
        CommoditySoldDTO vCPURequestCommoditySold = testContainer.getCommoditySoldList(3);
        assertFalse(vCPUCommoditySold.hasThresholds());
        assertFalse(vCPURequestCommoditySold.hasThresholds());
    }

    /**
     * Test that the user setting cannot override the entity's disabled move setting.
     */
    @Test
    public void testDoNotOverrideEntityMove() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setMovable(false));
        applySettings(TOPOLOGY_INFO, entity, MOVE_AUTOMATIC_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    /**
     * Test that move settings are enabled by the user setting.
     */
    @Test
    public void testApplyEnableMoveUserSetting() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE));

        // Movable is not set in the entity
        assertFalse(entity.getCommoditiesBoughtFromProviders(0).hasMovable());

        applySettings(TOPOLOGY_INFO, entity, MOVE_AUTOMATIC_SETTING);
        assertTrue(entity.getCommoditiesBoughtFromProviders(0).hasMovable()
                        && entity.getCommoditiesBoughtFromProviders(0).getMovable());
    }

    /**
     * Test that move settings are disabled by the user setting.
     */
    @Test
    public void testApplyDisableMoveUserSetting() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE));

        // Movable is not set in the entity
        assertFalse(entity.getCommoditiesBoughtFromProviders(0).hasMovable());

        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertTrue(entity.getCommoditiesBoughtFromProviders(0).hasMovable()
                && !entity.getCommoditiesBoughtFromProviders(0).getMovable());
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
                .setProviderId(vvId).setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            );

        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setOid(vvId)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(stId).setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .build());

        final TopologyEntityDTO.Builder stEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .setOid(stId);

        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, stEntity, applicator, MOVE_RECOMMEND_SETTING);

        // assert that move flag only apply to volume, not vm
        assertThat(vmEntity.getCommoditiesBoughtFromProvidersList().size(), is(1));
        assertThat(vmEntity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));

        assertThat(vvEntity.getCommoditiesBoughtFromProvidersList().size(), is(1));
        assertThat(vvEntity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));

        assertThat(stEntity.getCommoditiesBoughtFromProvidersList().size(), is(0));
    }

    /**
     * Test Move Unattached Volume.
     */
    @Test
    public void testMoveApplicatorForUnattachedVolume() {
        final long vvId = 123456L;
        final long stId = 345678L;

        // VV not attached to VM
        final TopologyEntityDTO.Builder vvEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setOid(vvId)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(stId)
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE));

        final TopologyEntityDTO.Builder stEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .setOid(stId);

        applySettings(TOPOLOGY_INFO, vvEntity, stEntity, applicator, MOVE_RECOMMEND_SETTING);

        assertThat(vvEntity.getCommoditiesBoughtFromProvidersList().size(), is(1));
        assertThat(vvEntity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));

        assertThat(stEntity.getCommoditiesBoughtFromProvidersList().size(), is(0));
    }

    /**
     * Test Delete Applicator for manual setting with entity's original AnalysisSetting.deletable True.
     */
    @Test
    public void testDeleteApplicatorForManualSettingWithEntityConsumerPolicyDeletableTrue() {
        final long vvId = 123456L;
        final long vmId = 234567L;
        final long stId = 345678L;

        // Given
        final boolean originalAnalysisSettingForVVDeletable = true;
        final Setting manualSettingForDeleteVolume = DELETE_MANUAL_SETTING;

        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(vmId)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(vvId)
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            );
        final TopologyEntityDTO.Builder vvEntity = createVirtualVolumeEntity(vvId, stId, originalAnalysisSettingForVVDeletable);
        final TopologyEntityDTO.Builder stEntity = createStorageTierEntity(stId);

        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, stEntity, applicator, manualSettingForDeleteVolume);

        assertThat(vvEntity.getAnalysisSettings().getDeletable(), is(true));
    }

    /**
     * Test Delete Applicator for manual setting with entity's original AnalysisSetting.deletable false.
     * Ensure the applicator do not remove the original setting.
     */
    @Test
    public void testDeleteApplicatorForManualSettingWithEntityConsumerPolicyDeletableFalse() {
        final long vvId = 123456L;
        final long vmId = 234567L;
        final long stId = 345678L;

        // Given
        final boolean originalAnalysisSettingForVVDeletable = false;
        final Setting manualSettingForDeleteVolume = DELETE_MANUAL_SETTING;

        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(vmId)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(vvId)
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE));
        final TopologyEntityDTO.Builder vvEntity = createVirtualVolumeEntity(vvId, stId, originalAnalysisSettingForVVDeletable);
        final TopologyEntityDTO.Builder stEntity = createStorageTierEntity(stId);

        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, stEntity, applicator, manualSettingForDeleteVolume);

        assertThat(vvEntity.getAnalysisSettings().getDeletable(), is(false));
    }

    /**
     * Test Delete Applicator for disabled setting.
     */
    @Test
    public void testDeleteApplicatorForDisabledSetting() {
        final long vvId = 123456L;
        final long vmId = 234567L;
        final long stId = 345678L;

        // Given
        final boolean originalAnalysisSettingForVVDeletable = true;
        final Setting manualSettingForDeleteVolume = DELETE_DISABLED_SETTING;

        final TopologyEntityDTO.Builder vmEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(vmId)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(vvId)
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setMovable(false));
        final TopologyEntityDTO.Builder vvEntity = createVirtualVolumeEntity(vvId, stId, originalAnalysisSettingForVVDeletable);
        final TopologyEntityDTO.Builder stEntity = createStorageTierEntity(stId);

        applySettings(TOPOLOGY_INFO, vvEntity, vmEntity, stEntity, applicator, manualSettingForDeleteVolume);

        assertThat(vvEntity.getAnalysisSettings().getDeletable(), is(false));
    }

    private static TopologyEntityDTO.Builder createStorageTierEntity(long stId) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .setOid(stId);
    }

    private static TopologyEntityDTO.Builder createVirtualVolumeEntity(
            long vvId, long stId, boolean originalAnalysisSettingForVVDeletable) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setOid(vvId)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setDeletable(originalAnalysisSettingForVVDeletable)
                .build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setProviderId(stId)
                .build());
    }

    private Object parametersForShopTogether() {
        TopologyInfo realtimeTopologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .build();

        TopologyInfo planTopologyInfo = TopologyInfo.newBuilder()
                .setPlanInfo(PlanTopologyInfo.getDefaultInstance())
                .build();

        return new Object[]{
                // ShopTogether : ENABLED, REALTIME, PROBE VALUE : FALSE
                // Probe value should supersede setting set in the UI
                new Object[]{SHOP_TOGETHER_ENABLED, realtimeTopologyInfo, false, false},
                // ShopTogether : ENABLED, REALTIME, PROBE VALUE : TRUE
                // Probe value should supersede setting set in the UI
                new Object[]{SHOP_TOGETHER_ENABLED, realtimeTopologyInfo, true, true},
                // ShopTogether : DISABLED, REALTIME, PROBE VALUE : FALSE
                // Override Probe value : Set false
                new Object[]{SHOP_TOGETHER_DISABLED, realtimeTopologyInfo, false, false},
                // ShopTogether : DISABLED, REALTIME, PROBE VALUE : TRUE
                // Override Probe value : Set false
                new Object[]{SHOP_TOGETHER_DISABLED, realtimeTopologyInfo, true, false},
                // ShopTogether : DISABLED, PLAN, Probe/PreviousStage VALUE : TRUE
                // Probe/PreviousStage value supersedes value in UI
                new Object[]{SHOP_TOGETHER_DISABLED, planTopologyInfo, true, true},
                // ShopTogether : DISABLED, PLAN, Probe/PreviousStage VALUE : FALSE
                // Probe/PreviousStage value supersedes value in UI
                new Object[]{SHOP_TOGETHER_DISABLED, planTopologyInfo, false, false},
                // ShopTogether : ENABLED, PLAN, Probe/PreviousStage VALUE : TRUE
                // Probe/PreviousStage value supersedes value in UI
                new Object[]{SHOP_TOGETHER_ENABLED, planTopologyInfo, true, true},
                // ShopTogether : ENABLED, PLAN, Probe/PreviousStage VALUE : False
                // Probe/PreviousStage value supersedes value in UI
                new Object[]{SHOP_TOGETHER_ENABLED, planTopologyInfo, false, false}
        };
    }

    /**
     * Test setting of shop together flag for VM (All cases explained in parameter method).
     * @param shopTogetherSetting :  can be ENABLED/DISABLED
     * @param topologyInfo : can be REALTIME/PLAN
     * @param probeOrEarlierStageValue : can be T/F
     * @param expectedFinalValue : final value will be T/F
     */
    @Test
    @Parameters(method = "parametersForShopTogether")
    public void testShopTogetherApplicatorForVMs(Setting shopTogetherSetting,
            TopologyInfo topologyInfo,
            boolean probeOrEarlierStageValue,
            boolean expectedFinalValue) {

        TopologyEntityDTO.Builder vm = TopologyEntityDTO.newBuilder()
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                        .setShopTogether(probeOrEarlierStageValue))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setMovable(true));
        applySettings(topologyInfo, vm, shopTogetherSetting);
        assertThat(vm.getAnalysisSettings().getShopTogether(), is(expectedFinalValue));
    }

    @Test
    public void testShopTogetherApplicatorForPMs() {
        final TopologyEntityDTO.Builder pmSNMNotSupport = TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setShopTogether(false))
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(PARENT_ID)
                                .setProviderEntityType(EntityType.DATACENTER_VALUE)
                                .setMovable(true));
        applySettings(TOPOLOGY_INFO, pmSNMNotSupport, MOVE_AUTOMATIC_SETTING);
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
    public void testMoveApplicatorForStorage() {
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
    public void testMoveApplicatorNoProviderForStorage() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
    }

    /**
     * Test application of {@link ConfigurableActionSettings#Move}.
     */
    @Test
    public void testMoveBusinessUserSettingApplication() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_USER_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.DESKTOP_POOL_VALUE));
        applySettings(TOPOLOGY_INFO, entity, BUSINESS_USER_MOVE_RECOMMEND_SETTING);
        Assert.assertEquals(1, entity.getCommoditiesBoughtFromProvidersCount());
        Assert.assertTrue(entity.getCommoditiesBoughtFromProviders(0).getMovable());
    }

    /**
     * Test that suspend and provision settings are disabled by the user setting.
     *
     */
    @Test
    public void testApplyDisabledSuspendAndProvisionUserSetting() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setDisplayName("Pod1")
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1000)));

        // Entity did not contain suspend and clone setting values.
        assertFalse(entity.getAnalysisSettings().hasSuspendable());
        assertFalse(entity.getAnalysisSettings().hasCloneable());


        // Disable the suspend and provision
        applySettings(TOPOLOGY_INFO, entity, SUSPEND_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getSuspendable());

        applySettings(TOPOLOGY_INFO, entity, PROVISION_DISABLED_SETTING);
        assertFalse(entity.getAnalysisSettings().getCloneable());
    }

    /**
     * Test that disabled suspend and provision settings in the entity
     * are not overridden by the user setting.
     */
    @Test
    public void testDoNotOverrideEntitySuspendAndProvision() {
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1000)))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                        .setCloneable(false)
                        .setSuspendable(false));

        // Entity contains suspend and clone setting values.
        assertTrue(entity.getAnalysisSettings().hasSuspendable()
                     && !entity.getAnalysisSettings().getSuspendable());
        assertTrue(entity.getAnalysisSettings().hasCloneable()
                    && !entity.getAnalysisSettings().getCloneable());

        // User settings cannot override the entity's disabled state
        applySettings(TOPOLOGY_INFO, entity, SUSPEND_RECOMMEND_SETTING);
        assertFalse(entity.getAnalysisSettings().getSuspendable());

        applySettings(TOPOLOGY_INFO, entity, PROVISION_RECOMMEND_SETTING);
        assertFalse(entity.getAnalysisSettings().getCloneable());
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
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1000)))
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
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setInstanceDiskSizeGb(INSTANCE_DISK_SIZE_GB)
                                .setInstanceDiskType(InstanceDiskType.HDD)
                                .setNumInstanceDisks(3)
                                .build())
                        .build());
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
        Assert.assertThat(commodityTypes, Matchers.containsInAnyOrder(CommodityType.INSTANCE_DISK_COUNT,
                        CommodityType.INSTANCE_DISK_TYPE, CommodityType.INSTANCE_DISK_SIZE));
        Assert.assertThat(soldCommodities.stream().map(CommoditySoldDTO.Builder::getIsResizeable)
                                        .collect(Collectors.toSet()),
                        CoreMatchers.is(Collections.singleton(false)));
        Assert.assertThat(soldCommodities.stream().map(CommoditySoldDTO.Builder::getActive)
                                        .collect(Collectors.toSet()),
                        CoreMatchers.is(Collections.singleton(true)));
        Assert.assertThat(getCommodityCapacity(soldCommodities, CommodityType.INSTANCE_DISK_SIZE),
                        CoreMatchers.is(INSTANCE_DISK_SIZE_GB * 1024D));
        Assert.assertThat(getCommodityCapacity(soldCommodities, CommodityType.INSTANCE_DISK_COUNT),
                        CoreMatchers.is(NUM_DISKS));
        Assert.assertThat(getCommodityCapacity(soldCommodities, CommodityType.INSTANCE_DISK_TYPE),
                CoreMatchers.is(SDKConstants.ACCESS_COMMODITY_CAPACITY));
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
        Assert.assertThat(getCommodityUsage(boughtCommodities, CommodityType.INSTANCE_DISK_COUNT),
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
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1000)))
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
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(1000)))
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
        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_PROVISIONED,
                EntitySettingSpecs.StorageProvisionedUtilization);
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
        testUtilizationSettings(EntityType.DATABASE, CommodityType.DTU,
                EntitySettingSpecs.DTUUtilization);
        testUtilizationSettings(EntityType.DATABASE_SERVER, CommodityType.STORAGE_AMOUNT,
                EntitySettingSpecs.ResizeTargetUtilizationStorageAmount);
    }

    /**
     * Tests application of RQ utilization setting.
     */
    @Test
    public void testReadyQueueUtilizationThresholdSetting() {
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q1_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q2_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q3_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q4_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q5_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q6_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q7_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q8_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q16_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q32_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.Q64_VCPU,
                EntitySettingSpecs.ReadyQueueUtilization);
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

    /**
     * Application component with two commodities, one resizeable and one not.
     *s
     * @return application component
     */
    private TopologyEntityDTO.Builder createAppWithTwoCommodities() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setOid(1)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.HEAP_VALUE))
                        .setIsResizeable(false))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.THREADS_VALUE))
                        .setIsResizeable(true));
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
                {EntityType.VIRTUAL_MACHINE, CommodityType.STORAGE_ACCESS,
                        IOPS_RESIZE_TARGET_UTILIZATION},
                {EntityType.BUSINESS_USER, CommodityType.IMAGE_CPU,
                        BU_IMAGE_CPU_RESIZE_TARGET_UTILIZATION},
                {EntityType.BUSINESS_USER, CommodityType.IMAGE_MEM,
                        BU_IMAGE_MEM_RESIZE_TARGET_UTILIZATION},
                {EntityType.BUSINESS_USER, CommodityType.IMAGE_STORAGE,
                        BU_IMAGE_STORAGE_RESIZE_TARGET_UTILIZATION}})
                .forEach(data -> testResizeTargetUtilizationCommodityBoughtApplicator(
                        (EntityType)data[0], (CommodityType)data[1], (Setting)data[2]));

        testResizeTargetUtilizationCommoditySoldApplicator(EntityType.DATABASE_SERVER,
                CommodityType.STORAGE_ACCESS, IOPS_RESIZE_TARGET_UTILIZATION);
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
     * Testing affect resize scaling policy on application component.
     */
    @Test
    public void testScalingPolicyResizeForAppComponent() {
        final TopologyEntityDTO.Builder builder = createAppWithTwoCommodities();
        final TopologyEntityDTO.Builder originalBuilder = builder.clone();

        SCALING_POLICY_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ScalingPolicyEnum.RESIZE.name()));

        applySettings(TOPOLOGY_INFO, builder, SCALING_POLICY_SETTING_BUILDER.build());

        // With RESIZE policy the commodities should not change
        assertEquals(originalBuilder.getCommoditySoldListList(),
                builder.getCommoditySoldListList());
        assertFalse(builder.getAnalysisSettings().getCloneable());
    }

    /**
     * Testing affect provision scaling policy on application component.
     */
    @Test
    public void testScalingPolicyProvisionForAppComponent() {
        final TopologyEntityDTO.Builder builder = createAppWithTwoCommodities();
        builder.getAnalysisSettingsBuilder().setCloneable(false);
        final TopologyEntityDTO.Builder originalBuilder = builder.clone();

        SCALING_POLICY_SETTING_BUILDER.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ScalingPolicyEnum.HORIZONTAL_SCALE.name()));

        // Verify that we have at least one resizeable commodity in the test setup
        assertTrue(builder.getCommoditySoldListList().stream()
                .anyMatch(TopologyDTO.CommoditySoldDTO::getIsResizeable));

        applySettings(TOPOLOGY_INFO, builder, SCALING_POLICY_SETTING_BUILDER.build());

        // With PROVISION policy all commodities should be set to resizeable false
        assertTrue(builder.getCommoditySoldListList().stream()
                .noneMatch(TopologyDTO.CommoditySoldDTO::getIsResizeable));

        // With PROVISION policy, the cloneable/suspendable setting should not change
        assertEquals(originalBuilder.getAnalysisSettings().getCloneable(),
                builder.getAnalysisSettings().getCloneable());
    }

    /**
     * Test setting min/max replicas for application component.
     */
    @Test
    public void testMinMaxReplicasForAppComponent() {
        final TopologyEntityDTO.Builder builder = createAppWithTwoCommodities();
        applySettings(TOPOLOGY_INFO, builder,
                MIN_POLICY_SETTING_BUILDER
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(2).build())
                        .build(),
                MAX_POLICY_SETTING_BUILDER
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(5).build())
                        .build());
        assertEquals(2, builder.getAnalysisSettings().getMinReplicas(), DELTA);
        assertEquals(5, builder.getAnalysisSettings().getMaxReplicas(), DELTA);
    }

    /**
     * Test setting invalid min replicas for application component.
     */
    @Test
    public void testInvalidMinReplicasForAppComponent() {
        final TopologyEntityDTO.Builder builder = createAppWithTwoCommodities();
        applySettings(TOPOLOGY_INFO, builder,
                MIN_POLICY_SETTING_BUILDER
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(-1).build())
                        .build());
        assertEquals(EntitySettingSpecs.MinReplicas.getNumericDefault(),
                builder.getAnalysisSettings().getMinReplicas(), DELTA);
        assertEquals(EntitySettingSpecs.MaxReplicas.getNumericDefault(),
                builder.getAnalysisSettings().getMaxReplicas(), DELTA);
    }

    /**
     * Test setting invalid max replicas for application component.
     */
    @Test
    public void testInvalidMaxReplicasForAppComponent() {
        final TopologyEntityDTO.Builder builder = createAppWithTwoCommodities();
        applySettings(TOPOLOGY_INFO, builder,
                MAX_POLICY_SETTING_BUILDER
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(200000).build())
                        .build());
        assertEquals(EntitySettingSpecs.MinReplicas.getNumericDefault(),
                builder.getAnalysisSettings().getMinReplicas(), DELTA);
        assertEquals(EntitySettingSpecs.MaxReplicas.getNumericDefault(),
                builder.getAnalysisSettings().getMaxReplicas(), DELTA);
    }

    /**
     * Test setting min replicas > max replicas.
     */
    @Test
    public void testMinReplicasLargerThanMaxReplicasForAppComponent() {
        final TopologyEntityDTO.Builder builder = createAppWithTwoCommodities();
        applySettings(TOPOLOGY_INFO, builder,
                MIN_POLICY_SETTING_BUILDER
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(5).build())
                        .build(),
                MAX_POLICY_SETTING_BUILDER
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(2).build())
                        .build());
        assertEquals(EntitySettingSpecs.MinReplicas.getNumericDefault(),
                builder.getAnalysisSettings().getMinReplicas(), DELTA);
        assertEquals(EntitySettingSpecs.MaxReplicas.getNumericDefault(),
                builder.getAnalysisSettings().getMaxReplicas(), DELTA);
    }

    /**
     * Tests application of {@link EntitySettingSpecs#EnableScaleActions} setting.
     */
    @Test
    public void testEnableScaleActionsApplicator() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.EnableScaleActions.getSettingName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false))
                .build();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PARENT_ID)
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setMovable(true));
        applySettings(TOPOLOGY_INFO, entity, setting);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    /**
     * Tests application of {@link EntitySettingSpecs#EnableDeleteActions} setting.
     */
    @Test
    public void testEnableDeleteActionsApplicator() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.EnableDeleteActions.getSettingName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false))
                .build();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setDeletable(true));
        applySettings(TOPOLOGY_INFO, entity, setting);
        assertThat(entity.getAnalysisSettings().getDeletable(), is(false));
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
     * Applies the specified settings to the specified topology builder. Add additional entities
     * to construct topology graph.
     *
     * @param topologyInfo {@link TopologyInfo}
     * @param entity {@link TopologyEntityDTO} the entity to apply the setting
     * @param otherEntity1 {@link TopologyEntityDTO} first other entity
     * @param otherEntity2 {@link TopologyEntityDTO} second toher entity
     * @param applicator the applicator to apply the setting
     * @param settings setting to be applied
     */
    private static void applySettings(@Nonnull final TopologyInfo topologyInfo,
                                      @Nonnull TopologyEntityDTO.Builder entity,
                                      @Nonnull TopologyEntityDTO.Builder otherEntity1,
                                      @Nonnull TopologyEntityDTO.Builder otherEntity2,
                                      @Nonnull EntitySettingsApplicator applicator,
                                      @Nonnull Setting... settings) {
        final long entityId = entity.getOid();
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(
            entityId, topologyEntityBuilder(entity),
            PARENT_ID, topologyEntityBuilder(PARENT_OBJECT.toBuilder()),
            otherEntity1.getOid(), topologyEntityBuilder(otherEntity1),
            otherEntity2.getOid(), topologyEntityBuilder(otherEntity2)));
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
