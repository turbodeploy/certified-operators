package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

@RunWith(Parameterized.class)
public class RangeAwareResizeParameterizedTests {

    private final CommodityAttribute changedAttribute;
    private final int commodityType;
    private final int entityType;
    private final float oldCapacity;
    private final float newCapacity;
    private final ActionMode expectedActionMode;

    private static final Map<String, Setting> rangeAwareSettingsForEntity = Maps.newHashMap();
    private EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);

    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    public RangeAwareResizeParameterizedTests(CommodityAttribute changedAttribute, int commodityType,
                                              int entityType, float oldCapacity, float newCapacity,
                                              ActionMode actionMode) {
        this.changedAttribute = changedAttribute;
        this.commodityType = commodityType;
        this.entityType = entityType;
        this.oldCapacity = oldCapacity;
        this.newCapacity = newCapacity;
        this.expectedActionMode = actionMode;
    }

    @BeforeClass
    public static void setUp() {
        EnumSettingValue DISABLED = EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()).build();
        EnumSettingValue AUTOMATIC = EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()).build();
        EnumSettingValue RECOMMEND = EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()).build();
        EnumSettingValue MANUAL = EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()).build();
        // range aware vMem settings
        Setting vMemMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemMaxThreshold.getSettingName())
                // In MB (16GB)
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(16384).build()).build();
        Setting vMemMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemMinThreshold.getSettingName())
                // In MB
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(512).build()).build();
        Setting vMemAboveMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemAboveMaxThreshold.getSettingName())
                .setEnumSettingValue(DISABLED).build();
        Setting vMemBelowMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemBelowMinThreshold.getSettingName())
                .setEnumSettingValue(AUTOMATIC).build();
        Setting vMemUpInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.getSettingName())
                .setEnumSettingValue(MANUAL).build();
        Setting vMemDownInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemDownInBetweenThresholds.getSettingName())
                .setEnumSettingValue(RECOMMEND).build();
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemMaxThreshold.getSettingName(), vMemMaxThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemMinThreshold.getSettingName(), vMemMinThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemAboveMaxThreshold.getSettingName(), vMemAboveMaxThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemBelowMinThreshold.getSettingName(), vMemBelowMinThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.getSettingName(), vMemUpInBetweenThresholds);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemDownInBetweenThresholds.getSettingName(), vMemDownInBetweenThresholds);

        // range aware vCpu settings
        Setting vCpuMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName())
                // In cores
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(8).build()).build();
        Setting vCpuMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName())
                // In cores
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(3).build()).build();
        Setting vCpuAboveMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuAboveMaxThreshold.getSettingName())
                .setEnumSettingValue(DISABLED).build();
        Setting vCpuBelowMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuBelowMinThreshold.getSettingName())
                .setEnumSettingValue(AUTOMATIC).build();
        Setting vCpuUpInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingName())
                .setEnumSettingValue(MANUAL).build();
        Setting vCpuDownInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.getSettingName())
                .setEnumSettingValue(RECOMMEND).build();
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName(), vCpuMaxThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName(), vCpuMinThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuAboveMaxThreshold.getSettingName(), vCpuAboveMaxThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuBelowMinThreshold.getSettingName(), vCpuBelowMinThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingName(), vCpuUpInBetweenThresholds);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.getSettingName(), vCpuDownInBetweenThresholds);

        // Regular resize setting
        Setting regularResizeSetting = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                .setEnumSettingValue(DISABLED).build();
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.Resize.getSettingName(), regularResizeSetting);
    }

    @SuppressWarnings("unused") // it is used reflectively
    @Parameters( name = "{index}:Resize attribute {0} of commodity {1} of entity type {2} from {3} to {4}. Expected mode = {5}" )
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Vmem
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300000, 500000, ActionMode.AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300000, 20000000, ActionMode.DISABLED},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300000, 8000000, ActionMode.MANUAL},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 8000000, 7000000, ActionMode.RECOMMEND},
                // Vcpu
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 2, ActionMode.AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 12, ActionMode.DISABLED},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 7, ActionMode.MANUAL},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 7, 4, ActionMode.RECOMMEND},
                // Test the edges
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 5, 8, ActionMode.MANUAL},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 12, 8, ActionMode.RECOMMEND},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 5, 3, ActionMode.RECOMMEND},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 1, 3, ActionMode.MANUAL},
                // Limit change
                {CommodityAttribute.LIMIT, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300000, 500000, ActionMode.DISABLED},
                // Non-VM resize
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.CONTAINER_VALUE, 300000, 500000, ActionMode.DISABLED},
                // Other commodity resize
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VSTORAGE_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300000, 500000, ActionMode.DISABLED},
        });
    }

    @Test
    public void testRangeAwareResize() {
        ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
                .setId(10289)
                .setExplanation(Explanation.getDefaultInstance())
                .setImportance(0);
        final ActionDTO.Action recommendation = actionBuilder
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(7L)
                        .setType(entityType))
                    .setCommodityAttribute(changedAttribute)
                    .setCommodityType(CommodityType.newBuilder().setType(commodityType))
                    .setOldCapacity(oldCapacity)
                    .setNewCapacity(newCapacity)))
            .build();
        Action action = new Action(recommendation, 1l, actionModeCalculator);
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(rangeAwareSettingsForEntity);
        ActionMode actualMode = actionModeCalculator.calculateActionMode(action, entitySettingsCache);
        assertEquals(expectedActionMode, actualMode);
    }
}
