package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
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
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

@RunWith(Parameterized.class)
public class RangeAwareResizeParameterizedTests {

    /**
     * The multiplier for changing megabyte to kilobyte.
     */
    private static final int MB = 1024;
    private final CommodityAttribute changedAttribute;
    private final int commodityType;
    private final int entityType;
    private final float oldCapacity;
    private final float newCapacity;
    private final ActionMode expectedActionMode;
    private final EnumSettingValue modeAboveMax;
    private final EnumSettingValue modeBelowMin;

    private static final Map<String, Setting> rangeAwareSettingsForEntity = Maps.newHashMap();
    private EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private static final EnumSettingValue DISABLED = EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()).build();
    private static final EnumSettingValue AUTOMATIC = EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()).build();
    private static final EnumSettingValue MANUAL = EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()).build();
    private static final EnumSettingValue RECOMMEND = EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()).build();

    public RangeAwareResizeParameterizedTests(CommodityAttribute changedAttribute, int commodityType,
                                              int entityType, float oldCapacity, float newCapacity,
                                              ActionMode actionMode, EnumSettingValue modeAboveMax,
                                              EnumSettingValue modeBelowMin) {
        this.changedAttribute = changedAttribute;
        this.commodityType = commodityType;
        this.entityType = entityType;
        this.oldCapacity = oldCapacity;
        this.newCapacity = newCapacity;
        this.expectedActionMode = actionMode;
        this.modeAboveMax = modeAboveMax;
        this.modeBelowMin = modeBelowMin;
    }

    @BeforeClass
    public static void setUp() {

        // range aware vMem settings
        Setting vMemMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemMaxThreshold.getSettingName())
                // In MB (16GB)
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(16384).build()).build();
        Setting vMemMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVmemMinThreshold.getSettingName())
                // In MB
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(512).build()).build();
        Setting vMemUpInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds.getSettingName())
                .setEnumSettingValue(MANUAL).build();
        Setting vMemDownInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds.getSettingName())
                .setEnumSettingValue(AUTOMATIC).build();
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemMaxThreshold.getSettingName(), vMemMaxThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVmemMinThreshold.getSettingName(), vMemMinThreshold);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds.getSettingName(), vMemUpInBetweenThresholds);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds.getSettingName(), vMemDownInBetweenThresholds);

        // range aware vCpu settings
        Setting vCpuMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName())
                // In cores
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(8).build()).build();
        Setting vCpuMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName())
                // In cores
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(3).build()).build();
        Setting vCpuUpInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds.getSettingName())
                .setEnumSettingValue(MANUAL).build();
        Setting vCpuDownInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
                .setEnumSettingValue(RECOMMEND).build();
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName(), vCpuMaxThreshold);
        rangeAwareSettingsForEntity.put(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName(), vCpuMinThreshold);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds.getSettingName(), vCpuUpInBetweenThresholds);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName(), vCpuDownInBetweenThresholds);

        // Regular resize setting
        Setting regularResizeSetting = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                .setEnumSettingValue(DISABLED).build();
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.Resize.getSettingName(), regularResizeSetting);
    }

    @SuppressWarnings("unused") // it is used reflectively
    @Parameters( name = "{index}:Resize attribute {0} of commodity {1} of entity type {2} from {3} to {4}. Expected mode = {5}" )
    public static Collection<Object[]> parametersForTestRangeAwareResize() {
        return Arrays.asList(new Object[][] {
                // Vmem
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300_000, 500_000, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300_000, 20_000_000, ActionMode.MANUAL, MANUAL, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300_000, 8_000_000, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 8_000_000, 7_000_000, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Vcpu
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 2, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 12, ActionMode.MANUAL, MANUAL, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 7, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 7, 4, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                // Test the edges
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 5, 8, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 12, 8, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 5, 3, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 1, 3, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                // Limit change
                {CommodityAttribute.LIMIT, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300_000, 500_000, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                // Reservation change
                {CommodityAttribute.RESERVED, CommodityDTO.CommodityType.MEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 500_000, 300_000, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Non-VM resize
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.CONTAINER_VALUE, 300_000, 500_000, ActionMode.DISABLED, DISABLED, AUTOMATIC},
                /*
                Adjust resize
                CPU
                min = 3 max = 8 cores
                Modes: Automated - min - up: Manual down: Recommend - max- Disabled
                Resize Up
                 */
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 1, 2, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 2, 4, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 2, 8, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 8, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 7, 8, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 9, 10, ActionMode.DISABLED, DISABLED, AUTOMATIC},
                // Resize up VCPU from max (8) to above max
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 8, 10, ActionMode.DISABLED, DISABLED, AUTOMATIC},
                // Test case resize CPU from below max to above max (max=8 cores)
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 1, 10, ActionMode.MANUAL, MANUAL, AUTOMATIC},
                // Resize Down
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 10, 9, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 9, 7, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 7, 4, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 4, 3, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 9, 3, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 2, 1, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Resize down VCPU from min (3) to below min
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 3, 1, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Test case resize CPU from above min to below min (min=3)
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VCPU_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 5, 1, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
                /*
                Mem
                min = 512MB max = 16GB
                Modes: Automated - min - up: Manual down: Recommend - max- Disabled
                Resize Up
                 */
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 100 * MB, 200 * MB, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 200 * MB, 800 * MB, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 200 * MB, 16_000 * MB, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 800 * MB, 12_000 * MB, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 12_000 * MB, 16_000 * MB, ActionMode.MANUAL, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 19_000 * MB, 25_000 * MB, ActionMode.DISABLED, DISABLED, AUTOMATIC},
                // Resize up VMem from max (16 GB) to above max
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 16_384 * MB, 25_000 * MB, ActionMode.DISABLED, DISABLED, AUTOMATIC},
                // Test case resize Mem from below max to above max (max=16GB)
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 1_000 * MB, 1_000_000 * MB, ActionMode.MANUAL, MANUAL, AUTOMATIC},

                // Resize Down
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 25_000 * MB, 19_000 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 19_000 * MB, 12_000 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 800 * MB, 512 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 19_000 * MB, 512 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 200 * MB, 100 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Resize down VMem from 512 MB to below min
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 512 * MB, 100 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Test case resize Mem from above min to below min (min=512MB)
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VMEM_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 100_000 * MB, 100 * MB, ActionMode.AUTOMATIC, DISABLED, AUTOMATIC},
                // Other commodity resize
                {CommodityAttribute.CAPACITY, CommodityDTO.CommodityType.VSTORAGE_VALUE,
                        EntityType.VIRTUAL_MACHINE_VALUE, 300_000, 500_000, ActionMode.RECOMMEND, DISABLED, AUTOMATIC},
        });

    }

    @Test
    public void testRangeAwareResize() {
        Setting vCpuAboveMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold.getSettingName())
                .setEnumSettingValue(modeAboveMax).build();
        Setting vMemBelowMinThreshold = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVmemBelowMinThreshold.getSettingName())
                .setEnumSettingValue(modeBelowMin).build();
        Setting vMemAboveMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVmemAboveMaxThreshold.getSettingName())
                .setEnumSettingValue(modeAboveMax).build();
        Setting vCpuBelowMinThreshold = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold.getSettingName())
                .setEnumSettingValue(modeBelowMin).build();
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold.getSettingName(), vCpuAboveMaxThreshold);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold.getSettingName(), vCpuBelowMinThreshold);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVmemAboveMaxThreshold.getSettingName(), vMemAboveMaxThreshold);
        rangeAwareSettingsForEntity.put(ConfigurableActionSettings.ResizeVmemBelowMinThreshold.getSettingName(), vMemBelowMinThreshold);

        ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
                .setId(10289)
                .setExplanation(Explanation.getDefaultInstance())
                .setDeprecatedImportance(0);
        final ActionDTO.Action recommendation = actionBuilder
            .setExecutable(true)
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
        Action action = new Action(recommendation, 1L, actionModeCalculator, 2233L);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(rangeAwareSettingsForEntity);
        ActionModeCalculator.ModeAndSchedule actualMode = actionModeCalculator.calculateActionModeAndExecutionSchedule(action,
            entitySettingsCache);
        assertEquals(expectedActionMode, actualMode.getMode());
    }
}
