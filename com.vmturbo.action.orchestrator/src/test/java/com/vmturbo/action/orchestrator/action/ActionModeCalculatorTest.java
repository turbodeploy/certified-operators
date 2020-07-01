package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionModeCalculator.ModeAndSchedule;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for calculating Action Mode.
 */
public class ActionModeCalculatorTest {

    private EntitiesAndSettingsSnapshot entitiesCache = mock(EntitiesAndSettingsSnapshot.class);

    private ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
        .setId(10289)
        .setExplanation(Explanation.getDefaultInstance())
        .setSupportingLevel(SupportLevel.SUPPORTED)
        .setDeprecatedImportance(0);

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    private static final EnumSettingValue DISABLED = EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()).build();
    private static final EnumSettingValue AUTOMATIC = EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()).build();

    private static final long VM_ID = 122L;
    private static final long SCHEDULE_ID = 505L;
    private static final long SCHEDULE_ID_2 = 606L;
    private static final long SCHEDULE_ID_3 = 707L;
    private static final String SCHEDULE_DISPLAY_NAME = "TestSchedule";
    private static final String TIMEZONE_ID = "America/Toronto";
    private static final String USER_NAME = "testUser";
    private static final long MAY_2_2020_5_47 = 1588441640000L;
    private static final long MAY_2_2020_6_47 = 1588445240000L;
    private static final long JAN_7_2020_12 = 1578441640000L;
    private static final long JAN_8_202_1 = 1578445240000L;
    private static final long ACTION_OID = 10289L;
    private static final long AN_HOUR_IN_MILLIS = 3600000L;

    @Test
    public void testSettingHostMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.PHYSICAL_MACHINE_VALUE);
        Action aoAction = new Action(action, 1L, actionModeCalculator, 223L);
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Move.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should use the value from settings.
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ActionModeCalculator.ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingHostMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.PHYSICAL_MACHINE_VALUE);
        Action aoAction = new Action(action, 1L, actionModeCalculator, 445L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    EntitySettingSpecs.Move.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingStorageMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.STORAGE_VALUE);
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.StorageMove.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 55L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should use the value from settings.
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingStorageMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.STORAGE_VALUE);
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2343L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    EntitySettingSpecs.StorageMove.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingCompoundMove() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(777L)
                                        // Host move
                                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(77L)
                                        // Associated storage move
                                        .setType(EntityType.STORAGE_VALUE)))))
                .build();
        // Different values for Move and StorageMove setting for this entity.
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(
                        EntitySettingSpecs.Move.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build(),
                        EntitySettingSpecs.StorageMove.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.MANUAL.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 23L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should choose the more conservative one.
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    @Test
    public void testNoSettingCompoundMove() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(777L)
                                        // Host move
                                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(77L)
                                        // Associated storage move
                                        .setType(EntityType.STORAGE_VALUE)))))
                .build();
        final ActionMode storageMoveDefaultMode =
                ActionMode.valueOf(EntitySettingSpecs.StorageMove.getSettingSpec().getEnumSettingValueType().getDefault());
        final ActionMode hostMoveDefaultMode =
                ActionMode.valueOf(EntitySettingSpecs.Move.getSettingSpec().getEnumSettingValueType().getDefault());
        final ActionMode expectedDefaultMode = storageMoveDefaultMode.compareTo(hostMoveDefaultMode) < 0 ? storageMoveDefaultMode : hostMoveDefaultMode;
        Action aoAction = new Action(action, 1L, actionModeCalculator, 234234L);

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(expectedDefaultMode)));
    }

    @Test
    public void testSettingScale() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        final String settingName = EntitySettingSpecs.Move.getSettingName();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(settingName,
                        Setting.newBuilder()
                                .setSettingSpecName(settingName)
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        final Action aoAction = new Action(action, 1L, actionModeCalculator, 23432);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingScale() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        final Action aoAction = new Action(action, 1L, actionModeCalculator, 2343L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    @Test
    public void testSettingResize() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Resize.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 444L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingResize() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 44L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
            is(ModeAndSchedule.of(ActionMode.valueOf(
                EntitySettingSpecs.Resize.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingReconfigure() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Reconfigure.getSettingName(),
                        Setting.newBuilder()
                            .setSettingSpecName(EntitySettingSpecs.Reconfigure.getSettingName())
                            .setEnumSettingValue(EnumSettingValue.newBuilder()
                                            .setValue(ActionMode.AUTOMATIC.name()))
                            .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 4545L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingReconfigure() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    EntitySettingSpecs.Reconfigure.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingProvision() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Provision.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Provision.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingProvision() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    EntitySettingSpecs.Provision.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingActivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Activate.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Activate.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingActivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    EntitySettingSpecs.Activate.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingDeactivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Suspend.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Suspend.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingDeactivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setDeactivate(Deactivate.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(7L)
                    .setType(1))))
            .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
            is(ModeAndSchedule.of(ActionMode.valueOf(
                EntitySettingSpecs.Suspend.getSettingSpec().getEnumSettingValueType().getDefault()))));
    }

    @Test
    public void testSettingDelete() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(7L)
                    .setType(1))))
            .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
            ImmutableMap.of(EntitySettingSpecs.Delete.getSettingName(),
                Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.Delete.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(ActionMode.AUTOMATIC.name()))
                    .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    @Test
    public void testNoSettingDelete() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(7L)
                    .setType(1))))
            .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    @Test
    public void testUnsetActionType() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder())
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test: Memory Resize Down in between threshold.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Enabled.
     * User Setting Mode: Manual.
     * Final Output mode: Recommend.
     */
    @Test
    public void testResizeDownWithHotAddAndNonDisruptiveEnabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeDownAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test: Memory Resize Down in between threshold.
     * In range mode: Manual.
     * Hot Add : False.
     * Conditions: Non Disruptive Mode: Enabled.
     * User Setting Mode: Manual.
     * Final Output mode: Recommend.
     */
    @Test
    public void testResizeDownWithHotAddDisabledAndNonDisruptiveEnabled() {
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeDownAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test: Memory Resize Down.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Disabled.
     * User Setting Mode: Manual.
     * Final Output mode: Manual.
     */
    @Test
    public void testResizeDownWithHotAddEnableNonDisruptiveDisabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeDownAction = getResizeDownAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    /**
     * Test: Memory Resize Down.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Disabled.
     * User Setting Mode: Manual.
     * Final Output mode: Manual.
     */
    @Test
    public void testResizeDownWithHotAddDisableNonDisruptiveDisabled() {
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = false;
        Action resizeDownAction = getResizeDownAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    /**
     * Test: Memory Resize Down in between threshold.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Enabled.
     * User Setting Mode: Manual.
     * Final Output mode: Disable.
     */
    @Test
    public void testResizeDownDisabledAndNonDisruptiveEnabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeDownAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.DISABLED)));
    }

    /**
     * Test: Memory Resize Down in between threshold.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Enabled.
     * User Setting Mode: Manual.
     * Final Output mode: Disable.
     */
    @Test
    public void testResizeDownDisabledAndNonDisruptiveDisabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeDownAction = getResizeDownAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.DISABLED)));
    }

    /**
     * Test: Memory Resize Up.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Enabled.
     * User Setting Mode: Manual.
     * Final Output mode: Manual.
     */
    @Test
    public void testResizeUpWithHotAddAndNonDisruptiveEnabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeDownAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    /**
     * Test: Memory Resize Up.
     * In range mode: Manual.
     * Hot Add : False.
     * Conditions: Non Disruptive Mode: Enabled.
     * User Setting Mode: Manual.
     * Final Output mode: Recommend.
     */
    @Test
    public void testResizeUpWithoutHotAddAndNonDisruptiveEnabled() {
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = true;
        Action resizeUpAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test: Memory Resize Up.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Disabled.
     * User Setting Mode: Manual.
     * Final Output mode: Manual.
     */
    @Test
    public void testResizeUpWithHotAddNonDisruptiveDisabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    /**
     * Test: Memory Resize Up.
     * In range mode: Manual.
     * Hot Add : False.
     * Conditions: Non Disruptive Mode: Disabled.
     * User Setting Mode: Manual.
     * Final Output mode: Manual.
     */
    @Test
    public void testResizeUpWithoutHotAddNonDisruptiveDisabled() {
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    /**
     * Test: Memory Resize Up.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Enable.
     * User Setting Mode: Manual.
     * Final Output mode: Disable.
     */
    @Test
    public void testDisableResizeUpWithNonDisrputiveEnabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeUpAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.DISABLED)));
    }

    /**
     * Test: Memory Resize Up.
     * In range mode: Manual.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: Disable.
     * User Setting Mode: Manual.
     * Final Output mode: Disable.
     */
    @Test
    public void testDisableResizeUpWithNonDisrputiveDisabled() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.DISABLED)));
    }

    /**
     * Test: Memory Resize for virtual machine for different commodities.
     */
    @Test
    public void testSpecsApplicableToActionForVMs() {
        long targetId = 7L;
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.DISABLED);
        final ActionDTO.Action vStorageAction = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(targetId)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)).setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VSTORAGE_VALUE).build()))
            ).build();
        final ActionDTO.Action memReservationAction = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(targetId)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)).setCommodityAttribute(CommodityAttribute.RESERVED).setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE).build()))
        ).build();
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);

        // VStorage commodities do not have a specific setting. We always return a RECOMMENDED
        // action mode
        assertThat(actionModeCalculator.specsApplicableToAction(vStorageAction, settingsForEntity).toArray().length,
            is(0));
        // We have two action modes because in addition to the one originated from the
        // settings there is also a EnforceNonDisruptive.
        assertThat(actionModeCalculator.specsApplicableToAction(memReservationAction, settingsForEntity).toArray().length,
            is(2));

    }

    /**
     * Checks that {@link ActionModeCalculator#specsApplicableToAction} choose correct {@link
     * EntitySettingSpecs} instance for the case when we want to move {@link
     * EntityType#BUSINESS_USER} between {@link EntityType#DESKTOP_POOL}s.
     */
    @Test
    public void checkSpecsApplicableToActionForBuMoves() {
        checkSpecsApplicableToActionForMoves(EntityType.BUSINESS_USER_VALUE,
                        EntityType.DESKTOP_POOL_VALUE, EntitySettingSpecs.BusinessUserMove);
    }

    /**
     * Checks that {@link ActionModeCalculator#specsApplicableToAction} choose correct {@link
     * EntitySettingSpecs} instance for the case when we want to move {@link
     * EntityType#VIRTUAL_MACHINE} between {@link EntityType#PHYSICAL_MACHINE}s.
     */
    @Test
    public void checkSpecsApplicableToActionForVmMoves() {
        checkSpecsApplicableToActionForMoves(EntityType.VIRTUAL_MACHINE_VALUE,
                        EntityType.PHYSICAL_MACHINE_VALUE, EntitySettingSpecs.Move);
    }

    /**
     * Checks that {@link ActionModeCalculator#specsApplicableToAction} choose correct {@link
     * EntitySettingSpecs} instance for the case when we want to move {@link
     * EntityType#VIRTUAL_MACHINE} between {@link EntityType#STORAGE}s.
     */
    @Test
    public void checkSpecsApplicableToActionForVmStorageMoves() {
        checkSpecsApplicableToActionForMoves(EntityType.VIRTUAL_MACHINE_VALUE,
                        EntityType.STORAGE_VALUE, EntitySettingSpecs.StorageMove);
    }

    private void checkSpecsApplicableToActionForMoves(int targetType, int providerType,
                    EntitySettingSpecs modeSettingSpecs) {
        final ActionDTO.Action moveAction = createMoveAction(targetType, providerType);
        final Setting moveActionModeSetting = createActionModeSetting(ActionMode.RECOMMEND,
                        modeSettingSpecs.getSettingName());
        final String enforceNonDisruptiveSettingName =
                        EntitySettingSpecs.EnforceNonDisruptive.getSettingName();
        final Setting enableNonDisruptiveSetting = Setting.newBuilder().setSettingSpecName(
                        enforceNonDisruptiveSettingName)
                        .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                        .build();
        final String moveSettingSpecName = EntitySettingSpecs.Move.getSettingName();
        final Map<String, Setting> settingsForTargetEntity = new HashMap<>();
        settingsForTargetEntity.put(modeSettingSpecs.getSettingName(), moveActionModeSetting);
        settingsForTargetEntity.put(moveSettingSpecName,
                        createActionModeSetting(ActionMode.MANUAL, moveSettingSpecName));
        settingsForTargetEntity.put(enforceNonDisruptiveSettingName, enableNonDisruptiveSetting);
        Assert.assertThat(actionModeCalculator
                        .specsApplicableToAction(moveAction, settingsForTargetEntity).findAny()
                        .get(), CoreMatchers.is(modeSettingSpecs));
    }

    @Nonnull
    private static Setting createActionModeSetting(ActionMode mode, String settingName) {
        return Setting.newBuilder().setSettingSpecName(settingName).setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(mode.name()).build()).build();
    }

    @Nonnull
    private ActionDTO.Action createMoveAction(int businessUserValue, int desktopPoolValue) {
        return actionBuilder.setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder().setId(7L).setType(businessUserValue))
                        .addChanges(ChangeProvider.newBuilder()
                                        .setDestination(ActionEntity.newBuilder().setId(77L)
                                                        .setType(desktopPoolValue))))).build();
    }

    /**
     * Test: Memory Resize for application component for different commodities.
     */
    @Test
    public void testSpecsApplicableToActionForAppComponents() {
        long vmId = 122L;
        long targetId = 7L;
        final Map<String, Setting> settingsForEntity = Collections.emptyMap();
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);

        // Heap Up for Application Components
        final ActionDTO.Action heapUpAction = actionBuilder.setInfo(
                ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(targetId)
                                        .setType(EntityType.APPLICATION_COMPONENT_VALUE)
                                        .build())
                                .setCommodityAttribute(CommodityAttribute.RESERVED)
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.HEAP_VALUE)
                                        .build())
                                .setOldCapacity(10.f)
                                .setNewCapacity(20.f)
                                .build())
                        .build()
        ).build();
        List<EntitySettingSpecs> entitySpecs = actionModeCalculator.specsApplicableToAction(heapUpAction, settingsForEntity).collect(Collectors.toList());
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(EntitySettingSpecs.ResizeUpHeap, entitySpecs.get(0));

        // Heap Up for Application Components
        final ActionDTO.Action heapDownAction = actionBuilder.setInfo(
                ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(targetId)
                                        .setType(EntityType.APPLICATION_COMPONENT_VALUE)
                                        .build())
                                .setCommodityAttribute(CommodityAttribute.RESERVED)
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.HEAP_VALUE)
                                        .build())
                                .setOldCapacity(20.f)
                                .setNewCapacity(10.f)
                                .build())
                        .build()
        ).build();
        entitySpecs = actionModeCalculator.specsApplicableToAction(heapDownAction, settingsForEntity).collect(Collectors.toList());
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(EntitySettingSpecs.ResizeDownHeap, entitySpecs.get(0));
    }

    /**
     * Test: Memory Resize for database server for different commodities.
     */
    @Test
    public void testSpecsApplicableToActionForDBServers() {
        long targetId = 7L;
        final Map<String, Setting> settingsForEntity = Collections.emptyMap();

        final ActionDTO.Action resizeUpDbMemAction = actionBuilder.setInfo(
                ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(targetId)
                                        .setType(EntityType.DATABASE_SERVER_VALUE)
                                        .build())
                                .setCommodityAttribute(CommodityAttribute.RESERVED)
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.DB_MEM_VALUE)
                                        .build())
                                .setOldCapacity(10.f)
                                .setNewCapacity(20.f)
                                .build())
                        .build()
        ).build();
        final ActionDTO.Action resizeDownDbMemAction = actionBuilder.setInfo(
                ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(targetId)
                                        .setType(EntityType.DATABASE_SERVER_VALUE)
                                        .build())
                                .setCommodityAttribute(CommodityAttribute.RESERVED)
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.DB_MEM_VALUE)
                                        .build())
                                .setOldCapacity(20.f)
                                .setNewCapacity(10.f)
                                .build())
                        .build()
        ).build();

        // Resize DBMem Up/Down for Database Server
        List<EntitySettingSpecs> entitySpecs = actionModeCalculator.specsApplicableToAction(resizeUpDbMemAction, settingsForEntity).collect(Collectors.toList());
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(EntitySettingSpecs.ResizeUpDBMem, entitySpecs.get(0));

        entitySpecs = actionModeCalculator.specsApplicableToAction(resizeDownDbMemAction, settingsForEntity).collect(Collectors.toList());
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(EntitySettingSpecs.ResizeDownDBMem, entitySpecs.get(0));
    }

    private Action getResizeDownAction(long vmId) {
        ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
            .setId(10289)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setExplanation(Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                    .setDeprecatedStartUtilization(25)
                    .setDeprecatedEndUtilization(50)
                    .build()))
            .setDeprecatedImportance(0);
        final ActionDTO.Action recommendation = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(vmId)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .setCommodityAttribute(CommodityAttribute.CAPACITY)
                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setOldCapacity(4)
                .setNewCapacity(2)))
            .build();
        Action action = new Action(recommendation, 1L, actionModeCalculator, 2244L);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        return action;
    }

    private Action getResizeUpAction(long vmId) {
        final ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
            .setId(ACTION_OID)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setExplanation(Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                    .setDeprecatedStartUtilization(100)
                    .setDeprecatedEndUtilization(50)
                    .build()))
            .setDeprecatedImportance(0);
        final ActionDTO.Action recommendation = actionBuilder
            .setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(vmId)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                    .setCommodityAttribute(CommodityAttribute.CAPACITY)
                    .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                    .setOldCapacity(2)
                    .setNewCapacity(4)))
                .build();
        Action action = new Action(recommendation, 1L, actionModeCalculator, 2244L);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        return action;
    }

    private Map<String, Setting> getSettingsForVM(boolean nonDisruptiveValue, com.vmturbo.api.enums.ActionMode inRangeActionMode) {
        Map<String, Setting> settings = Maps.newHashMap();
        Setting vCpuMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(8).build()).build();
        Setting vCpuMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(2).build()).build();
        Setting vCpuAboveMaxThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuAboveMaxThreshold.getSettingName())
                .setEnumSettingValue(DISABLED).build();
        Setting vCpuBelowMinThreshold = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuBelowMinThreshold.getSettingName())
                .setEnumSettingValue(AUTOMATIC).build();
        Setting vCpuUpInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingName())
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(inRangeActionMode.name()).build())
                .build();
        Setting vCpuDownInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.getSettingName())
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(inRangeActionMode.name()).build())
                .build();
        Setting enableNonDisruptiveSetting = Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.EnforceNonDisruptive.getSettingName())
                        .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(nonDisruptiveValue)).build();
        settings.put(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName(), vCpuMaxThreshold);
        settings.put(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName(), vCpuMinThreshold);
        settings.put(EntitySettingSpecs.ResizeVcpuAboveMaxThreshold.getSettingName(), vCpuAboveMaxThreshold);
        settings.put(EntitySettingSpecs.ResizeVcpuBelowMinThreshold.getSettingName(), vCpuBelowMinThreshold);
        settings.put(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingName(), vCpuUpInBetweenThresholds);
        settings.put(EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.getSettingName(), vCpuDownInBetweenThresholds);
        settings.put(EntitySettingSpecs.EnforceNonDisruptive.getSettingName(), enableNonDisruptiveSetting);
        return settings;
    }

    private ActionPartialEntity getVMEntity(long vmId, boolean hotAddSupported) {
        CommoditySoldDTO vCPU = CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setHotResizeInfo(HotResizeInfo.newBuilder()
                                            .setHotReplaceSupported(hotAddSupported))
                        .build();
        CommoditySoldDTO vMEM = CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                        .setHotResizeInfo(HotResizeInfo.newBuilder()
                                            .setHotReplaceSupported(hotAddSupported))
                        .build();
        ActionPartialEntity.Builder bldr = ActionPartialEntity.newBuilder()
            .setOid(vmId)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        if (hotAddSupported) {
            bldr.addCommTypesWithHotReplace(CommodityDTO.CommodityType.VCPU_VALUE)
                .addCommTypesWithHotReplace(CommodityDTO.CommodityType.VMEM_VALUE);
        }
        return bldr.build();
    }

    /**
     * This tests determining the mode of an action which is automatic and has execution window in
     * the future.
     */
    @Test
    public void testResizeWithAutomaticSchedule() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(2);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.RECOMMEND));
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
        assertThat(modeAndSchedule.getSchedule().getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(modeAndSchedule.getSchedule().getScheduleTimeZoneId(), is(TIMEZONE_ID));
        assertNull(modeAndSchedule.getSchedule().getAcceptingUser());
        assertThat(modeAndSchedule.getSchedule().getExecutionWindowActionMode(),
            is(ActionMode.AUTOMATIC));
        assertThat(modeAndSchedule.getSchedule().getScheduleStartTimestamp(), is(nextOccurrence));
        assertThat(modeAndSchedule.getSchedule().getScheduleEndTimestamp(),
            is(nextOccurrence + AN_HOUR_IN_MILLIS));
        assertNotNull(modeAndSchedule.getSchedule().toString());
        assertTrue(modeAndSchedule.getSchedule().equals(modeAndSchedule.getSchedule()));

        // ACT
        ActionDTO.ActionSpec.ActionSchedule translatedAction =
            modeAndSchedule.getSchedule().getTranslation();

        // ASSERT
        assertThat(translatedAction.getScheduleId(), is(SCHEDULE_ID));
        assertThat(translatedAction.getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(translatedAction.getScheduleTimezoneId(), is(TIMEZONE_ID));
        assertFalse(translatedAction.hasAcceptingUser());
        assertThat(translatedAction.getExecutionWindowActionMode(), is(ActionMode.AUTOMATIC));
        assertThat(translatedAction.getStartTimestamp(), is(nextOccurrence));
        assertThat(translatedAction.getEndTimestamp(),
            is(nextOccurrence + AN_HOUR_IN_MILLIS));
    }


    /**
     * This tests determining the mode of an action which is automated and has execution window in
     * the future.
     */
    @Test
    public void testResizeWithManualSchedule() {
        //ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.MANUAL);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
        assertThat(modeAndSchedule.getSchedule().getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(modeAndSchedule.getSchedule().getScheduleTimeZoneId(), is(TIMEZONE_ID));
        assertNull(modeAndSchedule.getSchedule().getAcceptingUser());
        assertThat(modeAndSchedule.getSchedule().getExecutionWindowActionMode(),
            is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getScheduleStartTimestamp(), is(nextOccurrence));
        assertThat(modeAndSchedule.getSchedule().getScheduleEndTimestamp(),
            is(nextOccurrence + AN_HOUR_IN_MILLIS));
        assertNotNull(modeAndSchedule.getSchedule().toString());
        assertTrue(modeAndSchedule.getSchedule().equals(modeAndSchedule.getSchedule()));

        //ACT
        ActionDTO.ActionSpec.ActionSchedule translatedAction =
            modeAndSchedule.getSchedule().getTranslation();

        // ASSERT
        assertThat(translatedAction.getScheduleId(), is(SCHEDULE_ID));
        assertThat(translatedAction.getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(translatedAction.getScheduleTimezoneId(), is(TIMEZONE_ID));
        assertFalse(translatedAction.hasAcceptingUser());
        assertThat(translatedAction.getExecutionWindowActionMode(), is(ActionMode.MANUAL));
        assertThat(translatedAction.getStartTimestamp(), is(nextOccurrence));
        assertThat(translatedAction.getEndTimestamp(),
            is(nextOccurrence + AN_HOUR_IN_MILLIS));

        // ARRANGE
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.of(USER_NAME));

        // ACT
        modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);
        translatedAction = modeAndSchedule.getSchedule().getTranslation();

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getAcceptingUser(), is(USER_NAME));
        assertThat(translatedAction.getAcceptingUser(), is(USER_NAME));

    }

    /**
     * Test the case when we select a mode for an action which has an associated schedule where
     * the schedule is unknown in the system. The mode calculator should choose recommend mode
     * for this action.
     */
    @Test
    public void testResizeWithUnknownSchedule() {
        //ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.MANUAL);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());
        when(entitiesCache.getScheduleMap()).thenReturn(Collections.emptyMap());

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);
        assertThat(modeAndSchedule, is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test the case when we select a mode for an action which has an associated schedule where
     * the schedule is has no future occurrence and is not currently active in the system. The mode
     * calculator should choose recommend mode for this action.
     */
    @Test
    public void testResizeWithScheduleWithNoFutureOccurrence() {
        //ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.MANUAL);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.RECOMMEND));
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
        assertThat(modeAndSchedule.getSchedule().getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(modeAndSchedule.getSchedule().getScheduleTimeZoneId(), is(TIMEZONE_ID));
        assertNull(modeAndSchedule.getSchedule().getAcceptingUser());
        assertThat(modeAndSchedule.getSchedule().getExecutionWindowActionMode(),
            is(ActionMode.MANUAL));
        assertNull(modeAndSchedule.getSchedule().getScheduleStartTimestamp());
        assertNull(modeAndSchedule.getSchedule().getScheduleEndTimestamp());
    }

    /**
     * Test the case when we select a mode for an action which has an associated schedule where
     * the schedule is active and the policy for resize is automatic. The mode
     * calculator should choose automatic mode for this action.
     */
    @Test
    public void testResizeWithAutomaticModeInSchedule() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(150000L))
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.AUTOMATIC));
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
        assertThat(modeAndSchedule.getSchedule().getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(modeAndSchedule.getSchedule().getScheduleTimeZoneId(), is(TIMEZONE_ID));
        assertNull(modeAndSchedule.getSchedule().getAcceptingUser());
        assertThat(modeAndSchedule.getSchedule().getExecutionWindowActionMode(), is(ActionMode.AUTOMATIC));
        assertThat(modeAndSchedule.getSchedule().getScheduleStartTimestamp(), is(nextOccurrence));
        assertThat(modeAndSchedule.getSchedule().getScheduleEndTimestamp(),
            is(currentTime + 150000L));
    }

    /**
     * Test the case when we select a mode for an action which has an associated schedule where
     * the schedule is active and the policy for resize is manual. The action has not been
     * accepted yet. The mode calculator should choose manual mode for this action.
     */
    @Test
    public void testResizeWithManualModeInScheduleNotAccepted() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.MANUAL);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(150000L))
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
        assertThat(modeAndSchedule.getSchedule().getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(modeAndSchedule.getSchedule().getScheduleTimeZoneId(), is(TIMEZONE_ID));
        assertNull(modeAndSchedule.getSchedule().getAcceptingUser());
        assertThat(modeAndSchedule.getSchedule().getExecutionWindowActionMode(), is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getScheduleStartTimestamp(), is(nextOccurrence));
        assertThat(modeAndSchedule.getSchedule().getScheduleEndTimestamp(),
            is(currentTime + 150000L));
    }

    /**
     * Test the case when we select a mode for an action which has an associated schedule where
     * the schedule is active and the policy for resize is manual. The action has been accepted.
     * The mode calculator should choose automatic mode for this action.
     */
    @Test
    public void testResizeWithManualModeInScheduleAccepted() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.MANUAL);
        List<Long> scheduleIds = Collections.singletonList(SCHEDULE_ID);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.of(USER_NAME));

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(150000L))
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
        assertThat(modeAndSchedule.getSchedule().getScheduleDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertThat(modeAndSchedule.getSchedule().getScheduleTimeZoneId(), is(TIMEZONE_ID));
        assertThat(modeAndSchedule.getSchedule().getAcceptingUser(), is(USER_NAME));
        assertThat(modeAndSchedule.getSchedule().getExecutionWindowActionMode(), is(ActionMode.MANUAL));
        assertThat(modeAndSchedule.getSchedule().getScheduleStartTimestamp(), is(nextOccurrence));
        assertThat(modeAndSchedule.getSchedule().getScheduleEndTimestamp(),
            is(currentTime + 150000L));
    }

    /**
     * This tests the case that there are multiple schedules associated with an action. None of
     * these schedules are active. We should associated the schedule with closer activation to
     * the action.
     */
    @Test
    public void testResizeWithMultipleSchedulesNoneActive() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        List<Long> scheduleIds = Arrays.asList(SCHEDULE_ID, SCHEDULE_ID_2);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(2);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        final long nextOccurrence2 = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule2 = actionSchedule.toBuilder()
            .setId(SCHEDULE_ID_2)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence2).build())
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID,
            actionSchedule, SCHEDULE_ID_2, actionSchedule2));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID_2));
    }

    /**
     * This tests the case that there are multiple schedules associated with an action. One of
     * these schedules is currently active. We should select the action which is currently active
     * and associate it with schedule
     */
    @Test
    public void testResizeWithMultipleSchedulesOneActive() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        List<Long> scheduleIds = Arrays.asList(SCHEDULE_ID, SCHEDULE_ID_2);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(2);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        ScheduleProto.Schedule actionSchedule2 = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID_2)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(150000L))
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID,
            actionSchedule, SCHEDULE_ID_2, actionSchedule2));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID_2));
    }

    /**
     * This tests the case that there are two schedules associated with an action. Two of
     * these schedules is currently active. We should select the action which stays active for a
     * longer period of time.
     */
    @Test
    public void testResizeWithMultipleSchedulesBothActive() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        List<Long> scheduleIds = Arrays.asList(SCHEDULE_ID, SCHEDULE_ID_2);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        final Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(2);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(100000L))
            .setStartTime(JAN_7_2020_12)
            .setEndTime(JAN_8_202_1)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        ScheduleProto.Schedule actionSchedule2 = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID_2)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(150000L))
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID,
            actionSchedule, SCHEDULE_ID_2, actionSchedule2));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID_2));
    }

    /**
     * This tests the case that there are three schedules associated with an action. One of the
     * schedules have no next occurrence and is not active, the second one has next occurrence but
     * is not active and third one is active. We should select the third schedule
     */
    @Test
    public void testResizeWithThreeSchedules() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity = getSettingsForVM(false,
            com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        List<Long> scheduleIds = Arrays.asList(SCHEDULE_ID, SCHEDULE_ID_2, SCHEDULE_ID_3);
        settingsForEntity.putAll(createScheduleSettings(scheduleIds));
        Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());


        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setStartTime(JAN_7_2020_12)
            .setEndTime(JAN_8_202_1)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule2 = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID_2)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence).build())
            .setStartTime(MAY_2_2020_5_47)
            .setEndTime(MAY_2_2020_6_47)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        final long nextOccurrence3 = currentTime + TimeUnit.DAYS.toMillis(2);
        ScheduleProto.Schedule actionSchedule3 = ScheduleProto.Schedule.newBuilder()
            .setId(SCHEDULE_ID_3)
            .setDisplayName(SCHEDULE_DISPLAY_NAME)
            .setNextOccurrence(ScheduleProto
                .Schedule.NextOccurrence.newBuilder().setStartTime(nextOccurrence3).build())
            .setActive(ScheduleProto.Schedule.Active.newBuilder().setRemainingActiveTimeMs(100000L))
            .setStartTime(JAN_7_2020_12)
            .setEndTime(JAN_8_202_1)
            .setTimezoneId(TIMEZONE_ID)
            .build();

        when(entitiesCache.getScheduleMap()).thenReturn(ImmutableMap.of(SCHEDULE_ID,
            actionSchedule, SCHEDULE_ID_2, actionSchedule2, SCHEDULE_ID_3, actionSchedule3));

        // ACT
        ModeAndSchedule modeAndSchedule = actionModeCalculator
            .calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID_3));
    }

    @Nonnull
    private Map<String, Setting> createScheduleSettings(List<Long> scheduleIds) {
        final Setting vCpuUpInBetweenThresholds = Setting.newBuilder()
            .setSettingSpecName(ActionSettingSpecs.getSubSettingFromActionModeSetting(
                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds, ActionSettingType.SCHEDULE))
            .setSortedSetOfOidSettingValue(SettingProto.SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(scheduleIds).build())
            .build();
        return ImmutableMap.of(
            ActionSettingSpecs.getSubSettingFromActionModeSetting(
                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds, ActionSettingType.SCHEDULE),
                vCpuUpInBetweenThresholds);
    }

}
