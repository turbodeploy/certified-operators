package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.components.common.setting.ConfigurableActionSettings.CloudComputeScale;
import static com.vmturbo.components.common.setting.ConfigurableActionSettings.CloudComputeScaleForPerf;
import static com.vmturbo.components.common.setting.ConfigurableActionSettings.CloudComputeScaleForSavings;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionModeCalculator.ActionSpecifications;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator.ModeAndSchedule;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

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

    private static final ConfigurableActionSettings[] ALL_SETTINGS =
            {CloudComputeScale,
                    CloudComputeScaleForSavings,
                    CloudComputeScaleForSavings};

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

    /**
     * Should return AUTOMATIC for a storage host action, with the target having an AUTOMATIC
     * host move action mode.
     */
    @Test
    public void testSettingHostMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.PHYSICAL_MACHINE_VALUE);
        Action aoAction = new Action(action, 1L, actionModeCalculator, 223L);
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.Move.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should use the value from settings.
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ActionModeCalculator.ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for host move.
     */
    @Test
    public void testNoSettingHostMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.PHYSICAL_MACHINE_VALUE);
        Action aoAction = new Action(action, 1L, actionModeCalculator, 445L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    ActionSettingSpecs.getSettingSpec(
                        ConfigurableActionSettings.Move.getSettingName())
                    .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return AUTOMATIC for a storage move action, with the target having an AUTOMATIC
     * storage move action mode.
     */
    @Test
    public void testSettingStorageMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.STORAGE_VALUE);
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.StorageMove.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 55L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should use the value from settings.
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for storage move.
     */
    @Test
    public void testNoSettingStorageMove() {
        final ActionDTO.Action action = createMoveAction(1, EntityType.STORAGE_VALUE);
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2343L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    ActionSettingSpecs.getSettingSpec(
                        ConfigurableActionSettings.StorageMove.getSettingName())
                    .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return the most conservative (MANUAL) for a compound move
     * action, with the target having resize thru moving set to AUTOMATIC but storage move set to
     * MANUAL.
     */
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
                .setExecutable(true)
                .build();
        // Different values for Move and StorageMove setting for this entity.
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(
                        ConfigurableActionSettings.Move.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build(),
                    ConfigurableActionSettings.StorageMove.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.MANUAL.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 23L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should choose the more conservative one.
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.MANUAL)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for compound move.
     */
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
            ActionMode.valueOf(
                ActionSettingSpecs.getSettingSpec(
                    ConfigurableActionSettings.StorageMove.getSettingName())
                    .getEnumSettingValueType().getDefault());
        final ActionMode hostMoveDefaultMode =
                ActionMode.valueOf(
                    ActionSettingSpecs.getSettingSpec(
                        ConfigurableActionSettings.Move.getSettingName())
                        .getEnumSettingValueType().getDefault());
        final ActionMode expectedDefaultMode = storageMoveDefaultMode.compareTo(hostMoveDefaultMode) < 0 ? storageMoveDefaultMode : hostMoveDefaultMode;
        Action aoAction = new Action(action, 1L, actionModeCalculator, 234234L);

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(expectedDefaultMode)));
    }

    /**
     * Should return AUTOMATIC for a scale action, with the target having an AUTOMATIC
     * scale action mode.
     */
    @Test
    public void testSettingScale() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        final String settingName = CloudComputeScale.getSettingName();
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

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for scale.
     */
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

    /**
     * Should return AUTOMATIC for a resize action, with the target having an AUTOMATIC
     * resize action mode.
     */
    @Test
    public void testSettingResize() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.Resize.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 444L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for resize.
     */
    @Test
    public void testNoSettingResize() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 44L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
            is(ModeAndSchedule.of(ActionMode.valueOf(
                ActionSettingSpecs.getSettingSpec(
                    ConfigurableActionSettings.Resize.getSettingName())
                    .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return AUTOMATIC for a reconfigure action, with the target having an AUTOMATIC
     * reconfigure action mode.
     */
    @Test
    public void testSettingReconfigure() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .setIsProvider(false)))
                .setExecutable(true)
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.Reconfigure.getSettingName(),
                        Setting.newBuilder()
                            .setSettingSpecName(ConfigurableActionSettings.Reconfigure.getSettingName())
                            .setEnumSettingValue(EnumSettingValue.newBuilder()
                                            .setValue(ActionMode.AUTOMATIC.name()))
                            .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 4545L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for reconfigure.
     */
    @Test
    public void testNoSettingReconfigure() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .setIsProvider(false)))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    ActionSettingSpecs.getSettingSpec(
                        ConfigurableActionSettings.Reconfigure.getSettingName())
                        .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return AUTOMATIC for a horizontal scale action, with the target having an AUTOMATIC
     * horizontal scale action mode and a MANUAL provision mode.
     */
    @Test
    public void testSettingHorizontalScale() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                    .setEntityToClone(ActionEntity.newBuilder()
                        .setId(7L)
                        .setType(EntityType.CONTAINER_POD_VALUE))))
                .setExecutable(true)
                .build();
        final String horizontalScaleUp = ConfigurableActionSettings.HorizontalScaleUp.getSettingName();
        final String provision = ConfigurableActionSettings.Provision.getSettingName();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(horizontalScaleUp,
                                Setting.newBuilder()
                                        .setSettingSpecName(horizontalScaleUp)
                                        .setEnumSettingValue(EnumSettingValue.newBuilder()
                                            .setValue(ActionMode.AUTOMATIC.name()))
                                        .build(),
                                provision,
                                Setting.newBuilder()
                                        .setSettingSpecName(provision)
                                        .setEnumSettingValue(EnumSettingValue.newBuilder()
                                            .setValue(ActionMode.MANUAL.name()))
                                        .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                   is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * Should return AUTOMATIC for a provision action, with the target having an AUTOMATIC
     * provision action mode.
     */
    @Test
    public void testSettingProvision() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.Provision.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Provision.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for provision.
     */
    @Test
    public void testNoSettingProvision() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    ActionSettingSpecs.getSettingSpec(
                        ConfigurableActionSettings.Provision.getSettingName())
                        .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return AUTOMATIC for a activate action, with the target having an AUTOMATIC
     * activate action mode.
     */
    @Test
    public void testSettingActivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.Activate.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Activate.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for activate.
     */
    @Test
    public void testNoSettingActivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
                is(ModeAndSchedule.of(ActionMode.valueOf(
                    ActionSettingSpecs.getSettingSpec(
                        ConfigurableActionSettings.Activate.getSettingName())
                        .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return AUTOMATIC for a suspend action, with the target having an AUTOMATIC
     * suspend action mode.
     */
    @Test
    public void testSettingDeactivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .setExecutable(true)
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(ConfigurableActionSettings.Suspend.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings.Suspend.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * ActionModeCalculator should return the default action mode when the target does have
     * and action mode setting for suspend.
     */
    @Test
    public void testNoSettingDeactivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setDeactivate(Deactivate.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(7L)
                    .setType(1))))
            .setExecutable(true)
            .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, null),
            is(ModeAndSchedule.of(ActionMode.valueOf(
                ActionSettingSpecs.getSettingSpec(
                    ConfigurableActionSettings.Suspend.getSettingName())
                    .getEnumSettingValueType().getDefault()))));
    }

    /**
     * Should return AUTOMATIC for a delete action, with the target having an AUTOMATIC
     * delete action mode.
     */
    @Test
    public void testSettingDelete() {
        Action aoAction = testSettingDeleteForEntityType(ConfigurableActionSettings.Delete, EntityType.STORAGE);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));

    }

    /**
     * Should return AUTOMATIC for a Delete Volume action, with the target having an AUTOMATIC
     * delete action mode.
     */
    @Test
    public void testSettingDeleteVolume() {
        Action aoAction = testSettingDeleteForEntityType(ConfigurableActionSettings.DeleteVolume,
                EntityType.VIRTUAL_VOLUME);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));

    }

    /**
     * Should return AUTOMATIC for Delete Application Service action, with the target having an AUTOMATIC
     * delete action mode.
     */
    @Test
    public void testSettingDeleteApplicationService() {
        Action aoAction = testSettingDeleteForEntityType(ConfigurableActionSettings.DeleteVirtualMachineSpec,
                EntityType.VIRTUAL_MACHINE_SPEC);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));

        aoAction = testSettingDeleteForEntityType(ConfigurableActionSettings.DeleteAppServicePlan,
                EntityType.APPLICATION_COMPONENT);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(aoAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));
    }

    /**
     * Should never generate Suspend Actions on {@link EntityType#APPLICATION_COMPONENT_SPEC}
     * and {@link EntityType#VIRTUAL_MACHINE_SPEC}.
     */
    @Test
    public void testNoSettingDeactivateApplicationService() {
        Action aoAction = testSettingDeleteForEntityType(ConfigurableActionSettings.Suspend,
                EntityType.VIRTUAL_MACHINE_SPEC);
        assertTrue(aoAction.getActionTranslation().getTranslationResultOrOriginal().getInfo().hasDelete());
        assertFalse(aoAction.getActionTranslation().getTranslationResultOrOriginal().getInfo().hasDeactivate());
        aoAction = testSettingDeleteForEntityType(ConfigurableActionSettings.Suspend,
                EntityType.APPLICATION_COMPONENT);
        assertFalse(aoAction.getActionTranslation().getTranslationResultOrOriginal().getInfo().hasDeactivate());
        assertTrue(aoAction.getActionTranslation().getTranslationResultOrOriginal().getInfo().hasDelete());
    }

    private Action testSettingDeleteForEntityType(
            @Nonnull final ConfigurableActionSettings setting,
            @Nonnull final EntityType entityType) {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(entityType.getNumber()))))
                .setExecutable(true)
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(setting.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(setting.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator, 2244L);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        return aoAction;
    }

    /**
     * ActionModeCalculator should return RECOMMEND for a delete action without a setting.
     */
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

    /**
     * ActionModeCalculator should not fail without action type.
     */
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
        Action resizeUpAction = getResizeUpAction(VM_ID);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.MANUAL)));

        // Not executable, action mode is RECOMMEND.
        resizeUpAction = getResizeUpAction(VM_ID, false);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
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

        // Not executable, action mode is RECOMMEND.
        resizeUpAction = getResizeUpAction(VM_ID, false);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test: Resize Up action with external approval.
     * Hot Add : False.
     * Conditions: Non Disruptive Mode: Disabled.
     * User Setting Mode: External Approval.
     * Final Output mode: External Approval.
     */
    @Test
    public void testResizeActionWithExternalApproval() {
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(VM_ID, SupportLevel.SHOW_ONLY);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.EXTERNAL_APPROVAL);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.EXTERNAL_APPROVAL)));

        // Not executable, action mode is EXTERNAL_APPROVAL.
        resizeUpAction = getResizeUpAction(VM_ID, false);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.EXTERNAL_APPROVAL)));
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

        // Not executable, action mode is RECOMMEND.
        resizeUpAction = getResizeUpAction(VM_ID, false);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
    }

    /**
     * Test: VCPU Resize with new CPSR.
     * In range mode: Recommended.
     * Hot Add : True.
     * Conditions: Non Disruptive Mode: True.
     * User Setting Mode: Manual.
     * Final Output mode: Recommended.
     */
    @Test
    public void testResizeCPSRWithHotAddNonDisruptive() {
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));
        Action resizeUpAction = getResizeUpAction(VM_ID, true, true);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
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

        // Not executable, action mode is RECOMMEND.
        resizeUpAction = getResizeUpAction(VM_ID, false);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
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

        // Not executable, action mode is DISABLED.
        resizeUpAction = getResizeUpAction(VM_ID, false);
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
            is(ModeAndSchedule.of(ActionMode.DISABLED)));
    }

    /**
     * Test: VCPU Limit removal.
     */
    @Test
    public void testLimitResizeWithNonDisrputiveEnabled() {
        // HotAdd Supported = 'true'
        // NonDisruptive Mode = 'true'
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeUpAction = createVCPULimitResizeUpAction(VM_ID, SupportLevel.SUPPORTED, true);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));

        // HotAdd Supported = 'false'
        // NonDisruptive Mode = 'true'
        hotAddSupported = false;
        final Map<String, Setting> settingsForEntityNoHotAdd = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntityNoHotAdd);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.RECOMMEND)));

        // HotAdd Supported = 'true'
        // NonDisruptive Mode = 'false'
        hotAddSupported = true;
        nonDisruptiveSetting = false;
        final Map<String, Setting> settingsForEntityDisruptive = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.AUTOMATIC);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntityDisruptive);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));
        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.AUTOMATIC)));

        // HotAdd Supported = 'false'
        // NonDisruptive Mode = 'false'
        hotAddSupported = false;
        final Map<String, Setting> settingsForEntityNoHotAddNotDisruptive = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.RECOMMEND);
        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntityNoHotAddNotDisruptive);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(Optional.of(getVMEntity(VM_ID, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction, entitiesCache),
                is(ModeAndSchedule.of(ActionMode.RECOMMEND)));
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

        // Not executable, action mode is DISABLED.
        resizeUpAction = getResizeUpAction(VM_ID, false);
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
        assertThat(actionModeCalculator.specsApplicableToAction(vStorageAction, settingsForEntity, Collections.emptySet()).toArray().length,
            is(0));
        // We have two action modes because and there is also an EnforceNonDisruptive.
        List<ActionSpecifications> actionSpecifications =
            actionModeCalculator.specsApplicableToAction(
                memReservationAction, settingsForEntity, Collections.emptySet()).collect(Collectors.toList());
        Assert.assertEquals(1, actionSpecifications.size());
        Assert.assertTrue(actionSpecifications.get(0).isNonDisruptiveEnforced());
    }

    /**
     * Checks that {@link ActionModeCalculator#specsApplicableToAction} choose correct {@link
     * EntitySettingSpecs} instance for the case when we want to move {@link
     * EntityType#BUSINESS_USER} between {@link EntityType#DESKTOP_POOL}s.
     */
    @Test
    public void checkSpecsApplicableToActionForBuMoves() {
        checkSpecsApplicableToActionForMoves(EntityType.BUSINESS_USER_VALUE,
                        EntityType.DESKTOP_POOL_VALUE, ConfigurableActionSettings.Move);
    }

    /**
     * Checks that {@link ActionModeCalculator#specsApplicableToAction} choose correct {@link
     * EntitySettingSpecs} instance for the case when we want to move {@link
     * EntityType#VIRTUAL_MACHINE} between {@link EntityType#PHYSICAL_MACHINE}s.
     */
    @Test
    public void checkSpecsApplicableToActionForVmMoves() {
        checkSpecsApplicableToActionForMoves(EntityType.VIRTUAL_MACHINE_VALUE,
                        EntityType.PHYSICAL_MACHINE_VALUE, ConfigurableActionSettings.Move);
    }

    /**
     * Checks that {@link ActionModeCalculator#specsApplicableToAction} choose correct {@link
     * EntitySettingSpecs} instance for the case when we want to move {@link
     * EntityType#VIRTUAL_MACHINE} between {@link EntityType#STORAGE}s.
     */
    @Test
    public void checkSpecsApplicableToActionForVmStorageMoves() {
        checkSpecsApplicableToActionForMoves(EntityType.VIRTUAL_MACHINE_VALUE,
                        EntityType.STORAGE_VALUE, ConfigurableActionSettings.StorageMove);
    }

    private void checkSpecsApplicableToActionForMoves(int targetType, int providerType,
                                                  ConfigurableActionSettings modeSettingSpecs) {
        final ActionDTO.Action moveAction = createMoveAction(targetType, providerType);
        final Setting moveActionModeSetting = createActionModeSetting(ActionMode.RECOMMEND,
                        modeSettingSpecs.getSettingName());
        final String enforceNonDisruptiveSettingName =
                        EntitySettingSpecs.EnforceNonDisruptive.getSettingName();
        final Setting enableNonDisruptiveSetting = Setting.newBuilder().setSettingSpecName(
                        enforceNonDisruptiveSettingName)
                        .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                        .build();
        final String moveSettingSpecName = ConfigurableActionSettings.Move.getSettingName();
        final Map<String, Setting> settingsForTargetEntity = new HashMap<>();
        settingsForTargetEntity.put(modeSettingSpecs.getSettingName(), moveActionModeSetting);
        settingsForTargetEntity.put(moveSettingSpecName,
                        createActionModeSetting(ActionMode.MANUAL, moveSettingSpecName));
        settingsForTargetEntity.put(enforceNonDisruptiveSettingName, enableNonDisruptiveSetting);
        Assert.assertThat(actionModeCalculator
                        .specsApplicableToAction(moveAction, settingsForTargetEntity, Collections.emptySet()).findAny()
                        .get().getConfigurableActionSetting(), CoreMatchers.is(modeSettingSpecs));

    }

    @Nonnull
    private static Setting createActionModeSetting(ActionMode mode, String settingName) {
        return Setting.newBuilder().setSettingSpecName(settingName).setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(mode.name()).build()).build();
    }

    @Nonnull
    private ActionDTO.Action createMoveAction(int businessUserValue, int desktopPoolValue) {
        return actionBuilder.setExecutable(true).setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder().setId(7L).setType(businessUserValue))
                        .addChanges(ChangeProvider.newBuilder()
                                        .setDestination(ActionEntity.newBuilder().setId(77L)
                                                        .setType(desktopPoolValue))))).build();
    }

    /**
     * Test: Resize for application component for different commodities.
     */
    @Test
    public void testSpecsApplicableToActionForAppComponents() {
        checkSpecsApplicableToAction(EntityType.APPLICATION_COMPONENT, CommodityDTO.CommodityType.HEAP,
            10.0f, 20.f, ConfigurableActionSettings.ResizeUpHeap);
        checkSpecsApplicableToAction(EntityType.APPLICATION_COMPONENT, CommodityDTO.CommodityType.HEAP,
            20.0f, 10.f, ConfigurableActionSettings.ResizeDownHeap);
        checkSpecsApplicableToAction(EntityType.APPLICATION_COMPONENT, CommodityDTO.CommodityType.CONNECTION,
            10.0f, 20.f, ConfigurableActionSettings.ResizeUpConnections);
        checkSpecsApplicableToAction(EntityType.APPLICATION_COMPONENT, CommodityDTO.CommodityType.CONNECTION,
            20.0f, 10.f, ConfigurableActionSettings.ResizeDownConnections);
        checkSpecsApplicableToAction(EntityType.APPLICATION_COMPONENT, CommodityDTO.CommodityType.THREADS,
            10.0f, 20.f, ConfigurableActionSettings.ResizeUpThreadPool);
        checkSpecsApplicableToAction(EntityType.APPLICATION_COMPONENT, CommodityDTO.CommodityType.THREADS,
            20.0f, 10.f, ConfigurableActionSettings.ResizeDownThreadPool);
    }

    /**
     * Test: Resize for database server for different commodities.
     */
    @Test
    public void testSpecsApplicableToActionForDBServers() {
        checkSpecsApplicableToAction(EntityType.DATABASE_SERVER, CommodityDTO.CommodityType.DB_MEM,
            10.0f, 20.f, ConfigurableActionSettings.ResizeUpDBMem);
        checkSpecsApplicableToAction(EntityType.DATABASE_SERVER, CommodityDTO.CommodityType.DB_MEM,
            20.0f, 10.f, ConfigurableActionSettings.ResizeDownDBMem);
        checkSpecsApplicableToAction(EntityType.DATABASE_SERVER, CommodityDTO.CommodityType.TRANSACTION_LOG,
            10.0f, 20.f, ConfigurableActionSettings.ResizeUpTransactionLog);
        checkSpecsApplicableToAction(EntityType.DATABASE_SERVER, CommodityDTO.CommodityType.TRANSACTION_LOG,
            20.0f, 10.f, ConfigurableActionSettings.ResizeDownTransactionLog);
        checkSpecsApplicableToAction(EntityType.DATABASE_SERVER, CommodityDTO.CommodityType.CONNECTION,
            10.0f, 20.f, ConfigurableActionSettings.ResizeUpConnections);
        checkSpecsApplicableToAction(EntityType.DATABASE_SERVER, CommodityDTO.CommodityType.CONNECTION,
            20.0f, 10.f, ConfigurableActionSettings.ResizeDownConnections);
    }

    private void checkSpecsApplicableToAction(
            EntityType entityType,
            CommodityDTO.CommodityType commodityType,
            float oldCapacity,
            float newCapacity,
            ConfigurableActionSettings expected) {
        long targetId = 7L;
        final Map<String, Setting> settingsForEntity = Collections.emptyMap();
        final Set<String> defaultSettingsForEntity = Collections.emptySet();
        final ActionDTO.Action resizeUpDbMemAction = actionBuilder.setInfo(
            ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(targetId)
                        .setType(entityType.getNumber())
                        .build())
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(commodityType.getNumber())
                        .build())
                    .setOldCapacity(oldCapacity)
                    .setNewCapacity(newCapacity)
                    .build())
                .build()
        ).build();

        List<ActionSpecifications> entitySpecs = actionModeCalculator.specsApplicableToAction(
                resizeUpDbMemAction,
                settingsForEntity,
                defaultSettingsForEntity)
            .collect(Collectors.toList());
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(expected, entitySpecs.get(0).getConfigurableActionSetting());
    }

    /**
     * Tests {@link ActionModeCalculator#specsApplicableToAction} method for disruptive
     * reversible Scale action.
     */
    @Test
    public void testSpecsApplicableToActionForDisruptiveReversibleScale() {
        // ARRANGE
        final ActionDTO.Action action = actionBuilder
                .setInfo(ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(1L)
                                        .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                                        .build())
                                .build())
                        .build())
                .setDisruptive(true)
                .setReversible(true)
                .build();

        // ACT
        final List<ActionSpecifications> entitySpecs = actionModeCalculator
                .specsApplicableToAction(action, Collections.emptyMap(), Collections.emptySet())
                .collect(Collectors.toList());

        // ASSERT
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(ConfigurableActionSettings.DisruptiveReversibleScaling,
                entitySpecs.get(0).getConfigurableActionSetting());
    }

    /**
     * Tests {@link ActionModeCalculator#specsApplicableToAction} method for disruptive
     * irreversible Scale action.
     */
    @Test
    public void testSpecsApplicableToActionForDisruptiveIrreversibleScale() {
        // ARRANGE
        final ActionDTO.Action action = actionBuilder
                .setInfo(ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(1L)
                                        .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                                        .build())
                                .build())
                        .build())
                .setDisruptive(true)
                .setReversible(false)
                .build();

        // ACT
        final List<ActionSpecifications> entitySpecs = actionModeCalculator
                .specsApplicableToAction(action, Collections.emptyMap(), Collections.emptySet())
                .collect(Collectors.toList());

        // ASSERT
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(ConfigurableActionSettings.DisruptiveIrreversibleScaling,
                entitySpecs.get(0).getConfigurableActionSetting());
    }

    /**
     * Tests {@link ActionModeCalculator#specsApplicableToAction} method for non-disruptive
     * reversible Scale action.
     */
    @Test
    public void testSpecsApplicableToActionForNonDisruptiveReversibleScale() {
        // ARRANGE
        final ActionDTO.Action action = actionBuilder
                .setInfo(ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(1L)
                                        .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                                        .build())
                                .build())
                        .build())
                .setDisruptive(false)
                .setReversible(true)
                .build();

        // ACT
        final List<ActionSpecifications> entitySpecs = actionModeCalculator
                .specsApplicableToAction(action, Collections.emptyMap(), Collections.emptySet())
                .collect(Collectors.toList());

        // ASSERT
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(ConfigurableActionSettings.NonDisruptiveReversibleScaling,
                entitySpecs.get(0).getConfigurableActionSetting());
    }

    /**
     * Tests {@link ActionModeCalculator#specsApplicableToAction} method for non-disruptive
     * irreversible Scale action.
     */
    @Test
    public void testSpecsApplicableToActionForNonDisruptiveIrreversibleScale() {
        // ARRANGE
        final ActionDTO.Action action = actionBuilder
                .setInfo(ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(1L)
                                        .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                                        .build())
                                .build())
                        .build())
                .setDisruptive(false)
                .setReversible(false)
                .build();

        // ACT
        final List<ActionSpecifications> entitySpecs = actionModeCalculator
                .specsApplicableToAction(action,  Collections.emptyMap(), Collections.emptySet())
                .collect(Collectors.toList());

        // ASSERT
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(ConfigurableActionSettings.NonDisruptiveIrreversibleScaling,
                entitySpecs.get(0).getConfigurableActionSetting());
    }

    /**
     * Tests {@link ActionModeCalculator#specsApplicableToAction} method.
     */
    @Test
    public void testSpecsApplicableToActionForScale() {
        //Volume Scale action without disruptiveness/reversibility flags.
        doTestSpecApplicableToActionForScale(EntityType.VIRTUAL_VOLUME_VALUE,
                ConfigurableActionSettings.CloudComputeScale);
        // DB scale action
        doTestSpecApplicableToActionForScale(EntityType.DATABASE_VALUE,
                ConfigurableActionSettings.CloudDBScale);
        // DB server action
        doTestSpecApplicableToActionForScale(EntityType.DATABASE_SERVER_VALUE,
                ConfigurableActionSettings.CloudDBServerScale);
    }

    private void doTestSpecApplicableToActionForScale(int entityType,
            ConfigurableActionSettings expectedValue) {
        // ARRANGE
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder().setTarget(ActionEntity.newBuilder()
                        .setId(1L)
                        .setType(entityType)
                        .build()).build())
                .build()).build();

        // ACT
        final List<ActionSpecifications> entitySpecs = actionModeCalculator
                .specsApplicableToAction(action, Collections.emptyMap(), Collections.emptySet())
                .collect(Collectors.toList());

        // ASSERT
        Assert.assertEquals(1, entitySpecs.size());
        Assert.assertEquals(expectedValue, entitySpecs.get(0).getConfigurableActionSetting());
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
            .setExecutable(true)
            .build();
        Action action = new Action(recommendation, 1L, actionModeCalculator, 2244L);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        return action;
    }

    private Action getResizeUpAction(long vmId) {
        return this.getResizeUpAction(vmId, SupportLevel.SUPPORTED);
    }

    private Action getResizeUpAction(long vmId, boolean executable) {
        return this.getResizeUpAction(vmId, SupportLevel.SUPPORTED, executable, false);
    }

    private Action getResizeUpAction(long vmId, boolean executable, boolean hasNewCpsr) {
        return this.getResizeUpAction(vmId, SupportLevel.SUPPORTED, executable, hasNewCpsr);
    }

    private Action getResizeUpAction(long vmId, SupportLevel supportLevel) {
        return this.getResizeUpAction(vmId, supportLevel, true, false);
    }

    private Action getResizeUpAction(long vmId, SupportLevel supportLevel, boolean executable, boolean hasNewCpsr) {
        final ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
            .setId(ACTION_OID)
            .setSupportingLevel(supportLevel)
            .setExplanation(Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                    .setDeprecatedStartUtilization(100)
                    .setDeprecatedEndUtilization(50)
                    .build()))
            .setDeprecatedImportance(0);
        Resize.Builder  resize = Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                        .setId(vmId)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .setCommodityAttribute(CommodityAttribute.CAPACITY)
                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setOldCapacity(2)
                .setNewCapacity(4);
        if (hasNewCpsr) {
            resize.setNewCpsr(2);
            resize.setOldCpsr(4);
        }
        final ActionDTO.Action recommendation = actionBuilder
            .setInfo(ActionInfo.newBuilder()
                .setResize(resize))
                .setExecutable(executable)
                .build();
        Action action = new Action(recommendation, 1L, actionModeCalculator, 2244L);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        return action;
    }

    private Action createVCPULimitResizeUpAction(long vmId, SupportLevel supportLevel, boolean executable) {
        final ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
                .setId(ACTION_OID)
                .setSupportingLevel(supportLevel)
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
                                .setCommodityAttribute(CommodityAttribute.LIMIT)
                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                .setOldCapacity(2)
                                .setNewCapacity(0)))
                .setExecutable(executable)
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
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold.getSettingName())
                .setEnumSettingValue(DISABLED).build();
        Setting vCpuBelowMinThreshold = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold.getSettingName())
                .setEnumSettingValue(AUTOMATIC).build();
        Setting vCpuUpInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds.getSettingName())
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(inRangeActionMode.name()).build())
                .build();
        Setting vCpuDownInBetweenThresholds = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(inRangeActionMode.name()).build())
                .build();
        Setting enableNonDisruptiveSetting = Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.EnforceNonDisruptive.getSettingName())
                        .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(nonDisruptiveValue)).build();
        settings.put(EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName(), vCpuMaxThreshold);
        settings.put(EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName(), vCpuMinThreshold);
        settings.put(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold.getSettingName(), vCpuAboveMaxThreshold);
        settings.put(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold.getSettingName(), vCpuBelowMinThreshold);
        settings.put(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds.getSettingName(), vCpuUpInBetweenThresholds);
        settings.put(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName(), vCpuDownInBetweenThresholds);
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
                ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds, ActionSettingType.SCHEDULE))
            .setSortedSetOfOidSettingValue(SettingProto.SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(scheduleIds).build())
            .build();
        return ImmutableMap.of(
            ActionSettingSpecs.getSubSettingFromActionModeSetting(
                ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds, ActionSettingType.SCHEDULE),
                vCpuUpInBetweenThresholds);
    }

    @Nonnull
    private Setting createWorkflowSetting(long workflowId, ActionSettingType actionSettingType) {
        final String vCpuUpInBetweenThresholdsWorkflowSettingName =
                ActionSettingSpecs.getSubSettingFromActionModeSetting(
                        ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
                        actionSettingType);
        final Setting vCpuUpInBetweenThresholdsWorkflowSetting = Setting.newBuilder()
                .setSettingSpecName(vCpuUpInBetweenThresholdsWorkflowSettingName)
                .setStringSettingValue(SettingProto.StringSettingValue.newBuilder()
                        .setValue(String.valueOf(workflowId))
                        .build())
                .build();
        return vCpuUpInBetweenThresholdsWorkflowSetting;
    }

    /**
     * Resize vm up configured with on generation and after execution workflows should place
     * READY, SUCCEEDED, and FAILED in the map returned by calculateWorkflowSettings.
     */
    @Test
    public void testVmemResizeUpInThresholdOnGenAfterExecWorkflowSettings() {
        final long afterExecWorkflowId = 706867209098681L;
        final long onGenWorkflowId = 123L;
        final long afterAuditVm = 706867209098714L;
        // This is the DTO taken from a real resize up action in dc17
        final ActionDTO.Action afterAuditedAction = makeResizeVmemUpAction(afterAuditVm);
        final long onGenAuditVm = 111L;
        final ActionDTO.Action onGenAuditedAction = makeResizeVmemUpAction(onGenAuditVm);

        when(entitiesCache.getSettingsForEntity(afterAuditVm)).thenReturn(
            makeVmemResizeBoundSettings()
                .put("afterExecResizeVmemUpInBetweenThresholdsActionWorkflow", Setting.newBuilder()
                    .setSettingSpecName("afterExecResizeVmemUpInBetweenThresholdsActionWorkflow")
                    .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addOids(afterExecWorkflowId)
                        .build())
                    .build())
                .build());
        when(entitiesCache.getSettingsForEntity(onGenAuditVm)).thenReturn(
            makeVmemResizeBoundSettings()
                .put("onGenResizeVmemUpInBetweenThresholdsActionWorkflow", Setting.newBuilder()
                    .setSettingSpecName("onGenResizeVmemUpInBetweenThresholdsActionWorkflow")
                    .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addOids(onGenWorkflowId)
                        .build())
                    .build())
                .build());

        Map<ActionState, Setting> actual = actionModeCalculator.calculateWorkflowSettings(afterAuditedAction, entitiesCache);
        Assert.assertTrue(actual.containsKey(ActionState.SUCCEEDED));
        Assert.assertEquals(Arrays.asList(afterExecWorkflowId),
            actual.get(ActionState.SUCCEEDED).getSortedSetOfOidSettingValue().getOidsList());
        Assert.assertTrue(actual.containsKey(ActionState.FAILED));
        Assert.assertEquals(Arrays.asList(afterExecWorkflowId),
            actual.get(ActionState.FAILED).getSortedSetOfOidSettingValue().getOidsList());

        actual = actionModeCalculator.calculateWorkflowSettings(onGenAuditedAction, entitiesCache);
        Assert.assertTrue(actual.containsKey(ActionState.READY));
        Assert.assertEquals(Arrays.asList(onGenWorkflowId),
            actual.get(ActionState.READY).getSortedSetOfOidSettingValue().getOidsList());
    }

    /**
     * Test that ModeAndSchedule calculates correctly for workflow action with execution schedule.
     * If there exists a replace workflow, action mode won't be affected by executability of an action.
     */
    @Test
    public void testExecutionScheduleForWorkflowAction() {
        // ARRANGE
        final Map<String, Setting> settingsForEntity =
                getSettingsForVM(false, com.vmturbo.api.enums.ActionMode.MANUAL);
        settingsForEntity.putAll(createScheduleSettings(Collections.singletonList(SCHEDULE_ID)));
        final Setting workflowSetting = createWorkflowSetting(1124142L, ActionSettingType.PRE);
        settingsForEntity.put(workflowSetting.getSettingSpecName(), workflowSetting);
        Action resizeUpAction = getResizeUpAction(VM_ID);

        when(entitiesCache.getSettingsForEntity(VM_ID)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(VM_ID)).thenReturn(
                Optional.of(getVMEntity(VM_ID, false)));
        long currentTime = System.currentTimeMillis();
        when(entitiesCache.getPopulationTimestamp()).thenReturn(currentTime);
        when(entitiesCache.getAcceptingUserForAction(
                resizeUpAction.getRecommendationOid())).thenReturn(Optional.empty());

        final long nextOccurrence = currentTime + TimeUnit.DAYS.toMillis(1);
        ScheduleProto.Schedule actionSchedule = ScheduleProto.Schedule.newBuilder()
                .setId(SCHEDULE_ID)
                .setDisplayName(SCHEDULE_DISPLAY_NAME)
                .setNextOccurrence(ScheduleProto.Schedule.NextOccurrence.newBuilder()
                        .setStartTime(nextOccurrence)
                        .build())
                .setActive(ScheduleProto.Schedule.Active.newBuilder()
                        .setRemainingActiveTimeMs(150000L))
                .setStartTime(MAY_2_2020_5_47)
                .setEndTime(MAY_2_2020_6_47)
                .setTimezoneId(TIMEZONE_ID)
                .build();

        when(entitiesCache.getScheduleMap()).thenReturn(
                ImmutableMap.of(SCHEDULE_ID, actionSchedule));

        // ACT
        ModeAndSchedule modeAndSchedule =
                actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction,
                        entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertNotNull(modeAndSchedule.getSchedule());
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));

        // create replace workflow setting
        final Setting replaceWorkflowSetting = createWorkflowSetting(11241433L, ActionSettingType.REPLACE);
        settingsForEntity.put(replaceWorkflowSetting.getSettingSpecName(), replaceWorkflowSetting);

        // ACT
        modeAndSchedule =
            actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction,
                entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertNotNull(modeAndSchedule.getSchedule());
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));

        // Not executable, action mode is still MANUAL if replace workflow exists.
        resizeUpAction = getResizeUpAction(VM_ID, false);

        // ACT
        modeAndSchedule =
            actionModeCalculator.calculateActionModeAndExecutionSchedule(resizeUpAction,
                entitiesCache);

        // ASSERT
        assertThat(modeAndSchedule.getMode(), is(ActionMode.MANUAL));
        assertNotNull(modeAndSchedule.getSchedule());
        assertThat(modeAndSchedule.getSchedule().getScheduleId(), is(SCHEDULE_ID));
    }

    /**
     * Test {@link ActionModeCalculator#getSpecsApplicableToScaleForPerf}.
     */
    @Test
    public void testGetSpecsApplicableToScale() {

        final ActionDTO.Action otherScale = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(EntityType.COMPUTE_TIER_VALUE)))).build();

        // regular scale for performance
        final ActionDTO.Action perfScale = createScaleForPerformanceAction(-200.0);

        // regular scale for performance with 0 savings/investment
        final ActionDTO.Action zeroPerfScale = createScaleForPerformanceAction(0.0);

        // scale for performance with savings
        final ActionDTO.Action perfWithSavingsScale = createScaleForPerformanceAction(100.0);

        // regular scale for efficiency
        final ActionDTO.Action effScale = createScaleForEfficiencyAction(100.0);

        // regular scale for efficiency with 0 savings/investment
        final ActionDTO.Action zeroEffScale = createScaleForEfficiencyAction(0.0);

        // scale for efficiency with investment
        final ActionDTO.Action effScaleWithInvestment = createScaleForEfficiencyAction(-100.0);

        // scale for performance with -0.0 savings/investment
        final ActionDTO.Action negativeZeroPerfScale = createScaleForPerformanceAction(-0.0);
        // scale for efficiency with -0.0 savings/investment
        final ActionDTO.Action negativeZeroEffScale = createScaleForEfficiencyAction(-0.0);

        final double roundingError = 1.9054859495826193e-9;

        // scale for performance with negative extremely small savings/investment
        final ActionDTO.Action negativeErrorPerfScale = createScaleForPerformanceAction(-roundingError);
        // scale for efficiency with  negative extremely small savings/investment
        final ActionDTO.Action negativeErrorEffScale = createScaleForEfficiencyAction(-roundingError);

        // scale for performance with extremely small savings/investment
        final ActionDTO.Action errorPerfScale = createScaleForPerformanceAction(roundingError);
        // scale for efficiency with  extremely small savings/investment
        final ActionDTO.Action errorEffScale = createScaleForEfficiencyAction(roundingError);

        final Setting recommend = Setting.newBuilder()
                .setSettingSpecName("")
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(ActionMode.RECOMMEND.name()).build())
                .build();

        final Setting manual = Setting.newBuilder().setSettingSpecName("").setEnumSettingValue(
                EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()).build()).build();

        final Setting automatic = Setting.newBuilder()
                .setSettingSpecName("")
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()).build())
                .build();

        final List<Setting> case1 = ImmutableList.of(manual, automatic, recommend);

        // all 3 settings are defined only in default policy
        final Set<String> defaultSettings = getDefaultPolicySettings();
        doGetScaleSpecTest(otherScale, case1, defaultSettings, CloudComputeScale);
        doGetScaleSpecTest(perfScale, case1, defaultSettings, CloudComputeScaleForPerf);
        doGetScaleSpecTest(effScale, case1, defaultSettings, CloudComputeScaleForSavings);
        doGetScaleSpecTest(perfWithSavingsScale, case1, defaultSettings, CloudComputeScaleForPerf);
        doGetScaleSpecTest(effScaleWithInvestment, case1, defaultSettings, CloudComputeScale);

        // all 3 settings are defined in custom policy
        // if 'Scale All' is specified - it overrides all other settings
        final Set<String> customSettings = Collections.emptySet();
        doGetScaleSpecTest(otherScale, case1, customSettings, CloudComputeScale);
        doGetScaleSpecTest(perfScale, case1, customSettings, CloudComputeScale);
        doGetScaleSpecTest(effScale, case1, customSettings, CloudComputeScale);
        doGetScaleSpecTest(perfWithSavingsScale, case1, customSettings, CloudComputeScale);
        doGetScaleSpecTest(effScaleWithInvestment, case1, customSettings, CloudComputeScale);

        // 'Scale for Performance' and 'Scale for Savings' are set
        final Set<String> onlyAllIsDefault = getDefaultPolicySettings(
                CloudComputeScaleForPerf, CloudComputeScaleForSavings);
        doGetScaleSpecTest(otherScale, case1, onlyAllIsDefault, CloudComputeScale);
        doGetScaleSpecTest(perfScale, case1, onlyAllIsDefault, CloudComputeScaleForPerf);
        doGetScaleSpecTest(effScale, case1, onlyAllIsDefault, CloudComputeScaleForSavings);
        doGetScaleSpecTest(perfWithSavingsScale, case1, onlyAllIsDefault, CloudComputeScaleForPerf);
        doGetScaleSpecTest(effScaleWithInvestment, case1, onlyAllIsDefault, CloudComputeScale);

        // 'Scale for Performance' is set in custom policy
        final Set<String> perfIsOverridden = getDefaultPolicySettings(
                CloudComputeScaleForPerf);
        //scale for perf and scale for savings set to automatic
        //we have perf action with savings, CloudComputeScaleForPerf is expected
        final List<Setting> case2 = ImmutableList.of(recommend, automatic, automatic);
        doGetScaleSpecTest(perfWithSavingsScale, case2, perfIsOverridden, CloudComputeScaleForPerf);

        final List<Setting> case3 = ImmutableList.of(recommend, manual, automatic);
        doGetScaleSpecTest(perfScale, case3, perfIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(zeroPerfScale, case3, perfIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(perfWithSavingsScale, case3, perfIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(effScaleWithInvestment, case3, perfIsOverridden, CloudComputeScale);

        doGetScaleSpecTest(negativeZeroPerfScale, case3, perfIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(negativeZeroEffScale, case3, perfIsOverridden, CloudComputeScaleForSavings);
        doGetScaleSpecTest(negativeErrorPerfScale, case3, perfIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(negativeErrorEffScale, case3, perfIsOverridden, CloudComputeScaleForSavings);
        doGetScaleSpecTest(errorPerfScale, case3, perfIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(errorEffScale, case3, perfIsOverridden, CloudComputeScaleForSavings);


        // 'Scale for Savings' is set in custom policy
        final Set<String> savingsIsOverridden = getDefaultPolicySettings(
                CloudComputeScaleForSavings);
        doGetScaleSpecTest(perfScale, case3, savingsIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(effScale, case3, savingsIsOverridden, CloudComputeScaleForSavings);
        doGetScaleSpecTest(zeroEffScale, case3, savingsIsOverridden, CloudComputeScaleForSavings);
        doGetScaleSpecTest(perfWithSavingsScale, case3, savingsIsOverridden,
                CloudComputeScaleForSavings);
        doGetScaleSpecTest(effScaleWithInvestment, case3, savingsIsOverridden, CloudComputeScale);


        doGetScaleSpecTest(negativeZeroPerfScale, case3, savingsIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(negativeZeroEffScale, case3, savingsIsOverridden, CloudComputeScaleForSavings);
        doGetScaleSpecTest(negativeErrorPerfScale, case3, savingsIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(negativeErrorEffScale, case3, savingsIsOverridden, CloudComputeScaleForSavings);
        doGetScaleSpecTest(errorPerfScale, case3, savingsIsOverridden, CloudComputeScaleForPerf);
        doGetScaleSpecTest(errorEffScale, case3, savingsIsOverridden, CloudComputeScaleForSavings);
    }

    /**
     * Create scale for performance action mock.
     *
     * @param savings savings amount
     * @return scale for performance action mock
     */
    private ActionDTO.Action createScaleForPerformanceAction(double savings) {
        return createScaleAction(ChangeProviderExplanation.newBuilder()
                .setCongestion(Congestion.newBuilder().build())
                .build(), savings);
    }

    /**
     * Create scale for efficiency action mock.
     *
     * @param savings savings amount
     * @return scale for efficiency action mock
     */
    private ActionDTO.Action createScaleForEfficiencyAction(double savings) {
        return createScaleAction(ChangeProviderExplanation.newBuilder()
                .setEfficiency(Efficiency.newBuilder().build())
                .build(), savings);
    }

    /**
     * Create action mock.
     *
     * @param changeProviderExplanation scale explanation
     * @param savings savings amount
     * @return action mock
     */
    private ActionDTO.Action createScaleAction(ChangeProviderExplanation changeProviderExplanation,
            double savings) {
        return actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(EntityType.COMPUTE_TIER_VALUE))))
                .setExplanation(Explanation.newBuilder()
                        .setScale(ScaleExplanation.newBuilder()
                                .addChangeProviderExplanation(changeProviderExplanation)))
                .setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(savings).build())
                .build();
    }

    /**
     * Create a setting->default policy mapping, all settings that are not specified will be
     * treated as default.
     *
     * @param settings settings that are specified in custom policy
     * @return setting->default policy mapping
     */
    private Set<String> getDefaultPolicySettings(
            ConfigurableActionSettings... settings) {

        Set<ConfigurableActionSettings> temp = new HashSet<>(Arrays.asList(ALL_SETTINGS));
        for (ConfigurableActionSettings setting : settings) {
            temp.remove(setting);
        }
        Set<String> result = new HashSet<>();
        for (ConfigurableActionSettings setting : temp) {
            result.add(setting.getSettingName());
        }

        return result;
    }

    /**
     * Get {@ActionSpecifications} applicable to action and compare it with expected value.
     *
     * @param action mocked action
     * @param settings entity settings
     * @param defaultSettings settings from default policies
     * @param expected expected result.
     */
    private void doGetScaleSpecTest(ActionDTO.Action action, List<Setting> settings,
            Set<String> defaultSettings, ConfigurableActionSettings expected) {
        final Map<String, Setting> settingsForTargetEntity = new HashMap<>();
        settingsForTargetEntity.put(CloudComputeScale.getSettingName(), settings.get(0));
        settingsForTargetEntity.put(CloudComputeScaleForPerf.getSettingName(), settings.get(1));
        settingsForTargetEntity.put(CloudComputeScaleForSavings.getSettingName(), settings.get(2));
        ConfigurableActionSettings result = actionModeCalculator.getVmScaleActionSetting(
                action, settingsForTargetEntity, defaultSettings);
        Assert.assertEquals(expected, result);
    }

    private static ImmutableMap.Builder<String, Setting> makeVmemResizeBoundSettings() {
        return ImmutableMap.<String, Setting>builder()
            .put("resizeVmemMinThreshold", Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(512.0f)
                    .build())
                .build())
            .put("resizeVmemMaxThreshold", Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(131072.0f)
                    .build())
                .build());
    }

    private static ActionDTO.Action makeResizeVmemUpAction(long vmOid) {
        return ActionDTO.Action.newBuilder()
            .setId(706867209099129L)
            .setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(vmOid)
                        .setType(EntityType.VIRTUAL_MACHINE.getNumber())
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .build())
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(53)
                        .build())
                    .setOldCapacity(3145728.0f)
                    .setNewCapacity(4194304.0f)
                    .setHotAddSupported(false)
                    .build())
                .buildPartial())
            .setDeprecatedImportance(1.0E22)
            .setExplanation(Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                    .setDeprecatedStartUtilization(1.0f)
                    .setDeprecatedEndUtilization(0.75f)
                    .build())
                .build())
            .setExecutable(true)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .build();
    }
}
