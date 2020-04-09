package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
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

    @Test
    public void testSettingHostMove() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(77L)
                                        // Move to host
                                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Move.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should use the value from settings.
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingHostMove() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .addChanges(ChangeProvider.newBuilder()
                            .setDestination(ActionEntity.newBuilder()
                                .setId(77L)
                                // Move to host
                                .setType(EntityType.PHYSICAL_MACHINE_VALUE)))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(ActionMode.valueOf(EntitySettingSpecs.Move.getSettingSpec().getEnumSettingValueType().getDefault())));
    }

    @Test
    public void testSettingStorageMove() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(77L)
                                        // Move to host
                                        .setType(EntityType.STORAGE_VALUE)))))
                .build();
        when(entitiesCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.StorageMove.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should use the value from settings.
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingStorageMove() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(ActionEntity.newBuilder()
                                        .setId(77L)
                                        // Move to host
                                        .setType(EntityType.STORAGE_VALUE)))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(ActionMode.valueOf(EntitySettingSpecs.StorageMove.getSettingSpec().getEnumSettingValueType().getDefault())));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        // Should choose the more conservative one.
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.MANUAL));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);

        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(expectedDefaultMode));
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
        final Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingScale() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        final Action aoAction = new Action(action, 1L, actionModeCalculator);
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(ActionMode.RECOMMEND));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingResize() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
            is(ActionMode.valueOf(EntitySettingSpecs.Resize.getSettingSpec().getEnumSettingValueType().getDefault())));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingReconfigure() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(ActionMode.valueOf(EntitySettingSpecs.Reconfigure.getSettingSpec().getEnumSettingValueType().getDefault())));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingProvision() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(ActionMode.valueOf(EntitySettingSpecs.Provision.getSettingSpec().getEnumSettingValueType().getDefault())));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingActivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
                is(ActionMode.valueOf(EntitySettingSpecs.Activate.getSettingSpec().getEnumSettingValueType().getDefault())));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
                is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingDeactivate() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setDeactivate(Deactivate.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(7L)
                    .setType(1))))
            .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
            is(ActionMode.valueOf(EntitySettingSpecs.Suspend.getSettingSpec().getEnumSettingValueType().getDefault())));
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
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        aoAction.getActionTranslation().setPassthroughTranslationSuccess();
        assertThat(actionModeCalculator.calculateActionMode(aoAction, entitiesCache),
            is(ActionMode.AUTOMATIC));
    }

    @Test
    public void testNoSettingDelete() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(7L)
                    .setType(1))))
            .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null),
            is(ActionMode.RECOMMEND));
    }

    @Test
    public void testUnsetActionType() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder())
                .build();
        Action aoAction = new Action(action, 1L, actionModeCalculator);
        assertThat(actionModeCalculator.calculateActionMode(aoAction, null), is(ActionMode.RECOMMEND));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeDownAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.RECOMMEND));
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
        long vmId = 122L;
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeDownAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.RECOMMEND));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeDownAction = getResizeDownAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.MANUAL));
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
        long vmId = 122L;
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = false;
        Action resizeDownAction = getResizeDownAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.MANUAL));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeDownAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.DISABLED));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeDownAction = getResizeDownAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.DISABLED));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeDownAction = getResizeUpAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeDownAction, entitiesCache), is(ActionMode.MANUAL));
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
        long vmId = 122L;
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = true;
        Action resizeUpAction = getResizeUpAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeUpAction, entitiesCache), is(ActionMode.RECOMMEND));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeUpAction, entitiesCache), is(ActionMode.MANUAL));
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
        long vmId = 122L;
        boolean hotAddSupported = false;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.MANUAL);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeUpAction, entitiesCache), is(ActionMode.MANUAL));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = true;
        Action resizeUpAction = getResizeUpAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeUpAction, entitiesCache), is(ActionMode.DISABLED));
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
        long vmId = 122L;
        boolean hotAddSupported = true;
        boolean nonDisruptiveSetting = false;
        Action resizeUpAction = getResizeUpAction(vmId);
        final Map<String, Setting> settingsForEntity = getSettingsForVM(nonDisruptiveSetting, com.vmturbo.api.enums.ActionMode.DISABLED);
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);
        when(entitiesCache.getEntityFromOid(vmId)).thenReturn(Optional.of(getVMEntity(vmId, hotAddSupported)));

        assertThat(actionModeCalculator.calculateActionMode(resizeUpAction, entitiesCache), is(ActionMode.DISABLED));
    }

    /**
     * Test: Memory Resize for virtual machine for different commodities.
     */
    @Test
    public void testSpecsApplicableToActionForVMs() {
        long vmId = 122L;
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
        when(entitiesCache.getSettingsForEntity(vmId)).thenReturn(settingsForEntity);

        // VStorage commodities do not have a specific setting. We always return a RECOMMENDED
        // action mode
        assertThat(actionModeCalculator.specsApplicableToAction(vStorageAction, settingsForEntity).toArray().length,
            is(0));
        // We have two action modes because in addition to the one originated from the
        // settings there is also a EnforceNonDisruptive.
        assertThat(actionModeCalculator.specsApplicableToAction(memReservationAction, settingsForEntity).toArray().length,
            is(2));
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
        Action action = new Action(recommendation, 1L, actionModeCalculator);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        return action;
    }

    private Action getResizeUpAction(long vmId) {
        final ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
            .setId(10289)
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
        Action action = new Action(recommendation, 1L, actionModeCalculator);
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
        return ActionPartialEntity.newBuilder()
            .setOid(vmId)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySold(vCPU)
            .addCommoditySold(vMEM)
            .build();
    }
}
