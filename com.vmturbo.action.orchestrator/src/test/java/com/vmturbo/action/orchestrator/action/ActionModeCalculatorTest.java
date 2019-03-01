package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ActionModeCalculatorTest {

    private EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);

    private ActionDTO.Action.Builder actionBuilder = ActionDTO.Action.newBuilder()
            .setId(10289)
            .setExplanation(Explanation.getDefaultInstance())
            .setImportance(0);

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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Move.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        // Should use the value from settings.
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.StorageMove.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        // Should use the value from settings.
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
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
        // Should choose the more conservative one.
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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

        assertThat(ActionModeCalculator.calculateActionMode(action, null),
                is(expectedDefaultMode));
    }

    @Test
    public void testSettingResize() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(7L)
                                .setType(1))))
                .build();
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Resize.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Reconfigure.getSettingName(),
                        Setting.newBuilder()
                            .setSettingSpecName(EntitySettingSpecs.Reconfigure.getSettingName())
                            .setEnumSettingValue(EnumSettingValue.newBuilder()
                                            .setValue(ActionMode.AUTOMATIC.name()))
                            .build()));
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Provision.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Provision.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Activate.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Activate.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
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
        when(entitySettingsCache.getSettingsForEntity(7L)).thenReturn(
                ImmutableMap.of(EntitySettingSpecs.Suspend.getSettingName(),
                        Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.Suspend.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue(ActionMode.AUTOMATIC.name()))
                                .build()));
        assertThat(ActionModeCalculator.calculateActionMode(action, entitySettingsCache),
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
        assertThat(ActionModeCalculator.calculateActionMode(action, null),
                is(ActionMode.valueOf(EntitySettingSpecs.Suspend.getSettingSpec().getEnumSettingValueType().getDefault())));
    }

    @Test
    public void testUnsetActionType() {
        final ActionDTO.Action action = actionBuilder.setInfo(ActionInfo.newBuilder())
                .build();
        assertThat(ActionModeCalculator.calculateActionMode(action, null), is(ActionMode.RECOMMEND));
    }
}
