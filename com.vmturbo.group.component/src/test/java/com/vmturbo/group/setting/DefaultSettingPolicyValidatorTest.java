package com.vmturbo.group.setting;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Optional;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.schedule.ScheduleStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Verifies that DefaultSettingPolicyValidator validates SettingPolicyInfo for missing and invalid
 * values.
 */
public class DefaultSettingPolicyValidatorTest {

    private static final String SETTING_NAME = "Storage Defaults";
    private static final String RESIZE_ACTION_MODE_SETTING_NAME =
        ActionSettingSpecs.getSubSettingFromActionModeSetting(
            ConfigurableActionSettings.Resize,
            ActionSettingType.ACTION_MODE
        );
    private static final String RESIZE_EXECUTION_SCHEDULE_SETTING_NAME =
        ActionSettingSpecs.getSubSettingFromActionModeSetting(
            ConfigurableActionSettings.Resize,
            ActionSettingType.SCHEDULE
        );

    private DefaultSettingPolicyValidator defaultSettingPolicyValidator;

    /**
     * Setup the mocks used by DefaultSettingPolicyValidator.
     */
    @Before
    public void setup() {
        SettingSpecStore settingSpecStore = mock(SettingSpecStore.class);
        when(settingSpecStore.getSettingSpec(RESIZE_ACTION_MODE_SETTING_NAME))
            .thenReturn(Optional.of(
                ActionSettingSpecs.getSettingSpec(RESIZE_ACTION_MODE_SETTING_NAME)));
        when(settingSpecStore.getSettingSpec(RESIZE_EXECUTION_SCHEDULE_SETTING_NAME))
            .thenReturn(Optional.of(
                ActionSettingSpecs.getSettingSpec(RESIZE_EXECUTION_SCHEDULE_SETTING_NAME)));

        defaultSettingPolicyValidator = new DefaultSettingPolicyValidator(
            settingSpecStore,
            mock(IGroupStore.class),
            mock(ScheduleStore.class),
            mock(Clock.class)
        );
    }

    /**
     * A policy with no execution schedule is valid.
     *
     * @throws InvalidItemException should not be thrown.
     */
    @Test
    public void testNoExecutionSchedule() throws InvalidItemException {
        SettingPolicyInfo settingPolicyInfo = SettingPolicyInfo.newBuilder()
            .setName(SETTING_NAME)
            .setDisplayName(SETTING_NAME)
            .setEntityType(EntityType.STORAGE_VALUE)
            // Add a Storage -> Resize -> ActionMode = MANUAL setting
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(ActionSettingSpecs.getSubSettingFromActionModeSetting(
                    ConfigurableActionSettings.Resize,
                    ActionSettingType.ACTION_MODE))
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue(ActionMode.MANUAL.toString())
                    .buildPartial())
                .buildPartial())
            .buildPartial();

        DSLContext dslContext = mock(DSLContext.class);
        defaultSettingPolicyValidator.validateSettingPolicy(
            dslContext,
            settingPolicyInfo,
            SettingPolicy.Type.DEFAULT
        );

        // No assert needed. No exception thrown is the assert. validateSettingPolicy throws
        // InvalidItemException when there's a validation issue. JUnit will fail the unit test
        // if an exception is thrown.
    }

    /**
     * A setting with execution schedule that is empty is valid. It has the same meaning as a policy
     * without an execution schedule.
     *
     * @throws InvalidItemException should not be thrown.
     */
    @Test
    public void testEmptyExecutionSchedule() throws InvalidItemException {
        SettingPolicyInfo settingPolicyInfo = SettingPolicyInfo.newBuilder()
            .setName(SETTING_NAME)
            .setDisplayName(SETTING_NAME)
            .setEntityType(EntityType.STORAGE_VALUE)
            // Add a Storage -> Resize -> ActionMode = MANUAL setting
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(RESIZE_ACTION_MODE_SETTING_NAME)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue(ActionMode.MANUAL.toString())
                    .buildPartial())
                .buildPartial())
            // Add an execution schedule with an empty list of schedule oids.
            // In OM-65713 we observed that 7.22.5 has execution schedule setting where the value is
            // empty.
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(RESIZE_EXECUTION_SCHEDULE_SETTING_NAME)
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    // don't add any oids to make an empty list of oids
                    .buildPartial())
                .buildPartial())
            .buildPartial();

        DSLContext dslContext = mock(DSLContext.class);
        defaultSettingPolicyValidator.validateSettingPolicy(
            dslContext,
            settingPolicyInfo,
            SettingPolicy.Type.DEFAULT
        );

        // No assert needed. No exception thrown is the assert. validateSettingPolicy throws
        // InvalidItemException when there's a validation issue. JUnit will fail the unit test
        // if an exception is thrown.
    }

}
