package com.vmturbo.group.setting;

import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.hamcrest.CoreMatchers;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Active;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.schedule.ScheduleStore;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Tests for {@link DefaultSettingPolicyValidator}.
 * <p>
 * Separated from {@link SettingStoreTest} so that we don't do the database setup/teardown
 * when running these.
 */
public class SettingPolicyValidatorTest {

    private static final long GROUP_ID = 99;

    private static final int ENTITY_TYPE = 10;

    private static final long SCHEDULE_OID_1 = 12L;
    private static final long SCHEDULE_OID_2 = 13L;

    private static final String SPEC_NAME = "spec";
    private static final long CURRENT_TIME = 1596646800000L;
    private static final long FUTURE_TIME = 1596650400000L;
    private static final long PAST_TIME = 1596643200000L;

    private final SettingSpecStore specStore = mock(SettingSpecStore.class);

    private final ScheduleStore scheduleStore = mock(ScheduleStore.class);

    private IGroupStore groupStore;

    private DefaultSettingPolicyValidator validator;

    private DSLContext context;

    private Clock clock;

    /**
     * Expected exception.
     */
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        clock = mock(Clock.class);
        groupStore = mock(IGroupStore.class);
        validator = new DefaultSettingPolicyValidator(specStore, groupStore, scheduleStore, clock);
        when(groupStore.getGroupsById(Collections.singleton(GROUP_ID))).thenReturn(
                Collections.singleton(Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ENTITY_TYPE))
                        .build()));
        when(specStore.getSettingSpec(any())).thenReturn(Optional.empty());
        Mockito.when(groupStore.getGroups(Mockito.any()))
                .thenReturn(Collections.singleton(Grouping.newBuilder()
                        .setId(GROUP_ID)
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ENTITY_TYPE))
                        .build()));
        context = Mockito.mock(DSLContext.class);
        when(clock.millis()).thenReturn(CURRENT_TIME);
        when(scheduleStore.getSchedules(context, Collections.singleton(SCHEDULE_OID_1))).thenReturn(
            Collections.singletonList(ScheduleProto.Schedule.newBuilder()
                .setId(SCHEDULE_OID_1)
                .setDisplayName("Test Schedule")
                .setOneTime(ScheduleProto.Schedule.OneTime.getDefaultInstance())
                .setStartTime(FUTURE_TIME)
                .setEndTime(FUTURE_TIME + TimeUnit.HOURS.toMillis(1))
                .build()
            ));
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingWithNoName() throws InvalidItemException {
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(Setting.newBuilder()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingSpecNotFound() throws InvalidItemException {
        when(specStore.getSettingSpec(any())).thenReturn(Optional.empty());
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting().build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyForGlobal() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting().build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingNoName() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, SettingPolicyInfo.newBuilder()
                .setEntityType(10)
                .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingNoEntityType() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, SettingPolicyInfo.newBuilder()
                .setName("Policy")
                .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidItemException.class)
    public void testDefaultSettingWithScope() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, SettingPolicyInfo.newBuilder()
            .setName("Policy")
            .setEntityType(10)
            .setScope(Scope.newBuilder().addGroups(GROUP_ID))
            .build(), Type.DEFAULT);
    }

    /**
     * Test when we set ActionMode setting + associated ExecutionSchedule setting.
     *
     * @throws InvalidItemException it should not happen
     */
    @Test
    public void testValidActionModeAndExecutionScheduleSettingsCombination()
            throws InvalidItemException {
        final SettingPolicyInfo settingPolicyInfo =
                setupScheduledMovePolicy(Collections.singletonList(SCHEDULE_OID_1));
        validator.validateSettingPolicy(context, settingPolicyInfo, Type.USER);
    }

    /**
     * Creates setting policy with several settings (action mode + associated execution schedule
     * settings).
     *
     * @param executionScheduleIds value for single execution schedule setting (in order to be
     * valid should have only ONE element)
     * @return setting policy
     */
    private SettingPolicyInfo setupScheduledMovePolicy(@Nonnull List<Long> executionScheduleIds) {
        final String moveExecutionScheduleSettingName =
            ActionSettingSpecs.getSubSettingFromActionModeSetting(
                ConfigurableActionSettings.Move, ActionSettingType.SCHEDULE);
        final String moveActionModeSettingName = ConfigurableActionSettings.Move.getSettingName();
        final String settingPolicyName = "testSettingPolicy";
        when(specStore.getSettingSpec(eq(moveExecutionScheduleSettingName))).thenReturn(Optional.of(
            SettingSpec.newBuilder()
                .setName(moveExecutionScheduleSettingName)
                .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                .setSortedSetOfOidSettingValueType(
                    SortedSetOfOidSettingValueType.getDefaultInstance())
                .build()));
        when(specStore.getSettingSpec(eq(moveActionModeSettingName))).thenReturn(Optional.of(
            SettingSpec.newBuilder()
                .setName(moveActionModeSettingName)
                .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                    .addAllEnumValues(Collections.singleton(ActionMode.MANUAL.name())))
                .build()));

        final Setting actionModeSetting = Setting.newBuilder()
            .setSettingSpecName(moveActionModeSettingName)
            .setEnumSettingValue(
                EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()).build())
            .build();
        final Setting executionScheduleSetting = Setting.newBuilder()
            .setSettingSpecName(moveExecutionScheduleSettingName)
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(executionScheduleIds)
                .build())
            .build();
        return SettingPolicyInfo.newBuilder()
            .setName(settingPolicyName)
            .setEntityType(ENTITY_TYPE)
            .addAllSettings(Arrays.asList(actionModeSetting, executionScheduleSetting))
            .build();
    }

    /**
     * Verifies that validation fails for a policy with an expired schedule.
     */
    @Test
    public void testValidatePolicyWithExpiredSchedule() {
        when(scheduleStore.getSchedules(context, Collections.singleton(SCHEDULE_OID_1))).thenReturn(
            Collections.singletonList((ScheduleProto.Schedule.newBuilder()
                .setId(SCHEDULE_OID_1)
                .setDisplayName("Test Schedule")
                .setOneTime(ScheduleProto.Schedule.OneTime.getDefaultInstance())
                .setStartTime(PAST_TIME)
                .setEndTime(PAST_TIME + TimeUnit.MINUTES.toMillis(30))
                .build()
            )));
        final SettingPolicyInfo settingPolicyInfo =
                setupScheduledMovePolicy(Collections.singletonList(SCHEDULE_OID_1));

        try {
            validator.validateSettingPolicy(context, settingPolicyInfo, Type.USER);
        } catch (InvalidItemException e) {
            Assert.assertThat(e.getMessage(), containsString("any future occurrences"));
            return;
        }
        Assert.fail("Test should not reach here.");
    }

    /**
     * Test that setting policy is invalid if it has execution schedule setting with more then
     * one schedule as value.
     *
     * @throws InvalidItemException if validation isn't passed for setting policy
     */
    @Test
    public void testInvalidPolicyWithSeveralExecutionSchedules() throws InvalidItemException {
        final SettingPolicyInfo settingPolicyInfo =
                setupScheduledMovePolicy(Arrays.asList(SCHEDULE_OID_1, SCHEDULE_OID_2));
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage(CoreMatchers.containsString("Execution schedule setting "
                + "`moveExecutionSchedule` should have only one schedule."));
        validator.validateSettingPolicy(context, settingPolicyInfo, Type.USER);
    }

    /**
     * Verifies that validation passes for a policy with an active schedule but without next
     * occurrences.
     */
    @Test
    public void testValidatePolicyWithActiveSchedule() {
        when(scheduleStore.getSchedules(context, Collections.singleton(SCHEDULE_OID_1))).thenReturn(
                Collections.singletonList((ScheduleProto.Schedule.newBuilder()
                        .setId(SCHEDULE_OID_1)
                        .setDisplayName("Test Schedule")
                        .setOneTime(ScheduleProto.Schedule.OneTime.getDefaultInstance())
                        .setActive(Active.newBuilder().setRemainingActiveTimeMs(100).build())
                        .build())));
        final SettingPolicyInfo settingPolicyInfo =
                setupScheduledMovePolicy(Collections.singletonList(SCHEDULE_OID_1));

        try {
            validator.validateSettingPolicy(context, settingPolicyInfo, Type.USER);
        } catch (InvalidItemException e) {
            Assert.fail("Test should not reach here.");
        }
    }

    /**
     * We couldn't set executionSchedule setting without corresponding actionMode setting in
     * setting policy.
     *
     * @throws InvalidItemException expected exception
     */
    @Test
    public void testInvalidSettingCombination() throws InvalidItemException {
        final String settingSpecName =
            ActionSettingSpecs.getSubSettingFromActionModeSetting(
                ConfigurableActionSettings.Move, ActionSettingType.SCHEDULE);
        final String settingPolicyName = "testSettingPolicy";

        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage(
                "Invalid Policy Setting: " + settingPolicyName + System.lineSeparator()
                        + "There is no corresponding ActionMode setting for current schedule setting "
                        + settingSpecName);
        when(specStore.getSettingSpec(eq(settingSpecName))).thenReturn(Optional.of(
                SettingSpec.newBuilder()
                        .setName(settingSpecName)
                        .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                        .setSortedSetOfOidSettingValueType(
                                SortedSetOfOidSettingValueType.getDefaultInstance())
                        .build()));

        final Setting executionScheduleSetting = Setting.newBuilder()
                .setSettingSpecName(settingSpecName)
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(Collections.singletonList(12L))
                        .build())
                .build();
        validator.validateSettingPolicy(context, SettingPolicyInfo.newBuilder()
                .setName(settingPolicyName)
                .setEntityType(ENTITY_TYPE)
                .addAllSettings(Collections.singletonList(executionScheduleSetting))
                .build(), Type.USER);
    }

    @Test
    public void testNumericSettingNoConstraints() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(10))
                        .build())
                .build(), Type.USER);
    }

    @Test
    public void testNumericSettingWithinConstraints() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                    .setMin(1.0f)
                    .setMax(1.2f))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(1.1f))
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testNumericSettingTooLow() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                        .setMin(1.0f)
                        .setMax(1.2f))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(0.9f))
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testNumericSettingTooHigh() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                        .setMin(1.0f)
                        .setMax(1.2f))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(1.3f))
                        .build())
                .build(), Type.USER);
    }

    @Test
    public void testStringSettingNoValidationRegex() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("foo"))
                        .build())
                .build(), Type.USER);
    }

    @Test
    public void testStringSettingValidationMatch() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.newBuilder()
                    .setValidationRegex("foo.*"))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("foo123"))
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testStringSettingInvalid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.newBuilder()
                        .setValidationRegex("foo.*"))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("boo123"))
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testEnumSettingInvalid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                        .addEnumValues("1").addEnumValues("2"))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.newBuilder()
                                .setValue("x"))
                        .build())
                .build(), Type.USER);
    }

    @Test
    public void testEnumSettingValid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                        .addEnumValues("1").addEnumValues("2"))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.newBuilder()
                                .setValue("2"))
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyEnumMismatchedTypes() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyBooleanMismatchedTypes() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyStringMismatchedTypes() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testDefaultSettingPolicyWithNoSpecName() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .clearSettingSpecName()
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build())
            .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testDefaultSettingPolicyWithBlankSpecName() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .setSettingSpecName("  ")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build())
            .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testDefaultSettingPolicyWithSchedule() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .clearSettingSpecName()
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build())
            .setScheduleId(1L)
            .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyNumericMismatchedTypes() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    /**
     * Setting value type should match.
     *
     * @throws InvalidItemException InvalidItemException
     */
    @Test
    public void testSettingPolicySetOfOidMismatchedTypes() throws InvalidItemException {
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Invalid Policy Setting: SettingPolicy"
            + System.lineSeparator() + "Mismatched value. Got BOOLEAN_SETTING_VALUE and expected "
            + "SORTED_SET_OF_OID_SETTING_VALUE");

        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build())
            .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyInvalidGroup() throws Exception {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        Mockito.when(groupStore.getGroups(Mockito.any())).thenReturn(Collections.emptyList());
        validator.validateSettingPolicy(context, newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testGroupScopeEntityTypeMismatch() throws Exception {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        Mockito.when(groupStore.getGroups(Mockito.any()))
                .thenReturn(Collections.singleton(Grouping.newBuilder()
                        .setId(7L)
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ENTITY_TYPE + 1))
                        .build()));
        validator.validateSettingPolicy(context, newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testGroupRetrievalDatabaseError() throws Exception {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .build()));
        when(groupStore.getGroups(any())).thenThrow(DataAccessException.class);
        validator.validateSettingPolicy(context, newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyAndSettingSpecEntityTypeMismatch()
            throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder()
                                        .addEntityType(ENTITY_TYPE + 1))))
                .build()));
        validator.validateSettingPolicy(context, newInfo()
                .addSettings(newSetting().build())
                .build(), Type.USER);
    }

    /**
     * Unordered list of oid is invalid.
     *
     * @throws InvalidItemException InvalidItemException
     */
    @Test
    public void testSettingPolicyUnorderedSetOfOid() throws InvalidItemException {
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Invalid Policy Setting: SettingPolicy"
            + System.lineSeparator() + "Value [3, 1, 2, 4] is not strictly ordered.");

        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(Arrays.asList(3L, 1L, 2L, 4L))).build())
            .build(), Type.USER);
    }

    /**
     * Not strictly ordered list of oid is invalid.
     *
     * @throws InvalidItemException InvalidItemException
     */
    @Test
    public void testSettingPolicyNotStrictlyOrderedSetOfOid() throws InvalidItemException {
        expectedException.expect(InvalidItemException.class);
        expectedException.expectMessage("Invalid Policy Setting: SettingPolicy"
            + System.lineSeparator() + "Value [1, 2, 2, 3] is not strictly ordered.");

        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(Arrays.asList(1L, 2L, 2L, 3L))).build())
            .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testDiscoveredMissingTargetId() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .addEnumValues("1").addEnumValues("2"))
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .addSettings(newSetting()
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue("2"))
                .build())
            .build(), Type.DISCOVERED);
    }

    @Test(expected = InvalidItemException.class)
    public void testUserWithTargetId() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .addEnumValues("1").addEnumValues("2"))
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .setTargetId(1234L)
            .addSettings(newSetting()
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue("2"))
                .build())
            .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testDefaultWithTargetId() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .addEnumValues("1").addEnumValues("2"))
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .setTargetId(1234L)
            .addSettings(newSetting()
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue("2"))
                .build())
            .build(), Type.DEFAULT);
    }

    @Test
    public void testDiscoveredWithTargetId() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .addEnumValues("1").addEnumValues("2"))
            .build()));
        validator.validateSettingPolicy(context, newInfo()
            .setTargetId(1234L)
            .addSettings(newSetting()
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue("2"))
                .build())
            .build(), Type.DISCOVERED);
    }

    private SettingPolicyInfo.Builder newInfo() {
        return SettingPolicyInfo.newBuilder()
                .setName("SettingPolicy")
                .setEntityType(ENTITY_TYPE)
                .setScope(Scope.newBuilder()
                    .addGroups(GROUP_ID));
    }

    private Setting.Builder newSetting() {
        return Setting.newBuilder()
                .setSettingSpecName(SPEC_NAME)
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance());
    }

    private SettingSpec.Builder newEntitySettingSpec() {
        return SettingSpec.newBuilder()
                .setName(SPEC_NAME)
                .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance());
    }

    /**
     * Validate default policies defined in {@link EntitySettingSpecs}.
     */
    @Test
    public void testDefaultPoliciesFromSpec() {
        final ThinTargetCache thinTargetCache = Mockito.mock(ThinTargetCache.class);
        DefaultSettingPolicyValidator validator = new DefaultSettingPolicyValidator(
                new EnumBasedSettingSpecStore(false, false, thinTargetCache),
                mock(IGroupStore.class), scheduleStore, clock);

            List<InvalidItemException> exceptions = Lists.newArrayList();
        List<SettingSpec> settings = Arrays.stream(EntitySettingSpecs.values()).map(
                EntitySettingSpecs::getSettingSpec).collect(Collectors.toList());
        Collection<SettingPolicyInfo> policies =
                DefaultSettingPolicyCreator.defaultSettingPoliciesFromSpecs(settings).values();
        for (SettingPolicyInfo policy : policies) {
            try {
                validator.validateSettingPolicy(context, policy, Type.DEFAULT);
            } catch (InvalidItemException e) {
                exceptions.add(e);
            }
        }
        Assert.assertTrue(invalidItemMessages(exceptions), exceptions.isEmpty());
    }

    private String invalidItemMessages(List<InvalidItemException> exceptions) {
        return "Invalid policies found:\n" + exceptions.stream()
                .map(Exception::getMessage)
                .collect(Collectors.joining("\n"));

    }
}
