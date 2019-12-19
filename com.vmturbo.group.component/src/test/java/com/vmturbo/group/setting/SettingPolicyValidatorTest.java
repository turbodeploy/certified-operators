package com.vmturbo.group.setting;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
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
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Daily;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.DayOfWeek;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Monthly;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Weekly;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.group.IGroupStore;

/**
 * Tests for {@link DefaultSettingPolicyValidator}.
 * <p>
 * Separated from {@link SettingStoreTest} so that we don't do the database setup/teardown
 * when running these.
 */
public class SettingPolicyValidatorTest {

    private static final long GROUP_ID = 99;

    private static final int ENTITY_TYPE = 10;

    private static final String SPEC_NAME = "spec";

    private final SettingSpecStore specStore = mock(SettingSpecStore.class);

    private IGroupStore groupStore;

    private DefaultSettingPolicyValidator validator;

    /**
     * Expected exception.
     */
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        groupStore = mock(IGroupStore.class);
        validator = new DefaultSettingPolicyValidator(specStore, groupStore);
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
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingWithNoName() throws InvalidItemException {
        validator.validateSettingPolicy(newInfo()
                .addSettings(Setting.newBuilder()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingSpecNotFound() throws InvalidItemException {
        when(specStore.getSettingSpec(any())).thenReturn(Optional.empty());
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting().build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyForGlobal() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting().build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingNoName() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
                .setEntityType(10)
                .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingNoEntityType() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
                .setName("Policy")
                .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidItemException.class)
    public void testDefaultSettingWithScope() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
            .setName("Policy")
            .setEntityType(10)
            .setScope(Scope.newBuilder().addGroups(GROUP_ID))
            .build(), Type.DEFAULT);
    }

    @Test
    public void testNumericSettingNoConstraints() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("boo123"))
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testEnumSettingInvalid() throws InvalidItemException{
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                        .addEnumValues("1").addEnumValues("2"))
                .build()));
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                        .build())
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testSettingPolicyNumericMismatchedTypes() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
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
        expectedException.expectMessage("Invalid setting policy: SettingPolicy" +
            System.lineSeparator() + "Mismatched value. Got BOOLEAN_SETTING_VALUE and expected SORTED_SET_OF_OID_SETTING_VALUE");

        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidItemException.class)
    public void testGroupRetrievalDatabaseError() throws Exception {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .build()));
        when(groupStore.getGroups(any())).thenThrow(DataAccessException.class);
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        expectedException.expectMessage("Invalid setting policy: SettingPolicy" +
            System.lineSeparator() + "Value [3, 1, 2, 4] is not strictly ordered.");

        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(newInfo()
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
        expectedException.expectMessage("Invalid setting policy: SettingPolicy" +
            System.lineSeparator() + "Value [1, 2, 2, 3] is not strictly ordered.");

        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
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
        validator.validateSettingPolicy(newInfo()
            .setTargetId(1234L)
            .addSettings(newSetting()
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue("2"))
                .build())
            .build(), Type.DISCOVERED);
    }

    @Test
    public void testDefaultSettingPolicyWithSchedule() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.getDefaultInstance())
                    .build();
            validator.validateSettingPolicy(info, Type.DEFAULT);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Default setting policy should not have a schedule.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleNoStartTime() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.getDefaultInstance())
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Setting policy schedule must have start datetime.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleNoDuration() {
        try {
            validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.getDefaultInstance())
                    .build(), Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Setting policy schedule must have a duration " +
                    "consisting of either an end time or a number of minutes.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleEndBeforeStart() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder()
                            .setStartTime(123456789L)
                            .setEndTime(112358132))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Setting policy schedule end time must be after " +
                    "start time.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleInvalidMinutes() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder().setMinutes(0))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Setting policy schedule duration must be one " +
                    "minute or greater.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleNoRecurrence() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.getDefaultInstance())
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Setting policy schedule recurrence must be one " +
                    "of OneTime, Daily, Weekly, or Monthly.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleOneTimeWithEnding() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder()
                            .setOneTime(OneTime.getDefaultInstance())
                            .setPerpetual(Perpetual.getDefaultInstance()))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "OneTime setting policy schedule cannot have end " +
                    "date or be perpetual.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleRecurringNoEnding() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder().setDaily(Daily.getDefaultInstance()))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Recurring setting policy schedule must have end " +
                    "date or be perpetual.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleEndingBeforeStart() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder()
                            .setStartTime(123456789L)
                            .setLastDate(112358132L))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Last date of recurring setting policy must be after " +
                    "first date.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleWeeklyNoDays() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder().setWeekly(Weekly.getDefaultInstance()))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Weekly setting policy schedule must have at least " +
                    "one active day.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleMonthlyNoDays() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder().setMonthly(Monthly.getDefaultInstance()))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage = "Monthly setting policy schedule must have at least " +
                    "one active day.";
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testSettingPolicyScheduleMonthlyBadDays() {
        try {
            final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                    .setSchedule(Schedule.newBuilder().setMonthly(Monthly.newBuilder()
                            .addDaysOfMonth(-5)
                            .addDaysOfMonth(400)
                            .addDaysOfMonth(0)
                            .addDaysOfMonth(1)
                            .addDaysOfMonth(31)
                            .addDaysOfMonth(32)))
                    .build();
            validator.validateSettingPolicy(info, Type.USER);
            fail();
        } catch (InvalidItemException e) {
            final String expectedMessage1 = "Monthly setting policy schedule can only have " +
                    "active day(s) 1-31. 400 is invalid.";
            final String expectedMessage2 = "Monthly setting policy schedule can only have " +
                    "active day(s) 1-31. -5 is invalid.";
            final String expectedMessage3 = "Monthly setting policy schedule can only have " +
                    "active day(s) 1-31. 32 is invalid.";
            final String expectedMessage4 = "Monthly setting policy schedule can only have " +
                    "active day(s) 1-31. 0 is invalid.";
            assertTrue(e.getMessage().contains(expectedMessage1));
            assertTrue(e.getMessage().contains(expectedMessage2));
            assertTrue(e.getMessage().contains(expectedMessage3));
            assertTrue(e.getMessage().contains(expectedMessage4));
        }
    }

    @Test
    public void testSettingPolicyScheduleMonthlyValid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        final SettingPolicyInfo info = newInfo()
                .setSchedule(Schedule.newBuilder()
                        .setStartTime(123456789L)
                        .setMinutes(30)
                        .setPerpetual(Perpetual.getDefaultInstance())
                        .setMonthly(Monthly.newBuilder().addDaysOfMonth(6)))
                .build();
        validator.validateSettingPolicy(info, Type.USER);
    }

    @Test
    public void testSettingPolicyScheduleWeeklyValid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        final SettingPolicyInfo info = newInfo()
                .setSchedule(Schedule.newBuilder()
                        .setStartTime(123456789L)
                        .setMinutes(30)
                        .setPerpetual(Perpetual.getDefaultInstance())
                        .setWeekly(Weekly.newBuilder().addDaysOfWeek(DayOfWeek.THURSDAY).build()))
                .build();
        validator.validateSettingPolicy(info, Type.USER);
    }

    @Test
    public void testSettingPolicyScheduleDailyValid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        final SettingPolicyInfo info = newInfo()
                .setSchedule(Schedule.newBuilder()
                        .setStartTime(123456789L)
                        .setMinutes(30)
                        .setPerpetual(Perpetual.getDefaultInstance())
                        .setDaily(Daily.getDefaultInstance()))
                .build();
        validator.validateSettingPolicy(info, Type.USER);
    }

    @Test
    public void testSettingPolicyScheduleOneTimeValid() throws InvalidItemException {
        when(specStore.getSettingSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        final SettingPolicyInfo info = newInfo()
                .setSchedule(Schedule.newBuilder()
                        .setStartTime(123456789L)
                        .setMinutes(30)
                        .setEndTime(987654321L)
                        .setOneTime(OneTime.getDefaultInstance()))
                .build();
        validator.validateSettingPolicy(info, Type.USER);
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

}
