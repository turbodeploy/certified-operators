package com.vmturbo.group.schedule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.util.Sets;
import org.jooq.DSLContext;
import org.jooq.TransactionalCallable;
import org.jooq.TransactionalRunnable;
import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.RecurrenceStart;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemDeleteException.ScheduleInUseDeleteException;
import com.vmturbo.group.common.ItemNotFoundException.ScheduleNotFoundException;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.TestGroupGenerator;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.setting.FileBasedSettingsSpecStore;
import com.vmturbo.group.setting.SettingPolicyValidator;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.group.setting.SettingsUpdatesSender;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for {@link ScheduleStore}.
 */
public class ScheduleStoreTest {
    private static final String DISPLAY_NAME = "Test Schedule 1";
    private static final String DISPLAY_NAME_PERPETUAL = "Perpetual Test Schedule 1";
    private static final long START_TIME = 1446760800000L;
    private static final long END_TIME = 1446766200000L;
    private static final long LAST_DATE = 1451624399000L;
    private static final long RECURRENCE_START_TINE = 1446809400000L;
    private static final String RECUR_RULE = "FREQ=MONTHLY;BYDAY=SA;BYSETPOS=-1;INTERVAL=1;";
    private static final String TIME_ZONE_ID = "America/Argentina/ComodRivadavia";

    private static final String SETTING_TEST_JSON_SETTING_SPEC_JSON =
        "setting-test-json/setting-spec.json";

    /** Expected exceptions to test against. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);
    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private ScheduleStore scheduleStore;
    private SettingSpecStore settingSpecStore;
    private SettingStore settingStore;
    private DSLContext dslContextSpy;
    private final ScheduleValidator scheduleValidator = new DefaultScheduleValidator();
    private final IdentityProvider identityProvider = new IdentityProvider(0);
    private SettingPolicyValidator settingPolicyValidator = mock(SettingPolicyValidator.class);
    private SettingsUpdatesSender settingsUpdatesSender = mock(SettingsUpdatesSender.class);
    private IGroupStore groupStore;

    private final Schedule oneTimeSchedule = Schedule.newBuilder()
        .setDisplayName(DISPLAY_NAME)
        .setStartTime(START_TIME)
        .setEndTime(END_TIME)
        .setTimezoneId(TIME_ZONE_ID)
        .setOneTime(OneTime.getDefaultInstance())
        .build();

    private final Schedule testScheduleWithLastDate = Schedule.newBuilder()
        .setDisplayName(DISPLAY_NAME)
        .setStartTime(START_TIME)
        .setEndTime(END_TIME)
        .setLastDate(LAST_DATE)
        .setRecurRule(RECUR_RULE)
        .setTimezoneId(TIME_ZONE_ID)
        .build();

    private final Schedule testSchedulePerpetual = Schedule.newBuilder()
        .setDisplayName(DISPLAY_NAME_PERPETUAL)
        .setStartTime(START_TIME)
        .setEndTime(END_TIME)
        .setRecurRule(RECUR_RULE)
        .setPerpetual(Perpetual.getDefaultInstance())
        .setTimezoneId(TIME_ZONE_ID)
        .build();

    private final Schedule testEmptySchedule = Schedule.newBuilder()
        .build();

    private static final SettingPolicyInfo INFO = SettingPolicyInfo.newBuilder()
        .setName("test")
            .addAllSettings(Arrays.asList(Setting.newBuilder()
                    .setSettingSpecName("TestSetting")
                    .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                    .build()))
        .build();
    private static final SettingPolicy USER_POLICY = SettingPolicy.newBuilder()
            .setId(10001L)
            .setInfo(INFO)
            .setSettingPolicyType(Type.USER)
            .build();

    /**
     * Setup test.
     */
    @Before
    public void setUp() {
        dslContextSpy = spy(dbConfig.getDslContext());
        settingSpecStore = new FileBasedSettingsSpecStore(SETTING_TEST_JSON_SETTING_SPEC_JSON);
        settingStore = new SettingStore(settingSpecStore, dslContextSpy, settingPolicyValidator,
                settingsUpdatesSender);
        scheduleStore = new ScheduleStore(dslContextSpy, scheduleValidator, identityProvider);
        groupStore = new GroupDAO(dslContextSpy);
    }

    /**
     * Test schedule validation.
     * @throws Exception If test throws any exceptions
     */
    @Test(expected = InvalidItemException.class)
    public void testScheduleValidation() throws Exception {
        scheduleStore.createSchedule(testEmptySchedule);
    }

    /**
     * Test get all schedules.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testGetAllSchedules() throws Exception {
        Schedule testSchedule2 = testScheduleWithLastDate.toBuilder()
            .setDisplayName("Test Schedule 2")
            .build();
        scheduleStore.createSchedule(testScheduleWithLastDate);
        scheduleStore.createSchedule(testSchedule2);

        Collection<Schedule> fetchedSchedules = scheduleStore.getSchedules();
        assertEquals(2, fetchedSchedules.size());
    }

    /**
     * Test get schedules by IDs.
     *
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testGetAllSchedulesByIds() throws Exception {
        Schedule testSchedule2 = testScheduleWithLastDate.toBuilder()
            .setDisplayName("Test Schedule 2")
            .build();
        final Schedule schedule1 = scheduleStore.createSchedule(testSchedulePerpetual);
        assertTrue(schedule1.hasId());
        final Schedule schedule2 = scheduleStore.createSchedule(testScheduleWithLastDate);
        assertTrue(schedule2.hasId());
        scheduleStore.createSchedule(testSchedule2);

        Collection<Schedule> fetchedSchedules = scheduleStore.getSchedules();
        assertEquals(3, fetchedSchedules.size());

       fetchedSchedules = scheduleStore.getSchedules(Sets.newLinkedHashSet(schedule1.getId(),
           schedule2.getId()));
        assertEquals(2, fetchedSchedules.size());
    }

    /**
     * Test get schedule with invalid ID.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testGetScheduleInvalidId() throws Exception {
        Optional<Schedule> fetchedSchedule = scheduleStore.getSchedule(0);
        assertFalse(fetchedSchedule.isPresent());
    }

    /**
     * Test create schedule with last dat and get it by id.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testCreateOneTimeScheduleThenGetById() throws Exception {
        testCreateSchedule(oneTimeSchedule, false);
    }

    /**
     * Test create schedule with last dat and get it by id.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testCreateScheduleWithLastDateThenGetById() throws Exception {
        testCreateSchedule(testScheduleWithLastDate, true);
    }

    /**
     * Test create perpetual schedule and get it by id.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testCreateSchedulePerpetualThenGetById() throws Exception {
        testCreateSchedule(testSchedulePerpetual, true);
    }

    private void testCreateSchedule(final Schedule scheduleTotest, boolean isPerpetual) throws Exception {
        Schedule schedule = scheduleStore.createSchedule(scheduleTotest);

        assertTrue(schedule.hasId());
        verifySchedule(scheduleTotest, schedule, isPerpetual);

        Optional<Schedule> fetchedSchedule = scheduleStore.getSchedule(schedule.getId());
        assertTrue(fetchedSchedule.isPresent());
        verifySchedule(scheduleTotest, fetchedSchedule.get(), isPerpetual);
    }

    /**
     * Test create duplicate schedule.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testCreateDuplicateSchedule() throws Exception {
        scheduleStore.createSchedule(testScheduleWithLastDate);
        thrown.expect(DuplicateNameException.class);
        scheduleStore.createSchedule(testScheduleWithLastDate);
    }

    /**
     * Test update schedule.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testUpdateSchedule() throws Exception {
        Schedule updatedSchedule = testScheduleWithLastDate.toBuilder()
            .setRecurRule("FREQ=WEEKLY;BYDAY=FR;INTERVAL=1;")
            .setTimezoneId("America/New_York")
            .build();
        Schedule origSchedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        Schedule retUpdatedSchedule = scheduleStore.updateSchedule(origSchedule.getId(),
            updatedSchedule);
        assertEquals(origSchedule.getId(), retUpdatedSchedule.getId());
        verifySchedule(updatedSchedule, retUpdatedSchedule, true);
    }

    /**
     * Test update schedule change recurrence pattern.
     *
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testUpdateScheduleChangeRecurrence() throws Exception {
        Schedule schedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        assertEquals(RECUR_RULE, schedule.getRecurRule());
        assertFalse(schedule.hasOneTime());
        Schedule updatedSchedule = testScheduleWithLastDate.toBuilder().clearRecurRule()
            .setOneTime(OneTime.getDefaultInstance())
            .build();
        thrown.expect(InvalidItemException.class);
        scheduleStore.updateSchedule(schedule.getId(),
            updatedSchedule);
        updatedSchedule = testScheduleWithLastDate.toBuilder().clearRecurRule()
            .setOneTime(OneTime.getDefaultInstance())
            .clearLastDate()
            .build();
        Schedule retUpdatedSchedule = scheduleStore.updateSchedule(schedule.getId(),
            updatedSchedule);
        assertTrue((retUpdatedSchedule.hasOneTime()));
    }

    /**
     * Test update schedule change last date.
     *
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testUpdateScheduleChangeLastDate() throws Exception {
        Schedule schedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        assertEquals(LAST_DATE, schedule.getLastDate());
        assertFalse(schedule.hasPerpetual());
        Schedule updatesSchedule = scheduleStore.updateSchedule(schedule.getId(),
            testScheduleWithLastDate.toBuilder().clearLastDate().setPerpetual(
                Perpetual.getDefaultInstance()).build());
        assertTrue(updatesSchedule.hasPerpetual());
    }

    /**
     * Tes update schedule set recurrence start time.
     *
     * @throws Exception If test throws any exceptions.
     */
    @Test
    public void testUpdateScheduleSetRecurrenceStartTime() throws Exception {
        Schedule schedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        assertFalse(schedule.hasRecurrenceStart());
        Schedule updatedSchedule = scheduleStore.updateSchedule(schedule.getId(),
            schedule.toBuilder().setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TINE)
                .build())
            .build());
        assertTrue(updatedSchedule.hasRecurrenceStart());
        assertEquals(RECURRENCE_START_TINE, updatedSchedule.getRecurrenceStart()
            .getRecurrenceStartTime());
    }

    /**
     * Test update schedule with recurrence start time set.
     *
     * @throws Exception If test throws any exceptions.
     */
    @Test
    public void testUpdateScheduleWithRecurrenceStartTimeSet() throws Exception {
        Schedule schedule = scheduleStore.createSchedule(testSchedulePerpetual);
        assertFalse(schedule.hasRecurrenceStart());
        Schedule scheduleWithRecurrence = scheduleStore.updateSchedule(schedule.getId(),
            schedule.toBuilder().setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(RECURRENCE_START_TINE)
                .build())
                .build());
        assertTrue(scheduleWithRecurrence.hasRecurrenceStart());
        assertEquals(RECURRENCE_START_TINE, scheduleWithRecurrence.getRecurrenceStart()
            .getRecurrenceStartTime());

        // Note that UI may not send deferred start time with every request so make sure we don't
        // overwrite the one that's already set
        Schedule scheduleWithRecurrenceAndUpdatedLastDate = scheduleStore.updateSchedule(
            scheduleWithRecurrence.getId(), scheduleWithRecurrence.toBuilder()
                .clearRecurrenceStart()
                .setLastDate(LAST_DATE)
                .build());
        assertEquals(LAST_DATE, scheduleWithRecurrenceAndUpdatedLastDate.getLastDate());
        assertTrue(scheduleWithRecurrenceAndUpdatedLastDate.hasRecurrenceStart());
        assertEquals(RECURRENCE_START_TINE, scheduleWithRecurrenceAndUpdatedLastDate.getRecurrenceStart()
            .getRecurrenceStartTime());
    }

    /**
     * Test update schedule with invalid ID.
     * @throws Exception If test throws any exceptions
     */
    @Test(expected = ScheduleNotFoundException.class)
    public void testUpdateScheduleInvalidId() throws Exception {
        scheduleStore.updateSchedule(0, testScheduleWithLastDate);
    }

    /**
     * Test update schedule with duplicate name.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testUpdateScheduleDuplicateName() throws Exception {
        Schedule testSchedule2 = testScheduleWithLastDate.toBuilder()
            .setDisplayName("Test Schedule 2")
            .build();
        Schedule updatedSchedule = testScheduleWithLastDate.toBuilder()
            .setDisplayName("Test Schedule 2")
            .build();

        Schedule origSchedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        scheduleStore.createSchedule(testSchedule2);
        thrown.expect(DuplicateNameException.class);
        scheduleStore.updateSchedule(origSchedule.getId(), updatedSchedule);
    }

    /**
     * Test delete schedule.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testDeleteSchedule() throws Exception {
        Schedule schedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        Optional<Schedule> fetchedSchedule = scheduleStore.getSchedule(schedule.getId());
        assertTrue(fetchedSchedule.isPresent());
        Schedule deletedSchedule = scheduleStore.deleteSchedule(schedule.getId());
        assertNotNull(deletedSchedule);
        verifySchedule(deletedSchedule, schedule, true);
        fetchedSchedule = scheduleStore.getSchedule(schedule.getId());
        assertFalse(fetchedSchedule.isPresent());
    }

    /**
     * Test that we shouldn't delete schedule if it used as execution window in any setting
     * policies.
     *
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testDeleteScheduleUsedAsExecutionWindow() throws Exception {
        Schedule schedule = scheduleStore.createSchedule(testScheduleWithLastDate);
        final TestGroupGenerator groupGenerator = new TestGroupGenerator();
        final Origin userOrigin = groupGenerator.createUserOrigin();
        final GroupDefinition groupDefinition = groupGenerator.createGroupDefinition();
        final Long groupId = 23L;
        groupStore.createGroup(groupId, userOrigin, groupDefinition,
                Collections.singleton(MemberType.newBuilder().setEntity(1).build()), false);

        final Setting actionModeSetting = Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()).build())
                .build();

        final Setting executionScheduleSetting = Setting.newBuilder()
                .setSettingSpecName(ActionSettingSpecs.getSubSettingFromActionModeSetting(
                    EntitySettingSpecs.Move, ActionSettingType.SCHEDULE))
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(Collections.singletonList(schedule.getId()))
                        .build())
                .build();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setName("testPolicy")
                        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .setScope(Scope.newBuilder().addGroups(groupId).build())
                        .addAllSettings(Arrays.asList(actionModeSetting, executionScheduleSetting))
                        .build())
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(settingPolicy));

        thrown.expect(ScheduleInUseDeleteException.class);
        // deletion should not be allowed
        scheduleStore.deleteSchedule(schedule.getId());
    }

    /**
     * Test delete schedule with invalid ID.
     * @throws Exception If test throws any exceptions
     */
    @Test(expected = ScheduleNotFoundException.class)
    public void testDeleteScheduleInvalidId() throws Exception {
        scheduleStore.deleteSchedule(0);
    }

    /**
     * Test schedule bulk delete.
     * @throws Exception If test throws any exceptions
     */
    @Test
    public void testScheduleBulkDelete() throws Exception {
        Set ids = Sets.newHashSet();
        Schedule schedule1 = scheduleStore.createSchedule(testScheduleWithLastDate);
        assertTrue(schedule1.hasId());
        ids.add(schedule1.getId());
        Schedule schedule2 = scheduleStore.createSchedule(testScheduleWithLastDate
            .toBuilder().setDisplayName("Test Schedule 2").build());
        assertTrue(schedule2.hasId());
        ids.add(schedule2.getId());
        Schedule schedule3 = scheduleStore.createSchedule(testScheduleWithLastDate
            .toBuilder().setDisplayName("Test Schedule 3").build());
        assertTrue(schedule3.hasId());
        ids.add(schedule3.getId());

        int deleted = scheduleStore.deleteSchedules(ids);
        assertEquals(3, deleted);
        assertFalse(scheduleStore.getSchedule(schedule1.getId()).isPresent());
        assertFalse(scheduleStore.getSchedule(schedule2.getId()).isPresent());
        assertFalse(scheduleStore.getSchedule(schedule3.getId()).isPresent());
    }

    /**
     * Test schedule bulk delete with invalid ids.
     * @throws Exception If test throws any exceptions
     */
    @Test(expected = ScheduleNotFoundException.class)
    public void testScheduleBulkDeleteWithInvalidId() throws Exception {
        Set ids = Sets.newHashSet();
        ids.add(-1L);
        ids.add(0L);
        Schedule schedule1 = scheduleStore.createSchedule(testScheduleWithLastDate);
        assertTrue(schedule1.hasId());
        ids.add(schedule1.getId());

        scheduleStore.deleteSchedules(ids);
    }

    /**
     * Test collect diags trowing DataAccessException.
     * @throws Exception If test throws any unexpected exceptions
     */
    @Test
    public void testCollectDiagsDataAccessException() throws Exception {
        doThrow(DataAccessException.class).when(dslContextSpy)
            .transactionResult(any(TransactionalCallable.class));
        thrown.expect(DiagnosticsException.class);
        scheduleStore.collectDiags(Mockito.mock(DiagnosticsAppender.class));
    }

    /**
     * Test restore diags throwing DataAccessException.
     *
     * @throws Exception If test throws any unexpected exceptions
     */
    @Test
    public void testRestoreDiagsDataAccessException() throws Exception {
        doThrow(DataAccessException.class).when(dslContextSpy)
            .transaction(any(TransactionalRunnable.class));
        List<String> diags = new ArrayList<>();
        thrown.expect(DiagnosticsException.class);
        scheduleStore.restoreDiags(diags);
    }

    private void verifySchedule(final Schedule expected, final Schedule actual, boolean isPerpetual) {
        assertEquals(expected.getDisplayName(), actual.getDisplayName());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getEndTime(), actual.getEndTime());
        assertEquals(expected.getLastDate(), actual.getLastDate());
        if (isPerpetual) {
            assertEquals(expected.hasPerpetual(), actual.hasPerpetual());
        } else {
            assertEquals(expected.hasOneTime(), actual.hasOneTime());
        }
        assertEquals(expected.getRecurRule(), actual.getRecurRule());
        assertEquals(expected.getTimezoneId(), actual.getTimezoneId());
    }

}
