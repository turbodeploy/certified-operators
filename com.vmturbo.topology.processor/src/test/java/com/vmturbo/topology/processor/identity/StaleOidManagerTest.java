package com.vmturbo.topology.processor.identity;

import static com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask.RecurrentTasksEnum.OID_DELETION;
import static com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask.RecurrentTasksEnum.OID_EXPIRATION;
import static com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask.RecurrentTasksEnum.OID_TIMESTAMP_UPDATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.hamcrest.Matchers;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidDeletionTask;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidExpirationTask;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider.GetExpiredEntities;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider.InsertRecurrentTask;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider.QueryCapture;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider.QueryCategory;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider.SetEntitiesLastSeen;
import com.vmturbo.topology.processor.identity.staleoidmanager.DataProvider.SetExpiredEntities;

/**
 * Tests for the {@link StaleOidManagerImpl} class.
 *
 * <p>The tests create a {@link StaleOidManagerImpl} instance and initialize it, with the side
 * effect of scheduling periodic executions of its {@link OidExpirationTask}. Most tests allow a
 * single execution of this task to complete and then verify results. A few tests set a non-zero
 * initial scheduling delay and verify pre-task effects. In all cases, the task executor is shut
 * down after test completion.</p>
 */
@RunWith(Parameterized.class)
public class StaleOidManagerTest {

    private static final Consumer<Set<Long>> NOOP_SET_CONSUMER = set -> {
    };
    private static final Runnable NOP_RUNNABLE = () -> {
    };
    private static final String INVALID_ENTITY_TYPE_NAME = "INVALID_ENTITY";
    private final SQLDialect dialect;

    /**
     * Run each test under two jOOQ {@link SQLDialect} values.
     *
     * @return parameter lists
     */
    @Parameters
    public static Object[][] generateTestData() {
        return new Object[][]{
                new Object[]{SQLDialect.MARIADB},
                new Object[]{SQLDialect.POSTGRES}};
    }

    private static final int TIMEOUT_SECONDS = 10;
    private StaleOidManagerImpl staleOidManager;
    private ScheduledFuture<?> cycleCompleteFuture;
    private DataProvider dataProvider;
    private final Set<Long> entityStoreOids = ImmutableSet.of(1L, 2L);
    private final long currentTime = System.currentTimeMillis();
    private final Clock clock = mock(Clock.class);
    private ScheduledExecutorService executor;
    private StaleOidManagerImpl.OidManagementParameters oidManagementParameters;

    /**
     * Create a new test class instance.
     *
     * @param dialect dialect to use for tests
     */
    public StaleOidManagerTest(SQLDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Shut down our executor after each test.
     */
    @After
    public void after() {
        executor.shutdownNow();
    }

    /**
     * Perform per-test setup using default configuration values.
     */
    private void init() {
        init(null, 0, true, null, false, false, 4, NOOP_SET_CONSUMER);
    }

    /**
     * Perform per-test setup.Most of this can't happen in a @{@link Before} method because it
     * depends on parameters that differ from test to test.
     *
     * @param executor             {@link ScheduledExecutorService} instance to use for task
     *                             execution
     * @param initialDelayHours    hours to delay initial {@link OidDeletionTask} execution. we set
     *                             this to 1 in tests that shouldn't execute an expiration cycle at
     *                             all
     * @param expireOids           whether to perform oid expiration
     * @param perEntityExpirations execution times that differ by entity type, or null
     * @param forceFail            true if every query executed during the test should fail
     * @param allowMultipleCycles  true if query executions should be allowed after the first task
     *                             execution is complete.
     * @param successfulTaskCount  value to be returned by any executed
     *                             {@link QueryCategory#COUNT_SUCCESSFUL_RECURRENT_TASKS} query
     * @param oidListener          listener to hear about expired oids, or null for none
     */
    private void init(ScheduledExecutorService executor, int initialDelayHours, boolean expireOids,
            Map<String, String> perEntityExpirations, boolean forceFail,
            boolean allowMultipleCycles, int successfulTaskCount,
            Consumer<Set<Long>> oidListener) {
        when(clock.millis()).thenReturn(currentTime);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        this.executor = executor != null ? executor : Executors.newScheduledThreadPool(1);
        this.dataProvider = new DataProvider(dialect);
        final DSLContext context = DSL.using(new MockConnection(dataProvider), dialect);
        this.oidManagementParameters = new StaleOidManagerImpl.OidManagementParameters.Builder(
                daysToMillis(1),
                context,
                expireOids,
                clock,
                hoursToMillis(6),
                true,
                2)
                .setExpirationDaysPerEntity(perEntityExpirations != null ? perEntityExpirations
                                                                         : Collections.emptyMap())
                .build();
        this.staleOidManager = new StaleOidManagerImpl(
                hoursToMillis(initialDelayHours), this.executor, oidManagementParameters);
        staleOidManager.initialize(() -> entityStoreOids, oidListener);
        if (!allowMultipleCycles) {
            // schedule a no-op task that runs one msec after the first of the recurring
            // oid expiration tasks. We will use the resulting Future to detect the end of the
            // first cycle. Tests that use a multithreaded executor will need to roll their own
            // detection since this will be unreliable in that case.
            this.cycleCompleteFuture = this.executor.schedule(NOP_RUNNABLE,
                    TimeUnit.HOURS.toMillis(initialDelayHours) + 1, TimeUnit.MILLISECONDS);
        }
        this.dataProvider.init(cycleCompleteFuture, forceFail, allowMultipleCycles,
                successfulTaskCount, entityStoreOids);
    }

    /**
     * Tests that a non-zero initial delay is honored, by verifying that there are no queries
     * executed immediately when an initial delay is present.
     *
     * <p>We cannot currently specify a nonzero delay of less than one hour, and we clearly don't
     * want to wait for an hour, so we just wait for a couple of seconds. If a task were erroneously
     * scheduled, we would see activity right away.</p>
     *
     * @throws Exception if there's an error detected by the first-cycle future.
     */
    @Test
    public void testInitialDelay() throws Exception {
        init(null, 1, true, null, false, false, 4, NOOP_SET_CONSUMER);
        try {
            cycleCompleteFuture.get(2, TimeUnit.SECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            assertThat(dataProvider.getAllQueries().size(), is(1));
        }
    }

    /**
     * Tests that no exception is ever propagated from an oidManagementTask. The reason we do not
     * want this is that any propagated exception would stop the scheduler to perform the tasks
     */
    @Test
    public void testExceptionInScheduledTask() {
        ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        ArgumentCaptor<? extends Runnable> oidManagementTask = ArgumentCaptor.forClass(
                Runnable.class);
        init(mockExecutor, 0, true, null, true, false, 4, NOOP_SET_CONSUMER);
        staleOidManager = new StaleOidManagerImpl(0, mockExecutor, oidManagementParameters);
        verify(mockExecutor).scheduleWithFixedDelay(oidManagementTask.capture(), anyLong(),
                anyLong(), any());
        try {
            oidManagementTask.getValue().run();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    /**
     * Tests that we correctly get the oids that will be expired.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testGetExpiredRecords() throws Exception {
        init();
        waitForCycleCompletion();
        List<QueryCapture> geeQueries = dataProvider.getQueries(QueryCategory.GET_EXPIRED_ENTITIES);
        assertThat(geeQueries.size(), is(1));
        GetExpiredEntities getExpiredEntities = (GetExpiredEntities)geeQueries.get(0);
        assertThat(getExpiredEntities.getExpiryTimestamp(),
                is(new Timestamp(currentTime - daysToMillis(1))));
    }

    /**
     * Tests that we correctly set the expired oids.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testSetExpiredRecords() throws Exception {
        init();
        waitForCycleCompletion();
        List<QueryCapture> seeQueries = dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES);
        assertThat(seeQueries.size(), is(1));
        SetExpiredEntities setExpiredEntities = (SetExpiredEntities)seeQueries.get(0);
        assertThat(setExpiredEntities.getExpiryTimestamp(),
                is(new Timestamp(currentTime - daysToMillis(1))));
    }

    /**
     * Tests that we correctly set the recurrent operation results.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testSetRecurrentOperation() throws Exception {
        init();
        waitForCycleCompletion();
        List<QueryCapture> irtQueries = dataProvider.getQueries(
                QueryCategory.INSERT_RECURRENT_TASK);
        assertThat(irtQueries.size(), is(3));
        InsertRecurrentTask firstQuery = (InsertRecurrentTask)irtQueries.get(0);
        // recurrent tasks use LocalDateTime to set their execution times in DB records, which
        // unfortunately discards timezone info and so looks just like the execution time with its
        // local timezone offset applied in reverse, and in an unrecoverable fashion! This should
        // be changed, but in the meantime we need to adjust that in this test in order to get a
        // reliable comparison.
        assertThat(instantFromLocalTimestamp(firstQuery.getExecutionTime()),
                // allow for a bit of time to have elapsed
                allOf(greaterThanOrEqualTo(Instant.ofEpochMilli(currentTime)),
                        lessThan(Instant.ofEpochMilli(currentTime).plusSeconds(1))));
    }

    /**
     * Tests that when the {@link StaleOidManagerImpl} is created with the expireOids flag equal to
     * false, only one query is performed by the task: the one to set the last_seen value.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testDoNotSetExpiredOidsBecauseOfFlag() throws Exception {
        init(null, 0, false, null, false, false, 4, NOOP_SET_CONSUMER);
        waitForCycleCompletion();
        assertThat(dataProvider.getQueries(QueryCategory.SET_ENTITIES_LAST_SEEN).size(), is(1));
        assertThat(dataProvider.getQueries(QueryCategory.GET_EXPIRED_ENTITIES).size(), is(0));
        assertThat(dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES).size(), is(0));
        assertThat(dataProvider.getQueries(QueryCategory.DELETE_EXPIRED_ENTITIES).size(), is(1));
    }

    /**
     * Tests that when there were not at least 50% successful last_seen updates in the last
     * EXPIRATION_DAYS we do not perform expiration.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testDoNotSetExpiredOids() throws Exception {
        init(null, 0, true, null, false, false,
                // just one timestamp-update task will not meet the 50% threshold when the expected
                // count for 1-day expiration and 6-hour update would be 4
                1, NOOP_SET_CONSUMER);
        waitForCycleCompletion();
        List<QueryCapture> selsQueries = dataProvider.getQueries(
                QueryCategory.SET_ENTITIES_LAST_SEEN);
        assertThat(selsQueries.size(), is(1));
        SetEntitiesLastSeen setEntitiesLastSeen = (SetEntitiesLastSeen)selsQueries.get(0);
        assertThat(setEntitiesLastSeen.getUpdatedOids(),
                containsInAnyOrder(entityStoreOids.toArray()));
        Map<String, List<InsertRecurrentTask>> tasksByOperationName = dataProvider.getQueries(
                        QueryCategory.INSERT_RECURRENT_TASK)
                .stream()
                .map(task -> (InsertRecurrentTask)task)
                .collect(Collectors.groupingBy(InsertRecurrentTask::getOperationName));
        // we should have registered one each of timestamp-update, oid-expiration and oid-deletion
        // tasks, but the oid-expiration task will have been a no-op because of the low ratio
        assertThat(tasksByOperationName.get(OID_TIMESTAMP_UPDATE.name()).size(), is(1));
        assertThat(tasksByOperationName.get(OID_EXPIRATION.name()).size(), is(1));
        assertThat(tasksByOperationName.get(OID_DELETION.name()).size(), is(1));
        // make sure that oid-expiration was a no-op by checking that it did not issue its update
        assertThat(dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES).size(), is(0));
        // but deletions take place regardless
        assertThat(dataProvider.getQueries(QueryCategory.DELETE_EXPIRED_ENTITIES).size(), is(1));
    }

    /**
     * Tests that the oid expiration process can be triggered asynchronously while the main task is
     * running.
     *
     * @throws InterruptedException if the staleOidManagerProcess is interrupted or canceled
     * @throws ExecutionException   if there is an issue in executing the staleOidManagerProcess
     * @throws TimeoutException     if the staleOidManagerProcess reaches a timeout
     */
    @Test
    public void testExpireOidsAsynchronously()
            throws InterruptedException, ExecutionException, TimeoutException {
        // we need a two-thread executor in order to have concurrent execution
        ScheduledExecutorService multithreadExecutor = Executors.newScheduledThreadPool(2);
        // start the normal scheduled execution
        init(multithreadExecutor, 0, true, null, false, true, 4, NOOP_SET_CONSUMER);
        // and then run an immediate execution
        staleOidManager.expireOidsImmediatly();
        // at this point we've completed all the operations of the immediate expiration (since the
        // above method waits for that before returning). The first scheduled cycle may still be
        // executing. So we now have either one or two idle threads.
        // We'll schedule a long-running no-op task that will consume a thread, leaving either zero
        // or one idle. Then we'll schedule another no-op task that should execute either
        // immediately or when that first-cycle thread completes. Then, if we wait on that second
        // no-op task, we should finally be in a state where both the scheduled and immediate tasks
        // are finished.
        executor.submit(() -> {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        this.cycleCompleteFuture = executor.schedule(NOP_RUNNABLE, 0, TimeUnit.MILLISECONDS);
        waitForCycleCompletion();
        assertThat(dataProvider.getQueries(QueryCategory.INSERT_RECURRENT_TASK).size(),
                greaterThanOrEqualTo(1));
        InsertRecurrentTask firstInsertRecurrrentTask =
                (InsertRecurrentTask)dataProvider.getQueries(QueryCategory.INSERT_RECURRENT_TASK)
                        .get(0);
        assertThat(instantFromLocalTimestamp(firstInsertRecurrrentTask.getExecutionTime()),
                allOf(greaterThanOrEqualTo(Instant.ofEpochMilli(currentTime)),
                        lessThanOrEqualTo(Instant.ofEpochMilli(currentTime).plusSeconds(1))));
        // we should have executed two of each operation that normally executes once per cycle.
        // we'll stream them to get the counts and to perform other checks via peeks
        assertThat(dataProvider.getQueries(QueryCategory.SET_ENTITIES_LAST_SEEN)
                .stream()
                .map(q -> (SetEntitiesLastSeen)q)
                .peek(q -> assertThat(q.getLastSeenTimestamp(), is(new Timestamp(currentTime))))
                .peek(q -> assertThat(q.getUpdatedOids(),
                        containsInAnyOrder(entityStoreOids.toArray())))
                .count(), is(2L));
        assertThat(dataProvider.getQueries(QueryCategory.GET_EXPIRED_ENTITIES)
                .stream()
                .map(q -> (GetExpiredEntities)q)
                .peek(q -> assertThat(q.getExpiryTimestamp(),
                        is(new Timestamp(currentTime - TimeUnit.DAYS.toMillis(1)))))
                .count(), is(2L));
        assertThat(dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES)
                .stream()
                .map(q -> (SetExpiredEntities)q)
                .peek(q -> assertThat(q.isExpired(), is(true)))
                .peek(q -> assertThat(q.getExpiryTimestamp(),
                        is(new Timestamp(currentTime - TimeUnit.DAYS.toMillis(1)))))
                .count(), is(2L));
    }

    /**
     * Test that adding a listener works, i.e. listener gets notified of expired oids.
     *
     * @throws InterruptedException when Future.get throws it.
     * @throws ExecutionException   when Future.get throws it.
     * @throws TimeoutException     when Future.get throws it.
     */
    @Test
    public void testListener() throws InterruptedException, ExecutionException, TimeoutException {
        final SetOnce<Set<Long>> oidSet = new SetOnce<>();
        init(null, 0, true, null, false, false, 4, oidSet::trySetValue);
        waitForCycleCompletion();
        assertThat(dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES).size(), is(1));
        SetExpiredEntities setExpiredEntities = (SetExpiredEntities)dataProvider.getQueries(
                QueryCategory.SET_EXPIRED_ENTITIES).get(0);
        assertThat(setExpiredEntities.isExpired(), is(true));
        assertThat(setExpiredEntities.getExpiryTimestamp().getTime(),
                is(currentTime - TimeUnit.DAYS.toMillis(1)));
        assertThat(oidSet.getValue().isPresent(), is(true));
        assertThat(oidSet.getValue().get(), Matchers.contains(entityStoreOids.toArray()));
    }

    /**
     * Test that having a null listener won't prevent the task from running.
     *
     * @throws InterruptedException when Future.get throws it.
     * @throws ExecutionException   when Future.get throws it.
     * @throws TimeoutException     when Future.get throws it.
     */
    @Test
    public void testNullListener()
            throws InterruptedException, ExecutionException, TimeoutException {
        init(null, 0, true, null, false, false, 4, null);
        waitForCycleCompletion();
        assertThat(dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES)
                .stream()
                .map(q -> (SetExpiredEntities)q)
                .peek(q -> assertThat(q.isExpired(), is(true)))
                .peek(q -> assertThat(q.getExpiryTimestamp().getTime(),
                        is(currentTime - TimeUnit.DAYS.toMillis(1))))
                .count(), is(1L));
    }

    /**
     * Tests that entity types that have a day set in the expirationDaysPerEntity are expired using
     * that expiration setting.
     *
     * @throws InterruptedException if the staleOidManagerProcess is interrupted or canceled
     * @throws ExecutionException   if there is an issue in executing the staleOidManagerProcess
     * @throws TimeoutException     if the staleOidManagerProcess reaches a timeout
     */
    @Test
    public void testExpirationsPerEntityType()
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, String> expirationDaysPerEntity = ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.name(), "2",
                EntityType.VIRTUAL_VOLUME.name(), "5",
                INVALID_ENTITY_TYPE_NAME, "-1");
        init(null, 0, true, expirationDaysPerEntity, false, false, 4, NOOP_SET_CONSUMER);
        waitForCycleCompletion();
        for (QueryCapture query : dataProvider.getQueries(QueryCategory.SET_EXPIRED_ENTITIES)) {
            SetExpiredEntities setExpiredEntities = (SetExpiredEntities)query;
            Timestamp expiryTimestamp = setExpiredEntities.getExpiryTimestamp();
            Optional.ofNullable(setExpiredEntities.getEntityTypeName())
                    .map(expirationDaysPerEntity::get)
                    .map(Integer::parseInt)
                    .ifPresent(expirationDays ->
                            assertThat(expiryTimestamp.getTime(),
                                    is(currentTime - TimeUnit.DAYS.toMillis(expirationDays))));
        }
    }

    /**
     * Tests that recurrent operations are correctly deleted by the {@link StaleOidManager} even if
     * no tasks are executed.
     *
     * @throws InterruptedException if sleep is interrupted
     */
    @Test
    public void testDeleteRecurrentOperationsWithNoTask() throws InterruptedException {
        init(null, 1, true, null, false, false, 4, NOOP_SET_CONSUMER);
        Thread.sleep(1000);
        List<QueryCapture> deleteRecurrentOperations = dataProvider.getQueries(
                QueryCategory.DELETE_RECURRENT_OPERATIONS);
        assertThat(deleteRecurrentOperations.size(), is(1));
    }

    /**
     * Tests that recurrent operations deletions are not performed by tasks, only by the main class
     * prior to scheduling any tasks.
     *
     * @throws ExecutionException   if there's an error executing first cycle
     * @throws InterruptedException if we're interrupted while waiting for first cycle
     * @throws TimeoutException     if first cycle takes too long
     */
    @Test
    public void testDeleteRecurrentOperationsWithOneTask()
            throws ExecutionException, InterruptedException, TimeoutException {
        init();
        waitForCycleCompletion();
        List<QueryCapture> deleteRecurrentOperations = dataProvider.getQueries(
                QueryCategory.DELETE_RECURRENT_OPERATIONS);
        assertThat(deleteRecurrentOperations.size(), is(1));
    }

    /**
     * Tests that the diags collection works properly.
     *
     * @throws DiagnosticsException if an error occurs
     */
    @Test
    public void testDiagsCollection() throws DiagnosticsException {
        init(null, 1, true, null, false, false, 4, NOOP_SET_CONSUMER);
        DiagnosticsAppender appender = mock(DiagnosticsAppender.class);
        staleOidManager.collectDiags(appender);
        verify(appender).appendString(eq(StaleOidManagerImpl.DIAGS_HEADER));
        verify(appender, times(2)).appendString(any((String.class)));
        String diagsFileName = staleOidManager.getFileName();
        assertThat(diagsFileName, is(StaleOidManagerImpl.DIAGS_FILE_NAME));
    }

    /**
     * Utility to "fix" timestamps that were obtained from {@link LocalDateTime} objects. The
     * conversion from {@link LocalDateTime} to {@link Timestamp} just copies individual component
     * values (hours, minutes, etc.) and ignores the timezone. Insertion into the DB then treats them
     * as UTC, so the impact is that the saved value is adjusted by the local timezone, but in
     * reverse! So we need to undo that adjustment. {@link LocalDateTime} has no business in our
     * code other than in the UI!!!
     *
     * @param timestamp bogus timestamp
     * @return fixed instant
     */
    private Instant instantFromLocalTimestamp(Timestamp timestamp) {
        Instant now = Instant.ofEpochMilli(clock.millis());
        Instant shifted = Timestamp.valueOf(LocalDateTime.ofInstant(now, clock.getZone()))
                .toInstant();
        long shift = Duration.between(now, shifted).toMillis();
        return Instant.ofEpochMilli(timestamp.getTime() - shift);
    }

    private long daysToMillis(int days) {
        return TimeUnit.DAYS.toMillis(days);
    }

    private long hoursToMillis(int hours) {
        return TimeUnit.HOURS.toMillis(hours);
    }

    private Object waitForCycleCompletion()
            throws InterruptedException, ExecutionException, TimeoutException {
        return cycleCompleteFuture.get(StaleOidManagerTest.TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
