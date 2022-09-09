package com.vmturbo.topology.processor.identity;

import static com.vmturbo.topology.processor.identity.recurrenttasks.OidExpirationTask.EXPIRATION_TASK_NAME;
import static com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask.RecurrentTasksEnum.OID_TIMESTAMP_UPDATE;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.db.tables.AssignedIdentity;
import com.vmturbo.topology.processor.db.tables.RecurrentTasks;
import com.vmturbo.topology.processor.db.tables.records.RecurrentTasksRecord;

/**
 * Class to test a {@link StaleOidManagerImpl}. These tests heavily rely on the
 * {@link MockDataProvider} in order to mock the interaction with the database. Each test
 * initializes a {@link StaleOidManagerImpl} and waits on the {@link ScheduledFuture} returned. Then the
 * {@link AssignedIdentityJooqProvider} intercepts all the queries and parses their bindings,
 * keeping them in the state. Once the number of performed queries reaches the
 * {@link StaleOidManagerTest#N_QUERIES_PER_EXPIRATION_TASK} we cancel the scheduled future and
 * perform the asserts, based on the values gathered into the
 * {@link AssignedIdentityJooqProvider}. This flow guarantees us that we stop a periodic task
 * only after it has performed at least once.
 */
@RunWith(JUnitParamsRunner.class)
public class StaleOidManagerTest {
    Object[] generateTestData() {
        return $(
                $(SQLDialect.MARIADB),
                $(SQLDialect.POSTGRES));
    }

    private static final int EXPIRATION_DAYS = 30;
    private static final int INITIAL_DELAY_HRS = 6;
    private static final int VALIDATION_FREQUENCY_HRS = 1;
    private static final int N_QUERIES_PER_EXPIRATION_TASK = 7;
    private static final int TIMEOUT_SECONDS = 10;
    private static final int RECORD_DELETION_RETENTION_DAYS = 180;
    private static final String EXPIRATION_EXCEPTION_MESSAGE = "Exception with OidExpirationTask";
    private static final int DELETED_OPERATIONS_N = 10;
    private StaleOidManagerImpl staleOidManager;
    private ScheduledFuture<?> staleOidManagerProcess;
    private AssignedIdentityJooqProvider assignedIdentityJooqProvider;
    private final Set<Long> entityStoreOids = new HashSet<>();
    private final long currentTime = System.currentTimeMillis();
    private final Clock clock = mock(Clock.class);
    private ScheduledExecutorService executor;
    private RecurrentTasksRecord recurrentTaskRecord;
    private StaleOidManagerImpl.OidManagementParameters oidManagementParameters;

    long oid1 = 1;
    long oid2 = 2;

    /**
     * Set up the context for each test.
     */
    @Before
    public void setUp() {
        this.assignedIdentityJooqProvider = new AssignedIdentityJooqProvider(N_QUERIES_PER_EXPIRATION_TASK, false,
                false);
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), SQLDialect.MARIADB);
        entityStoreOids.add(oid1);
        entityStoreOids.add(oid2);
        when(clock.millis()).thenReturn(currentTime);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        executor = Executors.newScheduledThreadPool(1);
        oidManagementParameters =
                new StaleOidManagerImpl.OidManagementParameters.Builder(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                        context, true, clock,
                        TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS), true, RECORD_DELETION_RETENTION_DAYS).build();
        staleOidManager = new StaleOidManagerImpl(0, executor, oidManagementParameters);
        recurrentTaskRecord = context.newRecord(RecurrentTasks.RECURRENT_TASKS)
                .values(LocalDateTime.now(), OID_TIMESTAMP_UPDATE.toString(), true, 10, "", "");
    }

    /**
     * Methods to be run after each test.
     */
    @After
    public void after() {
        executor.shutdownNow();
    }

    private void initialize() {
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
    }

    /**
     * Tests that there's no task execution right after the initialization. We should not perform a new task execution
     * we should be waiting at least for {@link StaleOidManagerTest#INITIAL_DELAY_HRS}
     *
     * @param dialect SQLDialect to use
     * @throws Exception if an error occurs in the staleOidManagerProcess
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testInitialDelay(SQLDialect dialect) throws Exception {
        staleOidManager = new StaleOidManagerImpl(TimeUnit.HOURS.toMillis(INITIAL_DELAY_HRS), executor, oidManagementParameters);
        ScheduledFuture<?> staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
        try {
            staleOidManagerProcess.get(2, TimeUnit.SECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            Assert.assertEquals(0, assignedIdentityJooqProvider.getLastSeenQuery().size());
        }
    }

    /**
     * Tests that no exception is ever propagated from an oidManagementTask. The reason we do not
     * want this is that any propagated exception would stop the scheduler to perform the tasks
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testExceptionInScheduledTask(SQLDialect dialect) {
        final AssignedIdentityJooqProvider assignedIdentityJooqProvider = new AssignedIdentityJooqProvider(4, true,
                false);
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), dialect);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        ArgumentCaptor<? extends Runnable> oidManagementTask = ArgumentCaptor.forClass(Runnable.class);
        final StaleOidManagerImpl.OidManagementParameters oidManagementParameters =
                new StaleOidManagerImpl.OidManagementParameters.Builder(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                        context, true, clock,
                        TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS), true, RECORD_DELETION_RETENTION_DAYS).build();
        staleOidManager = new StaleOidManagerImpl(0, executor, oidManagementParameters);
        initialize();
        Mockito.verify(executor).scheduleWithFixedDelay(oidManagementTask.capture(), anyLong(), anyLong(), any());
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
        initialize();
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                assignedIdentityJooqProvider.getGetExpiredRecordsQuery().getExpirationDate().getTime());
        }
    }

    /**
     * Tests that we correctly set the expired oids.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testSetExpiredRecords() throws Exception {
        initialize();
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            Assert.assertTrue(assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getIsExpired());
            assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationDate().getTime());
        }
    }

    /**
     * Tests that we correctly set the recurrent operation results.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testSetRecurrentOperation() throws Exception {
        initialize();
        try {
            staleOidManagerProcess.get(30, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTime), clock.getZone())),
                assignedIdentityJooqProvider.getSetRecurrentTaskQuery().getExecutionTime());
            assertEquals(EXPIRATION_TASK_NAME,
                assignedIdentityJooqProvider.getSetRecurrentTaskQuery().getOperationName());
        }
    }

    /**
     * Tests that when the {@link StaleOidManagerImpl} is created with the
     * expireOids flag equal to false, only one query is performed by the task: the one to set
     * the last_seen value.
     * @param dialect SQLDialect to use
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testDoNotSetExpiredOidsBecauseOfFlag(SQLDialect dialect) throws Exception {
        AssignedIdentityJooqProvider assignedIdentityJooqProvider =
                new AssignedIdentityJooqProvider(3, false, false);
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), dialect);
        final StaleOidManagerImpl.OidManagementParameters oidManagementParameters =
                new StaleOidManagerImpl.OidManagementParameters.Builder(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                        context, false, clock,
                        TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS), true, RECORD_DELETION_RETENTION_DAYS).build();
        StaleOidManagerImpl staleOidManagerWithoutExpiration = new StaleOidManagerImpl(0, executor, oidManagementParameters);
        staleOidManagerProcess = staleOidManagerWithoutExpiration.initialize(() -> entityStoreOids,
                set -> { });
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().get(0).getUpdatedOids(), entityStoreOids);
            assertNull(assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationDate());
        }
    }

    /**
     * Tests that when there were not at least 50% successful last_seen updates in the last
     * EXPIRATION_DAYS we do not perform expiration.
     * @param dialect SQLDialect to use
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testDoNotSetExpiredOids(SQLDialect dialect) throws Exception {
        AssignedIdentityJooqProvider assignedIdentityJooqProvider =
                new AssignedIdentityJooqProvider(5, false, true);
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), dialect);
        final StaleOidManagerImpl.OidManagementParameters oidManagementParameters =
                new StaleOidManagerImpl.OidManagementParameters.Builder(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                        context, true, clock,
                        TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS), true, RECORD_DELETION_RETENTION_DAYS).build();
        staleOidManager = new StaleOidManagerImpl(0, executor, oidManagementParameters);
        initialize();
        try {
            staleOidManagerProcess.get(100, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().get(0).getUpdatedOids(), entityStoreOids);
            assertTrue(assignedIdentityJooqProvider.getSetRecurrentTaskQuery().isSuccessful());
        }
    }

    /**
     * Tests that the oid expiration process can be triggered asynchronously while the main task
     * is running.
     *
     * @throws InterruptedException if the staleOidManagerProcess is interrupted or canceled
     * @throws ExecutionException if there is an issue in executing the staleOidManagerProcess
     * @throws TimeoutException if the staleOidManagerProcess reaches a timeout
     */
    @Test
    public void testExpireOidsAsynchronously() throws InterruptedException, ExecutionException, TimeoutException {
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
        staleOidManager.expireOidsImmediatly();
        assertEquals(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTime), clock.getZone())),
            assignedIdentityJooqProvider.getSetRecurrentTaskQuery().getExecutionTime());
        Assert.assertTrue(assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getIsExpired());
        assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationDate().getTime());
        assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            assignedIdentityJooqProvider.getGetExpiredRecordsQuery().getExpirationDate().getTime());
        assertEquals(currentTime,
            assignedIdentityJooqProvider.getLastSeenQuery().get(0).getQueryTimeStamp().getTime());
        assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().get(0).getUpdatedOids(), entityStoreOids);
    }

    /**
     * Test that adding a listener works, i.e. listener gets notified of expired oids.
     *
     * @throws InterruptedException when Future.get throws it.
     * @throws ExecutionException when Future.get throws it.
     * @throws TimeoutException when Future.get throws it.
     */
    @Test
    public void testListener()
            throws InterruptedException, ExecutionException, TimeoutException {
        final SetOnce<Set<Long>> oidSet = new SetOnce<>();
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, oidSet::trySetValue);
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            Assert.assertTrue(assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getIsExpired());
            assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                    assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationDate().getTime());
            assertTrue(oidSet.getValue().isPresent());
            assertEquals(entityStoreOids.size(), oidSet.getValue().get().size());
            assertThat(oidSet.getValue().get(), containsInAnyOrder(oid1, oid2));
        }
    }

    /**
     * Test that having a null listener won't prevent the task from running.
     *
     * @throws InterruptedException when Future.get throws it.
     * @throws ExecutionException when Future.get throws it.
     * @throws TimeoutException when Future.get throws it.
     */
    @Test
    public void testNullListener()
            throws InterruptedException, ExecutionException, TimeoutException {
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, null);
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            Assert.assertTrue(assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getIsExpired());
            assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                    assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationDate().getTime());
        }
    }

    /**
     * Tests that entity types that have a day set in the expirationDaysPerEntity are expired
     * using that timestamp.
     * @param dialect SQLDialect to use
     * @throws InterruptedException if the staleOidManagerProcess is interrupted or canceled
     * @throws ExecutionException if there is an issue in executing the staleOidManagerProcess
     * @throws TimeoutException if the staleOidManagerProcess reaches a timeout
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testExpirationsPerEntityType(SQLDialect dialect) throws InterruptedException, ExecutionException, TimeoutException {
        HashMap<String, String> expirationDaysPerEntity = new HashMap<>();
        expirationDaysPerEntity.put("VIRTUAL_MACHINE", "2");
        expirationDaysPerEntity.put("VIRTUAL_VOLUME", "5");
        int validEntities = expirationDaysPerEntity.size();

        //Add a non valid entity, make sure we do not throw an exception and we proceed with the task
        final String invalidEntity = "INVALID_ENTITY";
        expirationDaysPerEntity.put("INVALID_ENTITY", "-1");

        AssignedIdentityJooqProvider assignedIdentityJooqProvider =
                new AssignedIdentityJooqProvider(N_QUERIES_PER_EXPIRATION_TASK + 2 * validEntities, expirationDaysPerEntity);
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), dialect);
        entityStoreOids.add(oid1);
        entityStoreOids.add(oid2);
        when(clock.millis()).thenReturn(currentTime);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);

        oidManagementParameters =
                new StaleOidManagerImpl.OidManagementParameters.Builder(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
                        context, true, clock,
                        TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS), true, RECORD_DELETION_RETENTION_DAYS)
                        .setExpirationDaysPerEntity(expirationDaysPerEntity).setExpirationDaysPerEntity(expirationDaysPerEntity).build();
        staleOidManager = new StaleOidManagerImpl(0, executor, oidManagementParameters);
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            for (Entry<String, String> entry : expirationDaysPerEntity.entrySet()) {
                if (!entry.getKey().equals(invalidEntity)) {
                    int entityType = EntityType.valueOf(entry.getKey()).getNumber();
                    Assert.assertEquals(currentTime - TimeUnit.DAYS.toMillis(Integer.parseInt(expirationDaysPerEntity.get(entry.getKey()))),
                            assignedIdentityJooqProvider.getSetExpiredRecordsQuery()
                                    .getExpirationTimePerEntityType(entityType)
                                    .getTime());
                    Assert.assertEquals(currentTime - TimeUnit.DAYS.toMillis(Integer.parseInt(expirationDaysPerEntity.get(entry.getKey()))),
                            assignedIdentityJooqProvider.getGetExpiredRecordsQuery()
                                    .getExpirationTimePerEntityType(entityType)
                                    .getTime());
                }
            }
        }
    }

    /**
     * Tests that recurrent operations are correctly deleted by the first task. Additionally, tests that
     * no other following task is deleting them.
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testDeleteRecurrentOperations(SQLDialect dialect) {
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), dialect);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        initialize();
        assertEquals(DELETED_OPERATIONS_N,
                assignedIdentityJooqProvider.getDeleteRecurrentOperations().getDeletedRecords());
    }

    /**
     * Tests that the diags collection works properly.
     * @throws DiagnosticsException if an error occurs
     */
    @Test
    public void testDiagsCollection() throws DiagnosticsException {
        initialize();
        DiagnosticsAppender appender = mock(DiagnosticsAppender.class);
        staleOidManager.collectDiags(appender);
        verify(appender).appendString(eq(StaleOidManagerImpl.DIAGS_HEADER));
        verify(appender, times(2)).appendString(any((String.class)));

        String diagsFileName = staleOidManager.getFileName();
        assertEquals(StaleOidManagerImpl.DIAGS_FILE_NAME, diagsFileName);
    }

    /**
     * Class that intercepts the queries that are supposed to be sent to the database, parse them
     * and return the database records. There's one class for each query performed by the
     * {@link StaleOidManagerImpl}. Each of these classes will contain the bindings of the queries in
     * their states. After we performed {@link StaleOidManagerTest#N_QUERIES_PER_EXPIRATION_TASK}
     * queries, we cancel the scheduled task returned by the
     * {@link StaleOidManagerImpl#initialize(Supplier, Consumer)}
     */
    private class AssignedIdentityJooqProvider implements MockDataProvider {
        private final GetExpiredRecordsQuery getExpiredRecordsQuery;
        private final SetExpiredRecordsQuery setExpiredRecordsQuery;
        private final DeleteRecurrentOperations deleteRecurrentOperations;
        private final int queriesPerExpirationTask;
        private List<SetLastSeenQuery> lastSeenQuery = new ArrayList<>();
        private SetRecurrentTaskQuery setRecurrentTaskQuery;
        private HashMap<String, String> expirationDaysPerEntity;
        private boolean doNotReturnSuccessfulUpdates;

        private int nQueries;
        private boolean throwException;

        AssignedIdentityJooqProvider(final int queriesPerExpirationTask,
                final boolean throwException, boolean doNotReturnSuccessfulUpdates) {
            this.queriesPerExpirationTask = queriesPerExpirationTask;
            this.setExpiredRecordsQuery = new SetExpiredRecordsQuery();
            this.getExpiredRecordsQuery = new GetExpiredRecordsQuery();
            this.throwException = throwException;
            this.doNotReturnSuccessfulUpdates = doNotReturnSuccessfulUpdates;
            this.deleteRecurrentOperations = new DeleteRecurrentOperations();
        }

        AssignedIdentityJooqProvider(final int queriesPerExpirationTask,
                                     HashMap<String, String> expirationDaysPerEntity) {
            this.setExpiredRecordsQuery = new SetExpiredRecordsQuery();
            this.getExpiredRecordsQuery = new GetExpiredRecordsQuery();
            this.expirationDaysPerEntity = expirationDaysPerEntity;
            this.queriesPerExpirationTask = queriesPerExpirationTask;
            this.deleteRecurrentOperations = new DeleteRecurrentOperations();
        }

        @Override
        public MockResult[] execute(MockExecuteContext ctx) {
            nQueries += 1;
            MockResult mockResult = new MockResult();
            final DSLContext create = DSL.using(SQLDialect.MARIADB);

            if (isGetExpiredRecordsQuery(ctx.sql())) {
                getExpiredRecordsQuery.setExpirationDate((Timestamp)ctx.bindings()[0]);
                Result<Record1<Long>> record =
                    create.newResult(AssignedIdentity.ASSIGNED_IDENTITY.ID);
                entityStoreOids.forEach(oid -> record.add(create.newRecord(AssignedIdentity.ASSIGNED_IDENTITY.ID).values(oid)));
                if (expirationDaysPerEntity != null) {
                    getExpiredRecordsQuery.addExpirationPerEntityType((Timestamp)ctx.bindings()[0], (Integer)ctx.bindings()[1]);
                }
                mockResult = new MockResult(1, record);
            }
            if (isSetLastSeenQuery(ctx.sql())) {
                SetLastSeenQuery query = new SetLastSeenQuery((Timestamp)ctx.bindings()[0]);
                Arrays.stream(ctx.bindings()).skip(1).forEach(oid -> query.addUpdatedOid((Long)oid));
                lastSeenQuery.add(query);
            }
            if (isSetExpiredRecordsQuery(ctx.sql())) {
                setExpiredRecordsQuery.setExpired((Boolean)ctx.bindings()[0]);
                setExpiredRecordsQuery.setExpirationDate((Timestamp)ctx.bindings()[1]);
                if (expirationDaysPerEntity != null) {
                    setExpiredRecordsQuery.addExpirationPerEntityType((Timestamp)ctx.bindings()[1], (Integer)ctx.bindings()[2]);
                }
            }
            if (isSetRecurrentTaskQuery(ctx.sql())) {
                 setRecurrentTaskQuery =
                     new SetRecurrentTaskQuery((Timestamp)ctx.bindings()[0],
                             (String)ctx.bindings()[1], (Integer)ctx.bindings()[2], (Boolean)ctx.bindings()[3], (String)ctx.bindings()[4], (String)ctx.bindings()[5]);
            }
            if (isCountValidRecurrentTasksQuery(ctx.sql())) {
                if (doNotReturnSuccessfulUpdates) {
                    mockResult = new MockResult();
                } else {
                    Field<Integer> intField = DSL.count();
                    Result<Record1<Integer>> result = create.newResult(intField);
                    result.add(create.newRecord(intField).values((int)TimeUnit.DAYS.toHours(EXPIRATION_DAYS) / VALIDATION_FREQUENCY_HRS));
                    mockResult = new MockResult(1, result);
                }
            }
            if (isGetRecurrentTasksQuery(ctx.sql())) {
                Result<RecurrentTasksRecord> result =
                        create.newResult(RecurrentTasks.RECURRENT_TASKS);
                result.add(recurrentTaskRecord);
                mockResult = new MockResult(1, result);
            }

            if (isDeleteRecurrentOperationsQuery(ctx.sql())) {
                deleteRecurrentOperations.setDeletedRecords(DELETED_OPERATIONS_N);
            }

            if (nQueries >= queriesPerExpirationTask && staleOidManagerProcess != null) {
                staleOidManagerProcess.cancel(true);
            }
            if (throwException && nQueries > 1) {
                nQueries = 1;
                throw new NullPointerException(EXPIRATION_EXCEPTION_MESSAGE);
            }
            return new MockResult[]{
                mockResult
            };
        }

        public List<SetLastSeenQuery> getLastSeenQuery() {
            return lastSeenQuery;
        }

        public GetExpiredRecordsQuery getGetExpiredRecordsQuery() {
            return getExpiredRecordsQuery;
        }

        public SetRecurrentTaskQuery getSetRecurrentTaskQuery() {
            return setRecurrentTaskQuery;
        }

        public SetExpiredRecordsQuery getSetExpiredRecordsQuery() {
            return setExpiredRecordsQuery;
        }

        public DeleteRecurrentOperations getDeleteRecurrentOperations() {
            return deleteRecurrentOperations;
        }

        private boolean isGetExpiredRecordsQuery(String sql) {
            return sql.contains("select") && sql.contains("last_seen") && sql.contains("assigned_identity");
        }

        private boolean isSetLastSeenQuery(String sql) {
            return sql.contains("update") && sql.contains("last_seen") && !sql.contains("expired") && !sql.contains("recurrent_operations");
        }

        private boolean isSetExpiredRecordsQuery(String sql) {
            return sql.contains("update") && sql.contains("expired") && !sql.contains("errors");
        }

        private boolean isSetRecurrentTaskQuery(String sql) {
            return sql.contains("insert") && sql.contains("recurrent_tasks");
        }

        private boolean isCountValidRecurrentTasksQuery(String sql) {
            return sql.contains("select count(*)") && sql.contains("recurrent_tasks");
        }

        private boolean isGetRecurrentOperationsQuery(String sql) {
            return sql.contains("select") && sql.contains("recurrent_operations") && !sql.contains("count");
        }

        private boolean isGetRecurrentTasksQuery(String sql) {
            return sql.contains("select") && sql.contains("recurrent_tasks") && !sql.contains("count");
        }

        private boolean isDeleteRecurrentOperationsQuery(String sql) {
            return sql.contains("delete") && sql.contains("recurrent_operations");
        }
    }

    /**
     * Class that contains the bindings used by the query to set the last_seen timestamps.
     */
    private static class SetLastSeenQuery {
        private final Timestamp queryTimeStamp;
        private final Set<Long> updatedOids = new HashSet<>();


        SetLastSeenQuery(final Timestamp queryTimeStamp) {
            this.queryTimeStamp = queryTimeStamp;
        }

        public void addUpdatedOid(long oid) {
            updatedOids.add(oid);
        }

        public Timestamp getQueryTimeStamp() {
            return queryTimeStamp;
        }

        public Set<Long> getUpdatedOids() {
            return updatedOids;
        }
    }

    /**
     * Class that contains the bindings used by the query to get the expired records.
     */
    private static class GetExpiredRecordsQuery {
        private Timestamp expirationDate;
        private final HashMap<Integer, Timestamp> expirationByEntityType = new HashMap<>();

        public Timestamp getExpirationDate() {
            return expirationDate;
        }

        public void setExpirationDate(final Timestamp expirationDate) {
            this.expirationDate = expirationDate;
        }

        public void addExpirationPerEntityType(Timestamp expirationTime, Integer entityType) {
            expirationByEntityType.put(entityType, expirationTime);
        }

        public Timestamp getExpirationTimePerEntityType(Integer entityType) {
            return expirationByEntityType.get(entityType);
        }
    }


        /**
     * Class that contains the bindings used by the query to set the expired records.
     */
    private static class SetExpiredRecordsQuery {
        private boolean isExpired;
        private Timestamp expirationDate;
        private final HashMap<Integer, Timestamp> expirationByEntityType = new HashMap<>();

        public Timestamp getExpirationDate() {
            return expirationDate;
        }

        public boolean getIsExpired() {
            return isExpired;
        }

        public void addExpirationPerEntityType(Timestamp expirationTime, Integer entityType) {
            expirationByEntityType.put(entityType, expirationTime);
        }

        public Timestamp getExpirationTimePerEntityType(Integer entityType) {
            return expirationByEntityType.get(entityType);
        }

        public void setExpired(final boolean expired) {
            isExpired = expired;
        }

        public void setExpirationDate(final Timestamp expirationDate) {
            this.expirationDate = expirationDate;
        }
    }

    /**
     * Class that contains the bindings used by the query to set the results of the task into the
     * recurrent_operations table.
     */
    private static class SetRecurrentTaskQuery {
        private final Timestamp executionTime;
        private final boolean successFulUpdate;
        private final int affectedRows;
        private String summary;
        private String errors;
        private final String operationName;

        SetRecurrentTaskQuery(final Timestamp executionTime, String operationName, int affectedRows, boolean successFulUpdate,
                              String summary, String errors) {
            this.executionTime = executionTime;
            this.operationName = operationName;
            this.successFulUpdate = successFulUpdate;
            this.affectedRows = affectedRows;
            this.summary = summary;
            this.errors = errors;
        }

        public String getOperationName() {
            return operationName;
        }

        public Timestamp getExecutionTime() {
            return executionTime;
        }

        public String getErrors() {
            return errors;
        }

        public void setErrors(String errors) {
            this.errors = errors;
        }

        public boolean isSuccessful() {
            return successFulUpdate;
        }
    }

    /**
     * Class that contains the bindings used by the query to delete entries from the
     * recurrent operations table.
     */
    private static class DeleteRecurrentOperations {
        private int deletedRecords = 0;

        public void setDeletedRecords(int deletedRecords) {
            this.deletedRecords = deletedRecords;
        }

        public int getDeletedRecords() {
            return deletedRecords;
        }
    }
}
