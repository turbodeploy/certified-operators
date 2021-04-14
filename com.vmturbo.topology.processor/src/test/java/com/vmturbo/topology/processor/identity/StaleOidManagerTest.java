package com.vmturbo.topology.processor.identity;

import static com.vmturbo.topology.processor.db.Tables.RECURRENT_OPERATIONS;
import static com.vmturbo.topology.processor.identity.StaleOidManagerImpl.EXPIRATION_TASK_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Optional;
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

import org.jooq.DSLContext;
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

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.db.tables.AssignedIdentity;

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
public class StaleOidManagerTest {
    private static final int EXPIRATION_DAYS = 30;
    private static final int VALIDATION_FREQUENCY_HRS = 1;
    private static final int N_QUERIES_PER_EXPIRATION_TASK = 5;
    private static final int TIMEOUT_SECONDS = 10;

    private StaleOidManagerImpl staleOidManager;
    private ScheduledFuture<?> staleOidManagerProcess;
    private AssignedIdentityJooqProvider assignedIdentityJooqProvider;
    private final Set<Long> entityStoreOids = new HashSet<>();
    private final long currentTime = System.currentTimeMillis();
    private final Clock clock = mock(Clock.class);
    private ScheduledExecutorService executor;

    long oid1 = 1;
    long oid2 = 2;

    /**
     * Set up the context for each test.
     */
    @Before
    public void setUp() {
        this.assignedIdentityJooqProvider = new AssignedIdentityJooqProvider(N_QUERIES_PER_EXPIRATION_TASK);
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), SQLDialect.MARIADB);
        entityStoreOids.add(oid1);
        entityStoreOids.add(oid2);
        when(clock.millis()).thenReturn(currentTime);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        executor = Executors.newScheduledThreadPool(1);
        staleOidManager = new StaleOidManagerImpl(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS),
            context, clock, true, executor, new HashMap<>());
    }

    /**
     * Methods to be run after each test.
     */
    @After
    public void after() {
        executor.shutdownNow();
    }

    private void initializeLastExecutionYesterdayWithEmptyListener() {
        long yesterdayTimestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
        assignedIdentityJooqProvider.setLastExecutionTimeMs(Optional.of(yesterdayTimestamp));
        // initialize with a listener that does nothing
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
    }

    /**
     * Tests that we correctly fetch the timestamp of the latest operation and we perform a new
     * OidExpirationTask with the current timestamp.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testPresentValidationTime() throws Exception {
        initializeLastExecutionYesterdayWithEmptyListener();
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(currentTime,
                assignedIdentityJooqProvider.getLastSeenQuery().getQueryTimeStamp().getTime());
            assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().getUpdatedOids(), entityStoreOids);
        }
    }

    /**
     * Tests that there's no prior task execution. We should not perform a new task execution
     * since the we should be waiting at least
     * for {@link StaleOidManagerTest#VALIDATION_FREQUENCY_HRS}
     *
     * @throws Exception if an error occurs in the staleOidManagerProcess
     */
    @Test
    public void testNotPresentValidationTime() throws Exception {
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
        try {
            staleOidManagerProcess.get(2, TimeUnit.SECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            Assert.assertNull(assignedIdentityJooqProvider.getLastSeenQuery());
        }
    }

    /**
     * Tests that we correctly set the last seen timestamp to the oids that currently exist in
     * the entity store.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testSetLastSeenTimeStamp() throws Exception {
        initializeLastExecutionYesterdayWithEmptyListener();
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(currentTime,
                assignedIdentityJooqProvider.getLastSeenQuery().getQueryTimeStamp().getTime());
            assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().getUpdatedOids(), entityStoreOids);
        }
    }

    /**
     * Tests that we correctly get the oids that will be expired.
     *
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testGetExpiredRecords() throws Exception {
        initializeLastExecutionYesterdayWithEmptyListener();
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
        initializeLastExecutionYesterdayWithEmptyListener();
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
        initializeLastExecutionYesterdayWithEmptyListener();
        try {
            staleOidManagerProcess.get(30, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTime), clock.getZone())),
                assignedIdentityJooqProvider.getSetRecurrentOperationQuery().getExecutionTime());
            assertEquals(EXPIRATION_TASK_NAME,
                assignedIdentityJooqProvider.getSetRecurrentOperationQuery().getOperationName());
            assertEquals(String.format("Expired Oids : %d", entityStoreOids.size()),
                assignedIdentityJooqProvider.getSetRecurrentOperationQuery().getSummary());
        }
    }

    /**
     * Tests that when the {@link StaleOidManagerImpl} is created with the
     * expireOids flag equal to false, only two queries are performed by the task. One to get the
     * last operation time and the other to set the last_seen value.
     * @throws Exception if the staleOidManagerProcess is interrupted or canceled
     */
    @Test
    public void testDoNotSetExpiredOids() throws Exception {
        long yesterdayTimestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
        AssignedIdentityJooqProvider assignedIdentityJooqProvider =
            new AssignedIdentityJooqProvider(2);
        assignedIdentityJooqProvider.setLastExecutionTimeMs(Optional.of(yesterdayTimestamp));
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), SQLDialect.MARIADB);
        final Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(currentTime);

        StaleOidManagerImpl staleOidManagerWithoutExpiration =
            new StaleOidManagerImpl(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS),
            context, clock, false, Executors.newScheduledThreadPool(1), new HashMap<>());
        staleOidManagerProcess = staleOidManagerWithoutExpiration.initialize(() -> entityStoreOids,
                set -> { });
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            assertEquals(currentTime,
                assignedIdentityJooqProvider.getLastSeenQuery().getQueryTimeStamp().getTime());
            assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().getUpdatedOids(), entityStoreOids);
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
            assignedIdentityJooqProvider.getSetRecurrentOperationQuery().getExecutionTime());
        assertEquals(EXPIRATION_TASK_NAME,
            assignedIdentityJooqProvider.getSetRecurrentOperationQuery().getOperationName());
        assertEquals(String.format("Expired Oids : %d", entityStoreOids.size()),
            assignedIdentityJooqProvider.getSetRecurrentOperationQuery().getSummary());
        Assert.assertTrue(assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getIsExpired());
        assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationDate().getTime());
        assertEquals(currentTime - TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            assignedIdentityJooqProvider.getGetExpiredRecordsQuery().getExpirationDate().getTime());
        assertEquals(currentTime,
            assignedIdentityJooqProvider.getLastSeenQuery().getQueryTimeStamp().getTime());
        assertEquals(assignedIdentityJooqProvider.getLastSeenQuery().getUpdatedOids(), entityStoreOids);
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
        long yesterdayTimestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
        assignedIdentityJooqProvider.setLastExecutionTimeMs(Optional.of(yesterdayTimestamp));
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
     * Tests that entity types that have a day set in the expirationDaysPerEntity are expired
     * using that timestamp.
     *
     * @throws InterruptedException if the staleOidManagerProcess is interrupted or canceled
     * @throws ExecutionException if there is an issue in executing the staleOidManagerProcess
     * @throws TimeoutException if the staleOidManagerProcess reaches a timeout
     */
    @Test
    public void testExpirationsPerEntityType() throws InterruptedException, ExecutionException, TimeoutException {
        HashMap<String, String> expirationDaysPerEntity = new HashMap<>();
        expirationDaysPerEntity.put("VIRTUAL_MACHINE", "2");
        expirationDaysPerEntity.put("VIRTUAL_VOLUME", "5");

        AssignedIdentityJooqProvider assignedIdentityJooqProvider =
            new AssignedIdentityJooqProvider(N_QUERIES_PER_EXPIRATION_TASK, expirationDaysPerEntity);
        long yesterdayTimestamp = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();

        assignedIdentityJooqProvider.setLastExecutionTimeMs(Optional.of(yesterdayTimestamp));
        final DSLContext context = DSL.using(new MockConnection(assignedIdentityJooqProvider), SQLDialect.MARIADB);
        entityStoreOids.add(oid1);
        entityStoreOids.add(oid2);
        when(clock.millis()).thenReturn(currentTime);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        staleOidManager = new StaleOidManagerImpl(TimeUnit.DAYS.toMillis(EXPIRATION_DAYS),
            TimeUnit.HOURS.toMillis(VALIDATION_FREQUENCY_HRS),
            context, clock, true, Executors.newScheduledThreadPool(1), expirationDaysPerEntity);
        staleOidManagerProcess = staleOidManager.initialize(() -> entityStoreOids, set -> { });
        try {
            staleOidManagerProcess.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.fail();
        } catch (CancellationException e) {
            for (Entry<String, String> entry : expirationDaysPerEntity.entrySet()) {
                int entityType = EntityType.valueOf(entry.getKey()).getNumber();
                Assert.assertEquals(currentTime - TimeUnit.DAYS.toMillis(Integer.parseInt(expirationDaysPerEntity.get(entry.getKey()))),
                    assignedIdentityJooqProvider.getSetExpiredRecordsQuery().getExpirationTimePerEntityType(entityType).getTime());
                Assert.assertEquals(currentTime - TimeUnit.DAYS.toMillis(Integer.parseInt(expirationDaysPerEntity.get(entry.getKey()))),
                    assignedIdentityJooqProvider.getGetExpiredRecordsQuery().getExpirationTimePerEntityType(entityType).getTime());
            }
        }

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
        private SetLastSeenQuery lastSeenQuery;
        private GetExpiredRecordsQuery getExpiredRecordsQuery;
        private SetExpiredRecordsQuery setExpiredRecordsQuery;
        private SetRecurrentOperationQuery setRecurrentOperationQuery;
        private HashMap<String, String> expirationDaysPerEntity;

        private int nQueries;
        private final int queriesPerExpirationTask;
        private Optional<Long> lastExecutionTimeMs = Optional.empty();

        AssignedIdentityJooqProvider(final int queriesPerExpirationTask) {
            this.queriesPerExpirationTask = queriesPerExpirationTask;
            this.setExpiredRecordsQuery = new SetExpiredRecordsQuery();
            this.getExpiredRecordsQuery = new GetExpiredRecordsQuery();
        }

        AssignedIdentityJooqProvider(final int queriesPerExpirationTask,
                                     HashMap<String, String> expirationDaysPerEntity) {
            this.setExpiredRecordsQuery = new SetExpiredRecordsQuery();
            this.getExpiredRecordsQuery = new GetExpiredRecordsQuery();
            this.expirationDaysPerEntity = expirationDaysPerEntity;
            this.queriesPerExpirationTask = queriesPerExpirationTask + expirationDaysPerEntity.size() * 2;

        }

        @Override
        public MockResult[] execute(MockExecuteContext ctx) {
            nQueries += 1;
            MockResult result = new MockResult();
            final DSLContext create = DSL.using(SQLDialect.MARIADB);

            if (isGetLastExecutedOperation(ctx.sql())) {
                Result<Record1<LocalDateTime>> record =
                    create.newResult(RECURRENT_OPERATIONS.EXECUTION_TIME);
                if (lastExecutionTimeMs.isPresent()) {
                    record.add(create.newRecord(RECURRENT_OPERATIONS.EXECUTION_TIME)
                        .values(LocalDateTime.ofInstant(Instant.ofEpochMilli(lastExecutionTimeMs.get()),
                            ZoneId.systemDefault())));
                    }
                    result = new MockResult(1, record);
            }

            if (isGetExpiredRecordsQuery(ctx.sql())) {
                getExpiredRecordsQuery.setExpirationDate((Timestamp)ctx.bindings()[0]);
                Result<Record1<Long>> record =
                    create.newResult(AssignedIdentity.ASSIGNED_IDENTITY.ID);
                entityStoreOids.forEach(oid -> record.add(create.newRecord(AssignedIdentity.ASSIGNED_IDENTITY.ID).values(oid)));
                if (expirationDaysPerEntity != null && ctx.bindings().length == expirationDaysPerEntity.size()) {
                    getExpiredRecordsQuery.addExpirationPerEntityType((Timestamp)ctx.bindings()[0], (Integer)ctx.bindings()[1]);
                }
                result = new MockResult(1, record);
            }
            if (isSetLastSeenQuery(ctx.sql())) {
                lastSeenQuery = new SetLastSeenQuery((Timestamp)ctx.bindings()[0]);
                Arrays.stream(ctx.bindings()).skip(1).forEach(oid -> lastSeenQuery.addUpdatedOid((Long)oid));
            }
            if (isSetExpiredRecordsQuery(ctx.sql())) {
                setExpiredRecordsQuery.setExpired((Boolean)ctx.bindings()[0]);
                setExpiredRecordsQuery.setExpirationDate((Timestamp)ctx.bindings()[1]);
                if (expirationDaysPerEntity != null && ctx.bindings().length == expirationDaysPerEntity.size() + 1) {
                    setExpiredRecordsQuery.addExpirationPerEntityType((Timestamp)ctx.bindings()[1], (Integer)ctx.bindings()[2]);
                }
            }
            if (isSetRecurrentOperationQuery(ctx.sql())) {
                 setRecurrentOperationQuery =
                     new SetRecurrentOperationQuery((Timestamp)ctx.bindings()[0],
                     (String)ctx.bindings()[1], (String)ctx.bindings()[2]);
            }
            if (nQueries == queriesPerExpirationTask && staleOidManagerProcess != null) {
                staleOidManagerProcess.cancel(true);
            }
            return new MockResult[]{
                result
            };
        }

        public void setLastExecutionTimeMs(final Optional<Long> lastExecutionTimeMs) {
            this.lastExecutionTimeMs = lastExecutionTimeMs;
        }

        public SetLastSeenQuery getLastSeenQuery() {
            return lastSeenQuery;
        }

        public GetExpiredRecordsQuery getGetExpiredRecordsQuery() {
            return getExpiredRecordsQuery;
        }

        public SetRecurrentOperationQuery getSetRecurrentOperationQuery() {
            return setRecurrentOperationQuery;
        }

        public SetExpiredRecordsQuery getSetExpiredRecordsQuery() {
            return setExpiredRecordsQuery;
        }

        private boolean isGetExpiredRecordsQuery(String sql) {
            return sql.contains("select") && sql.contains("last_seen");
        }

        private boolean isSetLastSeenQuery(String sql) {
            return sql.contains("update") && sql.contains("last_seen") && !sql.contains("expired");
        }

        private boolean isSetExpiredRecordsQuery(String sql) {
            return sql.contains("update") && sql.contains("expired");
        }

        private boolean isGetLastExecutedOperation(String sql) {
            return sql.contains("select") && sql.contains("recurrent_operations");
        }

        private boolean isSetRecurrentOperationQuery(String sql) {
            return sql.contains("insert") && sql.contains("recurrent_operations");
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

        public boolean isExpired() {
            return isExpired;
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
    private static class SetRecurrentOperationQuery {
        private final Timestamp executionTime;
        private final String operationName;
        private final String summary;


        SetRecurrentOperationQuery(final Timestamp executionTime, final String operationName, final String summary) {
            this.executionTime = executionTime;
            this.operationName = operationName;
            this.summary = summary;
        }

        public Timestamp getExecutionTime() {
            return executionTime;
        }

        public String getOperationName() {
            return operationName;
        }

        public String getSummary() {
            return summary;
        }
    }
}
