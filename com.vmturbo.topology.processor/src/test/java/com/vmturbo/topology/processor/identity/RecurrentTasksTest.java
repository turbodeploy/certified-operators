package com.vmturbo.topology.processor.identity;

import static com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask.BATCH_SIZE;
import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.vmturbo.topology.processor.db.tables.AssignedIdentity;
import com.vmturbo.topology.processor.identity.StaleOidManagerImpl.OidExpirationResultRecord;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidDeletionTask;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidExpirationTask;
import com.vmturbo.topology.processor.identity.recurrenttasks.OidTimestampUpdateTask;

/**
 * Class to test a {@link com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask}.
 */
@RunWith(JUnitParamsRunner.class)
public class RecurrentTasksTest {
    Object[] generateTestData() {
        return $(
                $(SQLDialect.MARIADB),
                $(SQLDialect.POSTGRES));
    }

    private static final String ERROR_MESSAGE = "Error with the recurrent task";
    private final Set<Long> entityStoreOids = new HashSet<>();
    private final long currentTime = System.currentTimeMillis();
    private final Clock clock = mock(Clock.class);

    long oid1 = 1;
    long oid2 = 2;

    /**
     * Set up the context for each test.
     */
    @Before
    public void setUp() {
        entityStoreOids.add(oid1);
        entityStoreOids.add(oid2);
        when(clock.millis()).thenReturn(currentTime);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
    }

    /**
     * Tests that we can properly run a {@link com.vmturbo.topology.processor.identity.recurrenttasks.OidTimestampUpdateTask}.
     *
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testOidTimestampUpdate(SQLDialect dialect) {
        Set<Long> customEntityStoreOids = new HashSet<>();
        final long oidSize = 110;
        for (long i = 0; i < oidSize; i++) {
            customEntityStoreOids.add(i);
        }
        final OidTimestampUpdateJooqProvider oidTimestampUpdateJooqProvider = new OidTimestampUpdateJooqProvider();
        final long currentTimeMs = System.currentTimeMillis();
        final DSLContext context = DSL.using(new MockConnection(oidTimestampUpdateJooqProvider), dialect);
        final OidExpirationResultRecord oidExpirationResultRecord = mock(OidExpirationResultRecord.class);
        final OidTimestampUpdateTask task = new OidTimestampUpdateTask(currentTimeMs, clock, context, () -> customEntityStoreOids, oidExpirationResultRecord);
        task.run();
        assertTrue(oidTimestampUpdateJooqProvider.isSuccessful());
        Assert.assertEquals(customEntityStoreOids.size(), oidTimestampUpdateJooqProvider.getUpdatedRecords());
        Assert.assertEquals((int)Math.ceil((double)oidSize / BATCH_SIZE), oidTimestampUpdateJooqProvider.getNQueries());
    }

    /**
     * Tests that we can properly run a {@link com.vmturbo.topology.processor.identity.recurrenttasks.OidExpirationTask}.
     *
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testOidExpiration(SQLDialect dialect) {
        final Map<Integer, Long> entityExpirationByEntityTimeMs = Collections.emptyMap();
        final OidExpirationJooqProvider oidExpirationJooqProvider = new OidExpirationJooqProvider();
        final long currentTimeMs = System.currentTimeMillis();
        final long entityExpirationTimeMs = TimeUnit.DAYS.toMillis(1);
        final Consumer<Set<Long>> notifyExpiredOids = mock(Consumer.class);
        final DSLContext context = DSL.using(new MockConnection(oidExpirationJooqProvider), dialect);
        final OidExpirationResultRecord oidExpirationResultRecord = mock(OidExpirationResultRecord.class);
        final OidExpirationTask task = new OidExpirationTask(entityExpirationTimeMs, true,
                notifyExpiredOids, entityExpirationByEntityTimeMs, oidExpirationResultRecord, currentTimeMs, clock, context);
        task.run();

        final long expectedExpirationDate = Instant.ofEpochMilli(currentTimeMs).minus(Duration.ofDays(1)).toEpochMilli();
        assertTrue(oidExpirationJooqProvider.isSuccessful());
        Assert.assertEquals(expectedExpirationDate, oidExpirationJooqProvider.getGetExpiredDate().getTime());
        Assert.assertEquals(expectedExpirationDate, oidExpirationJooqProvider.getSetExpiredDate().getTime());
        Mockito.verify(notifyExpiredOids).accept(entityStoreOids);
    }

    /**
     * Tests that we can properly run a {@link OidExpirationTask} with different times set for each entity type.
     *
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testOidExpirationWithExpirationByEntityType(SQLDialect dialect) {
        final Map<Integer, Long> entityExpirationByEntityTimeMs = new HashMap<>();
        entityExpirationByEntityTimeMs.put(10, TimeUnit.DAYS.toMillis(42));
        final OidExpirationJooqProvider oidExpirationJooqProvider = new OidExpirationJooqProvider();
        final long currentTimeMs = System.currentTimeMillis();
        final long entityExpirationTimeMs = TimeUnit.DAYS.toMillis(1);
        final Consumer<Set<Long>> notifyExpiredOids = mock(Consumer.class);

        final DSLContext context = DSL.using(new MockConnection(oidExpirationJooqProvider), dialect);
        final OidExpirationResultRecord oidExpirationResultRecord = mock(OidExpirationResultRecord.class);
        final OidExpirationTask task = new OidExpirationTask(entityExpirationTimeMs, true,
                notifyExpiredOids, entityExpirationByEntityTimeMs, oidExpirationResultRecord, currentTimeMs, clock, context);
        task.run();

        assertTrue(oidExpirationJooqProvider.isSuccessful());
        for (Integer entityType : entityExpirationByEntityTimeMs.keySet()) {
            final long expectedTime = Instant.ofEpochMilli(currentTimeMs).minus(Duration.ofMillis(entityExpirationByEntityTimeMs.get(entityType))).toEpochMilli();
            final long actualTime = oidExpirationJooqProvider.getGetEntityExpirationByEntityTimeMs().get(entityType).toInstant().toEpochMilli();
            Assert.assertEquals(expectedTime, actualTime);
        }

        for (Integer entityType : entityExpirationByEntityTimeMs.keySet()) {
            final long expectedTime = Instant.ofEpochMilli(currentTimeMs).minus(Duration.ofMillis(entityExpirationByEntityTimeMs.get(entityType))).toEpochMilli();
            final long actualTime = oidExpirationJooqProvider.getSetEntityExpirationByEntityTimeMs().get(entityType).toInstant().toEpochMilli();
            Assert.assertEquals(expectedTime, actualTime);
        }
        Mockito.verify(notifyExpiredOids).accept(entityStoreOids);
    }

    /**
     * Tests that we can properly run a {@link com.vmturbo.topology.processor.identity.recurrenttasks.OidDeletionTask}.
     *
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testOidDeletion(SQLDialect dialect) {
        final OidDeletionJooqProvider oidDeletionJooqProvider = new OidDeletionJooqProvider();
        final long currentTimeMs = System.currentTimeMillis();
        final DSLContext context = DSL.using(new MockConnection(oidDeletionJooqProvider), dialect);
        final int expiredRecordsRetentionDays = 180;
        final OidDeletionTask task = new OidDeletionTask(currentTimeMs, clock, context, true, expiredRecordsRetentionDays);
        task.run();
        final long expectedDeletionDate = Instant.ofEpochMilli(currentTimeMs).minus(Duration.ofDays(expiredRecordsRetentionDays)).toEpochMilli();

        assertTrue(oidDeletionJooqProvider.isSuccessful());
        Assert.assertEquals(1, oidDeletionJooqProvider.getNQueries());
        Assert.assertEquals(expectedDeletionDate, oidDeletionJooqProvider.getDeletionDate().toInstant().toEpochMilli());
    }

    /**
     * Tests that that we properly handle an exception in a recurrent task.
     *
     * @param dialect SQLDialect to use
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testFailedTask(SQLDialect dialect) {
        Set<Long> customEntityStoreOids = new HashSet<>();
        final long oidSize = 110;
        for (long i = 0; i < oidSize; i++) {
            customEntityStoreOids.add(i);
        }
        final FailedTaskJooqProvider failedTaskJooqProvider = new FailedTaskJooqProvider();
        final long currentTimeMs = System.currentTimeMillis();
        final DSLContext context = DSL.using(new MockConnection(failedTaskJooqProvider), dialect);
        final OidExpirationResultRecord oidExpirationResultRecord = mock(OidExpirationResultRecord.class);
        final OidTimestampUpdateTask task = new OidTimestampUpdateTask(currentTimeMs, clock, context, () -> customEntityStoreOids, oidExpirationResultRecord);
        task.run();
        Assert.assertEquals(ERROR_MESSAGE, failedTaskJooqProvider.getError());
    }

    /**
     * Abstract lass that intercepts the queries that are supposed to be sent to the database, it parses them
     * and returns the database records. There should be one class for each implementation of a RecurrentTask
     * {@link com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask}.
     */
    private abstract static class RecurrentTaskJooqProvider implements MockDataProvider {
        private boolean successful;

        public String getError() {
            return error;
        }

        private String error;

        @Override
        public MockResult[] execute(MockExecuteContext ctx) {
            if (ctx.sql().contains("recurrent_task")) {
                verifySuccessfulTask(ctx);
                return new MockResult[]{
                        new MockResult(1, null)
                };
            }
            return parseQuery(ctx);
        }

        abstract MockResult[] parseQuery(MockExecuteContext ctx);

        boolean isSuccessful() {
            return successful;
        }

        void verifySuccessfulTask(MockExecuteContext ctx) {
            successful = (Boolean)ctx.bindings()[3];
            error = (String)ctx.bindings()[5];
        }
    }

    /**
     * Class to handle queries performed by a {@link OidTimestampUpdateTask} task.
     */
    private static class OidTimestampUpdateJooqProvider extends RecurrentTaskJooqProvider {
        private int nQueries = 0;
        private int updatedRecords = 0;

        @Override
        public MockResult[] parseQuery(MockExecuteContext ctx) {
            nQueries += 1;
            final DSLContext create = DSL.using(SQLDialect.MARIADB);
            Field<Integer> intField = DSL.count();
            Result<Record1<Integer>> result = create.newResult(intField);
            updatedRecords += ((Object[])ctx.bindings()).length - 1;
            MockResult mockResult = new MockResult(42, result);
            return new MockResult[]{
                    mockResult
            };
        }

        public int getNQueries() {
            return nQueries;
        }

        public int getUpdatedRecords() {
            return updatedRecords;
        }
    }

    /**
     * Class to handle queries performed by a {@link OidExpirationTask} task.
     */
    private class OidExpirationJooqProvider extends RecurrentTaskJooqProvider {
        private Timestamp getExpiredDate;
        private Timestamp setExpiredDate;
        private final Map<Integer, Timestamp> getEntityExpirationByEntityTimeMs = new HashMap<>();
        private final Map<Integer, Timestamp> setEntityExpirationByEntityTimeMs = new HashMap<>();

        @Override
        public MockResult[] parseQuery(MockExecuteContext ctx) {
            final DSLContext create = DSL.using(SQLDialect.MARIADB);
            MockResult mockResult = new MockResult();
            if (ctx.sql().contains("select")) {
                if (ctx.sql().contains("entity_type")) {
                    getEntityExpirationByEntityTimeMs.put((Integer)ctx.bindings()[1], ((Timestamp)ctx.bindings()[0]));
                }
                getExpiredDate = (Timestamp)ctx.bindings()[0];
                Result<Record1<Long>> record =
                        create.newResult(AssignedIdentity.ASSIGNED_IDENTITY.ID);
                entityStoreOids.forEach(oid -> record.add(create.newRecord(AssignedIdentity.ASSIGNED_IDENTITY.ID).values(oid)));
                mockResult = new MockResult(1, record);
            } else if (!ctx.sql().contains("recurrent_operations")) {
                if (ctx.sql().contains("entity_type")) {
                    setEntityExpirationByEntityTimeMs.put((Integer)ctx.bindings()[2], ((Timestamp)ctx.bindings()[1]));
                }
                setExpiredDate = (Timestamp)ctx.bindings()[1];
            }
            return new MockResult[]{
                    mockResult
            };
        }

        public Timestamp getGetExpiredDate() {
            return getExpiredDate;
        }

        public Timestamp getSetExpiredDate() {
            return setExpiredDate;
        }

        public Map<Integer, Timestamp> getGetEntityExpirationByEntityTimeMs() {
            return getEntityExpirationByEntityTimeMs;
        }

        public Map<Integer, Timestamp> getSetEntityExpirationByEntityTimeMs() {
            return setEntityExpirationByEntityTimeMs;
        }
    }

    /**
     * Class to handle queries performed by a {@link OidDeletionTask} task.
     */
    private static class OidDeletionJooqProvider extends RecurrentTaskJooqProvider {
        private int nQueries = 0;
        private Timestamp deletionDate;

        @Override
        public MockResult[] parseQuery(MockExecuteContext ctx) {
            nQueries += 1;
            final DSLContext create = DSL.using(SQLDialect.MARIADB);
            deletionDate = ((Timestamp)ctx.bindings()[0]);
            Result<Record1<Long>> record =
                    create.newResult(AssignedIdentity.ASSIGNED_IDENTITY.ID);
            return new MockResult[]{
                    new MockResult(1, record)
            };
        }

        public int getNQueries() {
            return nQueries;
        }

        public Timestamp getDeletionDate() {
            return deletionDate;
        }
    }

    /**
     * Class to handle queries performed by a {@link OidTimestampUpdateTask} task.
     */
    private static class FailedTaskJooqProvider extends RecurrentTaskJooqProvider {
        @Override
        public MockResult[] parseQuery(MockExecuteContext ctx) {
            throw new RuntimeException(ERROR_MESSAGE);
        }
    }
}
