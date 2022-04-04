package com.vmturbo.action.orchestrator.action;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.TableRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.tables.records.ExecutedActionsChangeWindowRecord;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit test for {@link ExecutedActionsChangeWindowDaoImpl}.
 */
@RunWith(Parameterized.class)
public class ExecutedActionsChangeWindowDaoImplTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    ExecutedActionsChangeWindowDao executedActionsChangeWindowDao;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public ExecutedActionsChangeWindowDaoImplTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Set up for tests.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     */
    @Before
    public void setUp() throws SQLException, UnsupportedDialectException, InterruptedException {
        executedActionsChangeWindowDao = new ExecutedActionsChangeWindowDaoImpl(dsl, Clock.systemUTC());
    }

    /**
     * Test logic of getExecutedActionsChangeWindowMap method.
     */
    @Test
    public void testGetExecutedActionsChangeWindowMap() {
        // Insert records for 2 entities, each have 2 ExecutedActionsChangeWindow records.
        // Note that the timestamps for the records of entity 1001L are not in ascending order.
        final Collection<TableRecord<?>> inserts = new ArrayList<>();
        inserts.add(createRecord(100L, 1000L,
                LocalDateTime.of(2022, 3, 15, 10, 30)));
        inserts.add(createRecord(101L, 1000L,
                LocalDateTime.of(2022, 3, 16, 10, 30)));
        inserts.add(createRecord(102L, 1001L,
                LocalDateTime.of(2022, 3, 17, 10, 30)));
        inserts.add(createRecord(103L, 1001L,
                LocalDateTime.of(2022, 3, 14, 10, 30)));
        dsl.batchInsert(inserts).execute();

        // Invoke the getExecutedActionsChangeWindowMap with entity OIDs of the two entities.
        Map<Long, List<ExecutedActionsChangeWindow>> result =
                executedActionsChangeWindowDao.getExecutedActionsChangeWindowMap(Arrays.asList(1000L, 1001L));

        // Verify the map returned has two entries.
        Assert.assertEquals(2, result.size());

        // Verify the records for entity 1000L.
        List<ExecutedActionsChangeWindow> changeWindows = result.get(1000L);
        List<ExecutedActionsChangeWindow> expectedChangeWindows = Arrays.asList(
                createExecutedActionsChangeWindow(1000L, 100L,
                        LocalDateTime.of(2022, 3, 15, 10, 30)),
                createExecutedActionsChangeWindow(1000L, 101L,
                        LocalDateTime.of(2022, 3, 16, 10, 30)));
        Assert.assertEquals(expectedChangeWindows, changeWindows);

        // Verify the records for entity 1001L. Note that the actions in the list is in ascending order.
        changeWindows = result.get(1001L);
        expectedChangeWindows = Arrays.asList(
                createExecutedActionsChangeWindow(1001L, 103L,
                        LocalDateTime.of(2022, 3, 14, 10, 30)),
                createExecutedActionsChangeWindow(1001L, 102L,
                        LocalDateTime.of(2022, 3, 17, 10, 30)));
        Assert.assertEquals(expectedChangeWindows, changeWindows);
    }

    private ExecutedActionsChangeWindowRecord createRecord(long actionOid, long entityOid,
            LocalDateTime startTime) {
        ExecutedActionsChangeWindowRecord record = new ExecutedActionsChangeWindowRecord();
        record.setActionOid(actionOid);
        record.setEntityOid(entityOid);
        record.setStartTime(startTime);
        return record;
    }

    private ExecutedActionsChangeWindow createExecutedActionsChangeWindow(long entityOid,
            long actionOid, LocalDateTime startTime) {
        return ExecutedActionsChangeWindow.newBuilder()
                .setEntityOid(entityOid)
                .setStartTime(TimeUtil.localTimeToMillis(startTime, Clock.systemUTC()))
                .setActionOid(actionOid)
                .build();
    }
}
