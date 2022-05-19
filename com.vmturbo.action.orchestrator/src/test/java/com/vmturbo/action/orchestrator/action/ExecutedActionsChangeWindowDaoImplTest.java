package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ExecutedActionsChangeWindow.EXECUTED_ACTIONS_CHANGE_WINDOW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit test for {@link ExecutedActionsChangeWindowDaoImpl}.
 */
@RunWith(Parameterized.class)
public class ExecutedActionsChangeWindowDaoImplTest extends MultiDbTestBase {
    private final Clock clock = Clock.systemUTC();

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

    private static final long actionId1 = 101L;
    private static final long actionId2 = 102L;

    /**
     * Initial list of action window entries.
     */
    private static final List<Pair<Long, Long>> actionIdEntityIdList = ImmutableList.of(
            ImmutablePair.of(actionId1, 10001L),
            ImmutablePair.of(actionId2, 10001L),
            ImmutablePair.of(201L, 20001L)
    );

    /**
     * Action entries for which start time needs to be set.
     */
    private static final Set<ActionLivenessInfo> startTimingInfo = ImmutableSet.of(
            ActionLivenessInfo.newBuilder()
                    .setActionOid(actionId1)
                    .setTimestamp(1651399200000L) // May-01-22, 10 AM
                    .setLivenessState(LivenessState.LIVE)
                    .build(),

            ActionLivenessInfo.newBuilder()
                    .setActionOid(actionId2)
                    .setTimestamp(1654077600000L) // Jun-01-22, 10 AM
                    .setLivenessState(LivenessState.LIVE)
                    .build()
    );

    /**
     * Action entries for which end time and reason code needs to be set.
     */
    private static final Set<ActionLivenessInfo> endTimingInfo = ImmutableSet.of(
            ActionLivenessInfo.newBuilder()
                    .setActionOid(actionId1)
                    .setTimestamp(1652608800000L) // May-15-22, 10 AM
                    .setLivenessState(LivenessState.REVERTED)
                    .build(),

            ActionLivenessInfo.newBuilder()
                    .setActionOid(actionId2)
                    .setTimestamp(1656583200000L) // Jun-30-22, 10 AM
                    .setLivenessState(LivenessState.SUPERSEDED)
                    .build()
    );

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
        executedActionsChangeWindowDao = new ExecutedActionsChangeWindowDaoImpl(dsl, clock, 1);
    }

    /**
     * Test logic of getExecutedActionsChangeWindowMap method.
     */
    @Test
    public void testGetExecutedActionsChangeWindowMap() {
        final LocalDateTime dateMar14 = LocalDateTime.of(2022, 3, 14, 10, 30);
        final LocalDateTime dateMar15 = LocalDateTime.of(2022, 3, 15, 10, 30);
        final LocalDateTime dateMar16 = LocalDateTime.of(2022, 3, 16, 10, 30);
        final LocalDateTime dateMar17 = LocalDateTime.of(2022, 3, 17, 10, 30);

        // Insert records for 2 entities, each have 2 ExecutedActionsChangeWindow records.
        // Note that the timestamps for the records of entity 1001L are not in ascending order.
        final Collection<TableRecord<?>> inserts = new ArrayList<>();
        inserts.add(createRecord(100L, 1000L, dateMar15));
        inserts.add(createRecord(101L, 1000L, dateMar16));
        inserts.add(createRecord(102L, 1001L, dateMar17));
        inserts.add(createRecord(103L, 1001L, dateMar14));
        dsl.batchInsert(inserts).execute();

        // Invoke the getExecutedActionsChangeWindowMap with entity OIDs of the two entities.
        Map<Long, List<ExecutedActionsChangeWindow>> result =
                executedActionsChangeWindowDao.getActionsByEntityOid(Arrays.asList(1000L, 1001L));

        // Verify the map returned has two entries.
        Assert.assertEquals(2, result.size());

        // Verify the records for entity 1000L.
        List<ExecutedActionsChangeWindow> changeWindows = result.get(1000L);
        List<ExecutedActionsChangeWindow> expectedChangeWindows = Arrays.asList(
                createExecutedActionsChangeWindow(1000L, 100L, dateMar15),
                createExecutedActionsChangeWindow(1000L, 101L, dateMar16));
        Assert.assertEquals(expectedChangeWindows, changeWindows);

        // Verify the records for entity 1001L. Note that the actions in the list is in ascending order.
        changeWindows = result.get(1001L);
        expectedChangeWindows = Arrays.asList(
                createExecutedActionsChangeWindow(1001L, 103L, dateMar14),
                createExecutedActionsChangeWindow(1001L, 102L, dateMar17));
        Assert.assertEquals(expectedChangeWindows, changeWindows);
    }

    /**
     * Tests persisting of succeeded cloud workload actions to the executed_actions_change_window table.
     *
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    @Test
    public void saveExecutedAction() throws ActionStoreOperationException {
        List<ExecutedActionsChangeWindowRecord> records = insertInitialActionRecords();
        List<Pair<Long, Long>> actualOidPairs = records.stream()
                .map(rec -> ImmutablePair.of(rec.getActionOid(), rec.getEntityOid()))
                .collect(Collectors.toList());
        assertTrue(CollectionUtils.isEqualCollection(actionIdEntityIdList, actualOidPairs));
    }

    /**
     * Tests updating of timing info for action records. Checks start time, end time and reason.
     *
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    @Test
    public void updateActionLivenessInfo() throws ActionStoreOperationException {
        // Insert initial action change records, without any start time set.
        insertInitialActionRecords();

        // Now set start time for some of those records, and verify it is set correctly.
        executedActionsChangeWindowDao.updateActionLivenessInfo(startTimingInfo);
        List<ExecutedActionsChangeWindowRecord> records = getAllRecords();
        assertEquals(3, records.size());
        records.forEach(rec -> {
            long actionId = rec.getActionOid();
            final Optional<ActionLivenessInfo> livenessInfo = startTimingInfo.stream()
                    .filter(info -> info.getActionOid() == actionId)
                    .findFirst();
            LocalDateTime actualStartTime = rec.getStartTime();
            if (!livenessInfo.isPresent()) {
                // For actionId 201, we will not find a start time entry.
                assertNull(actualStartTime);
            } else {
                // There should be start time entries for actionId 101 and 102, so check that.
                assertEquals(livenessInfo.get().getTimestamp(), TimeUtil.localTimeToMillis(
                        actualStartTime, clock));
            }
        });

        // Now set end time and liveness state and verify that.
        executedActionsChangeWindowDao.updateActionLivenessInfo(endTimingInfo);
        records = getAllRecords();
        assertEquals(3, records.size());
        records.forEach(rec -> {
            long actionId = rec.getActionOid();
            final Optional<ActionLivenessInfo> livenessInfo = endTimingInfo.stream()
                    .filter(info -> info.getActionOid() == actionId)
                    .findFirst();
            LocalDateTime actualEndTime = rec.getEndTime();
            Integer livenessState = rec.getLivenessState();
            if (!livenessInfo.isPresent()) {
                // For actionId 201, we will not have an end time.
                assertNull(actualEndTime);
                // Default liveness state is NEW, so check that.
                assertNotNull(livenessState);
                assertEquals(LivenessState.NEW.getNumber(), (int)livenessState);
            } else {
                // There should be end time and state for actionId 101 and 102, so check that.
                assertEquals(livenessInfo.get().getTimestamp(), TimeUtil.localTimeToMillis(
                        actualEndTime, clock));
                assertEquals(livenessInfo.get().getLivenessState().getNumber(), (int)livenessState);
            }
        });
    }

    /**
     * Testing live action query.
     *
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    @Test
    public void fetchLiveActions() throws ActionStoreOperationException {
        // Insert initial action change records, without any start time set.
        insertInitialActionRecords();

        final Set<LivenessState> liveState = ImmutableSet.of(LivenessState.LIVE);
        // 3 records were inserted, but none of them have start time set yet, so they are not
        // yet live, so expect empty results.
        List<ExecutedActionsChangeWindow> actionList = new ArrayList<>();
        executedActionsChangeWindowDao.getActionsByLivenessState(liveState, actionList::add);
        assertTrue(actionList.isEmpty());

        // Now set start time for a couple of those records and thus make them live, because
        // we are not setting end time yet.
        executedActionsChangeWindowDao.updateActionLivenessInfo(startTimingInfo);
        actionList.clear();
        executedActionsChangeWindowDao.getActionsByLivenessState(liveState, actionList::add);
        assertEquals(2, actionList.size());

        // Verify action ids match.
        Set<Long> actualActionIds = actionList.stream()
                .map(ExecutedActionsChangeWindow::getActionOid)
                .collect(Collectors.toSet());
        Set<Long> expectedActionIds = startTimingInfo.stream()
                .map(ActionLivenessInfo::getActionOid)
                .collect(Collectors.toSet());
        assertTrue(CollectionUtils.isEqualCollection(expectedActionIds, actualActionIds));

        // Verify that if 1 actionId is specified, we get results for only that one, rather than both
        // the live entries.
        actionList.clear();
        expectedActionIds = ImmutableSet.of(actionId2);
        executedActionsChangeWindowDao.getActionsByLivenessState(liveState, expectedActionIds,
                actionList::add);
        assertEquals(1, actionList.size());
        actualActionIds = actionList.stream()
                .map(ExecutedActionsChangeWindow::getActionOid)
                .collect(Collectors.toSet());
        assertTrue(CollectionUtils.isEqualCollection(expectedActionIds, actualActionIds));

        // Verify that 2 'other' action ids that don't exist, we get empty results back.
        actionList.clear();
        expectedActionIds = ImmutableSet.of(70001L, 70002L);
        executedActionsChangeWindowDao.getActionsByLivenessState(liveState, expectedActionIds,
                actionList::add);
        assertTrue(actionList.isEmpty());

        // Now set end time for the 2 actions, thus making them no longer live. Verify results.
        executedActionsChangeWindowDao.updateActionLivenessInfo(endTimingInfo);
        actionList.clear();
        executedActionsChangeWindowDao.getActionsByLivenessState(liveState, actionList::add);
        assertTrue(actionList.isEmpty());
    }

    /**
     * Inserts a set of initial action records into the change window table, asserts to verify
     * the expected record counts before and after insertion.
     *
     * @return List of inserted records.
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    @Nonnull
    private List<ExecutedActionsChangeWindowRecord> insertInitialActionRecords()
            throws ActionStoreOperationException {
        assertTrue(getAllRecords().isEmpty());
        for (Pair<Long, Long> p : actionIdEntityIdList) {
            long entityOid = p.getRight();
            executedActionsChangeWindowDao.saveExecutedAction(p.getLeft(), entityOid);
        }
        List<ExecutedActionsChangeWindowRecord> records = getAllRecords();
        assertEquals(3, records.size());
        return records;
    }

    /**
     * Gets all current records from the change window table.
     *
     * @return List of all records.
     */
    private List<ExecutedActionsChangeWindowRecord> getAllRecords() {
        return dsl.selectFrom(EXECUTED_ACTIONS_CHANGE_WINDOW).fetch();
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
                .setStartTime(TimeUtil.localTimeToMillis(startTime, clock))
                .setActionOid(actionOid)
                .setLivenessState(LivenessState.NEW)
                .build();
    }
}
