package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ExecutedActionsChangeWindow.EXECUTED_ACTIONS_CHANGE_WINDOW;
import static junit.framework.TestCase.assertFalse;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
        inserts.add(createRecord(100L, 1000L, dateMar15, LivenessState.SUPERSEDED));
        inserts.add(createRecord(101L, 1000L, dateMar16, LivenessState.LIVE));
        inserts.add(createRecord(102L, 1001L, dateMar17, LivenessState.LIVE));
        inserts.add(createRecord(103L, 1001L, dateMar14, LivenessState.SUPERSEDED));
        inserts.add(createRecord(104L, 1001L, dateMar17, LivenessState.NEW));
        dsl.batchInsert(inserts).execute();

        // Invoke the getExecutedActionsChangeWindowMap with entity OIDs of the two entities.
        Map<Long, List<ExecutedActionsChangeWindow>> result =
                executedActionsChangeWindowDao.getActionsByEntityOid(Arrays.asList(1000L, 1001L));

        // Verify the map returned has two entries.
        Assert.assertEquals(2, result.size());

        // Verify the records for entity 1000L.
        List<ExecutedActionsChangeWindow> changeWindows = result.get(1000L);
        List<ExecutedActionsChangeWindow> expectedChangeWindows = Arrays.asList(
                createExecutedActionsChangeWindow(1000L, 100L, dateMar15, LivenessState.SUPERSEDED),
                createExecutedActionsChangeWindow(1000L, 101L, dateMar16, LivenessState.LIVE));
        Assert.assertEquals(expectedChangeWindows, changeWindows);

        // Verify the records for entity 1001L.
        // The action with liveness state "NEW" will not be in the result.
        changeWindows = result.get(1001L);
        expectedChangeWindows = Arrays.asList(
                createExecutedActionsChangeWindow(1001L, 103L, dateMar14, LivenessState.SUPERSEDED),
                createExecutedActionsChangeWindow(1001L, 102L, dateMar17, LivenessState.LIVE));
        Assert.assertEquals(expectedChangeWindows.size(), changeWindows.size());
        Assert.assertTrue(expectedChangeWindows.containsAll(changeWindows));
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
        getAllActionsByLivenessState(liveState, Collections.emptySet(), actionList::add);
        assertTrue(actionList.isEmpty());

        // Now set start time for a couple of those records and thus make them live, because
        // we are not setting end time yet.
        executedActionsChangeWindowDao.updateActionLivenessInfo(startTimingInfo);
        actionList.clear();
        getAllActionsByLivenessState(liveState, Collections.emptySet(), actionList::add);
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
        getAllActionsByLivenessState(liveState, expectedActionIds, actionList::add);
        assertEquals(1, actionList.size());
        actualActionIds = actionList.stream()
                .map(ExecutedActionsChangeWindow::getActionOid)
                .collect(Collectors.toSet());
        assertTrue(CollectionUtils.isEqualCollection(expectedActionIds, actualActionIds));

        // Verify that 2 'other' action ids that don't exist, we get empty results back.
        actionList.clear();
        expectedActionIds = ImmutableSet.of(70001L, 70002L);
        getAllActionsByLivenessState(liveState, expectedActionIds, actionList::add);
        assertTrue(actionList.isEmpty());

        // Now set end time for the 2 actions, thus making them no longer live. Verify results.
        executedActionsChangeWindowDao.updateActionLivenessInfo(endTimingInfo);
        actionList.clear();
        getAllActionsByLivenessState(liveState, Collections.emptySet(), actionList::add);
        assertTrue(actionList.isEmpty());
    }

    /**
     * Adds records to change window dir to enable testing for paged results. Also verifies count.
     *
     * @param recordCount How many to insert.
     * @return initial action id that was used to insert records.
     * @throws ActionStoreOperationException Thrown on DB error.
     */
    private long addPagingRecords(long recordCount) throws ActionStoreOperationException {
        // Insert 20 records - with action ids: 1001 -> 1020.
        long initialId = 1001;
        for (long actionOid = initialId; actionOid < initialId + recordCount; actionOid++) {
            long entityOid = actionOid * 10;
            executedActionsChangeWindowDao.saveExecutedAction(actionOid, entityOid);
        }
        // Verify count 20.
        assertEquals(recordCount, getAllRecords().size());
        return initialId;
    }

    /**
     * Tests results paging with chunk size 7, with record count being 20.
     *
     * @throws ActionStoreOperationException Thrown on DB error.
     */
    @Test
    public void pagedResultsChunk7() throws ActionStoreOperationException {
        long recordCount = 20;
        long initialId = addPagingRecords(recordCount);

        final Set<LivenessState> states = ImmutableSet.of(LivenessState.NEW);
        final Set<Long> actionOids = Collections.emptySet();

        // Query paged results - with chunk size 7, no specific action ids in query.
        int chunkSize = 7;
        ExecutedActionsChangeWindowDao dao = new ExecutedActionsChangeWindowDaoImpl(dsl, clock,
                chunkSize);
        final List<ExecutedActionsChangeWindow> results = new ArrayList<>();
        int expectedResults = chunkSize;

        // 1st page (1001 -> 1007). Verify we got all requested results as per the chunk size.
        Optional<Long> nextCursor = dao.getActionsByLivenessState(states, actionOids, 0,
                results::add);
        long startId = initialId;
        long endId = startId + chunkSize - 1;

        assertTrue(nextCursor.isPresent());
        checkPageResults(false, nextCursor.get(), expectedResults, results, startId, endId);
        results.clear();

        // 2nd page (1008 -> 1014). Next cursor points to 1007 (the last oid we got from previous
        // page). We use that as the 'currentCursor' for the next page call, and we will get results
        // with start id of 1008 onwards.
        startId = nextCursor.get() + 1;
        endId = startId + chunkSize - 1;
        nextCursor = dao.getActionsByLivenessState(states, actionOids, nextCursor.get(), results::add);

        assertTrue(nextCursor.isPresent());
        checkPageResults(false, nextCursor.get(), expectedResults, results, startId, endId);
        results.clear();

        // 3rd page (1015 -> 1020), total 6 records.
        expectedResults = 6;
        startId = nextCursor.get() + 1;
        endId = startId + chunkSize - 1;
        nextCursor = dao.getActionsByLivenessState(states, actionOids, nextCursor.get(), results::add);

        // No more pages, so there should not be a next cursor value.
        assertFalse(nextCursor.isPresent());
        checkPageResults(true, null, expectedResults, results, startId, endId);
        results.clear();
    }

    /**
     * Fetches 20 results with a chunk size of 20, all in a single page.
     *
     * @throws ActionStoreOperationException Thrown on DB error.
     */
    @Test
    public void pagedResultsChunk20() throws ActionStoreOperationException {
        long recordCount = 20;
        long initialId = addPagingRecords(recordCount);

        final Set<LivenessState> states = ImmutableSet.of(LivenessState.NEW);
        final Set<Long> actionOids = Collections.emptySet();

        // Query paged results - with chunk size 20, no specific action ids in query.
        int chunkSize = 20;
        ExecutedActionsChangeWindowDao dao = new ExecutedActionsChangeWindowDaoImpl(dsl, clock,
                chunkSize);
        final List<ExecutedActionsChangeWindow> results = new ArrayList<>();

        // Query with a different chunk size: 20. First page has all 20 results.
        dao = new ExecutedActionsChangeWindowDaoImpl(dsl, clock, chunkSize);
        long startId = initialId;
        long endId = startId + chunkSize - 1;

        Optional<Long> nextCursor = dao.getActionsByLivenessState(states, actionOids, 0,
                results::add);
        assertTrue(nextCursor.isPresent());
        checkPageResults(false, nextCursor.get(), chunkSize, results, startId, endId);
        results.clear();

        // Do another page, as we don't know yet there are no more results, this should return 0.
        startId = nextCursor.get() + 1;
        endId = startId + chunkSize - 1;
        nextCursor = dao.getActionsByLivenessState(states, actionOids, nextCursor.get(),
                results::add);
        assertFalse(nextCursor.isPresent());
        assertTrue(results.isEmpty());
        checkPageResults(true, null, 0, results, startId, endId);
        results.clear();
    }

    /**
     * Fetches all 20 results in a single page (of chunk size 30).
     *
     * @throws ActionStoreOperationException Thrown on DB error.
     */
    @Test
    public void pagedResultsChunk30() throws ActionStoreOperationException {
        long recordCount = 20;
        long initialId = addPagingRecords(recordCount);

        final Set<LivenessState> states = ImmutableSet.of(LivenessState.NEW);
        final Set<Long> actionOids = Collections.emptySet();

        // Query with a larger chunk size (30), we should get all results in 1 shot.
        int chunkSize = 30;
        ExecutedActionsChangeWindowDao dao = new ExecutedActionsChangeWindowDaoImpl(dsl, clock,
                chunkSize);
        final List<ExecutedActionsChangeWindow> results = new ArrayList<>();
        long startId = initialId;
        long endId = startId + chunkSize - 1;

        Optional<Long> nextCursor = dao.getActionsByLivenessState(states, actionOids, 0,
                results::add);
        assertFalse(nextCursor.isPresent());
        checkPageResults(true, null, (int)recordCount, results, startId, endId);
        results.clear();
    }

    /**
     * Tests paged action results, but with optional action ids specified in query. Only records
     * matching those action oids are returned in pages.
     *
     * @throws ActionStoreOperationException Thrown on DB error.
     */
    @Test
    public void pagedResultsWithActionIds() throws ActionStoreOperationException {
        long recordCount = 20;
        long initialId = addPagingRecords(recordCount);

        final Set<LivenessState> states = ImmutableSet.of(LivenessState.NEW);
        // Query specific action oids only, even ones. 1002 -> 1020, total 10 records.
        final Set<Long> actionOids = getAllRecords().stream()
                .map(ExecutedActionsChangeWindowRecord::getActionOid)
                .filter(actionOid -> actionOid % 2 == 0)
                .collect(Collectors.toSet());

        // Query with a larger chunk size (7), we should get results in 2 pages, total of 10 records.
        // 1002, 1004, ... , 1012, 1014.
        int chunkSize = 7;
        ExecutedActionsChangeWindowDao dao = new ExecutedActionsChangeWindowDaoImpl(dsl, clock,
                chunkSize);
        final List<ExecutedActionsChangeWindow> results = new ArrayList<>();
        long startId = initialId + 1; // 1002
        long endId = initialId + 2 * chunkSize - 1; // 1014

        Optional<Long> nextCursor = dao.getActionsByLivenessState(states, actionOids, 0,
                results::add);
        assertTrue(nextCursor.isPresent());
        checkPageResults(false, nextCursor.get(), chunkSize, results, startId, endId);
        results.clear();

        // Do another page, we should get remaining 3 more records - 1016, 1018, 1020.
        startId = nextCursor.get() + 2; // 1016
        int expectedResults = 3;
        endId = startId + (expectedResults - 1) * 2; // 1020
        nextCursor = dao.getActionsByLivenessState(states, actionOids, nextCursor.get(),
                results::add);
        assertFalse(nextCursor.isPresent());
        checkPageResults(true, null, expectedResults, results, startId, endId);
        results.clear();
    }

    /**
     * Util method to verify page results.
     *
     * @param isLastPage True if last page.
     * @param nextCursor Next cursor value.
     * @param expectedResults How many records expected.
     * @param results DB records.
     * @param startId Start action id.
     * @param endId End action id.
     */
    private void checkPageResults(boolean isLastPage, @Nullable final Long nextCursor,
            int expectedResults, @Nonnull final List<ExecutedActionsChangeWindow> results,
            long startId, long endId) {
        assertEquals(expectedResults, results.size());
        if (isLastPage) {
            // We don't expect a next cursor if this is the last page.
            assertTrue(Objects.isNull(nextCursor));
        } else {
            // If not last page, verify that we got at least one record.
            assertNotNull(nextCursor);
            assertTrue(nextCursor > startId);
            assertEquals(endId, (long)nextCursor);
            if (expectedResults > 0) {
                assertEquals(endId, results.get(results.size() - 1).getActionOid());
            }
        }
        if (expectedResults > 0) {
            assertEquals(startId, results.get(0).getActionOid());
        }
    }

    /**
     * Gets all results by paging them.
     *
     * @param states States to query for.
     * @param actionIds Action ids to query for.
     * @param consumer Receiver of results.
     * @throws ActionStoreOperationException Thrown on DB error.
     */
    private void getAllActionsByLivenessState(@Nonnull final Set<LivenessState> states,
            @Nonnull final Set<Long> actionIds,
            @Nonnull final Consumer<ExecutedActionsChangeWindow> consumer)
            throws ActionStoreOperationException {
        Optional<Long> nextCursor;
        long currentCursor = 0;
        do {
            if (actionIds.isEmpty()) {
                nextCursor = executedActionsChangeWindowDao.getActionsByLivenessState(states,
                        currentCursor, consumer);
            } else {
                nextCursor = executedActionsChangeWindowDao.getActionsByLivenessState(states,
                        actionIds, currentCursor, consumer);
            }
            if (!nextCursor.isPresent()) {
                // End of results.
                break;
            }
            currentCursor = nextCursor.get();
        } while (true);
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
            LocalDateTime startTime, LivenessState state) {
        ExecutedActionsChangeWindowRecord record = new ExecutedActionsChangeWindowRecord();
        record.setActionOid(actionOid);
        record.setEntityOid(entityOid);
        record.setStartTime(startTime);
        record.setLivenessState(state.getNumber());
        return record;
    }

    private ExecutedActionsChangeWindow createExecutedActionsChangeWindow(long entityOid,
            long actionOid, LocalDateTime startTime, LivenessState state) {
        return ExecutedActionsChangeWindow.newBuilder()
                .setEntityOid(entityOid)
                .setStartTime(TimeUtil.localTimeToMillis(startTime, clock))
                .setActionOid(actionOid)
                .setLivenessState(state)
                .build();
    }
}
