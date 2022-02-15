package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;
import static org.jooq.impl.DSL.inline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.topology.processor.TestTopologyProcessorDbEndpointConfig;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ActionRecordNotFoundException;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.enums.EntityActionStatus;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;

@RunWith(Parameterized.class)
public class EntityActionDaoImpTest extends MultiDbTestBase {

    /**
     * Provide parameters for rests.
     *
     * @return parameter values
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public EntityActionDaoImpTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(TopologyProcessor.TOPOLOGY_PROCESSOR, configurableDbDialect, dialect,
                "topology-processor",
                TestTopologyProcessorDbEndpointConfig::tpEndpoint);
        this.dsl = super.getDslContext();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EntityActionDaoImp controllableDaoImp;

    private static final int IN_PROGRESS_EXPIRED_SECONDS = 3000;

    private static final int SUCCEED_EXPIRED_SECONDS = 1000;

    private static final int ACTIVATE_SUCCEED_SECONDS = 3600;

    private static final int RESIZE_SUCCEED_SECONDS = 1000;

    private static final int SCALE_SUCCEED_SECONDS = 2000;

    final long actionId = 123L;

    final Set<Long> entityIds = Sets.newHashSet(1L, 2L);

    private static final long MOVE_ACTION_ID = 20L;

    private static final long RESIZE_ACTION_ID = 30L;

    private static final long SCALE_ACTION_ID = 40L;

    private static final long MOVE_ENTITY_ID = 200L;

    private static final long RESIZE_ENTITY_ID = 300L;

    private static final long SCALE_ENTITY_ID = 400L;

    private static final long COMMON_ENTITY_ID = 1;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        this.controllableDaoImp = new EntityActionDaoImp(dsl, SUCCEED_EXPIRED_SECONDS,
                IN_PROGRESS_EXPIRED_SECONDS, ACTIVATE_SUCCEED_SECONDS,
                SCALE_SUCCEED_SECONDS, RESIZE_SUCCEED_SECONDS);
    }
    
    @Test
    public void testInsertQueuedControllable() {
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        final Result<EntityActionRecord> records = dsl.selectFrom(ENTITY_ACTION).fetch();
        final Set<Long> resultEntityIds = records.stream()
                .map(EntityActionRecord::getEntityId)
                .collect(Collectors.toSet());
        assertEquals(2, resultEntityIds.size());
        assertEquals(entityIds, resultEntityIds);
        assertTrue(records.stream()
                .allMatch(record -> record.getStatus().equals(EntityActionStatus.queued)));
    }

    /**
     * Tests that a call to the {@link EntityActionDaoImp#deleteMoveActions(List)} method removes
     * move entries pertaining to the supplied entity from the database, but doesn't effect other
     * action types nor other entities.
     */
    @Test
    public void testDeleteMoveActions() {
        // Populate database.
        controllableDaoImp.insertAction(MOVE_ACTION_ID, ActionType.MOVE,
                                        Sets.newHashSet(10L, COMMON_ENTITY_ID, 2L));
        controllableDaoImp.insertAction(MOVE_ACTION_ID + 1, ActionType.MOVE_TOGETHER,
                                        Sets.newHashSet(20L, COMMON_ENTITY_ID, 3L));
        controllableDaoImp.insertAction(RESIZE_ACTION_ID, ActionType.RIGHT_SIZE,
                                        Sets.newHashSet(COMMON_ENTITY_ID));
        controllableDaoImp.insertAction(SCALE_ACTION_ID, ActionType.SCALE,
                                        Sets.newHashSet(SCALE_ENTITY_ID));

        // Call method-under-test.
        controllableDaoImp.deleteMoveActions(Collections.singletonList(1L));

        // Assert post-execution state of the database.
        final List<EntityActionRecord> records = dsl.selectFrom(ENTITY_ACTION)
                                        .orderBy(ENTITY_ACTION.ACTION_ID, ENTITY_ACTION.ENTITY_ID)
                                        .fetch();
        assertEquals(6, records.size());
        assertEquals(MOVE_ACTION_ID, (long)records.get(0).getActionId());
        assertEquals(2L, (long)records.get(0).getEntityId());
        assertEquals(MOVE_ACTION_ID, (long)records.get(1).getActionId());
        assertEquals(10L, (long)records.get(1).getEntityId());
        assertEquals(MOVE_ACTION_ID + 1, (long)records.get(2).getActionId());
        assertEquals(3L, (long)records.get(2).getEntityId());
        assertEquals(MOVE_ACTION_ID + 1, (long)records.get(3).getActionId());
        assertEquals(20L, (long)records.get(3).getEntityId());
        assertEquals(RESIZE_ACTION_ID, (long)records.get(4).getActionId());
        assertEquals(COMMON_ENTITY_ID, (long)records.get(4).getEntityId());
        assertEquals(SCALE_ACTION_ID, (long)records.get(5).getActionId());
        assertEquals(SCALE_ENTITY_ID, (long)records.get(5).getEntityId());
    }


    @Test
    public void testUpdateInProgressControllable() throws ActionRecordNotFoundException {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L);
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        controllableDaoImp.insertAction(newActionId, ActionItemDTO.ActionType.MOVE, newEntityIds);
        controllableDaoImp.updateActionState(actionId, ActionState.IN_PROGRESS);
        final List<EntityActionRecord> records = dsl.selectFrom(ENTITY_ACTION).fetch().stream()
                .filter(record -> record.getActionId() == actionId)
                .collect(Collectors.toList());
        assertEquals(2, records.size());
        assertTrue(records.stream()
                .allMatch(record -> record.getStatus().equals(EntityActionStatus.in_progress)));
    }

    @Test
    public void testUpdateInProgressControllableNotFoundException() throws
            ActionRecordNotFoundException {
        final long newActionId = 456L;
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        expectedException.expect(ActionRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.IN_PROGRESS);
    }

    @Test
    public void testUpdateSucceedControllable() throws ActionRecordNotFoundException {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L);
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        controllableDaoImp.insertAction(newActionId, ActionItemDTO.ActionType.MOVE, newEntityIds);
        controllableDaoImp.updateActionState(actionId, ActionState.SUCCEEDED);
        final List<EntityActionRecord> records = dsl.selectFrom(ENTITY_ACTION).fetch().stream()
                .filter(record -> record.getActionId() == actionId)
                .collect(Collectors.toList());
        assertEquals(2, records.size());
        assertTrue(records.stream()
                .allMatch(record -> record.getStatus().equals(EntityActionStatus.succeed)));
    }

    @Test
    public void testUpdatedSucceedControllableNotFoundException() throws
            ActionRecordNotFoundException {
        final long newActionId = 456L;
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        expectedException.expect(ActionRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.SUCCEEDED);
    }

    @Test
    public void testUpdateFailedControllable() throws ActionRecordNotFoundException {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L);
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        controllableDaoImp.insertAction(newActionId, ActionItemDTO.ActionType.MOVE, newEntityIds);
        controllableDaoImp.updateActionState(actionId, ActionState.FAILED);
        final List<EntityActionRecord> records = dsl.selectFrom(ENTITY_ACTION).fetch().stream()
                .filter(record -> record.getActionId() == actionId)
                .collect(Collectors.toList());
        assertEquals(2, records.size());
        assertTrue(records.stream()
                .allMatch(record -> record.getStatus().equals(EntityActionStatus.failed)));
    }

    @Test
    public void testUpdatedFailedControllableNotFoundException() throws
            ActionRecordNotFoundException {
        final long newActionId = 456L;
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        expectedException.expect(ActionRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.FAILED);
    }

    @Test
    public void testGetEntityIdsAfterDeleteExpired() {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L, 4L, 5L);
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        controllableDaoImp.insertAction(newActionId, ActionItemDTO.ActionType.MOVE, newEntityIds);
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        final LocalDateTime expiredTimeInProgress = currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS * 2);
        final LocalDateTime expiredTimeSucceed = currentTime.minusSeconds(SUCCEED_EXPIRED_SECONDS * 2);
        // update entity 1 status to in progress and change its update time to expired time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, expiredTimeInProgress)
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.in_progress))
                .where(ENTITY_ACTION.ENTITY_ID.eq(1L))
                .execute();
        // update entity 2 status to succeed and change its update time to expired time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, expiredTimeSucceed)
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.succeed))
                .where(ENTITY_ACTION.ENTITY_ID.eq(2L))
                .execute();
        // update entity 3 status to succeed and change its update time to current time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime)
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.succeed))
                .where(ENTITY_ACTION.ENTITY_ID.eq(3L))
                .execute();
        // update entity 4 status to failed and change its update time to current time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime)
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.failed))
                .where(ENTITY_ACTION.ENTITY_ID.eq(4L))
                .execute();
        // update entity 5 status to queued and change its update time to current time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime)
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.queued))
                .where(ENTITY_ACTION.ENTITY_ID.eq(5L))
                .execute();
        final Set<Long> results = controllableDaoImp.getNonControllableEntityIds();
        assertEquals(2L, results.size());
        assertTrue(results.contains(3L));
    }

    @Test
    public void testGetNonSuspendableEntityIdsAfterActionSucceed() {
        controllableDaoImp.insertAction(100L,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Collections.singletonList(200L)));
        controllableDaoImp.insertAction(101L,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Collections.singletonList(201L)));
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        // update the status of the activate action for entity with id 100l to succeed and make sure
        // the timestamp is within the period of ACTIVATE_SUCCEED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(ACTIVATE_SUCCEED_SECONDS / 2))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.succeed))
                .where(ENTITY_ACTION.ACTION_ID.eq(100L))
                .execute();
        // update the status of the activate action for entity with id 101l to succeed and make sure
        // the timestamp is beyond the period of ACTIVATE_SUCCEED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(ACTIVATE_SUCCEED_SECONDS * 2))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.succeed))
                .where(ENTITY_ACTION.ACTION_ID.eq(101L))
                .execute();
        final Set<Long> results = controllableDaoImp.getNonSuspendableEntityIds();
        // only entity 100l will be considered as non-suspendable
        assertEquals(1, results.size());
        assertTrue(results.contains(200L));
    }

    @Test
    public void testGetNonSuspendableEntityIdsAfterRecordExpired() {
        controllableDaoImp.insertAction(100L,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Collections.singletonList(200L)));
        controllableDaoImp.insertAction(101L,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Collections.singletonList(201L)));
        controllableDaoImp.insertAction(102L,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Collections.singletonList(202L)));
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        // update the status of the activate action for entity with id 100l to in_progress and make sure
        // the timestamp is beyond the period of IN_PROGRESS_EXPIRED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS * 2))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.in_progress))
                .where(ENTITY_ACTION.ACTION_ID.eq(100L))
                .execute();
        // update the status of the activate action for entity with id 101l to queued and make sure
        // the timestamp is beyond the period of IN_PROGRESS_EXPIRED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS * 3))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.queued))
                .where(ENTITY_ACTION.ACTION_ID.eq(101L))
                .execute();
        // update the status of the activate action for entity with id 102l to in_progress and make sure
        // the timestamp is within the period of IN_PROGRESS_EXPIRED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS / 2))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.in_progress))
                .where(ENTITY_ACTION.ACTION_ID.eq(102L))
                .execute();
        final Set<Long> results = controllableDaoImp.getNonSuspendableEntityIds();
        // only entity 100l will be considered as non-suspendable
        assertEquals(1, results.size());
        assertTrue(results.contains(202L));
    }

    /**
     * Check resize (scale) and resize down eligibility for expired action Ids.
     */
    @Test
    public void testIneligibleForResizeAndResizeDownEntityIdsExpired() {
        insertActions();
        assertEquals(1, controllableDaoImp.ineligibleForScaleEntityIds().size());
        assertEquals(1, controllableDaoImp.ineligibleForResizeDownEntityIds().size());
        assertTrue(controllableDaoImp.ineligibleForScaleEntityIds().contains(SCALE_ENTITY_ID));
        assertTrue(controllableDaoImp.ineligibleForResizeDownEntityIds().contains(RESIZE_ENTITY_ID));

        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);

        // Test InProgress Expired
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS + 1))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.queued))
                .where(ENTITY_ACTION.ACTION_ID.eq(SCALE_ACTION_ID))
                .execute();

        assertTrue(controllableDaoImp.ineligibleForScaleEntityIds().isEmpty());

        // Test SUCCEEDED Expired
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(SCALE_SUCCEED_SECONDS + 1))
                .set(ENTITY_ACTION.STATUS, inline(EntityActionStatus.succeed))
                .where(ENTITY_ACTION.ACTION_ID.eq(RESIZE_ACTION_ID))
                .execute();

        assertTrue(controllableDaoImp.ineligibleForResizeDownEntityIds().isEmpty());
    }

    /**
     * Check resize (scale) and resize down eligibility for FAILED action state.
     * @throws ActionRecordNotFoundException if action record not found.
     */
    @Test
    public void testIneligibleForResizeAndResizeDownEntityIds() throws ActionRecordNotFoundException {
        insertActions();

        assertEquals(1, controllableDaoImp.ineligibleForScaleEntityIds().size());
        assertEquals(1, controllableDaoImp.ineligibleForResizeDownEntityIds().size());
        assertTrue(controllableDaoImp.ineligibleForScaleEntityIds().contains(SCALE_ENTITY_ID));
        assertTrue(controllableDaoImp.ineligibleForResizeDownEntityIds().contains(RESIZE_ENTITY_ID));

        // Update state of one of the actions to 'FAILED' -> DB should clear this action
        controllableDaoImp.updateActionState(SCALE_ACTION_ID, ActionState.FAILED);

        // Only scale action is affected
        assertTrue(controllableDaoImp.ineligibleForScaleEntityIds().isEmpty());
        assertTrue(controllableDaoImp.ineligibleForResizeDownEntityIds().contains(RESIZE_ENTITY_ID));
        assertEquals(0, dsl.selectFrom(ENTITY_ACTION)
            .where(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.failed)))
            .fetch().size());

        // Update state of RESIZE action
        controllableDaoImp.updateActionState(RESIZE_ACTION_ID, ActionState.FAILED);
        assertTrue(controllableDaoImp.ineligibleForResizeDownEntityIds().isEmpty());
        assertEquals(0, dsl.selectFrom(ENTITY_ACTION)
            .where(ENTITY_ACTION.STATUS.eq(inline(EntityActionStatus.failed)))
            .fetch().size());
    }

    private void insertActions() {
        controllableDaoImp.insertAction(MOVE_ACTION_ID, ActionItemDTO.ActionType.MOVE, Sets.newHashSet(MOVE_ENTITY_ID));
        controllableDaoImp.insertAction(RESIZE_ACTION_ID, ActionType.RIGHT_SIZE, Sets.newHashSet(RESIZE_ENTITY_ID));
        controllableDaoImp.insertAction(SCALE_ACTION_ID, ActionType.SCALE, Sets.newHashSet(SCALE_ENTITY_ID));
    }
}
