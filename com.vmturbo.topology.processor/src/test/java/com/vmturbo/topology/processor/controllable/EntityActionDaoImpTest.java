package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ActionRecordNotFoundException;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.enums.EntityActionStatus;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;

public class EntityActionDaoImpTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(TopologyProcessor.TOPOLOGY_PROCESSOR);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DSLContext dsl = dbConfig.getDslContext();

    private EntityActionDaoImp controllableDaoImp = new EntityActionDaoImp(dsl, SUCCEED_EXPIRED_SECONDS,
                                                IN_PROGRESS_EXPIRED_SECONDS, ACTIVATE_SUCCEED_SECONDS,
                                                SCALE_SUCCEED_SECONDS, RESIZE_SUCCEED_SECONDS);

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
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.in_progress)
                .where(ENTITY_ACTION.ENTITY_ID.eq(1L))
                .execute();
        // update entity 2 status to succeed and change its update time to expired time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, expiredTimeSucceed)
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.succeed)
                .where(ENTITY_ACTION.ENTITY_ID.eq(2L))
                .execute();
        // update entity 3 status to succeed and change its update time to current time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime)
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.succeed)
                .where(ENTITY_ACTION.ENTITY_ID.eq(3L))
                .execute();
        // update entity 4 status to failed and change its update time to current time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime)
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.failed)
                .where(ENTITY_ACTION.ENTITY_ID.eq(4L))
                .execute();
        // update entity 5 status to queued and change its update time to current time.
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime)
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.queued)
                .where(ENTITY_ACTION.ENTITY_ID.eq(5L))
                .execute();
        final Set<Long> results = controllableDaoImp.getNonControllableEntityIds();
        assertEquals(2L, results.size());
        assertTrue(results.contains(3L));
    }

    @Test
    public void testGetNonSuspendableEntityIdsAfterActionSucceed() {
        controllableDaoImp.insertAction(100l,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Arrays.asList(200l)));
        controllableDaoImp.insertAction(101l,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Arrays.asList(201l)));
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        // update the status of the activate action for entity with id 100l to succeed and make sure
        // the timstamp is within the period of ACTIVATE_SUCCEED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(ACTIVATE_SUCCEED_SECONDS / 2))
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.succeed)
                .where(ENTITY_ACTION.ACTION_ID.eq(100L))
                .execute();
        // update the status of the activate action for entity with id 101l to succeed and make sure
        // the timstamp is beyond the period of ACTIVATE_SUCCEED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(ACTIVATE_SUCCEED_SECONDS * 2))
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.succeed)
                .where(ENTITY_ACTION.ACTION_ID.eq(101L))
                .execute();
        final Set<Long> results = controllableDaoImp.getNonSuspendableEntityIds();
        // only entity 100l will be considered as non-suspendable
        assertTrue(results.size() == 1);
        assertTrue(results.contains(200l));
    }

    @Test
    public void testGetNonSuspendableEntityIdsAfterRecordExpired() {
        controllableDaoImp.insertAction(100l,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Arrays.asList(200l)));
        controllableDaoImp.insertAction(101l,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Arrays.asList(201l)));
        controllableDaoImp.insertAction(102l,
                ActionItemDTO.ActionType.START,
                new HashSet<>(Arrays.asList(202l)));
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        // update the status of the activate action for entity with id 100l to in_progress and make sure
        // the timstamp is beyond the period of IN_PROGRESS_EXPIRED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS * 2))
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.in_progress)
                .where(ENTITY_ACTION.ACTION_ID.eq(100L))
                .execute();
        // update the status of the activate action for entity with id 101l to queued and make sure
        // the timstamp is beyond the period of IN_PROGRESS_EXPIRED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS * 3))
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.queued)
                .where(ENTITY_ACTION.ACTION_ID.eq(101L))
                .execute();
        // update the status of the activate action for entity with id 102l to in_progress and make sure
        // the timstamp is within the period of IN_PROGRESS_EXPIRED_SECONDS
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(IN_PROGRESS_EXPIRED_SECONDS / 2))
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.in_progress)
                .where(ENTITY_ACTION.ACTION_ID.eq(102L))
                .execute();
        final Set<Long> results = controllableDaoImp.getNonSuspendableEntityIds();
        // only entity 100l will be considered as non-suspendable
        assertTrue(results.size() == 1);
        assertTrue(results.contains(202l));
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
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.queued)
                .where(ENTITY_ACTION.ACTION_ID.eq(SCALE_ACTION_ID))
                .execute();

        assertTrue(controllableDaoImp.ineligibleForScaleEntityIds().isEmpty());

        // Test SUCCEEDED Expired
        dsl.update(ENTITY_ACTION)
                .set(ENTITY_ACTION.UPDATE_TIME, currentTime.minusSeconds(SCALE_SUCCEED_SECONDS + 1))
                .set(ENTITY_ACTION.STATUS, EntityActionStatus.succeed)
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

        // Update state of RESIZE action
        controllableDaoImp.updateActionState(RESIZE_ACTION_ID, ActionState.FAILED);
        assertTrue(controllableDaoImp.ineligibleForResizeDownEntityIds().isEmpty());
    }

    private void insertActions() {
        controllableDaoImp.insertAction(MOVE_ACTION_ID, ActionItemDTO.ActionType.MOVE, Sets.newHashSet(MOVE_ENTITY_ID));
        controllableDaoImp.insertAction(RESIZE_ACTION_ID, ActionType.RIGHT_SIZE, Sets.newHashSet(RESIZE_ENTITY_ID));
        controllableDaoImp.insertAction(SCALE_ACTION_ID, ActionType.SCALE, Sets.newHashSet(SCALE_ENTITY_ID));
    }

}
