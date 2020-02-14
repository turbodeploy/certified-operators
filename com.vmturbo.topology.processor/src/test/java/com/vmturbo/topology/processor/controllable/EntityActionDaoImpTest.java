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

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ControllableRecordNotFoundException;
import com.vmturbo.topology.processor.db.enums.EntityActionStatus;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=topology_processor"})
public class EntityActionDaoImpTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Flyway flyway;

    private EntityActionDaoImp controllableDaoImp;

    private final int IN_PROGRESS_EXPIRED_SECONDS = 3000;

    private final int SUCCEED_EXPIRED_SECONDS = 1000;

    private final int ACTIVATE_SUCCEED_SECONDS = 3600;

    final long actionId = 123L;

    final Set<Long> entityIds = Sets.newHashSet(1L, 2L);

    private DSLContext dsl;

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        controllableDaoImp = new EntityActionDaoImp(dsl, SUCCEED_EXPIRED_SECONDS,
                IN_PROGRESS_EXPIRED_SECONDS, ACTIVATE_SUCCEED_SECONDS);
    }

    @After
    public void teardown() {
        flyway.clean();
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

    @Test
    public void testUpdateInProgressControllable() throws ControllableRecordNotFoundException {
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
    public void testUpdateInProgressControllableNotFoundException() throws ControllableRecordNotFoundException {
        final long newActionId = 456L;
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        expectedException.expect(ControllableRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.IN_PROGRESS);
    }

    @Test
    public void testUpdateSucceedControllable() throws ControllableRecordNotFoundException {
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
    public void testUpdatedSucceedControllableNotFoundException() throws ControllableRecordNotFoundException {
        final long newActionId = 456L;
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        expectedException.expect(ControllableRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.SUCCEEDED);
    }

    @Test
    public void testUpdateFailedControllable() throws ControllableRecordNotFoundException {
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
    public void testUpdatedFailedControllableNotFoundException() throws ControllableRecordNotFoundException {
        final long newActionId = 456L;
        controllableDaoImp.insertAction(actionId, ActionItemDTO.ActionType.MOVE, entityIds);
        expectedException.expect(ControllableRecordNotFoundException.class);
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
}
