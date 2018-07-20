package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Result;
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

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
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
                IN_PROGRESS_EXPIRED_SECONDS);
    }

    @Test
    public void testInsertQueuedControllable() {
        controllableDaoImp.insertAction(actionId, entityIds);
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
        controllableDaoImp.insertAction(actionId, entityIds);
        controllableDaoImp.insertAction(newActionId, newEntityIds);
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
        controllableDaoImp.insertAction(actionId, entityIds);
        expectedException.expect(ControllableRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.IN_PROGRESS);
    }

    @Test
    public void testUpdateSucceedControllable() throws ControllableRecordNotFoundException {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L);
        controllableDaoImp.insertAction(actionId, entityIds);
        controllableDaoImp.insertAction(newActionId, newEntityIds);
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
        controllableDaoImp.insertAction(actionId, entityIds);
        expectedException.expect(ControllableRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.SUCCEEDED);
    }

    @Test
    public void testUpdateFailedControllable() throws ControllableRecordNotFoundException {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L);
        controllableDaoImp.insertAction(actionId, entityIds);
        controllableDaoImp.insertAction(newActionId, newEntityIds);
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
        controllableDaoImp.insertAction(actionId, entityIds);
        expectedException.expect(ControllableRecordNotFoundException.class);
        controllableDaoImp.updateActionState(newActionId, ActionState.FAILED);
    }

    @Test
    public void testGetEntityIdsAfterDeleteExpired() {
        final long newActionId = 456L;
        final Set<Long> newEntityIds = Sets.newHashSet(3L, 4L);
        controllableDaoImp.insertAction(actionId, entityIds);
        controllableDaoImp.insertAction(newActionId, newEntityIds);
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
        final Set<Long> results = controllableDaoImp.getNonControllableEntityIds();
        assertEquals(1L, results.size());
        assertTrue(results.contains(3L));
    }
}
