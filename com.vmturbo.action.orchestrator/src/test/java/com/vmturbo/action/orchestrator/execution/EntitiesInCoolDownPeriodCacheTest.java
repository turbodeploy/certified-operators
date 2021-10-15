package com.vmturbo.action.orchestrator.execution;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.action.orchestrator.execution.affected.entities.EntitiesInCoolDownPeriodCache;
import com.vmturbo.action.orchestrator.execution.affected.entities.EntityActionInfoState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Verifies the cache remembers entities affected by actions and old entries are expired.
 */
@RunWith(MockitoJUnitRunner.class)
public class EntitiesInCoolDownPeriodCacheTest {

    private static final long ACTION_ID = 1L;
    private static final long ENTITY_ID = 100L;
    private static final long START_TIME = 1000L;
    private static final long SHORT_TIMEOUT = 1000L;
    private static final long LONG_TIMEOUT = 2000L;

    private static final long EXPIRED_SUCCEEDED_ACTION = 1L;
    private static final long EXPIRED_SUCCEEDED_ENTITY = 100L;
    private static final long ACTIVE_SUCCEEDED_ACTION = 2L;
    private static final long ACTIVE_SUCCEEDED_ENTITY = 200L;
    private static final long EXPIRED_IN_PROGRESS_ACTION = 3L;
    private static final long EXPIRED_IN_PROGRESS_ENTITY = 300L;
    private static final long ACTIVE_IN_PROGRESS_ACTION = 4L;
    private static final long ACTIVE_IN_PROGRESS_ENTITY = 400L;
    private static final long ACTIVE_FAILED_ACTION = 5L;
    private static final long ACTIVE_FAILED_ENTITY = 500L;

    @Mock
    private Clock clock;

    private EntitiesInCoolDownPeriodCache entitiesInCoolDownPeriodCache;

    /**
     * Sets up a EntitiesInCoolDownPeriodCache with mocks, shared by the tests that can.
     */
    @Before
    public void init() {
        when(clock.millis()).thenReturn(START_TIME);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(START_TIME));
        when(clock.getZone()).thenReturn(ZoneId.systemDefault());
        entitiesInCoolDownPeriodCache = new EntitiesInCoolDownPeriodCache(clock, 1440);
    }

    /**
     * Inserting an entity action record with no changes in time should return it.
     */
    @Test
    public void testInsert() {
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTION_ID,
                ActionTypeCase.MOVE,
                EntityActionInfoState.IN_PROGRESS,
                ImmutableSet.of(ENTITY_ID));
        Set<Long> actual = entitiesInCoolDownPeriodCache.getAffectedEntitiesByActionType(
                ImmutableSet.of(ActionTypeCase.MOVE),
                SHORT_TIMEOUT,
                SHORT_TIMEOUT);
        assertEquals(ImmutableSet.of(ENTITY_ID), actual);
    }

    /**
     * Inserting a action type and asking for a different type should not an empty set, without an exception.
     */
    @Test
    public void testUnrelatedType() {
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTION_ID,
                ActionTypeCase.MOVE,
                EntityActionInfoState.IN_PROGRESS,
                ImmutableSet.of(ENTITY_ID));
        Set<Long> actual = entitiesInCoolDownPeriodCache.getAffectedEntitiesByActionType(
                ImmutableSet.of(ActionTypeCase.RESIZE),
                SHORT_TIMEOUT,
                SHORT_TIMEOUT);
        assertEquals(Collections.emptySet(), actual);
    }

    /**
     * Sending the same actionId should keep everything as it was before, except update the datetime and state.
     */
    @Test
    public void testUpdate() {
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTION_ID,
                ActionTypeCase.MOVE,
                EntityActionInfoState.IN_PROGRESS,
                ImmutableSet.of(ENTITY_ID));
        when(clock.millis()).thenReturn(START_TIME + LONG_TIMEOUT);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(START_TIME + LONG_TIMEOUT));
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTION_ID,
                ActionTypeCase.RESIZE,
                EntityActionInfoState.SUCCEEDED,
                ImmutableSet.of(ACTION_ID, 200L));

        Set<Long> actual = entitiesInCoolDownPeriodCache.getAffectedEntitiesByActionType(
                ImmutableSet.of(ActionTypeCase.MOVE),
                SHORT_TIMEOUT,
                // state should be updated to SUCCEEDED so this cooldown should be used
                LONG_TIMEOUT);
        // There should be only one entity, since the list of entity ids should not change.
        assertEquals(ImmutableSet.of(ENTITY_ID), actual);
    }

    /**
     * Expired entries should not be returned. An entry can expire for three reasons:
     * <ol>
     *     <li>The IN_PROGRESS action has not updated for inProgressActionCoolDownMsec</li>
     *     <li>The SUCCEEDED action has not updated for succeededActionsCoolDownMsec</li>
     *     <li>The action has FAILED.</li>
     * </ol>
     */
    @Test
    public void testCacheCleanup() {
        entitiesInCoolDownPeriodCache.insertOrUpdate(EXPIRED_SUCCEEDED_ACTION,
                ActionTypeCase.MOVE,
                EntityActionInfoState.SUCCEEDED,
                ImmutableSet.of(EXPIRED_SUCCEEDED_ENTITY));
        entitiesInCoolDownPeriodCache.insertOrUpdate(EXPIRED_IN_PROGRESS_ACTION,
                ActionTypeCase.MOVE,
                EntityActionInfoState.IN_PROGRESS,
                ImmutableSet.of(EXPIRED_IN_PROGRESS_ENTITY));
        when(clock.millis()).thenReturn(START_TIME + SHORT_TIMEOUT);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(START_TIME + SHORT_TIMEOUT));
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTIVE_SUCCEEDED_ACTION,
                ActionTypeCase.MOVE,
                EntityActionInfoState.SUCCEEDED,
                ImmutableSet.of(ACTIVE_SUCCEEDED_ENTITY));
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTIVE_IN_PROGRESS_ACTION,
                ActionTypeCase.MOVE,
                EntityActionInfoState.IN_PROGRESS,
                ImmutableSet.of(ACTIVE_IN_PROGRESS_ENTITY));
        entitiesInCoolDownPeriodCache.insertOrUpdate(ACTIVE_FAILED_ACTION,
                ActionTypeCase.MOVE,
                EntityActionInfoState.FAILED,
                ImmutableSet.of(ACTIVE_FAILED_ENTITY));

        when(clock.millis()).thenReturn(START_TIME + LONG_TIMEOUT);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(START_TIME + LONG_TIMEOUT));

        Set<Long> actual = entitiesInCoolDownPeriodCache.getAffectedEntitiesByActionType(
                ImmutableSet.of(ActionTypeCase.MOVE),
                SHORT_TIMEOUT,
                SHORT_TIMEOUT);
        assertEquals(ImmutableSet.of(ACTIVE_SUCCEEDED_ENTITY, ACTIVE_IN_PROGRESS_ENTITY), actual);
    }

}