package com.vmturbo.action.orchestrator.action;

import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Table.Cell;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.AuditedActionsManager.AuditedActionsUpdate;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * AuditedActionsManager should remember which audited actions were sent, which have a cleared
 * timestamp, and which have expired. AuditedActionsManager should persist these to
 * AuditedActionsDAO without blocking the calling thread.
 */
public class AuditedActionsManagerTest {

    private static final long RECOVERY_INTERNAL_MSEC = TimeUnit.MINUTES.toMillis(10L);

    @Mock
    private AuditActionsPersistenceManager auditActionsPersistenceManager;

    private ScheduledExecutorService scheduledExecutorService;

    private AuditedActionsManager auditedActionsManager;

    @Captor
    private ArgumentCaptor<Collection<AuditedActionInfo>> auditedActionsCaptor;

    /**
     * Sets up a AuditedActionsManager with mocks, shared by the tests that can.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        auditedActionsManager = new AuditedActionsManager(
            auditActionsPersistenceManager,
            scheduledExecutorService,
            RECOVERY_INTERNAL_MSEC
        );
    }

    /**
     * A cache with nothing in it should always return false. An empty cache should not fail.
     */
    @Test
    public void testNothingInCache() {
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 1L));
    }

    /**
     * AuditedActionsManager should load the records already in the database when initialized.
     */
    @Test
    public void testLoadPersisted() {
        when(auditActionsPersistenceManager.getActions()).thenReturn(Arrays.asList(
            new AuditedActionInfo(1L, 1L, null),
            new AuditedActionInfo(1L, 2L, 1000L)
        ));
        auditedActionsManager = new AuditedActionsManager(
            auditActionsPersistenceManager,
            scheduledExecutorService,
            RECOVERY_INTERNAL_MSEC
        );

        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 1L));
        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 2L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 3L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(2L, 1L));

        final Collection<Cell<Long, Long, Optional<Long>>> actualCells =
                auditedActionsManager.getAlreadySentActions();
        Assert.assertEquals(2, actualCells.size());
        final Map<Pair<Long, Long>, Optional<Long>> actualMap = actualCells.stream()
                .collect(Collectors.toMap(cell -> new Pair<>(cell.getRowKey(), cell.getColumnKey()),
                        cell -> cell.getValue()));
        Assert.assertEquals(Optional.empty(), actualMap.get(new Pair<>(1L, 1L)));
        Assert.assertEquals(Optional.of(1000L), actualMap.get(new Pair<>(1L, 2L)));
    }

    /**
     * Adding to the cache should make it sent. Giving it cleared timestamp maintain it in the
     * cache. Removing it from the cache should should mark it as no longer sent.
     *
     * @throws ActionStoreOperationException shouldn't happen
     */
    @Test
    @Ignore
    public void testChangingCache() throws ActionStoreOperationException {
        final AuditedActionsUpdate update = new AuditedActionsUpdate();
        final AuditedActionInfo newAuditedAction = new AuditedActionInfo(1L, 1L, null);
        final AuditedActionInfo recentlyClearedAuditedAction = new AuditedActionInfo(1L, 2L, 1000L);
        update.addAuditedAction(newAuditedAction);
        update.addRecentlyClearedAction(recentlyClearedAuditedAction);
        auditedActionsManager.persistAuditedActionsUpdates(update);

        // added to the cache
        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 1L));
        // added to the cache with a timestamp is okay
        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 2L));
        // these two were never added to the cache
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 3L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(2L, 1L));
        final Collection<Cell<Long, Long, Optional<Long>>> actualCells =
                auditedActionsManager.getAlreadySentActions();
        Assert.assertEquals(2, actualCells.size());
        Map<Pair<Long, Long>, Optional<Long>> actualMap = toMap(actualCells);
        Assert.assertEquals(Optional.empty(), actualMap.get(new Pair<>(1L, 1L)));
        Assert.assertEquals(Optional.of(1000L), actualMap.get(new Pair<>(1L, 2L)));

        // now clearing an expired entry from the cache
        final AuditedActionsUpdate expiredUpdate = new AuditedActionsUpdate();
        final AuditedActionInfo expiredClearedAuditedAction = new AuditedActionInfo(1L, 2L, null);
        expiredUpdate.addExpiredClearedAction(expiredClearedAuditedAction);
        auditedActionsManager.persistAuditedActionsUpdates(expiredUpdate);

        // still in the cache because it wasn't removed
        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 1L));
        // removed from the cache
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 2L));
        // these two were never added to the cache
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 3L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(2L, 1L));
        final Collection<Cell<Long, Long, Optional<Long>>> afterExpiredCells =
                auditedActionsManager.getAlreadySentActions();
        Assert.assertEquals(1, afterExpiredCells.size());
        Map<Pair<Long, Long>, Optional<Long>> afterExpiredMap = toMap(afterExpiredCells);
        Assert.assertEquals(Optional.empty(), afterExpiredMap.get(new Pair<>(1L, 1L)));
        Assert.assertFalse(afterExpiredMap.containsKey(new Pair<>(1L, 2L)));

        // execute all sync up tasks in order to check that we persisted all audited action
        // updates in DB. Checks that in-memory bookkeeping cache is synchronized with DB cache.
        scheduledExecutorService.shutdown();
        Mockito.verify(auditActionsPersistenceManager, Mockito.times(1))
                .persistActions(auditedActionsCaptor.capture());
        Assert.assertEquals(
            Arrays.asList(newAuditedAction, recentlyClearedAuditedAction),
            auditedActionsCaptor.getValue());
        Mockito.verify(auditActionsPersistenceManager, Mockito.times(1))
                .removeActions(Mockito.eq(Collections.singletonList(
                        Pair.create(expiredClearedAuditedAction.getRecommendationId(),
                                expiredClearedAuditedAction.getWorkflowId()))));
    }

    private static Map<Pair<Long, Long>, Optional<Long>> toMap(
            Collection<Cell<Long, Long, Optional<Long>>> cells) {
        return cells.stream()
                .collect(Collectors.toMap(cell -> new Pair<>(cell.getRowKey(), cell.getColumnKey()),
                        cell -> cell.getValue()));
    }
}
