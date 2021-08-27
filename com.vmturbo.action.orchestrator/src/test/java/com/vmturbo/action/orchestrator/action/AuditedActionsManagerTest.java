package com.vmturbo.action.orchestrator.action;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.AuditedActionsManager.AuditedActionsUpdate;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * AuditedActionsManager should remember which audited actions were sent, which have a cleared
 * timestamp, and which have expired. AuditedActionsManager should persist these to
 * AuditedActionsDAO without blocking the calling thread.
 */
public class AuditedActionsManagerTest {

    private static final long RECOVERY_INTERNAL_MINUTES = 1L;
    private static final String VMEM_RESIZE_UP_ONGEN = ActionSettingSpecs.getSubSettingFromActionModeSetting(
        ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
        ActionSettingType.ON_GEN);

    @Mock
    private AuditActionsPersistenceManager auditActionsPersistenceManager;

    @Mock
    private BlockingDeque<AuditedActionsUpdate> auditedActionsUpdateBatches;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    private AuditedActionsManager auditedActionsManager;

    @Captor
    private ArgumentCaptor<AuditedActionsUpdate> auditedActionsUpdateCaptor;

    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;

    /**
     * Sets up a AuditedActionsManager with mocks, shared by the tests that can.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        when(scheduledExecutorService.submit(runnableCaptor.capture())).thenReturn(null);
        auditedActionsManager = new AuditedActionsManager(
            auditActionsPersistenceManager,
            scheduledExecutorService,
            RECOVERY_INTERNAL_MINUTES,
            auditedActionsUpdateBatches
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
            new AuditedActionInfo(1L, 1L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty()),
            new AuditedActionInfo(1L, 2L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.of(1000L))
        ));
        // need to create a new instance since the load happens in the constructor
        auditedActionsManager = new AuditedActionsManager(
            auditActionsPersistenceManager,
            scheduledExecutorService,
            RECOVERY_INTERNAL_MINUTES
        );

        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 1L));
        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 2L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 3L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(2L, 1L));

        final Collection<AuditedActionInfo> auditedActions =
                auditedActionsManager.getAlreadySentActions();
        Assert.assertEquals(2, auditedActions.size());
        final Map<Pair<Long, Long>, Optional<Long>> actualMap = toMap(auditedActions);
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
    public void testChangingCache() {
        final AuditedActionsUpdate update = new AuditedActionsUpdate();
        final AuditedActionInfo newAuditedAction =
            new AuditedActionInfo(1L, 1L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo recentlyClearedAuditedAction =
            new AuditedActionInfo(1L, 2L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.of(1000L));
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
        final Collection<AuditedActionInfo> auditedActions =
                auditedActionsManager.getAlreadySentActions();
        Assert.assertEquals(2, auditedActions.size());
        Map<Pair<Long, Long>, Optional<Long>> actualMap = toMap(auditedActions);
        Assert.assertEquals(Optional.empty(), actualMap.get(new Pair<>(1L, 1L)));
        Assert.assertEquals(Optional.of(1000L), actualMap.get(new Pair<>(1L, 2L)));

        // check that the updates are added to the queue
        verify(auditedActionsUpdateBatches, Mockito.times(1))
            .addLast(auditedActionsUpdateCaptor.capture());
        AuditedActionsUpdate actualUpdate = auditedActionsUpdateCaptor.getValue();
        Assert.assertEquals(update.getAuditedActions(), actualUpdate.getAuditedActions());
        Assert.assertEquals(update.getRecentlyClearedActions(), actualUpdate.getRecentlyClearedActions());
        Assert.assertEquals(update.getRemovedAudits(), actualUpdate.getRemovedAudits());

        // now clearing an expired entry from the cache
        Mockito.reset(auditedActionsUpdateBatches);
        final AuditedActionsUpdate expiredUpdate = new AuditedActionsUpdate();
        final AuditedActionInfo expiredClearedAuditedAction = new AuditedActionInfo(1L, 2L, 1L, VMEM_RESIZE_UP_ONGEN, null);
        expiredUpdate.addAuditedActionForRemoval(expiredClearedAuditedAction);
        auditedActionsManager.persistAuditedActionsUpdates(expiredUpdate);

        // still in the cache because it wasn't removed
        Assert.assertTrue(auditedActionsManager.isAlreadySent(1L, 1L));
        // removed from the cache
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 2L));
        // these two were never added to the cache
        Assert.assertFalse(auditedActionsManager.isAlreadySent(1L, 3L));
        Assert.assertFalse(auditedActionsManager.isAlreadySent(2L, 1L));
        final Collection<AuditedActionInfo> afterExpiredActions =
                auditedActionsManager.getAlreadySentActions();
        Assert.assertEquals(1, afterExpiredActions.size());
        Map<Pair<Long, Long>, Optional<Long>> afterExpiredMap = toMap(afterExpiredActions);
        Assert.assertEquals(Optional.empty(), afterExpiredMap.get(new Pair<>(1L, 1L)));
        Assert.assertFalse(afterExpiredMap.containsKey(new Pair<>(1L, 2L)));

        // check that the updates are added to the queue
        verify(auditedActionsUpdateBatches, Mockito.times(1))
            .addLast(auditedActionsUpdateCaptor.capture());
        AuditedActionsUpdate actualExpireUpdate = auditedActionsUpdateCaptor.getValue();
        Assert.assertEquals(expiredUpdate.getAuditedActions(), actualExpireUpdate.getAuditedActions());
        Assert.assertEquals(expiredUpdate.getRecentlyClearedActions(), actualExpireUpdate.getRecentlyClearedActions());
        Assert.assertEquals(expiredUpdate.getRemovedAudits(), actualExpireUpdate.getRemovedAudits());
    }

    /**
     * Runnable added to the scheduler to process the next batch in the queue.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testQueueProcessed() throws Exception {
        final AuditedActionsUpdate update = new AuditedActionsUpdate();
        final AuditedActionInfo newAuditedAction = new AuditedActionInfo(1L, 1L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo recentlyClearedAuditedAction =
                new AuditedActionInfo(1L, 2L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.of(1000L));
        final AuditedActionInfo expiredAction = new AuditedActionInfo(1L, 3L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        update.addAuditedAction(newAuditedAction);
        update.addRecentlyClearedAction(recentlyClearedAuditedAction);
        update.addAuditedActionForRemoval(expiredAction);
        when(auditedActionsUpdateBatches.take()).thenReturn(update);

        Runnable queueProcessRunnable = runnableCaptor.getValue();
        reset(scheduledExecutorService);
        queueProcessRunnable.run();

        verify(auditActionsPersistenceManager)
            .persistActions(eq(Arrays.asList(newAuditedAction, recentlyClearedAuditedAction)));
        verify(auditActionsPersistenceManager)
            .removeActionWorkflows(eq(Arrays.asList(Pair.create(
                expiredAction.getRecommendationId(), expiredAction.getWorkflowId()))));

        // when finished, it should schedule another run of the runnable
        verify(scheduledExecutorService, times(1))
                .submit((Runnable)any());
    }

    /**
     * When persist fails, the runnable should be rescheduled with RECOVERY_INTERNAL_MINUTES delay.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testPersistFailed() throws Exception {
        final AuditedActionsUpdate update = new AuditedActionsUpdate();
        final AuditedActionInfo newAuditedAction = new AuditedActionInfo(1L, 1L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo recentlyClearedAuditedAction =
                new AuditedActionInfo(1L, 2L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.of(1000L));
        final AuditedActionInfo expiredAction = new AuditedActionInfo(1L, 3L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        update.addAuditedAction(newAuditedAction);
        update.addRecentlyClearedAction(recentlyClearedAuditedAction);
        update.addAuditedActionForRemoval(expiredAction);
        when(auditedActionsUpdateBatches.take()).thenReturn(update);
        doThrow(new ActionStoreOperationException("Testing storage failure"))
            .when(auditActionsPersistenceManager).persistActions(any());

        Runnable queueProcessRunnable = runnableCaptor.getValue();
        reset(scheduledExecutorService);
        queueProcessRunnable.run();

        // when finished, it should schedule another run of the runnable
        verify(scheduledExecutorService, times(1))
            .schedule((Runnable)any(), eq(RECOVERY_INTERNAL_MINUTES), eq(TimeUnit.MINUTES));
    }

    /**
     * When the thread is interrupted, it should not crash and it should not reschedule a
     * runnable since the application is shutting down.
     *
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testInterruptDoesNotCrash() throws InterruptedException {
        final AuditedActionsUpdate update = new AuditedActionsUpdate();
        final AuditedActionInfo newAuditedAction = new AuditedActionInfo(1L, 1L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo recentlyClearedAuditedAction =
                new AuditedActionInfo(1L, 2L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.of(1000L));
        final AuditedActionInfo expiredAction = new AuditedActionInfo(1L, 3L, 1L, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        update.addAuditedAction(newAuditedAction);
        update.addRecentlyClearedAction(recentlyClearedAuditedAction);
        update.addAuditedActionForRemoval(expiredAction);
        doThrow(new InterruptedException())
            .when(auditedActionsUpdateBatches).take();

        Runnable queueProcessRunnable = runnableCaptor.getValue();
        reset(scheduledExecutorService);
        // Should not block and should not throw an exception.
        queueProcessRunnable.run();
        // Should not schedule another Runnable
        verify(scheduledExecutorService, times(0))
            .submit((Runnable)any());
        verify(scheduledExecutorService, times(0))
            .schedule((Runnable)any(), anyLong(), any());
    }

    private static Map<Pair<Long, Long>, Optional<Long>> toMap(
            Collection<AuditedActionInfo> auditedActions) {
        return auditedActions.stream()
                .collect(Collectors.toMap(
                        action -> new Pair<>(action.getRecommendationId(), action.getWorkflowId()),
                        AuditedActionInfo::getClearedTimestamp));
    }
}
