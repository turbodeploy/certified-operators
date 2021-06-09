package com.vmturbo.market;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.CloudCostStatsAvailable;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.market.runner.Analysis;

/**
 * Tests for AnalysisRICoverageListener.
 */
public class AnalysisRICoverageListenerTest {

    private AnalysisRICoverageListener listener;

    private static final long topologyId1 = 11111L;
    private static final long topologyId2 = 22222L;
    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;

    /**
     * Initializes AnalysisRICoverageListener instance.
     */
    @Before
    public void setUp() {
        listener = new AnalysisRICoverageListener();
    }

    /**
     * Test that Analysis with same topology id as cost notification results in the notification
     * being returned by the listener.
     *
     * @throws ExecutionException   on exception encountered in the task.
     * @throws InterruptedException on interruption encountered by the task.
     * @throws TimeoutException if we timeout waiting for future
     */
    @Test
    public void testNotificationRecdSameTopologyId() throws ExecutionException,
            InterruptedException, TimeoutException {
        final Analysis analysis = createAnalysis(topologyId1);
        final Future<CostNotification> costNotificationFuture =
                listener.receiveCostNotification(analysis);
        final CostNotification costNotification = createCostNotification(topologyId1);
        listener.onCostNotificationReceived(costNotification);
        Assert.assertEquals(costNotification, costNotificationFuture.get(1L, TimeUnit.SECONDS));
    }

    /**
     * Make sure that status update notifications which are of the wrong type are ignored.
     *
     * @throws ExecutionException   on exception encountered in the task
     * @throws InterruptedException on interruption encountered by the task
     * @throws TimeoutException     expected exception
     */
    @Test(expected = TimeoutException.class)
    public void testReceiverIgnoresIrrelevantStatusType() throws ExecutionException,
            InterruptedException, TimeoutException {
        final Analysis analysis = createAnalysis(topologyId1);
        final Future<CostNotification> costNotificationFuture =
                listener.receiveCostNotification(analysis);
        CostNotification costNotification = createWrongTypeCostNotification(topologyId1);
        listener.onCostNotificationReceived(costNotification);
        try {
            costNotificationFuture.get(1L, TimeUnit.SECONDS);
        } finally {
            costNotificationFuture.cancel(true);
        }
    }

    /**
     * Make sure that cost notifications that are not StatusUpdate notifications are ignored..
     *
     * @throws ExecutionException   on exception encountered in the task
     * @throws InterruptedException on interruption encountered by the task
     * @throws TimeoutException     expected exception
     */
    @Test(expected = TimeoutException.class)
    public void testReceiverIgnoresWrongNotificationFlavor() throws ExecutionException,
            InterruptedException, TimeoutException {
        final Analysis analysis = createAnalysis(topologyId1);
        final Future<CostNotification> costNotificationFuture =
                listener.receiveCostNotification(analysis);
        CostNotification costNotification = createWrongOneofFlavorNotification(topologyId1);
        listener.onCostNotificationReceived(costNotification);
        try {
            costNotificationFuture.get(1L, TimeUnit.SECONDS);
        } finally {
            costNotificationFuture.cancel(true);
        }
    }

    /**
     * Test that Analysis with *different* topology id as cost notification results in a timeout and
     * a failed notification is returned by the listener.
     *
     * @throws InterruptedException on exception encountered in the task.
     * @throws ExecutionException   on interruption encountered by the task.
     */
    @Ignore("Test involves a 10-minute wait.")
    @Test
    public void testFailNotificationDifferentTopologyIds() throws InterruptedException,
            ExecutionException {
        final Analysis analysis = createAnalysis(topologyId1);
        final Future<CostNotification> costNotificationFuture =
                listener.receiveCostNotification(analysis);
        final CostNotification costNotification = createCostNotification(topologyId2);
        listener.onCostNotificationReceived(costNotification);
        final CostNotification receivedNotification = costNotificationFuture.get();
        Assert.assertEquals(Status.FAIL, receivedNotification.getStatusUpdate().getStatus());
    }

    /**
     * Test that 2 Analysis instances with different topology ids receive the appropriate cost
     * notification from the listener.
     *
     * @throws ExecutionException on exception encountered in the task.
     * @throws InterruptedException on interruption encountered by the task.
     */
    @Test
    public void testMultipleNotificationsForMultipleAnalysis() throws ExecutionException,
            InterruptedException {
        final Analysis analysis1 = createAnalysis(topologyId1);
        final Analysis analysis2 = createAnalysis(topologyId2);
        final Future<CostNotification> costNotificationFuture1 =
                listener.receiveCostNotification(analysis1);
        final Future<CostNotification> costNotificationFuture2 =
                listener.receiveCostNotification(analysis2);
        final CostNotification costNotification1 = createCostNotification(topologyId1);
        final CostNotification costNotification2 = createCostNotification(topologyId2);
        listener.onCostNotificationReceived(costNotification1);
        Assert.assertEquals(costNotification1, costNotificationFuture1.get());
        Assert.assertFalse(costNotificationFuture2.isDone());
        listener.onCostNotificationReceived(costNotification2);
        Assert.assertEquals(costNotification2, costNotificationFuture2.get());
    }

    /**
     * TopologyId1 is being processed by market. But costNotification sent by cost is of
     * TopologyId2. So we will never recover from this. So, we should get FAIL status.
     * @throws ExecutionException on exception encountered in the task.
     * @throws InterruptedException on interruption encountered by the task.
     */
    @Test
    public void testCostComponentAhead() throws ExecutionException, InterruptedException {
        final Analysis analysis1 = createAnalysis(topologyId1);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(REAL_TIME_TOPOLOGY_CONTEXT_ID)
            .setTopologyId(topologyId1)
            .setCreationTime(1L)
            .build();
        when(analysis1.getTopologyInfo()).thenReturn(topologyInfo);
        final Future<CostNotification> costNotificationFuture1 =
            listener.receiveCostNotification(analysis1);
        final CostNotification costNotification2 = createCostNotification(topologyId2, 2L);
        listener.onCostNotificationReceived(costNotification2);
        CostNotification actualCostNotification = costNotificationFuture1.get();
        Assert.assertEquals(Status.FAIL, actualCostNotification.getStatusUpdate().getStatus());
    }

    /**
     * TopologyId2 is being processed by market. But costNotification sent by cost is of
     * TopologyId1. So we will continue to wait. And finally when CostNotification is for
     * TopologyId2, we will finish with SUCCESS.
     * @throws ExecutionException on exception encountered in the task.
     * @throws InterruptedException on interruption encountered by the task.
     */
    @Test
    public void testMarketComponentAhead() throws ExecutionException, InterruptedException {
        final Analysis analysis2 = createAnalysis(topologyId2);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(REAL_TIME_TOPOLOGY_CONTEXT_ID)
            .setTopologyId(topologyId2)
            .setCreationTime(2L)
            .build();
        when(analysis2.getTopologyInfo()).thenReturn(topologyInfo);
        final Future<CostNotification> costNotificationFuture2 =
            listener.receiveCostNotification(analysis2);
        final CostNotification costNotification1 = createCostNotification(topologyId1, 1L);
        listener.onCostNotificationReceived(costNotification1);
        Assert.assertFalse(costNotificationFuture2.isDone());
        final CostNotification costNotification2 = createCostNotification(topologyId2, 2L);
        listener.onCostNotificationReceived(costNotification2);
        CostNotification actualCostNotification = costNotificationFuture2.get();
        Assert.assertEquals(actualCostNotification, costNotification2);
    }

    /**
     * CostNotification is sent by cost.
     * And then market comes to onCostNotificationReceived.
     * @throws ExecutionException on exception encountered in the task.
     * @throws InterruptedException on interruption encountered by the task.
     */
    @Test
    public void testCostNotificationReceivedBeforeMarketProcessesIt() throws ExecutionException,
        InterruptedException {
        final CostNotification costNotification1 = createCostNotification(topologyId1, 1L);
        listener.onCostNotificationReceived(costNotification1);
        final Analysis analysis1 = createAnalysis(topologyId1);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(REAL_TIME_TOPOLOGY_CONTEXT_ID)
            .setTopologyId(topologyId1)
            .setCreationTime(1L)
            .build();
        when(analysis1.getTopologyInfo()).thenReturn(topologyInfo);
        final Future<CostNotification> costNotificationFuture1 =
            listener.receiveCostNotification(analysis1);
        CostNotification actualCostNotification = costNotificationFuture1.get();
        Assert.assertEquals(costNotification1, actualCostNotification);
    }

    private Analysis createAnalysis(final long topologyId) {
        final Analysis analysis = mock(Analysis.class);
        when(analysis.getTopologyId()).thenReturn(topologyId);
        return analysis;
    }

    private CostNotification createCostNotification(final long topologyId, long topologyCreationTime) {
        return createCostNotificationBuilder(topologyId, topologyCreationTime).build();
    }

    private CostNotification createCostNotification(final long topologyId) {
        return createCostNotificationBuilder(topologyId, null).build();
    }

    private CostNotification.Builder createCostNotificationBuilder(final long topologyId, Long topologyCreationTime) {
        CostNotification.Builder builder = CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder().setStatus(Status.SUCCESS)
                .setType(StatusUpdateType.SOURCE_RI_COVERAGE_UPDATE)
                .setTopologyId(topologyId)
                .build());
        if (topologyCreationTime != null) {
            builder.getStatusUpdateBuilder().setTopologyCreationTime(topologyCreationTime);
        }
        return builder;
    }

    private CostNotification createWrongTypeCostNotification(final long topologyId) {
        return CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder().setStatus(Status.SUCCESS)
                        .setType(StatusUpdateType.PLAN_ENTITY_COST_UPDATE)
                        .setTopologyId(topologyId)
                        .build())
                .build();
    }

    private CostNotification createWrongOneofFlavorNotification(final long topologyId) {
        return CostNotification.newBuilder()
                .setCloudCostStatsAvailable(CloudCostStatsAvailable.newBuilder()
                        .setSnapshotDate(topologyId))
                .build();
    }
}
