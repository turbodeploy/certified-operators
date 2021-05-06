package com.vmturbo.market;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.market.runner.Analysis;

/**
 * Tests for AnalysisRICoverageListener.
 */
public class AnalysisRICoverageListenerTest {

    private AnalysisRICoverageListener listener;

    private static final long topologyId1 = 11111L;
    private static final long topologyId2 = 22222L;

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
     * @throws ExecutionException on exception encountered in the task.
     * @throws InterruptedException on interruption encountered by the task.
     */
    @Test
    public void testNotificationRecdSameTopologyId() throws ExecutionException,
            InterruptedException {
        final Analysis analysis = createAnalysis(topologyId1);
        final Future<CostNotification> costNotificationFuture =
                listener.receiveCostNotification(analysis);
        final CostNotification costNotification = createCostNotification(topologyId1);
        listener.onCostNotificationReceived(costNotification);
        Assert.assertEquals(costNotification, costNotificationFuture.get());
    }

    /**
     * Test that Analysis with *different* topology id as cost notification results in a timeout
     * and a failed notification is returned by the listener.
     *
     * @throws InterruptedException on exception encountered in the task.
     * @throws ExecutionException on interruption encountered by the task.
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

    private Analysis createAnalysis(final long topologyId) {
        final Analysis analysis = mock(Analysis.class);
        when(analysis.getTopologyId()).thenReturn(topologyId);
        return analysis;
    }

    private CostNotification createCostNotification(final long topologyId) {
        return CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder().setStatus(Status.SUCCESS)
                        .setType(StatusUpdateType.SOURCE_RI_COVERAGE_UPDATE)
                        .setTopologyId(topologyId)
                        .build())
                .build();
    }
}