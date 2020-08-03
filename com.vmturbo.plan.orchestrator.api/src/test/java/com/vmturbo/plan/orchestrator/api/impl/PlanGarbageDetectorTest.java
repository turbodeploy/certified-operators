package com.vmturbo.plan.orchestrator.api.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.PlanDeleted;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;

/**
 * Unit tests for the {@link PlanGarbageDetector}.
 */
public class PlanGarbageDetectorTest {

    private PlanServiceMole planBackend = spy(PlanServiceMole.class);

    /**
     * gRPC server for testing.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(planBackend);

    private ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);

    private PlanGarbageCollector garbageCollector = mock(PlanGarbageCollector.class);

    private PlanGarbageDetector planGarbageDetector;

    private static final long REALTIME_CTX = 77;

    private static final long INTERVAL_HRS = 1;

    private static final long EXISTING_PLAN_ID = 123;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        planGarbageDetector = new PlanGarbageDetector(PlanServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            INTERVAL_HRS, TimeUnit.HOURS, scheduledExecutorService, garbageCollector, REALTIME_CTX);

        doReturn(Collections.singletonList(PlanInstance.newBuilder()
            .setPlanId(EXISTING_PLAN_ID)
            .setStatus(PlanStatus.SUCCEEDED)
            .build())).when(planBackend).getAllPlans(any());
    }

    /**
     * When a "plan deleted" notification is received, make sure we try to delete the plan.
     */
    @Test
    public void testOnPlanDeleted() {
        planGarbageDetector.onPlanDeleted(PlanDeleted.newBuilder()
            .setPlanId(EXISTING_PLAN_ID)
            .setStatusBeforeDelete(PlanStatus.SUCCEEDED)
            .build());

        verify(garbageCollector).deletePlanData(EXISTING_PLAN_ID);
    }

    /**
     * Test asynchronous plan deletion - only invalid/non-existing ones should get deleted.
     */
    @Test
    public void testDeleteInvalidPlans() {
        final long nonExistingPlan = 234;

        when(garbageCollector.listPlansWithData())
            .thenReturn(Collections.singletonList(() -> Sets.newHashSet(EXISTING_PLAN_ID, nonExistingPlan)));

        runDeleteOperation();

        verify(garbageCollector).deletePlanData(nonExistingPlan);
    }

    /**
     * Test that realtime "plan id"s get ignored for deletion.
     */
    @Test
    public void testIgnoreRealtimeId() {
        when(garbageCollector.listPlansWithData())
            .thenReturn(Collections.singletonList(() -> Sets.newHashSet(EXISTING_PLAN_ID, REALTIME_CTX)));

        runDeleteOperation();

        verify(garbageCollector, never()).deletePlanData(anyLong());
    }

    /**
     * Shouldn't delete anything if PO responds with an error (unable to list valid plans).
     */
    @Test
    public void testFetchPlansException() {
        doReturn(Optional.of(Status.NOT_FOUND.asException())).when(planBackend).getAllPlansError(any());
        final long nonExistingPlan = 234;

        when(garbageCollector.listPlansWithData())
            .thenReturn(Collections.singletonList(() -> Sets.newHashSet(EXISTING_PLAN_ID, nonExistingPlan)));

        runDeleteOperation();

        verify(garbageCollector, never()).deletePlanData(anyLong());
    }

    /**
     * Failure to list SOME plan IDs shouldn't stop deletion of known stale plan ids.
     */
    @Test
    public void testFailingListOperation() {
        final long nonExistingPlan = 234;

        when(garbageCollector.listPlansWithData())
            .thenReturn(Arrays.asList(
                () -> {
                    throw new RuntimeException("BOO!");
                },
                () -> Sets.newHashSet(nonExistingPlan)));

        runDeleteOperation();

        verify(garbageCollector).deletePlanData(nonExistingPlan);
    }

    /**
     * Failure to delete one stale plan shouldn't affect deletion of other stale plans.
     */
    @Test
    public void testFailingDeleteOperation() {
        final long nonExistingPlan1 = 345;
        final long nonExistingPlan2 = 234;

        doThrow(new RuntimeException("BOO")).when(garbageCollector).deletePlanData(nonExistingPlan1);
        when(garbageCollector.listPlansWithData())
            .thenReturn(Collections.singletonList(() -> Sets.newHashSet(nonExistingPlan1, nonExistingPlan2)));

        runDeleteOperation();

        verify(garbageCollector).deletePlanData(nonExistingPlan1);
        verify(garbageCollector).deletePlanData(nonExistingPlan2);
    }

    /**
     * Capture the async runnable and trigger it manually.
     */
    private void runDeleteOperation() {
        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduledExecutorService).scheduleAtFixedRate(captor.capture(),
            eq(INTERVAL_HRS), eq(INTERVAL_HRS), eq(TimeUnit.HOURS));
        captor.getValue().run();
    }

}