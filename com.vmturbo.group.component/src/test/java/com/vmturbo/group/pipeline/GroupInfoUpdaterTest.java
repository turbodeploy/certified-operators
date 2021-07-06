package com.vmturbo.group.pipeline;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;

/**
 * Unit tests for {@link GroupInfoUpdater}.
 */
public class GroupInfoUpdaterTest {

    private final GroupInfoUpdatePipelineFactory factory =
            mock(GroupInfoUpdatePipelineFactory.class);

    private final GroupSeverityUpdater severityUpdater = mock(GroupSeverityUpdater.class);

    private final ExecutorService executorService = mock(ExecutorService.class);

    private final long realtimeContextId = 777777L;

    /**
     * Tests that a pipeline can run.
     *
     * @throws PipelineException to satisfy compiler.
     * @throws InterruptedException to satisfy compiler.
     */
    @Test
    public void testPipelineRuns() throws PipelineException, InterruptedException {
        final GroupInfoUpdater runner = new GroupInfoUpdater(factory, severityUpdater,
                realtimeContextId, newDirectExecutorService());
        // GIVEN
        final long topologyid = 1234L;
        GroupInfoUpdatePipeline pipeline = mock(GroupInfoUpdatePipeline.class);
        when(factory.newPipeline(topologyid)).thenReturn(pipeline);
        // WHEN
        runner.onSourceTopologyAvailable(topologyid, realtimeContextId);
        // THEN
        verify(factory, times(1)).newPipeline(topologyid);
        verify(pipeline, times(1)).run(any());
    }

    /**
     * Tests that if we receive a notification that's not for a live topology (e.g. it's for a plan
     * topology), we don't run a pipeline.
     */
    @Test
    public void testPipelineIsSkippedOnPlanTopology() {
        final GroupInfoUpdater runner = new GroupInfoUpdater(factory, severityUpdater,
                realtimeContextId, executorService);
        final long notRealtimeContextId = 2L;
        // WHEN
        runner.onSourceTopologyAvailable(1234L, notRealtimeContextId);
        // THEN
        verify(executorService, times(0)).execute(any());
        verify(factory, times(0)).newPipeline(Mockito.anyLong());
    }

    /**
     * Tests that only one pipeline can run at a time, and notifications that arrive while a
     * pipeline is running are skipped.
     *
     * @throws PipelineException to satisfy compiler.
     * @throws InterruptedException to satisfy compiler.
     */
    @Test
    public void testNotificationsAreIgnoredWhilePipelineRuns()
            throws PipelineException, InterruptedException {
        ExecutorService executor = mock(ExecutorService.class);
        final GroupInfoUpdater runner = new GroupInfoUpdater(factory, severityUpdater,
                realtimeContextId, executor);
        final long topologyid = 1234L;
        GroupInfoUpdatePipeline pipeline = mock(GroupInfoUpdatePipeline.class);
        when(factory.newPipeline(topologyid)).thenReturn(pipeline);
        // four notifications arrive
        runner.onSourceTopologyAvailable(topologyid, realtimeContextId);
        runner.onSourceTopologyAvailable(topologyid, realtimeContextId);
        runner.onSourceTopologyAvailable(topologyid, realtimeContextId);
        runner.onSourceTopologyAvailable(topologyid, realtimeContextId);
        // verify that the executor service gets triggered only once (for the first notification)
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor, times(1)).execute(captor.capture());
        // pipeline runs
        captor.getValue().run();
        verify(pipeline, times(1)).run(any());

        // a new notification arrives after the first pipeline has finished
        runner.onSourceTopologyAvailable(topologyid, realtimeContextId);
        // verify that the executor service gets triggered
        verify(executor, times(2)).execute(captor.capture());
        captor.getValue().run();
        verify(pipeline, times(2)).run(any());
    }

    /**
     * Tests that only one severity update can run at a time, and notifications for severity updates
     * that arrive while another severity update is already running are skipped.
     *
     * @throws InterruptedException to satisfy compiler
     */
    @Test
    public void testQueueMultipleSeverityUpdates() throws InterruptedException {
        final ExecutorService executor = mock(ExecutorService.class);
        final GroupInfoUpdater runner = new GroupInfoUpdater(factory, severityUpdater,
                realtimeContextId, executor);
        // two requests arrive
        runner.onEntitySeverityClientCacheRefresh();
        runner.onEntitySeverityClientCacheRefresh();
        runner.onEntitySeverityClientCacheRefresh();
        runner.onEntitySeverityClientCacheRefresh();
        // verify that the executor service gets triggered only once (for the first notification)
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor, times(1)).execute(captor.capture());
        // pipeline runs
        captor.getValue().run();
        verify(severityUpdater, times(1)).refreshGroupSeverities();

        // a new notification arrives after the first severity update has finished
        runner.onEntitySeverityClientCacheRefresh();
        // verify that the executor service gets triggered
        verify(executor, times(2)).execute(captor.capture());
        captor.getValue().run();
        verify(severityUpdater, times(2)).refreshGroupSeverities();
    }
}
