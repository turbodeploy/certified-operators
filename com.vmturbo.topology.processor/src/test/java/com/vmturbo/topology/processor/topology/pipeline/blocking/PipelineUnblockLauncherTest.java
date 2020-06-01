package com.vmturbo.topology.processor.topology.pipeline.blocking;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Unit tests for the {@link PipelineUnblockLauncher}.
 */
public class PipelineUnblockLauncherTest {

    private TopologyPipelineExecutorService  pipelineExecutorService = mock(TopologyPipelineExecutorService.class);

    private TargetStore targetStore = mock(TargetStore.class);

    /**
     * Test that the initialization logic invokes the unblock operation.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testInitialization() throws Exception {

        PipelineUnblock unblock = new ImmediateUnblock(pipelineExecutorService);
        PipelineUnblockLauncher pipelineUnblockLauncher = new PipelineUnblockLauncher(unblock, targetStore);

        pipelineUnblockLauncher.initialize();

        verify(pipelineExecutorService, timeout(10_000)).unblockBroadcasts();
    }

    /**
     * Test initialization priority relative to the target store.
     */
    @Test
    public void testInitializationPriority() {
        when(targetStore.priority()).thenReturn(1);
        PipelineUnblock unblock = new ImmediateUnblock(pipelineExecutorService);
        PipelineUnblockLauncher pipelineUnblockLauncher = new PipelineUnblockLauncher(unblock, targetStore);
        // Should have a smaller priority, so that it gets initialized later.
        assertTrue(pipelineUnblockLauncher.priority() < targetStore.priority());
    }

}