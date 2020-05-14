package com.vmturbo.topology.processor.topology.pipeline.blocking;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import net.jpountz.lz4.LZ4FrameOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumpFilename;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.blocking.DiscoveryBasedUnblock.DiscoveryBasedUnblockFactory;

/**
 * Unit tests for {@link DiscoveryBasedUnblock}.
 */
public class DiscoveryBasedUnblockTest {

    private final Logger logger = LogManager.getLogger(getClass());

    private TargetStore targetStore = mock(TargetStore.class);

    private IOperationManager operationManager = mock(IOperationManager.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final int targetShortCircuitCount = 2;

    private static final long PROBE_ID = 123L;

    private final long maxDiscoveryWaitMs = 100_000;

    private final long maxProbeRegistrationWaitMs = 10_000;

    private TopologyPipelineExecutorService pipelineExecutorService = mock(TopologyPipelineExecutorService.class);

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private File dumpDir;

    private DiscoveryBasedUnblockFactory unblockBroadcastsFactory;


    /**
     * Create a temporary folder for cached responses.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Common setup code before every test.
     * @throws IOException id the dumpDir can't be created
     */
    @Before
    public void setup() throws IOException {
        dumpDir = new File(tmpFolder.newFolder("cached-responses-root"), "");
        final BinaryDiscoveryDumper discoveryDumper = new BinaryDiscoveryDumper(dumpDir);
        unblockBroadcastsFactory = new DiscoveryBasedUnblockFactory(
            targetStore, operationManager, clock, targetShortCircuitCount, maxDiscoveryWaitMs,
            maxProbeRegistrationWaitMs, TimeUnit.MILLISECONDS, identityProvider, discoveryDumper,
            true);
        IdentityGenerator.initPrefix(1L);
        when(identityProvider.generateOperationId()).thenAnswer(invocation -> IdentityGenerator.next());
    }

    /**
     * Test the timeout case - if discoveries are not successful after the configured time limit,
     * unblock.
     */
    @Test
    public void testWaitForDiscoveryTimeout() {
        final Target t1 = setupTarget(1L);
        newLastDiscovery(t1.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        // The discovery is still in progress.
        assertFalse(unblock.runIteration());

        clock.addTime(maxDiscoveryWaitMs, ChronoUnit.MILLIS);

        // Timed out.
        assertTrue(unblock.runIteration());
    }

    /**
     * Test a target with no discovery for more than the configured period does not block
     * broadcasts.
     */
    @Test
    public void testProbeRegistrationTimeout() {
        final Target t1 = setupTarget(1L);
        when(operationManager.getLastDiscoveryForTarget(t1.getId(), DiscoveryType.FULL))
                .thenReturn(Optional.empty());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        assertFalse(unblock.runIteration());

        // Getting close to the threshold.
        clock.addTime(maxProbeRegistrationWaitMs - 1, ChronoUnit.MILLIS);

        // Still not at threshold - waiting for probe to register.
        assertFalse(unblock.runIteration());

        clock.addTime(1, ChronoUnit.MILLIS);

        // Probe still not registered. We ignore that target from now on.
        assertTrue(unblock.runIteration());
    }

    /**
     * Test that we wait for a target that has no associated discovery for more than the configured
     * period BUT then gets an associated discovery while we are waiting for other targets.
     */
    @Test
    public void testProbeRegistrationTimeoutThenWaiting() {
        final Target t1 = setupTarget(1L);
        final Target t2 = setupTarget(2L);
        final Discovery t2d1 = newLastDiscovery(t2.getId());
        when(operationManager.getLastDiscoveryForTarget(t1.getId(), DiscoveryType.FULL))
                .thenReturn(Optional.empty());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1, t2));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        assertFalse(unblock.runIteration());

        // Got to the threshold, but we are still waiting for t2's discovery to complete.
        clock.addTime(maxProbeRegistrationWaitMs, ChronoUnit.MILLIS);

        assertFalse(unblock.runIteration());

        // Some more time passes.
        clock.addTime(10, ChronoUnit.MILLIS);
        assertFalse(unblock.runIteration());

        // Probe registered - AFTER the threshold, but while we are still waiting for another discovery.
        final Discovery t1d1 = newLastDiscovery(t1.getId());
        assertFalse(unblock.runIteration());

        // The other discovery succeeds. But now we will still be waiting for t1's discovery.
        t2d1.success();

        assertFalse(unblock.runIteration());

        t1d1.success();

        // Both discoveries now completed.
        assertTrue(unblock.runIteration());
    }

    /**
     * Test that when all target discoveries are successful the unblock operation completes.
     */
    @Test
    public void testWaitForDiscoverySuccess() {
        final Target t1 = setupTarget(1L);
        final Discovery t1d1 = newLastDiscovery(t1.getId());
        t1d1.success();
        final Target t2 = setupTarget(2L);
        final Discovery t2d1 = newLastDiscovery(t2.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1, t2));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        // One of the targets is still in progress.
        assertFalse(unblock.runIteration());

        t2d1.success();

        assertTrue(unblock.runIteration());
    }

    /**
     * Test that the unblock operation completes when a target fails more than the configured
     * number of times in a row.
     */
    @Test
    public void testTargetShortCircuit() {
        final Target t1 = setupTarget(1L);
        final Discovery t1d1 = newLastDiscovery(t1.getId());
        t1d1.success();
        final Target t2 = setupTarget(2L);
        final Discovery t2d1 = newLastDiscovery(t2.getId());
        t2d1.fail();
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1, t2));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        assertFalse(unblock.runIteration());
        // At the second iteration, t2 has Status == FAILED, but it's still the same failed discovery
        // so we shouldn't double-count it.
        assertFalse(unblock.runIteration());

        // Next discovery failed too. This hits the short-circuit limit (2).
        final Discovery t2d2 = newLastDiscovery(t2.getId());
        t2d2.fail();

        assertTrue(unblock.runIteration());
    }

    /**
     * Test that targets removed in the middle of the unblock operation do not affect the
     * unblocking.
     */
    @Test
    public void testTargetRemoval() {
        final Target t1 = setupTarget(1L);
        final Discovery t1d1 = newLastDiscovery(t1.getId());
        t1d1.success();
        final Target t2 = setupTarget(2L);
        newLastDiscovery(t2.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1, t2));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        assertFalse(unblock.runIteration());

        // t2 got deleted - target store won't return it anymore.
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        // Should unblock.
        assertTrue(unblock.runIteration());
    }

    /**
     * Test that targets added in the middle of the unblock operation get taken into account
     * (this is important for derived targets in particular).
     */
    @Test
    public void testTargetAddition() {
        final Target t1 = setupTarget(1L);
        final Discovery t1d1 = newLastDiscovery(t1.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        assertFalse(unblock.runIteration());

        t1d1.success();
        final Target t2 = setupTarget(2L);
        final Discovery t2d1 = newLastDiscovery(t2.getId());

        when(targetStore.getAll()).thenReturn(Arrays.asList(t1, t2));

        assertFalse(unblock.runIteration());

        t2d1.success();

        assertTrue(unblock.runIteration());
    }

    /**
     * Test the run loop.
     */
    @Test
    public void testRun() {
        when(pipelineExecutorService.areBroadcastsBlocked()).thenReturn(true);

        final Target t1 = setupTarget(1L);
        final Discovery t1d1 = newLastDiscovery(t1.getId());
        t1d1.success();
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        unblock.run();

        verify(pipelineExecutorService, timeout(30_000)).unblockBroadcasts();
    }

    /**
     * Test that interrupting the thread causes the pipeline executor to unblock.
     */
    @Test
    public void testRunInterrupted() {
        when(pipelineExecutorService.areBroadcastsBlocked()).thenReturn(true);

        final Target t1 = setupTarget(1L);
        // A discovery is in progress. It will never complete in this test.
        newLastDiscovery(t1.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        final DiscoveryBasedUnblock unblock =
                unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);
        Thread thread = new Thread(unblock);
        thread.start();
        thread.interrupt();

        // An interrupt should still unblock broadcasts.
        verify(pipelineExecutorService, timeout(30_000)).unblockBroadcasts();
    }

    /**
     * Test that a runtime exception in the thread causes the pipeline executor to unblock.
     */
    @Test
    public void testRunRuntimeException() {
        when(pipelineExecutorService.areBroadcastsBlocked()).thenReturn(true);

        final Target t1 = setupTarget(1L);
        // A discovery is in progress. It will never complete in this test.
        newLastDiscovery(t1.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(t1));

        final DiscoveryBasedUnblock unblock =
                spy(unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService));
        doThrow(new RuntimeException("BOO")).when(unblock).runIteration();

        // This should return immediately because of the runtime exception.
        unblock.run();

        // A runtime exception should still unblock broadcasts.
        verify(pipelineExecutorService).unblockBroadcasts();
    }

    /**
     * Test that loading cached responses at startup unlock the broadcast.
     */
    @Test
    public void testLoadingCachedResponses() {
        final long targetId = 1;
        writeDiscoveryResponse(targetId);
        Target mockTarget = setupTarget(targetId);
        newLastDiscovery(mockTarget.getId());
        when(targetStore.getAll()).thenReturn(Arrays.asList(mockTarget));
        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(mockTarget));
        when(mockTarget.getProbeId()).thenReturn(1L);
        final DiscoveryBasedUnblock unblock =
            unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);

        // Before loading the responses
        assertFalse(unblock.runIteration());

        unblock.run();

        // After loading the responses
        assertTrue(unblock.runIteration());

        verify(pipelineExecutorService).unblockBroadcasts();
    }

    /**
     * Test that loading cached responses for only a subset of the availables target doesn't
     * unlock the broadcast.
     */
    @Test
    public void testLoadingCachedResponsesWithMissingTarget() {
        final long targetId = 1;
        writeDiscoveryResponse(targetId);
        Target mockTarget1 = setupTarget(targetId);
        newLastDiscovery(mockTarget1.getId());
        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(mockTarget1));
        when(mockTarget1.getProbeId()).thenReturn(1L);

        Target mockTarget2 = setupTarget(2);
        newLastDiscovery(mockTarget2.getId());
        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(mockTarget1));
        when(mockTarget1.getProbeId()).thenReturn(2L);

        when(targetStore.getAll()).thenReturn(Arrays.asList(mockTarget1, mockTarget2));

        final DiscoveryBasedUnblock unblock =
            unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);

        assertFalse(unblock.runIteration());

        unblock.run();
        // We could only load one target, so the broadcast is still blocked
        assertFalse(unblock.runIteration());

        verify(pipelineExecutorService).unblockBroadcasts();
    }

    /**
     * Test that loading cached responses for only a subset of the availables target doesn't
     * unlock the broadcast.
     */
    @Test
    public void testDeleteTargetDiscoveryResponse() {
        final long targetId = 1;
        writeDiscoveryResponse(targetId);
        when(targetStore.getTarget(targetId)).thenReturn(Optional.ofNullable(null));

        final DiscoveryBasedUnblock unblock =
            unblockBroadcastsFactory.newUnblockOperation(pipelineExecutorService);

        Assert.assertEquals(1, dumpDir.list().length);

        unblock.run();

        Assert.assertEquals(0, dumpDir.list().length);
    }

    private Target setupTarget(long targetId) {
        Target mockTarget = mock(Target.class);
        when(mockTarget.getId()).thenReturn(targetId);
        return mockTarget;
    }

    private Discovery newLastDiscovery(long targetId) {
        Discovery mockDiscovery = new Discovery(PROBE_ID, targetId, identityProvider);
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(mockDiscovery));
        return mockDiscovery;
    }

    private void writeDiscoveryResponse(final long targetId) {
        final DiscoveryResponse discoveryResponse = DiscoveryResponse.newBuilder()
            .addEntityDTO(EntityDTO.newBuilder().setId("foo").setEntityType(EntityType.VIRTUAL_MACHINE)).build();
        final String sanitizedTargetName = DiscoveryDumpFilename.sanitize(String.valueOf(targetId));
        final DiscoveryDumpFilename ddf =
            new DiscoveryDumpFilename(sanitizedTargetName, new Date(), DiscoveryType.FULL);
        final File dtoFile = ddf.getFile(dumpDir, false, true);
        try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(dtoFile))) {
            os.write(discoveryResponse.toByteArray());
            logger.trace("Successfully saved text discovery response");
        } catch (IOException e) {
            logger.error("Could not save " + dtoFile.getAbsolutePath(), e);
        }
    }
}
