package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueueImpl;
import com.vmturbo.topology.processor.communication.queues.IDiscoveryQueueElement;
import com.vmturbo.topology.processor.operation.OperationTestUtilities;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test functionality of TransportDiscoveryWorker.
 */
public class RemoteMediationServerWithDiscoveryWorkersTest {

    /**
     * Feature flag rule.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(
            FeatureFlags.ENABLE_TP_PROBE_SECURITY);

    private static final String VC_PROBE_TYPE = "VC";

    private static final long probeIdVc = 1111L;

    private static final long target1Id = 1010L;

    private static final long target2Id = 2020L;

    private static final long verify_timeout_millis = 10000L;

    private Target target1 = mock(Target.class);

    private Target target2 = mock(Target.class);

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport1;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport2;

    private Discovery discovery1 = mock(Discovery.class);

    private Discovery discovery2 = mock(Discovery.class);

    private Discovery discovery3 = mock(Discovery.class);

    private Discovery discovery4 = mock(Discovery.class);

    private DiscoveryBundle discoveryBundle1 = mock(DiscoveryBundle.class);

    private DiscoveryBundle discoveryBundle2 = mock(DiscoveryBundle.class);

    private DiscoveryBundle discoveryBundle3 = mock(DiscoveryBundle.class);

    private DiscoveryBundle discoveryBundle4 = mock(DiscoveryBundle.class);

    private final DiscoveryRequest discoveryRequest1 = DiscoveryRequest.newBuilder()
            .setProbeType(VC_PROBE_TYPE)
            .setDiscoveryType(DiscoveryType.FULL)
            .build();

    private final DiscoveryRequest discoveryRequest2 = DiscoveryRequest.newBuilder()
            .setProbeType(VC_PROBE_TYPE)
            .setDiscoveryType(DiscoveryType.FULL)
            .build();

    private final DiscoveryRequest discoveryRequest3 = DiscoveryRequest.newBuilder()
            .setProbeType(VC_PROBE_TYPE)
            .setDiscoveryType(DiscoveryType.FULL)
            .build();

    private final DiscoveryRequest discoveryRequest4 = DiscoveryRequest.newBuilder()
            .setProbeType(VC_PROBE_TYPE)
            .setDiscoveryType(DiscoveryType.FULL)
            .build();

    private RunnableToDiscoveryBundle discoveryMethod1;

    private RunnableToDiscoveryBundle discoveryMethod2;

    private RunnableToDiscoveryBundle discoveryMethod3;

    private RunnableToDiscoveryBundle discoveryMethod4;

    @Mock
    private BiConsumer<Discovery, Exception> errorHandler1;

    @Mock
    private BiConsumer<Discovery, Exception> errorHandler2;

    private final ProbeInfo probeInfoVc1 = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType(VC_PROBE_TYPE)
            .setIncrementalRediscoveryIntervalSeconds(15)
            .build();

    private final ContainerInfo containerInfo = ContainerInfo.newBuilder()
            .addProbes(probeInfoVc1)
            .build();

    private AggregatingDiscoveryQueue queue;

    private final ProbeStore probeStore = mock(ProbeStore.class);

    private final ProbeContainerChooser probeContainerChooser = mock(ProbeContainerChooser.class);

    private final TargetStore targetStore = mock(TargetStore.class);

    private RemoteMediationServer remoteMediationServer;

    private String target1IdFields = "target1";

    private String target2IdFields = "target2";

    private long discoveryWorkerPollingTimeoutSecs = 3L;

    /**
     * Argument captor for simulating returning a permit to the TransportWorker.
     */
    @Captor
    private ArgumentCaptor<Exception> exceptionArgumentCaptor;

    @Captor
    private ArgumentCaptor<MediationServerMessage> transportSendCaptor;

    /**
     * Set up the mocks and create the TransportDiscoveryWorkers.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        queue = spy(new AggregatingDiscoveryQueueImpl(probeStore, probeContainerChooser));
        doReturn(Optional.of(probeIdVc)).when(probeStore).getProbeIdForType(eq(VC_PROBE_TYPE));
        doReturn(Optional.of(probeInfoVc1)).when(probeStore).getProbe(eq(probeIdVc));
        doReturn(true).when(probeStore).isProbeConnected(eq(probeIdVc));
        doReturn(Optional.of(probeInfoVc1)).when(probeStore).getProbeInfoForType(eq(VC_PROBE_TYPE));
        TargetSpec defaultSpec = TargetSpec.getDefaultInstance();
        when(target1.getProbeId()).thenReturn(probeIdVc);
        when(target1.getSpec()).thenReturn(defaultSpec);
        when(target1.getId()).thenReturn(target1Id);
        when(target1.getSerializedIdentifyingFields()).thenReturn(target1IdFields);
        when(target1.getProbeInfo()).thenReturn(probeInfoVc1);
        when(target1.getDisplayName()).thenReturn(target1IdFields);
        when(target2.getProbeId()).thenReturn(probeIdVc);
        when(target2.getSpec()).thenReturn(defaultSpec);
        when(target2.getId()).thenReturn(target2Id);
        when(target2.getSerializedIdentifyingFields()).thenReturn(target2IdFields);
        when(target2.getProbeInfo()).thenReturn(probeInfoVc1);
        when(target2.getDisplayName()).thenReturn(target2IdFields);
        setupBundles();
        remoteMediationServer = Mockito.spy(
                new RemoteMediationServerWithDiscoveryWorkers(probeStore,
                        Mockito.mock(ProbePropertyStore.class),
                        probeContainerChooser,
                        queue,
                        1,
                        1, discoveryWorkerPollingTimeoutSecs,
                        Mockito.mock(TargetStore.class)));
    }

    private void setupBundles() {
        discoveryMethod1 = new RunnableToDiscoveryBundle(discoveryBundle1);
        discoveryMethod2 = new RunnableToDiscoveryBundle(discoveryBundle2);
        discoveryMethod3 = new RunnableToDiscoveryBundle(discoveryBundle3);
        discoveryMethod4 = new RunnableToDiscoveryBundle(discoveryBundle4);
        when(discoveryBundle1.getDiscoveryRequest())
                .thenReturn(discoveryRequest1);
        when(discoveryBundle2.getDiscoveryRequest())
                .thenReturn(discoveryRequest2);
        when(discoveryBundle3.getDiscoveryRequest())
                .thenReturn(discoveryRequest3);
        when(discoveryBundle4.getDiscoveryRequest())
                .thenReturn(discoveryRequest4);
        when(discoveryBundle1.getDiscoveryMessageHandler())
                .thenReturn(mock(DiscoveryMessageHandler.class));
        when(discoveryBundle2.getDiscoveryMessageHandler())
                .thenReturn(mock(DiscoveryMessageHandler.class));
        when(discoveryBundle3.getDiscoveryMessageHandler())
                .thenReturn(mock(DiscoveryMessageHandler.class));
        when(discoveryBundle4.getDiscoveryMessageHandler())
                .thenReturn(mock(DiscoveryMessageHandler.class));
        when(discoveryBundle1.getDiscovery()).thenReturn(discovery1);
        when(discoveryBundle2.getDiscovery()).thenReturn(discovery2);
        when(discoveryBundle3.getDiscovery()).thenReturn(discovery3);
        when(discoveryBundle4.getDiscovery()).thenReturn(discovery4);
        when(discovery1.getTargetId()).thenReturn(target1Id);
        when(discovery2.getTargetId()).thenReturn(target2Id);
        when(discovery3.getTargetId()).thenReturn(target1Id);
        when(discovery4.getTargetId()).thenReturn(target2Id);
    }

    /**
     * Test that when the TransportDiscoveryWorker starts, it runs only 1 discovery since it only
     * has 1 permit.
     *
     * @throws Exception if a problem occurs with the Probe or if
     * OperationTestUtilities.waitForEvent throws one.
     */
    @Test
    public void testBasicFunctionality() throws Exception {
        remoteMediationServer.registerTransport(containerInfo, transport1);
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);

        // wait until transport1 handler thread has been started and processed discovery off the
        // queue.
        OperationTestUtilities.waitForEvent(() -> discoveryMethod1.runnable != null);

        // verify we tried to get the DiscoveryBundle
        verify(discoveryBundle1, timeout(verify_timeout_millis).times(2)).getDiscoveryRequest();
        // clean up the transport workers associated with this transport
        remoteMediationServer.processContainerClose(transport1);
    }

    /**
     * Test that when we queue 2 discoveries and run two workers, each one only discovers 1 target
     * as they each have one permit. When we queue up 2 more discoveries and then release the
     * permits, each worker again discovers its target.
     *
     * @throws Exception if there is an exception with a probe or transport or if
     * OperationTestUtilities.waitForEvent throws one.
     */
    @Test
    public void testMultipleWorkers() throws Exception {
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        queue.offerDiscovery(target2, DiscoveryType.FULL, discoveryMethod2, errorHandler2, false);
        remoteMediationServer.registerTransport(containerInfo, transport1);

        // wait until transport1 handler thread has been started and processed discovery off the
        // queue.
        OperationTestUtilities.waitForEvent(() -> discoveryMethod1.runnable != null);

        // start the next discovery worker and it should grab the next discovery
        remoteMediationServer.registerTransport(containerInfo, transport2);

        // wait for second discovery to be processed
        OperationTestUtilities.waitForEvent(() -> discoveryMethod2.runnable != null);

        // Now we queue up 2 more discoveries - they should only go to their assigned transports
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod3, errorHandler1, false);
        queue.offerDiscovery(target2, DiscoveryType.FULL, discoveryMethod4, errorHandler2, false);

        // Nothing will happen until we free up the transport workers by returning a permit to each
        discoveryMethod1.runnable.run();
        discoveryMethod2.runnable.run();

        // Wait for both discoveries to run
        OperationTestUtilities.waitForEvent(() -> discoveryMethod3.runnable != null);
        OperationTestUtilities.waitForEvent(() -> discoveryMethod4.runnable != null);

        verify(transport1, timeout(verify_timeout_millis).times(2))
                .send(transportSendCaptor.capture());
        verify(transport2, timeout(verify_timeout_millis).times(2))
                .send(transportSendCaptor.capture());
        assertEquals(discoveryRequest1,
                transportSendCaptor.getAllValues().get(0).getDiscoveryRequest());
        assertEquals(discoveryRequest1,
                transportSendCaptor.getAllValues().get(2).getDiscoveryRequest());
        assertEquals(discoveryRequest2,
                transportSendCaptor.getAllValues().get(1).getDiscoveryRequest());
        assertEquals(discoveryRequest2,
                transportSendCaptor.getAllValues().get(3).getDiscoveryRequest());

        // shut down the worker threads
        remoteMediationServer.processContainerClose(transport1);
        remoteMediationServer.processContainerClose(transport2);
    }

    /**
     * Currently Ignored due to intermittent test failures on Jenkins. See OM-67791.
     * Test that TransportDiscoveryWorker is able to process a containerClosed message even though
     * it is actively polling for discoveries off the queue.
     *
     * @throws InterruptedException if Thread.sleep is interrupted.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testTransportClosed() throws InterruptedException, ProbeException {
        remoteMediationServer.registerTransport(containerInfo, transport1);
        // When transport thread has started, it will call takeNextQueueDiscovery once for FULL
        // and once for INCREMENTAL. If a thread times out waiting for a discovery to be queued,
        // it will call takeNextQueuedDiscovery multiple times.
        verify(queue, timeout(verify_timeout_millis).atLeastOnce())
                .takeNextQueuedDiscovery(eq(transport1), eq(Collections.singleton(probeIdVc)),
                        eq(DiscoveryType.FULL),
                        eq(TimeUnit.SECONDS.toMillis(discoveryWorkerPollingTimeoutSecs)));
        verify(queue, timeout(verify_timeout_millis).atLeastOnce())
                .takeNextQueuedDiscovery(eq(transport1), eq(Collections.singleton(probeIdVc)),
                        eq(DiscoveryType.INCREMENTAL),
                        eq(TimeUnit.SECONDS.toMillis(discoveryWorkerPollingTimeoutSecs)));

        // close the transport, which should also interrupt the worker thread
        remoteMediationServer.processContainerClose(transport1);
        // wait for queue to be notified of transport closure
        verify(queue, timeout(verify_timeout_millis)).handleTransportRemoval(transport1,
                Collections.singleton(probeIdVc));

        // wait for threads to timeout waiting for the queue - add an extra second because on a
        // slow build machine, we could end up getting here at about same time as worker thread
        // starts its wait
        Thread.sleep(TimeUnit.SECONDS.toMillis(discoveryWorkerPollingTimeoutSecs + 1));

        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        // Verify we didn't try to run the discovery.
        Thread.sleep(1000L);
        assertNull(discoveryMethod1.runnable);
    }

    /**
     * Test that if a transport is closed while registration is being processed, the related
     * transport worker is closed.
     *
     * @throws InterruptedException when Thread.sleep is interrupted.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testTransportClosesBeforeRegistered() throws InterruptedException, ProbeException {
        // simulate transport already being closed by the time it is registered
        doThrow(new IllegalStateException("transport closed"))
                .when(transport1).addEventHandler(any());
        remoteMediationServer.registerTransport(containerInfo, transport1);
        // sleep to let the transport worker thread start
        Thread.sleep(500L);

        // By now, the registration of transport should have occurred, causing the processing of
        // the closed transport and shutting down the worker thread. So if we queue a discovery,
        // it should remain in the queue, as there is no thread active to service the queue.
        final IDiscoveryQueueElement queuedDiscovery = queue.offerDiscovery(target1,
                DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);

        // Sleep to let any workers that do exist process the queued discovery.
        Thread.sleep(500L);

        // Check that the discovery we queued is still there.
        final SetOnce<Optional<IDiscoveryQueueElement>> takenDiscovery = new SetOnce<>();
        // this should timeout after 3 seconds
        takenDiscovery.trySetValue(queue.takeNextQueuedDiscovery(transport1,
            Collections.singletonList(probeIdVc), DiscoveryType.FULL, discoveryWorkerPollingTimeoutSecs));
        assertTrue(takenDiscovery.getValue().isPresent());
        assertTrue(takenDiscovery.getValue().get().isPresent());
        assertEquals(queuedDiscovery, takenDiscovery.getValue().get().get());
    }

    /**
     * Test that if you close the last transport that services a probe type, any queued discoveries
     * for that probe type fail with a ProbeException.
     *
     * @throws ProbeException when queue.offerDiscovery throws it.
     */
    @Test
    public void testClosingLastTransportFailsDiscoveries() throws ProbeException {
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        queue.offerDiscovery(target2, DiscoveryType.FULL, discoveryMethod2, errorHandler2, false);
        // doReturn must come before registerTransport call to avoid a race condition between
        // discovery worker thread and this thread changing the isProbeConnected behavior
        doReturn(false).when(probeStore).isProbeConnected(eq(probeIdVc));
        remoteMediationServer.registerTransport(containerInfo, transport1);
        final ProbeException probeException = new ProbeException("Probe type is not connected.");
        when(discoveryBundle2.getException()).thenReturn(probeException);
        remoteMediationServer.processContainerClose(transport1);
        verify(discoveryBundle2, timeout(verify_timeout_millis))
                .setException(exceptionArgumentCaptor.capture());
        assertTrue(exceptionArgumentCaptor.getValue() instanceof ProbeException);
        verify(errorHandler2, timeout(verify_timeout_millis)).accept(discovery2, probeException);
    }

    /**
     * Helper class to take and record a Runnable and return a mock DiscoveryBundle to facilitate
     * tracking of progress in the discovery process.
     */
    private class RunnableToDiscoveryBundle implements Function<Runnable, DiscoveryBundle> {

        DiscoveryBundle bundle;

        Runnable runnable;

        RunnableToDiscoveryBundle(DiscoveryBundle bundle) {
            this.bundle = bundle;
        }

        @Override
        public DiscoveryBundle apply(Runnable runnable) {
            this.runnable = runnable;
            return bundle;
        }
    }
}
