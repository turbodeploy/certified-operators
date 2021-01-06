package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueueImpl;
import com.vmturbo.topology.processor.communication.queues.IDiscoveryQueueElement;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test functionality of TransportDiscoveryWorker.
 */
public class RemoteMediationServerWithDiscoveryWorkersTest {

    private static final String VC_PROBE_TYPE = "VC";

    private static final long probeIdVc = 1111L;

    private static final long target1Id = 1010L;

    private static final long target2Id = 2020L;

    private Target target1 = mock(Target.class);

    private Target target2 = mock(Target.class);

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport1;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport2;

    private Discovery discovery1 = mock(Discovery.class);

    private DiscoveryBundle discoveryBundle1 = mock(DiscoveryBundle.class);

    private DiscoveryBundle discoveryBundle2 = mock(DiscoveryBundle.class);

    private final DiscoveryRequest discoveryRequest1 = DiscoveryRequest.newBuilder()
            .setProbeType(VC_PROBE_TYPE)
            .setDiscoveryType(DiscoveryType.FULL)
            .build();

    @Mock
    private Function<Runnable, DiscoveryBundle> discoveryMethod1;

    @Mock
    private Function<Runnable, DiscoveryBundle> discoveryMethod2;

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

    private RemoteMediationServer remoteMediationServer;

    private String target1IdFields = "target1";

    private String target2IdFields = "target2";

    /**
     * Argument captor for simulating returning a permit to the TransportWorker.
     */
    @Captor
    private ArgumentCaptor<Runnable> runnableArgumentCaptor;

    /**
     * Set up the mocks and create the TransportDiscoveryWorkers.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        queue = spy(new AggregatingDiscoveryQueueImpl(probeStore));
        when(probeStore.getProbeIdForType(VC_PROBE_TYPE)).thenReturn(Optional.of(probeIdVc));
        when(probeStore.getProbe(probeIdVc)).thenReturn(Optional.of(probeInfoVc1));
        when(discoveryMethod1.apply(any(Runnable.class))).thenReturn(discoveryBundle1);
        when(discoveryMethod2.apply(any(Runnable.class))).thenReturn(discoveryBundle2);
        when(target1.getProbeId()).thenReturn(probeIdVc);
        when(target1.getId()).thenReturn(target1Id);
        when(target1.getSerializedIdentifyingFields()).thenReturn(target1IdFields);
        when(target1.getProbeInfo()).thenReturn(probeInfoVc1);
        when(target2.getProbeId()).thenReturn(probeIdVc);
        when(target2.getId()).thenReturn(target2Id);
        when(target2.getSerializedIdentifyingFields()).thenReturn(target2IdFields);
        when(target2.getProbeInfo()).thenReturn(probeInfoVc1);
        when(discoveryBundle1.getDiscoveryRequest()).thenReturn(discoveryRequest1);
        remoteMediationServer = Mockito.spy(
                new RemoteMediationServerWithDiscoveryWorkers(probeStore,
                        Mockito.mock(ProbePropertyStore.class),
                        new ProbeContainerChooserImpl(probeStore),
                        queue,
                        1,
                        1));
    }

    /**
     * Test that when the TransportDiscoveryWorker starts, it runs only 1 discovery since it only
     * has 1 permit.
     *
     * @throws InterruptedException if Thread.sleep is interrupted.
     */
    @Test
    public void testBasicFunctionality() throws InterruptedException {
        remoteMediationServer.registerTransport(containerInfo, transport1);
        when(discoveryBundle1.getDiscovery()).thenReturn(discovery1);
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        // sleep to let the transport worker run the discovery
        Thread.sleep(100L);
        // Worker should call takeNextQueuedDiscovery once to get the queued discovery and then
        // it will be out of permits, so no further calls can occur
        verify(queue).takeNextQueuedDiscovery(eq(transport1),
                eq(Collections.singleton(probeIdVc)),
                eq(DiscoveryType.FULL));
        // verify we tried to get the DiscoveryBundle
        verify(discoveryMethod1).apply(any(Runnable.class));
        verify(discoveryBundle1, times(2)).getDiscoveryRequest();
        // this should cause the run method to exit
        remoteMediationServer.processContainerClose(transport1);
    }

    private void setupBundles() {
        when(discoveryBundle1.getDiscoveryRequest())
                .thenReturn(discoveryRequest1);
        when(discoveryBundle2.getDiscoveryRequest())
                .thenReturn(discoveryRequest1);
        when(discoveryBundle1.getDiscoveryMessageHandler())
                .thenReturn(mock(DiscoveryMessageHandler.class));
        when(discoveryBundle2.getDiscoveryMessageHandler())
                .thenReturn(mock(DiscoveryMessageHandler.class));
        when(discoveryBundle1.getDiscovery()).thenReturn(discovery1);
    }

    /**
     * Test that when we queue 2 discoveries and run two workers, each one only discovers 1 target
     * as they each have one permit. When we queue up 2 more discoveries and then release the
     * permits, each worker again discovers its target.
     *
     * @throws InterruptedException if Thread.sleep is interrupted.
     * @throws CommunicationException if transport.send throws it.
     */
    @Test
    public void testMultipleWorkers() throws InterruptedException, CommunicationException {
        setupBundles();
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        queue.offerDiscovery(target2, DiscoveryType.FULL, discoveryMethod2, errorHandler2, false);
        remoteMediationServer.registerTransport(containerInfo, transport1);
        // sleep to let the transport worker run the discovery
        Thread.sleep(100L);
        // Worker should call takeNextQueuedDiscovery once to get the queued discovery and then
        // it will be out of permits, so no further calls can occur
        verify(queue).takeNextQueuedDiscovery(eq(transport1),
                eq(Collections.singleton(probeIdVc)),
                eq(DiscoveryType.FULL));
        // verify we tried to run the discovery
        verify(discoveryMethod1).apply(runnableArgumentCaptor.capture());
        verify(transport1).send(any());
        // start the next discovery worker and it should grab the next discovery
        remoteMediationServer.registerTransport(containerInfo, transport2);
        Thread.sleep(100L);
        verify(discoveryMethod2).apply(runnableArgumentCaptor.capture());
        verify(transport2).send(any());

        // Now we've captured both runnables that should free up permits when called.  We'll queue
        // up the discoveries again and see if they run.
        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        queue.offerDiscovery(target2, DiscoveryType.FULL, discoveryMethod2, errorHandler2, false);

        assertEquals(2, runnableArgumentCaptor.getAllValues().size());
        runnableArgumentCaptor.getAllValues().forEach(runnable -> runnable.run());

        verify(discoveryMethod1).apply(any(Runnable.class));
        verify(discoveryMethod2).apply(any(Runnable.class));
        verify(transport1).send(any());
        verify(transport2).send(any());

        // this should cause the run method to exit
        remoteMediationServer.processContainerClose(transport1);
        remoteMediationServer.processContainerClose(transport2);
    }

    /**
     * Test that TransportDiscoveryWorker is able to process a containerClosed message even though
     * it is actively polling for discoveries off the queue.
     *
     * @throws InterruptedException if Thread.sleep is interrupted.
     */
    @Test
    public void testTransportClosed() throws InterruptedException {
        setupBundles();
        remoteMediationServer.registerTransport(containerInfo, transport1);
        // sleep to let the transport worker thread start
        Thread.sleep(100L);

        // close the transport, which should also interrupt the worker thread
        remoteMediationServer.processContainerClose(transport1);
        // sleep to let the transport worker thread exit
        Thread.sleep(100L);

        queue.offerDiscovery(target1, DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);
        // Verify we didn't try to run the discovery. takeNextQueuedDiscovery should have only been
        // called once and then interrupted and never called again.
        verify(queue, times(1)).takeNextQueuedDiscovery(any(),
                any(),
                eq(DiscoveryType.FULL));
    }

    /**
     * Test that if a transport is closed while registration is being processed, the related
     * transport worker is clsoed.
     *
     * @throws InterruptedException when Thread.sleep is interrupted.
     */
    @Test
    public void testTransportClosesBeforeRegistered() throws InterruptedException {
        setupBundles();
        // simulate transport already being closed by the time it is registered
        doThrow(new IllegalStateException("transport closed"))
                .when(transport1).addEventHandler(any());
        remoteMediationServer.registerTransport(containerInfo, transport1);
        // sleep to let the transport worker thread start
        Thread.sleep(100L);

        // By now, the registration of transport should have occurred, causing the processing of
        // the closed transport and shutting down the worker thread. So if we queue a discovery,
        // it should remain in the queue, as there is no thread active to service the queue.
        final IDiscoveryQueueElement queuedDiscovery = queue.offerDiscovery(target1,
                DiscoveryType.FULL, discoveryMethod1, errorHandler1, false);

        // Sleep to let any workers that do exist process the queued discovery.
        Thread.sleep(100L);

        // Check that the discovery we queued is still there.
        final SetOnce<Optional<IDiscoveryQueueElement>> takenDiscovery = new SetOnce<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // Execute queue.takeNextQueuedDiscovery() on a different thread since it may block forever.
        executor.execute(() -> {
            try {
                takenDiscovery.trySetValue(queue.takeNextQueuedDiscovery(transport1, Collections.singletonList(probeIdVc), DiscoveryType.FULL));
            } catch (InterruptedException e) {
                takenDiscovery.trySetValue(Optional.empty());
            }
        });
        // Sleep to give thread chance to take discovery
        Thread.sleep(100L);
        assertTrue(takenDiscovery.getValue().isPresent());
        assertTrue(takenDiscovery.getValue().get().isPresent());
        assertEquals(queuedDiscovery, takenDiscovery.getValue().get().get());
        executor.shutdownNow();
    }
}
