package com.vmturbo.topology.processor.communication.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.operation.OperationTestUtilities;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test functionality of AggregatingDiscoveryQueueImpl.
 */
public class AggregatingDiscoveryQueueTest {

    private static final int INCREMENTAL_INTERVAL = 15;

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private final TestProbeStore probeStore = new TestProbeStore(identityProvider);

    private final long probeId1 = 1111L;

    private final long probeId2 = 2222L;

    private final long targetId1 = 1010L;

    private final long targetId2 = 2020L;

    private final long targetId3 = 3030L;

    private final Target target1 = Mockito.mock(Target.class);

    private final Target target2 = Mockito.mock(Target.class);

    private final Target target3 = Mockito.mock(Target.class);

    private final String target1IdentifyingFields = "ABC";

    private final String target2IdentifyingFields = "DEF";

    private final String target3IdentifyingFields = "GHI";

    private final ProbeInfo probeInfo1 = createProbeInfo("VC", true);

    private final ProbeInfo probeInfo2 = createProbeInfo("HyperV", false);

    @Mock
    private Function<Runnable, DiscoveryBundle> discoveryMethodMock1;

    @Mock
    private Function<Runnable, DiscoveryBundle> discoveryMethodMock2;

    @Mock
    private Function<Runnable, DiscoveryBundle> discoveryMethodMock3;

    @Mock
    private BiConsumer<Discovery, Exception> errorHandler;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transportVc1;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transportVc2;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transportHyperV1;

    private AggregatingDiscoveryQueue queue;

    /**
     * Expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Set up Mocks.
     *
     * @throws ProbeException if there's an error in registering the probes
     */
    @Before
    public void setup() throws ProbeException {
        MockitoAnnotations.initMocks(this);
        try {
            when(identityProvider.getProbeId(probeInfo1)).thenReturn(probeId1);
            when(identityProvider.getProbeId(probeInfo2)).thenReturn(probeId2);
        } catch (IdentityProviderException e) {
            e.printStackTrace();
        }
        when(target1.getProbeId()).thenReturn(probeId1);
        when(target2.getProbeId()).thenReturn(probeId1);
        when(target3.getProbeId()).thenReturn(probeId2);
        when(target1.getProbeInfo()).thenReturn(probeInfo1);
        when(target2.getProbeInfo()).thenReturn(probeInfo1);
        when(target3.getProbeInfo()).thenReturn(probeInfo2);
        when(target1.getSerializedIdentifyingFields()).thenReturn(target1IdentifyingFields);
        when(target2.getSerializedIdentifyingFields()).thenReturn(target2IdentifyingFields);
        when(target3.getSerializedIdentifyingFields()).thenReturn(target3IdentifyingFields);
        when(target1.getId()).thenReturn(targetId1);
        when(target2.getId()).thenReturn(targetId2);
        when(target3.getId()).thenReturn(targetId3);
        TargetSpec defaultSpec = TargetSpec.getDefaultInstance();
        when(target1.getSpec()).thenReturn(defaultSpec);
        when(target2.getSpec()).thenReturn(defaultSpec);
        when(target3.getSpec()).thenReturn(defaultSpec);
        probeStore.registerNewProbe(probeInfo1, transportVc1);
        probeStore.registerNewProbe(probeInfo1, transportVc2);
        probeStore.registerNewProbe(probeInfo2, transportHyperV1);

        queue = new AggregatingDiscoveryQueueImpl(probeStore);
    }

    private IDiscoveryQueueElement addToQueueAndVerify(Target target, DiscoveryType discoveryType,
            Function<Runnable, DiscoveryBundle> discoveryMethod, boolean runImmediately) throws ProbeException {
        final IDiscoveryQueueElement addedElement = queue.offerDiscovery(target, discoveryType,
                discoveryMethod, errorHandler, runImmediately);
        assertNotNull(addedElement);
        assertEquals(target, addedElement.getTarget());
        assertEquals(discoveryType, addedElement.getDiscoveryType());
        assertEquals(runImmediately, addedElement.runImmediately());
        return addedElement;
    }

    private Future<Optional<IDiscoveryQueueElement>> callTakeOnThread(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull List<Long> probeTypes,
            @Nonnull DiscoveryType discoveryType) {
        Future<Optional<IDiscoveryQueueElement>> future = Executors.newSingleThreadExecutor()
                .submit(() -> {
                    try {
                        return queue.takeNextQueuedDiscovery(transport, probeTypes, discoveryType);
                    } catch (InterruptedException e) {
                        return Optional.empty();
                    }
                });
        // add a short sleep to give submitted chance time to run
        try {
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return future;
    }

    /**
     * Test that when you add elements to different probe types, you get back the correct values.
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testAddTwoDifferentProbeTypes() throws InterruptedException, ExecutionException, ProbeException {
        // queue two VC discoveries for two different targets
        final IDiscoveryQueueElement firstAdd = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);
        final IDiscoveryQueueElement secondAdd = addToQueueAndVerify(target2, DiscoveryType.FULL,
                discoveryMethodMock2, false);

        // poll for VC discovery and assert that we got the first queued discovery
        final Future<Optional<IDiscoveryQueueElement>> firstGet = callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(firstGet.isDone());
        assertEquals(firstAdd, firstGet.get().get());

        // poll for a HyperV discovery and make sure we get nothing back
        final Future<Optional<IDiscoveryQueueElement>> secondGet = callTakeOnThread(transportHyperV1,
                Collections.singletonList(probeId2), DiscoveryType.FULL);
        assertFalse(secondGet.isDone());

        // Now add a HyperV discovery
        final IDiscoveryQueueElement hypervAdd = addToQueueAndVerify(target3, DiscoveryType.FULL,
                discoveryMethodMock3, false);
        Thread.sleep(100L);
        assertTrue(secondGet.isDone());
        assertEquals(hypervAdd, secondGet.get().get());

        // poll for VC discovery and assert that we got the second queued discovery
        final Future<Optional<IDiscoveryQueueElement>> thirdGet = callTakeOnThread(transportVc2,
                Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(thirdGet.isDone());
        assertEquals(secondAdd, thirdGet.get().get());
    }

    /**
     * Test that incremental discoveries go to a different queue than FULL.
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testIncrementalWithFull() throws InterruptedException, ExecutionException, ProbeException {
        final IDiscoveryQueueElement firstFull = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);

        // take an incremental discovery and make sure we get nothing back
        final Future<Optional<IDiscoveryQueueElement>> firstGet = callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.INCREMENTAL);

        assertFalse(firstGet.isDone());

        // Add incremental discovery
        final IDiscoveryQueueElement firstIncr = addToQueueAndVerify(target1, DiscoveryType.INCREMENTAL,
                discoveryMethodMock1, false);
        Thread.sleep(100L);
        // Remove FULL discovery
        assertEquals(firstFull, queue.takeNextQueuedDiscovery(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.FULL).get());

        // Check that incremental discovery was properly taken from the queue
        assertEquals(firstIncr, firstGet.get().get());

    }

    /**
     * Test that when a target is deleted, it is removed from the relevant queue.
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testTargetDeletion() throws InterruptedException, ExecutionException, ProbeException {
        // queue two VC discoveries for two different targets
        final IDiscoveryQueueElement firstAdd = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);
        final IDiscoveryQueueElement secondAdd = addToQueueAndVerify(target2, DiscoveryType.FULL,
                discoveryMethodMock2, false);

        // At this point, target1 is first in the queue with target2 behind it. If we delete
        // target1, target2 should move to the front of the queue.
        queue.handleTargetRemoval(probeId1, targetId1);
        final Future<Optional<IDiscoveryQueueElement>> firstGet = callTakeOnThread(transportVc2,
                Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(firstGet.isDone());
        assertEquals(secondAdd, firstGet.get().get());
    }

    /**
     * Test that we can assign targets to specific transports and that these associations are
     * broken gracefully when the transport is closed.
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testDedicatedTransportForTarget() throws InterruptedException, ExecutionException, ProbeException {
        // tie target1 to transportVc1
        queue.assignTargetToTransport(transportVc1, target1);

        // this discovery should be added to a queue dedicated to transportVc1
        final IDiscoveryQueueElement firstAdd = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);

        // transportVc2 should not be able to get this discovery
        final Future<Optional<IDiscoveryQueueElement>> secondTransportGet =
                callTakeOnThread(transportVc2, Collections.singletonList(probeId1),
                        DiscoveryType.FULL);
        assertFalse(secondTransportGet.isDone());

        // test that transportVc1 can get the discovery
        final Future<Optional<IDiscoveryQueueElement>> firstTransportGet = callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(firstTransportGet.isDone());
        assertEquals(firstAdd, firstTransportGet.get().get());

        // add target1 again - it should still be on the queue dedicated to transportVc1
        final IDiscoveryQueueElement reAddTarget1 = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);

        // transportVc2 can still not get it
        assertFalse(secondTransportGet.isDone());

        // when transportVc1 is deleted, queued discovery should be re-assigned to probe ID queue
        // and transportVc2 should be able to get it now
        queue.handleTransportRemoval(transportVc1, Collections.singleton(probeId1));
        Thread.sleep(100L);
        assertTrue(secondTransportGet.isDone());
        assertEquals(reAddTarget1, secondTransportGet.get().get());
    }

    /**
     * Test that we can assign targets with channels specific transports. Once one transport is
     * removed check that the target can be discovered only by other containers with the same
     * channel
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testDedicatedTransportForTargetWithLabel() throws InterruptedException,
        ExecutionException, ProbeException {
        String channel = "channel1";

        queue.parseContainerInfoWithTransport(ContainerInfo.newBuilder().setCommunicationBindingChannel(channel).build(), transportVc1);
        queue.parseContainerInfoWithTransport(ContainerInfo.newBuilder().setCommunicationBindingChannel(channel).build(), transportVc2);

        TargetSpec specWithLabel =
            TargetSpec.newBuilder().setProbeId(probeId1).setCommunicationBindingChannel(channel).build();
        when(target1.getSpec()).thenReturn(specWithLabel);

       addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);

        //It can be taken by transport two since they are on the same channel and the target
        // hasn't been assigned a transport yet
        Future<Optional<IDiscoveryQueueElement>> secondTransportGet =
            callTakeOnThread(transportVc2, Collections.singletonList(probeId1),
                DiscoveryType.FULL);
        assertTrue(secondTransportGet.isDone());


        queue.assignTargetToTransport(transportVc1, target1);

        addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);
        // transportVc2 should not be able to get this discovery, since the target has an
        // assigned transport
        secondTransportGet =
            callTakeOnThread(transportVc2, Collections.singletonList(probeId1),
                DiscoveryType.FULL);
        assertFalse(secondTransportGet.isDone());

        // test that transportVc1 can get the discovery once transport 1 has been deleted
        final Future<Optional<IDiscoveryQueueElement>> firstTransportGet =
            callTakeOnThread(transportVc1,
            Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(firstTransportGet.isDone());

        // when transportVc1 is deleted, queued discovery should be re-assigned to probe ID queue
        // and transportVc2 should be able to get it now
        addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);
        queue.handleTransportRemoval(transportVc1, Collections.singleton(probeId1));
        Thread.sleep(100L);
        assertTrue(secondTransportGet.isDone());
    }

    /**
     * Test that if the channel of the target changes, we properly discover the target with the
     * transports on the same channel. Even if that target had a persistent connection.
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testDiscoveryWithUpdatedChannel() throws InterruptedException,
        ExecutionException, ProbeException {
        String channel1 = "channel1";
        String channel2 = "channel2";

        queue.parseContainerInfoWithTransport(ContainerInfo.newBuilder().setCommunicationBindingChannel(channel1).build(), transportVc1);
        queue.parseContainerInfoWithTransport(ContainerInfo.newBuilder().setCommunicationBindingChannel(channel2).build(), transportVc2);

        queue.assignTargetToTransport(transportVc1, target1);

        TargetSpec specWithLabel =
            TargetSpec.newBuilder().setProbeId(probeId1).setCommunicationBindingChannel(channel1).build();
        when(target1.getSpec()).thenReturn(specWithLabel);

        addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);

        // Should not be taken by transport2, since it is on another channel
        Future<Optional<IDiscoveryQueueElement>> secondTransportGet =
            callTakeOnThread(transportVc2, Collections.singletonList(probeId1),
                DiscoveryType.FULL);
        assertFalse(secondTransportGet.isDone());


        Future<Optional<IDiscoveryQueueElement>> firstTransportGet =
            callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(firstTransportGet.isDone());

        // Update the target to be on channel 2
        TargetSpec newSpecWithLabel =
            TargetSpec.newBuilder().setProbeId(probeId1).setCommunicationBindingChannel(channel2).build();
        when(target1.getSpec()).thenReturn(newSpecWithLabel);

        addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);

       firstTransportGet =
            callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.FULL);

        assertFalse(firstTransportGet.isDone());
        assertTrue(secondTransportGet.isDone());
    }

    /**
     * Test that we can assign targets with channels to specific transports. Once one transport is
     * removed the property of the channel should remain
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test(expected = ProbeException.class)
    public void testNoAvailableTransportsOnChannel() throws InterruptedException,
        ExecutionException, ProbeException {
        String channel = "channel1";

        TargetSpec specWithLabel =
            TargetSpec.newBuilder().setProbeId(probeId1).setCommunicationBindingChannel(channel).build();
        when(target1.getSpec()).thenReturn(specWithLabel);

        addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);
    }

    /**
     * Test that we can assign targets with channels to specific transports. Once one transport is
     * removed the property of the channel should remain
     *
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test(expected = ProbeException.class)
    public void testNoAvailableProbes() throws ProbeException {
        final long nonRegistereedProbeId = 333L;

        when(target1.getProbeId()).thenReturn(nonRegistereedProbeId);
        addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);
    }

    /**
     * Test that when you have two targets with the same identifying field from different probe
     * types, we only keep a persistent queue by transport for the probe that is persistent and that
     * targets from non persistent probe type just go to a probe type queue as usual. This is a
     * concern since the ContainerInfo object just has a set of identifying strings of targets - no
     * probe type information.
     *
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testNonPersistentTargetWithSameIdAsPersistentTarget() throws ProbeException {
        // tie target1 to transportVc1
        queue.assignTargetToTransport(transportVc1, target1);

        // make target3 have same identifying fields as target1
        when(target3.getSerializedIdentifyingFields()).thenReturn(target1IdentifyingFields);

        // this discovery should be added to a queue dedicated to transportVc1
        final IDiscoveryQueueElement firstAdd = addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);

        // If we tried to add target3 discovery to the queue created for transportVc1, it would fail
        // and we'd get null back.  Make sure this doesn't happen.
        final IDiscoveryQueueElement secondAdd = addToQueueAndVerify(target3, DiscoveryType.FULL,
            discoveryMethodMock1, false);
    }

    /**
     * Test that we can assign channels to specific targets and that those constraint are properly
     * respected. Discoveries on those targets  should be only done with transports with the same
     * channel.
     *
     * @throws InterruptedException if Thread.sleep or Future.get throws it.
     * @throws ExecutionException if Future.get throws it.
     * @throws ProbeException if a problem occurs with the probe
     */
    @Test
    public void testTargetWithChannel() throws InterruptedException, ExecutionException, ProbeException {
        // tie target1 to transportVc1
        final String channel = "channel";
        TargetSpec specWithLabel =
            TargetSpec.newBuilder().setProbeId(probeId1).setCommunicationBindingChannel(channel).build();
        when(target1.getSpec()).thenReturn(specWithLabel);

        queue.parseContainerInfoWithTransport(ContainerInfo.newBuilder().setCommunicationBindingChannel(channel).build(), transportVc1);

        final IDiscoveryQueueElement firstAdd = addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);

        // transportVc2 should not be able to get this discovery
        final Future<Optional<IDiscoveryQueueElement>> secondTransportGet =
            callTakeOnThread(transportVc2, Collections.singletonList(probeId1),
                DiscoveryType.FULL);
        assertFalse(secondTransportGet.isDone());

        // test that transportVc1 can get the discovery
        final Future<Optional<IDiscoveryQueueElement>> firstTransportGet = callTakeOnThread(transportVc1,
            Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertTrue(firstTransportGet.isDone());
        assertEquals(firstAdd, firstTransportGet.get().get());

        // add target1 again - it should still be on the queue dedicated to transportVc1
        final IDiscoveryQueueElement reAddTarget1 = addToQueueAndVerify(target1, DiscoveryType.FULL,
            discoveryMethodMock1, false);

        // transportVc2 can still not get it
        assertFalse(secondTransportGet.isDone());

        // when transportVc1 is deleted, transportVc2 should still not get it
        queue.handleTransportRemoval(transportVc1, Collections.singleton(probeId1));
        Thread.sleep(100L);
        assertFalse(secondTransportGet.isDone());
    }

    private ProbeInfo createProbeInfo(@Nonnull String probeType,
            boolean persistent) {
        final ProbeInfo.Builder builder = ProbeInfo.newBuilder(ProbeInfo.getDefaultInstance());
        if (persistent) {
            builder.setIncrementalRediscoveryIntervalSeconds(INCREMENTAL_INTERVAL);
        }
        builder.setProbeType(probeType);
        builder.setProbeCategory(ProbeCategory.HYPERVISOR.getCategory());
        return builder.build();
    }

    /**
     * Test that we throw a ProbeException when you try to queue a discovery for a probe that is
     * not connected to the TP.
     *
     * @throws ProbeException when the probe associated with the target is not connected.
     */
    @Test
    public void testDisconnectedProbe() throws ProbeException {
        probeStore.removeTransport(transportVc1);
        probeStore.removeTransport(transportVc2);
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(String.format("Probe %s is not connected", probeId1));
        final DiscoveryBundle bundle = mock(DiscoveryBundle.class);
        final Discovery discovery = mock(Discovery.class);
        final ProbeException probeException = new ProbeException("Test exception");
        when(discoveryMethodMock1.apply(any())).thenReturn(bundle);
        when(bundle.getDiscovery()).thenReturn(discovery);
        when(bundle.getException()).thenReturn(probeException);
        addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);
        verify(errorHandler).accept(discovery, probeException);
    }

    /**
     * Test that when the last transport for a probe type is unregistered, the queue for that probe
     * type is flushed.
     *
     * @throws Exception when there is a ProbeStoreException or if we get an exception waiting.
     */
    @Test
    public void testLastTransportRemovalForProbeTypeFlushesQueue() throws Exception {
        // queue two VC discoveries for two different targets and one hyperV discovery
        final IDiscoveryQueueElement addTarget1 = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);
        final IDiscoveryQueueElement addTarget2 = addToQueueAndVerify(target2, DiscoveryType.FULL,
                discoveryMethodMock2, false);
        final IDiscoveryQueueElement addTarget3 = addToQueueAndVerify(target3, DiscoveryType.FULL,
                discoveryMethodMock3, false);

        // remove first VC transport from probe store
        probeStore.removeTransport(transportVc1);
        queue.handleTransportRemoval(transportVc1, Collections.singleton(probeId1));
        // This should have no effect on queue contents as transportVc2 is still registered.
        // If a target is already in the queue, we should get back the existing queue element when
        // we try to add the target to the queue.
        assertEquals(addTarget3, addToQueueAndVerify(target3, DiscoveryType.FULL,
                discoveryMethodMock3, false));
        assertEquals(addTarget1, addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false));
        assertEquals(addTarget2, addToQueueAndVerify(target2, DiscoveryType.FULL,
                discoveryMethodMock2, false));

        // Now remove the only remaining VC transport.  This should drain the VC queue, but leave
        // the HyperV queue alone.
        probeStore.removeTransport(transportVc2);
        queue.handleTransportRemoval(transportVc2, Collections.singleton(probeId1));
        assertEquals(addTarget3, addToQueueAndVerify(target3, DiscoveryType.FULL,
                discoveryMethodMock3, false));

        // Try to get a VC discovery from the queue on a new thread. The call should never return.
        final Future<Optional<IDiscoveryQueueElement>> firstGet = callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.FULL);
        assertFalse(OperationTestUtilities.waitForEventAndReturnResult(
                () -> firstGet.isDone(), 5L));
    }
}
