package com.vmturbo.topology.processor.communication.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
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
     * Set up Mocks.
     */
    @Before
    public void setup() {
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
        when(target1.getSerializedIdentifyingFields()).thenReturn(target1IdentifyingFields);
        when(target2.getSerializedIdentifyingFields()).thenReturn(target2IdentifyingFields);
        when(target3.getSerializedIdentifyingFields()).thenReturn(target3IdentifyingFields);
        when(target1.getId()).thenReturn(targetId1);
        when(target2.getId()).thenReturn(targetId2);
        when(target3.getId()).thenReturn(targetId3);
        queue = new AggregatingDiscoveryQueueImpl(probeStore);
    }

    private IDiscoveryQueueElement addToQueueAndVerify(Target target, DiscoveryType discoveryType,
            Function<Runnable, DiscoveryBundle> discoveryMethod, boolean runImmediately) {
        final IDiscoveryQueueElement addedElement = queue.offerDiscovery(target, discoveryType,
                discoveryMethod, errorHandler, runImmediately);
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
     */
    @Test
    public void testAddTwoDifferentProbeTypes() throws InterruptedException, ExecutionException {
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
        Thread.sleep(10L);
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
     */
    @Test
    public void testIncrementalWithFull() throws InterruptedException, ExecutionException {
        final IDiscoveryQueueElement firstFull = addToQueueAndVerify(target1, DiscoveryType.FULL,
                discoveryMethodMock1, false);

        // take an incremental discovery and make sure we get nothing back
        final Future<Optional<IDiscoveryQueueElement>> firstGet = callTakeOnThread(transportVc1,
                Collections.singletonList(probeId1), DiscoveryType.INCREMENTAL);

        assertFalse(firstGet.isDone());

        // Add incremental discovery
        final IDiscoveryQueueElement firstIncr = addToQueueAndVerify(target1, DiscoveryType.INCREMENTAL,
                discoveryMethodMock1, false);
        Thread.sleep(10L);
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
     */
    @Test
    public void testTargetDeletion() throws InterruptedException, ExecutionException {
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
     */
    @Test
    public void testDedicatedTransportForTarget() throws InterruptedException, ExecutionException {
        // tie target1 to transportVc1
        queue.assignTargetToTransport(transportVc1, target1IdentifyingFields);

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
        queue.handleTransportRemoval(transportVc1);
        Thread.sleep(10L);
        assertTrue(secondTransportGet.isDone());
        assertEquals(reAddTarget1, secondTransportGet.get().get());
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
}
