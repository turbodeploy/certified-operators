package com.vmturbo.topology.processor.probes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for {@link RemoteProbeStore}.
 */
public class RemoteProbeStoreTest {

    private IdentityProvider idProvider;
    private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
    private RemoteProbeStore store;

    private final ProbeInfo probeInfo = Probes.emptyProbe;
    private final ContainerInfo containerInfo = ContainerInfo.newBuilder().build();
    private final KeyValueStore keyValueStore = mock(KeyValueStore.class);
    private final ActionMergeSpecsRepository actionMergeSpecsRepository = mock(ActionMergeSpecsRepository.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
        createTransport();

    @Before
    public void setup() {
        idProvider = mock(IdentityProvider.class);
        store = new RemoteProbeStore(keyValueStore, idProvider, stitchingOperationStore, actionMergeSpecsRepository);
    }

    @Test
    public void testRegisterNewProbe() throws Exception {
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);
        assertFalse(store.registerNewProbe(probeInfo, containerInfo, transport));

        verify(stitchingOperationStore).setOperationsForProbe(eq(1234L), eq(probeInfo), eq(store.getProbeOrdering()));
    }

    @Test
    public void testGetProbeByName() throws Exception {
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);
        store.registerNewProbe(probeInfo, containerInfo, transport);

        assertTrue(store.getProbeIdForType(probeInfo.getProbeType()).isPresent());
        assertFalse(store.getProbeIdForType("non-existing-probe").isPresent());
    }

    @Test
    public void testGetProbeByCategory() throws Exception {
        // create a probe with a category that has an underscore in the enum as this is the
        // case that was previously broken
        final ProbeInfo probeInfoWithCategory = ProbeInfo.newBuilder(Probes.emptyProbe)
                .setProbeCategory(ProbeCategory.DATABASE_SERVER.getCategory())
                .setUiProbeCategory(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory())
                .build();
        final long probeId = 12345L;
        when(idProvider.getProbeId(probeInfoWithCategory)).thenReturn(probeId);
        store.registerNewProbe(probeInfoWithCategory, containerInfo, transport);
        List<Long> dbServerProbeIds = store.getProbeIdsForCategory(ProbeCategory.DATABASE_SERVER);
        assertEquals(1, dbServerProbeIds.size());
        assertEquals(probeId, (long) dbServerProbeIds.get(0));
    }

    @Test
    public void testRegisterNewProbeNotifiesListeners() throws Exception {
        ProbeStoreListener listener = mock(ProbeStoreListener.class);
        store.addListener(listener);
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);

        store.registerNewProbe(probeInfo, containerInfo, transport);
        verify(listener).onProbeRegistered(1234L, probeInfo);
    }

    /**
     * Test that registerNewProbe notifies listeners without holding the datalock.
     *
     * @throws IdentityProviderException if idProvider.getProbeId throws it.
     * @throws InterruptedException if CountDownLatch.await throws it.
     * @throws ExecutionException if ScheduledFuture.get() throws it.
     */
    @Test
    public void testRegisterNewProbeNotifiesWithoutLock()
            throws IdentityProviderException, InterruptedException, ExecutionException {
        final CountDownLatch listenerCalled = new CountDownLatch(1);
        final CountDownLatch completeListener = new CountDownLatch(1);
        final SetOnce<Boolean> latchOccurredBeforeTimeout = new SetOnce<>();

        ProbeStoreListener listener = new ProbeStoreListener() {
            @Override
            public void onProbeRegistered(long probeId, ProbeInfo probe) {
                listenerCalled.countDown();
                try {
                    latchOccurredBeforeTimeout.trySetValue(completeListener.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    latchOccurredBeforeTimeout.trySetValue(false);
                }
            }
        };
        store.addListener(listener);
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);

        // register the probe on another thread
        final ScheduledFuture<Boolean> future = Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            try {
                store.registerNewProbe(probeInfo, containerInfo, transport);
                return latchOccurredBeforeTimeout.getValue().get();
            } catch (ProbeException e) {
                fail("Probe Exception registering probe: " + e);
                return false;
            }
        }, 0L, TimeUnit.SECONDS);

        // wait for the listener to be notified
        final boolean received = listenerCalled.await(10L, TimeUnit.SECONDS);
        assertTrue(received);
        // check that we can make a call that requires the store's datalock while a listener is
        // still processing
        assertEquals(probeInfo, store.getProbe(1234L).get());
        completeListener.countDown();
        assertTrue("Lock was held while notifying listeners.", future.get());
    }

    @Test
    public void testRemoveListenerWhenPresent() throws Exception {
        ProbeStoreListener listener = mock(ProbeStoreListener.class);

        store.addListener(listener);
        assertTrue(store.removeListener(listener));
    }

    @Test
    public void testRemoveListenerWhenAbsent() throws Exception {
        assertFalse(store.removeListener(mock(ProbeStoreListener.class)));
    }

    /**
     * Tests adding/deleting transports with communication binding channels.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testTransportsWithChannel() throws Exception {
        // test probe and its probe ID
        final ProbeInfo probeFoo = Probes.defaultProbe;
        final long probeFooId = 123L;
        when(idProvider.getProbeId(probeFoo)).thenReturn(probeFooId);
        // test target
        final String channel = "nyc";
        final TargetSpec targetSpec = TargetSpec.newBuilder().setProbeId(probeFooId)
                .setCommunicationBindingChannel(channel).build();
        final Target target = mock(Target.class);
        when(target.getProbeId()).thenReturn(probeFooId);
        when(target.getSpec()).thenReturn(targetSpec);
        // 2nd probe to assist on the testing
        final ProbeInfo probeBar = Probes.emptyProbe;
        final long probeBarId = 456;
        when(idProvider.getProbeId(probeBar)).thenReturn(probeBarId);

        // No probe registered => no transports
        assertEquals(false, store.isAnyTransportConnectedForTarget(target));
        Collection<ProbeRegistrationDescription> probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be no probe registration", 0, probeRegistrations.size());

        // One registered probe with no channel => still zero transports
        store.registerNewProbe(probeFoo,containerInfo, transport);
        assertEquals(false, store.isAnyTransportConnectedForTarget(target));
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be one probe registration", 1, probeRegistrations.size());
        final long probeRegistrationId = probeRegistrations.iterator().next().getId();
        final Optional<ProbeRegistrationDescription> probeRegistration = store.getProbeRegistrationById(probeRegistrationId);
        assertTrue("Probe registration with id " + probeRegistrationId + " should be present",
                probeRegistration.isPresent());
        assertEquals(probeRegistrations.iterator().next(), probeRegistration.get());

        // Add another transport with the channel this time => 1 transport
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 = createTransport();
        final ContainerInfo containerInfo2 = ContainerInfo.newBuilder().setCommunicationBindingChannel(channel).build();
        store.registerNewProbe(probeFoo, containerInfo2, transport2);
        Collection<ITransport<MediationServerMessage, MediationClientMessage>> transportsWithChannel
                = store.getTransportsForTarget(target);
        assertEquals(true, store.isAnyTransportConnectedForTarget(target));
        assertEquals(1, transportsWithChannel.size());
        assertEquals(transport2, transportsWithChannel.iterator().next());
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be 2 probe registrations", 2, probeRegistrations.size());

        // Add the 3rd transport with the channel, so we have two choices ;)
        final ITransport<MediationServerMessage, MediationClientMessage> transport3 = createTransport();
        final ContainerInfo containerInfo3 = ContainerInfo.newBuilder().setCommunicationBindingChannel(channel).build();
        store.registerNewProbe(probeFoo, containerInfo3, transport3);
        transportsWithChannel = store.getTransportsForTarget(target);
        assertEquals(true, store.isAnyTransportConnectedForTarget(target));
        assertEquals(2, transportsWithChannel.size());
        assertEquals(ImmutableSet.of(transport2, transport3), transportsWithChannel.stream().collect(Collectors.toSet()));
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be 3 probe registrations", 3, probeRegistrations.size());

        // Add the 4th transport with a different channel; that wouldn't add to the choices and
        // there are still only two choices.
        final ITransport<MediationServerMessage, MediationClientMessage> transport4 = createTransport();
        final ContainerInfo containerInfo4 = ContainerInfo.newBuilder().setCommunicationBindingChannel("sfo").build();
        store.registerNewProbe(probeFoo, containerInfo4, transport4);
        transportsWithChannel = store.getTransportsForTarget(target);
        assertEquals(true, store.isAnyTransportConnectedForTarget(target));
        assertEquals(2, transportsWithChannel.size());
        assertEquals(ImmutableSet.of(transport2, transport3), transportsWithChannel.stream().collect(Collectors.toSet()));
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be 4 probe registrations", 4, probeRegistrations.size());

        // Add the 5th transport with the same channel but a different probe; that wouldn't add to
        // the choices and there are still only the two choices.
        final ITransport<MediationServerMessage, MediationClientMessage> transport5 = createTransport();
        final ContainerInfo containerInfo5 = ContainerInfo.newBuilder().setCommunicationBindingChannel(channel).build();
        store.registerNewProbe(probeBar, containerInfo5, transport5);
        transportsWithChannel = store.getTransportsForTarget(target);
        assertEquals(true, store.isAnyTransportConnectedForTarget(target));
        assertEquals(2, transportsWithChannel.size());
        assertEquals(ImmutableSet.of(transport2, transport3), transportsWithChannel.stream().collect(Collectors.toSet()));
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be 5 probe registrations", 5, probeRegistrations.size());

        // Remove the 2nd transport => only transport3 left for channel nyc
        store.removeTransport(transport2);
        transportsWithChannel = store.getTransportsForTarget(target);
        assertEquals(true, store.isAnyTransportConnectedForTarget(target));
        assertEquals(1, transportsWithChannel.size());
        assertEquals(transport3, transportsWithChannel.iterator().next());
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be 4 probe registrations left", 4, probeRegistrations.size());

        // Remove the 3rd transport => none left for channel nyc
        store.removeTransport(transport3);
        assertEquals(false, store.isAnyTransportConnectedForTarget(target));
        probeRegistrations = store.getAllProbeRegistrations();
        assertNotNull(probeRegistrations);
        assertEquals("There should be 3 probe registrations left", 3, probeRegistrations.size());
    }

    /**
     * Tests to add transport with the same probe with different order of order-insensitive
     * fields (identifying fields ids). It is expected, that transport is added, but no new probe
     * is registered.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testAddTransportForExistingProbe() throws Exception {
        ProbeStoreListener listener = mock(ProbeStoreListener.class);
        store.addListener(listener);
        final String tgt1 = "tgt1";
        final String tgt2 = "tgt2";
        final ProbeInfo probe1 = ProbeInfo.newBuilder(Probes.defaultProbe)
                .addTargetIdentifierField(tgt1)
                .addTargetIdentifierField(tgt2)
                .build();
        final ProbeInfo probe2 = ProbeInfo.newBuilder(Probes.defaultProbe)
                .addTargetIdentifierField(tgt2)
                .addTargetIdentifierField(tgt1)
                .build();
        when(idProvider.getProbeId(any(ProbeInfo.class))).thenReturn(1234L);

        final ITransport<MediationServerMessage, MediationClientMessage> transport2 = createTransport();
        store.registerNewProbe(probe1, containerInfo, transport);
        store.registerNewProbe(probe2, containerInfo, transport2);
        final long probeId = store.getProbes().keySet().iterator().next();
        Assert.assertEquals(2, store.getTransport(probeId).size());
        verify(listener).onProbeRegistered(1234L, probe1);
    }

    /**
     * Tests adding a new incompatible probe with the same probe type. It is expected, that
     * exception is thrown and transport is not registered.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testAddTransportForIncompatibleProbe() throws Exception {
        ProbeStoreListener listener = mock(ProbeStoreListener.class);
        store.addListener(listener);
        final String tgt1 = "tgt1";
        final String tgt2 = "tgt2";
        final ProbeInfo probe1 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .addTargetIdentifierField(tgt2)
            .build();
        final ProbeInfo probe2 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .build();
        when(idProvider.getProbeId(probe1)).thenReturn(123L);

        store.registerNewProbe(probe1, containerInfo, transport);

        // Incompatibility.
        when(idProvider.getProbeId(any())).thenThrow(new IdentityProviderException("BOO!"));
        expectedException.expect(ProbeException.class);
        store.registerNewProbe(probe2, containerInfo, transport);
    }

    /**
     * Tests adding a new incompatible probe with the same probe type, after the previous
     * transport has gone away. A ProbeException will be thrown because a target that was created
     * using the previous probe specification may exist.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testUpdatedProbeRejected() throws Exception {
        ProbeStoreListener listener = mock(ProbeStoreListener.class);
        store.addListener(listener);
        final String tgt1 = "tgt1";
        final String tgt2 = "tgt2";
        final ProbeInfo probe1 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .addTargetIdentifierField(tgt2)
            .build();
        final ProbeInfo probe2 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .build();
        when(idProvider.getProbeId(probe1)).thenReturn(123L);

        store.registerNewProbe(probe1, containerInfo, transport);
        store.removeTransport(transport);
        verify(stitchingOperationStore).removeOperationsForProbe(anyLong());
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();

        when(idProvider.getProbeId(any())).thenThrow(new IdentityProviderException("BOO!"));
        expectedException.expect(ProbeException.class);
        store.registerNewProbe(probe2, containerInfo, transport2);
    }

    /**
     * Tests adding a probe with updated registration information of the same probe type, after the previous
     * transport has gone away. The new registration information should replace the old. This simulates the
     * case where a user upgrades their probe version in a backward-compatible way.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testUpdatedProbePermitted() throws Exception {
        ProbeStoreListener listener = mock(ProbeStoreListener.class);
        store.addListener(listener);
        final String tgt1 = "tgt1";
        final ProbeInfo probe1 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .build();
        final ProbeInfo probe2 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .setDiscoversSupplyChain(true)
            .build();
        final long probeId = 1234L;
        Mockito.when(idProvider.getProbeId(any(ProbeInfo.class))).thenReturn(probeId);

        store.registerNewProbe(probe1, containerInfo, transport);
        assertEquals(probe1, store.getProbe(probeId).get());
        store.removeTransport(transport);
        verify(stitchingOperationStore).removeOperationsForProbe(anyLong());
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();

        store.registerNewProbe(probe2, containerInfo, transport2);
        assertEquals(probe2, store.getProbe(probeId).get());
    }

    /**
     * Test that the action merge repository is updated appropriately as probes are registered
     * or restored through diags.
     *
     * @throws Exception if an exception occurs.
     */
    @Test
    public void testActionMergeRepositoryUpdated() throws Exception {
        final String tgt1 = "tgt1";
        final String tgt2 = "tgt2";
        final ProbeInfo probe1 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt1)
            .build();
        final ProbeInfo probe2 = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addTargetIdentifierField(tgt2)
            .build();
        when(idProvider.getProbeId(probe1)).thenReturn(1234L);
        when(idProvider.getProbeId(probe2)).thenReturn(2345L);

        store.registerNewProbe(probe1,containerInfo,  transport);
        verify(actionMergeSpecsRepository).setPoliciesForProbe(eq(1234L), eq(probe1));

        store.overwriteProbeInfo(ImmutableMap.of(2345L, probe2));
        verify(actionMergeSpecsRepository).clear();
        verify(actionMergeSpecsRepository).setPoliciesForProbe(eq(2345L), eq(probe2));
    }

    /**
     * Tests that we can properly compare the relative stitching order of 2 probes and that we get
     * the correct set of probe categories that a particular probe type is dependent on for
     * stitching.
     *
     * @throws Exception
     */
    @Test
    public void testCompareProbeStitchingOrder() throws Exception {
        final ProbeInfo storageProbe = ProbeInfo.newBuilder().setProbeType("probe-type-stitching-order-1")
                .setProbeCategory("Storage").setUiProbeCategory("Storage").addTargetIdentifierField("targetId").build();
        final ProbeInfo hypervisorProbe = ProbeInfo.newBuilder().setProbeType("probe-type-stitching-order-2")
                .setProbeCategory("HYPERVISOR").setUiProbeCategory("Hypervisor").addTargetIdentifierField("targetId").build();
        final ProbeInfo fabricProbe = ProbeInfo.newBuilder().setProbeType("probe-type-stitching-order-3")
                .setProbeCategory("Fabric").setUiProbeCategory("Fabric and Network").addTargetIdentifierField("targetId").build();
        final long storageProbeId = 2345L;
        final long hypervisorProbeId = 3456L;
        final long fabricProbeId = 4567L;
        Mockito.when(idProvider.getProbeId(storageProbe)).thenReturn(storageProbeId);
        Mockito.when(idProvider.getProbeId(hypervisorProbe)).thenReturn(hypervisorProbeId);
        Mockito.when(idProvider.getProbeId(fabricProbe)).thenReturn(fabricProbeId);
        store.registerNewProbe(storageProbe, containerInfo, transport);
        assertEquals(storageProbe, store.getProbe(storageProbeId).get());
        store.registerNewProbe(hypervisorProbe, containerInfo, transport);
        assertEquals(hypervisorProbe, store.getProbe(hypervisorProbeId).get());
        store.registerNewProbe(fabricProbe, containerInfo, transport);
        ProbeStitchingOperation storageOp = new ProbeStitchingOperation(storageProbeId,
                new StorageStitchingOperation());
        ProbeStitchingOperation hyperVisorOp = new ProbeStitchingOperation(hypervisorProbeId,
                new StorageStitchingOperation());
        ProbeStitchingOperation fabricOp = new ProbeStitchingOperation(fabricProbeId,
                new StorageStitchingOperation());
        assertEquals(fabricProbe, store.getProbe(fabricProbeId).get());
        assertEquals(-1, store.getProbeOrdering().compare(hyperVisorOp, storageOp));
        assertEquals(1, store.getProbeOrdering().compare(storageOp, hyperVisorOp));
        Set<ProbeCategory> storageStitchWith =
                store.getProbeOrdering().getCategoriesForProbeToStitchWith(storageProbe);
        assertEquals(2, storageStitchWith.size());
        assertTrue(storageStitchWith.contains(ProbeCategory.HYPERVISOR));
    }

    @SuppressWarnings("unchecked")
    private static ITransport<MediationServerMessage, MediationClientMessage> createTransport() {
        return (ITransport<MediationServerMessage, MediationClientMessage>)mock(
            ITransport.class);
    }
}
