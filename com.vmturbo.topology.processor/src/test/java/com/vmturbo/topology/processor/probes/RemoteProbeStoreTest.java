package com.vmturbo.topology.processor.probes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for {@link RemoteProbeStore}.
 */
public class RemoteProbeStoreTest {

    private IdentityProvider idProvider;
    private final StitchingOperationStore stitchingOperationStore = Mockito.mock(StitchingOperationStore.class);
    private RemoteProbeStore store;

    private final ProbeInfo probeInfo = Probes.emptyProbe;
    private final KeyValueStore keyValueStore = Mockito.mock(KeyValueStore.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
        createTransport();

    @Before
    public void setup() {
        idProvider = Mockito.mock(IdentityProvider.class);
        store = new RemoteProbeStore(keyValueStore, idProvider, stitchingOperationStore);
    }

    @Test
    public void testRegisterNewProbe() throws Exception {
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);
        assertFalse(store.registerNewProbe(probeInfo, transport));

        verify(stitchingOperationStore).setOperationsForProbe(eq(1234L), eq(probeInfo), eq(store.getProbeOrdering()));
    }

    @Test
    public void testGetProbeByName() throws Exception {
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);
        store.registerNewProbe(probeInfo, transport);

        assertTrue(store.getProbeIdForType(probeInfo.getProbeType()).isPresent());
        assertFalse(store.getProbeIdForType("non-existing-probe").isPresent());
    }

    @Test
    public void testGetProbeByCategory() throws Exception {
        // create a probe with a category that has an underscore in the enum as this is the
        // case that was previously broken
        final ProbeInfo probeInfoWithCategory = ProbeInfo.newBuilder(Probes.emptyProbe)
                .setProbeCategory(ProbeCategory.DATABASE_SERVER.getCategory())
                .build();
        final long probeId = 12345L;
        when(idProvider.getProbeId(probeInfoWithCategory)).thenReturn(probeId);
        store.registerNewProbe(probeInfoWithCategory, transport);
        List<Long> dbServerProbeIds = store.getProbeIdsForCategory(ProbeCategory.DATABASE_SERVER);
        assertEquals(1, dbServerProbeIds.size());
        assertEquals(probeId, (long) dbServerProbeIds.get(0));
    }

    @Test
    public void testRegisterNewProbeNotifiesListeners() throws Exception {
        ProbeStoreListener listener = Mockito.mock(ProbeStoreListener.class);
        store.addListener(listener);
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);

        store.registerNewProbe(probeInfo, transport);
        verify(listener).onProbeRegistered(1234L, probeInfo);
    }

    @Test
    public void testRemoveListenerWhenPresent() throws Exception {
        ProbeStoreListener listener = Mockito.mock(ProbeStoreListener.class);

        store.addListener(listener);
        assertTrue(store.removeListener(listener));
    }

    @Test
    public void testRemoveListenerWhenAbsent() throws Exception {
        assertFalse(store.removeListener(Mockito.mock(ProbeStoreListener.class)));
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
        ProbeStoreListener listener = Mockito.mock(ProbeStoreListener.class);
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
        store.registerNewProbe(probe1, transport);
        store.registerNewProbe(probe2, transport2);
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
        ProbeStoreListener listener = Mockito.mock(ProbeStoreListener.class);
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

        store.registerNewProbe(probe1, transport);

        // Incompatibility.
        when(idProvider.getProbeId(any())).thenThrow(new IdentityProviderException("BOO!"));
        expectedException.expect(ProbeException.class);
        store.registerNewProbe(probe2, transport);
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
        ProbeStoreListener listener = Mockito.mock(ProbeStoreListener.class);
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

        store.registerNewProbe(probe1, transport);
        store.removeTransport(transport);
        verify(stitchingOperationStore).removeOperationsForProbe(anyLong());
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();

        when(idProvider.getProbeId(any())).thenThrow(new IdentityProviderException("BOO!"));
        expectedException.expect(ProbeException.class);
        store.registerNewProbe(probe2, transport2);
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
        ProbeStoreListener listener = Mockito.mock(ProbeStoreListener.class);
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

        store.registerNewProbe(probe1, transport);
        assertEquals(probe1, store.getProbe(probeId).get());
        store.removeTransport(transport);
        verify(stitchingOperationStore).removeOperationsForProbe(anyLong());
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();

        store.registerNewProbe(probe2, transport2);
        assertEquals(probe2, store.getProbe(probeId).get());

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
                .setProbeCategory("Storage").addTargetIdentifierField("targetId").build();
        final ProbeInfo hypervisorProbe = ProbeInfo.newBuilder().setProbeType("probe-type-stitching-order-2")
                .setProbeCategory("HYPERVISOR").addTargetIdentifierField("targetId").build();
        final ProbeInfo fabricProbe = ProbeInfo.newBuilder().setProbeType("probe-type-stitching-order-3")
                .setProbeCategory("Fabric").addTargetIdentifierField("targetId").build();
        final long storageProbeId = 2345L;
        final long hypervisorProbeId = 3456L;
        final long fabricProbeId = 4567L;
        Mockito.when(idProvider.getProbeId(storageProbe)).thenReturn(storageProbeId);
        Mockito.when(idProvider.getProbeId(hypervisorProbe)).thenReturn(hypervisorProbeId);
        Mockito.when(idProvider.getProbeId(fabricProbe)).thenReturn(fabricProbeId);
        store.registerNewProbe(storageProbe, transport);
        assertEquals(storageProbe, store.getProbe(storageProbeId).get());
        store.registerNewProbe(hypervisorProbe, transport);
        assertEquals(hypervisorProbe, store.getProbe(hypervisorProbeId).get());
        store.registerNewProbe(fabricProbe, transport);
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
        return (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(
            ITransport.class);
    }
}
