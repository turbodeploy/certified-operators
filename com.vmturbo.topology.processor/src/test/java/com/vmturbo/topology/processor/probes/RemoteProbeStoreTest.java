package com.vmturbo.topology.processor.probes;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
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

        verify(stitchingOperationStore).setOperationsForProbe(eq(1234L), eq(probeInfo));
    }

    @Test
    public void testGetProbeByName() throws Exception {
        when(idProvider.getProbeId(probeInfo)).thenReturn(1234L);
        store.registerNewProbe(probeInfo, transport);

        assertTrue(store.getProbeIdForType(probeInfo.getProbeType()).isPresent());
        assertFalse(store.getProbeIdForType("non-existing-probe").isPresent());
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
        when(idProvider.getProbeId(Mockito.any(ProbeInfo.class))).thenReturn(1234L);

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
    public void testAddTransportForincompatibleProbe() throws Exception {
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
        when(idProvider.getProbeId(Mockito.any(ProbeInfo.class))).thenReturn(1234L);

        store.registerNewProbe(probe1, transport);
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage("differs from already registered probe");
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
    public void testUpdatedProbe() throws Exception {
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
        final long probeId = 1234L;
        Mockito.when(idProvider.getProbeId(Mockito.any(ProbeInfo.class))).thenReturn(probeId);

        store.registerNewProbe(probe1, transport);
        store.removeTransport(transport);
        verify(stitchingOperationStore).removeOperationsForProbe(anyLong());
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
                createTransport();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage("differs from already registered probe");
        store.registerNewProbe(probe2, transport2);
    }

    @SuppressWarnings("unchecked")
    private static ITransport<MediationServerMessage, MediationClientMessage> createTransport() {
        return (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(
                ITransport.class);
    }
}