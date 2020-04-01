package com.vmturbo.topology.processor.probes;

import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.FakeTransport.TransportPair;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Unit test probe registration process in {@link RemoteMediationServer}.
 *
 * Load the TargetConfig to provide a target store to use for RemoteMediationServer.
 */
public class ProbeRegistrationTest {

    private static final String DEFAULT_PROBE_TYPE = "Some probe type";

    private ITransport<MediationServerMessage, MediationClientMessage> server;
    private RemoteMediationServer remoteMediation;
    private IdentityProvider identityProvider;
    private ProbeInfo.Builder probeInfoBuilder;

    private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
    private final KeyValueStore keyValueStore = mock(KeyValueStore.class);

    @Before
    public final void init() {

        final TransportPair<MediationServerMessage, MediationClientMessage> transportPair =
                        FakeTransport.createSymmetricTransport();
        server = transportPair.getServerTransport();
        identityProvider = new IdentityProviderImpl(
                new IdentityService(
                        new IdentityServiceInMemoryUnderlyingStore(
                                mock(IdentityDatabaseStore.class)),
                        new HeuristicsMatcher()),
                new MapKeyValueStore(), new ProbeInfoCompatibilityChecker(), 0L);
        remoteMediation = new RemoteMediationServer(new RemoteProbeStore(keyValueStore,
                identityProvider, stitchingOperationStore), Mockito.mock(ProbePropertyStore.class));
        probeInfoBuilder = ProbeInfo.newBuilder(Probes.defaultProbe);
    }

    /**
     * Tests registration of one probe.
     */
    @Test
    public void testProbeRegistration() {
        final ProbeInfo probeInfo = probeInfoBuilder.build();
        final ContainerInfo containerInfo = ContainerInfo.newBuilder().addProbes(probeInfo).build();

        remoteMediation.registerTransport(containerInfo, server);
        Assert.assertEquals(Collections.singleton(probeInfo), remoteMediation.getConnectedProbes());
    }

    /**
     * Tests registration of different probes.
     */
    @Test
    public void testSeveralProbesRegistration() {
        final ProbeInfo probeInfo1 = probeInfoBuilder.build();
        probeInfoBuilder.setProbeType(DEFAULT_PROBE_TYPE + "2");
        final ProbeInfo probeInfo2 = probeInfoBuilder.build();
        probeInfoBuilder.setProbeType(DEFAULT_PROBE_TYPE + "3");
        final ProbeInfo probeInfo3 = probeInfoBuilder.build();
        final ContainerInfo containerInfo =
                        ContainerInfo.newBuilder().addProbes(probeInfo1).addProbes(probeInfo2)
                                        .addProbes(probeInfo3).build();
        remoteMediation.registerTransport(containerInfo, server);
        Assert.assertEquals(Sets.newHashSet(probeInfo1, probeInfo2, probeInfo3),
                        remoteMediation.getConnectedProbes());
    }

    /**
     * Tests registration of different transport with the same probe.
     */
    @Test
    public void testSameProbeDifferentTransports() {
        final ProbeInfo probeInfo = probeInfoBuilder.build();
        final ContainerInfo containerInfo = ContainerInfo.newBuilder().addProbes(probeInfo).build();
        remoteMediation.registerTransport(containerInfo, server);
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
                        FakeTransport.<MediationServerMessage, MediationClientMessage>createSymmetricTransport()
                                        .getServerTransport();
        Assert.assertEquals(Sets.newHashSet(probeInfo), remoteMediation.getConnectedProbes());
        remoteMediation.registerTransport(containerInfo, transport2);
        Assert.assertEquals(Sets.newHashSet(probeInfo), remoteMediation.getConnectedProbes());
        server.close();
        Assert.assertEquals(Sets.newHashSet(probeInfo), remoteMediation.getConnectedProbes());
        transport2.close();
        Assert.assertEquals(Collections.emptySet(), remoteMediation.getConnectedProbes());
    }

    /**
     * Tests registration of invalid probe.
     */
    @Test
    public void testInvalidProbesRegistration() {
        final ProbeInfo probeInfo1 = probeInfoBuilder.build();
        probeInfoBuilder.setProbeCategory("Another category").setUiProbeCategory("uiProbeCat");
        final ProbeInfo probeInfo2 = probeInfoBuilder.build();
        final ContainerInfo containerInfo =
                        ContainerInfo.newBuilder().addProbes(probeInfo1).addProbes(probeInfo2)
                                        .build();
        remoteMediation.registerTransport(containerInfo, server);
        Assert.assertEquals(Sets.newHashSet(probeInfo1), remoteMediation.getConnectedProbes());
    }
}
