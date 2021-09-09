package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo.TargetIdSet;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Class that tests a PersistentListenerProbeContainerChooser.
 */
public class ProbeContainerChooserTest {

    ProbeStore probeStore;
    private static final  Long probeId = 1L;
    private static final String targetIdentifier1 = "targetId1";
    private static final String targetIdentifier2 = "targetId2";
    private static final String targetIdentifier3 = "targetId3";

    private final Target target1 = Mockito.mock(Target.class);
    private final Target target2 = Mockito.mock(Target.class);
    private final Target target3 = Mockito.mock(Target.class);

    private TargetSpec mockSpec = TargetSpec.getDefaultInstance();

    private final MediationServerMessage validationRequest =
        MediationServerMessage.newBuilder().setValidationRequest(ValidationRequest.newBuilder()
            .setProbeType("pType").build()).build();

    private final MediationServerMessage discoveryRequest =
        MediationServerMessage.newBuilder().setDiscoveryRequest(DiscoveryRequest.newBuilder()
            .setProbeType("pType").setDiscoveryType(DiscoveryType.FULL).build()).build();

    /**
     * Initialize variables and mocks.
     */
    @Before
    public void setup() {
        probeStore = Mockito.mock(ProbeStore.class);

        Mockito.when(probeStore.getProbe(probeId)).thenReturn(Optional.of(Probes.incrementalProbe));
        Mockito.when(target1.getSerializedIdentifyingFields()).thenReturn(targetIdentifier1);
        Mockito.when(target1.getProbeId()).thenReturn(probeId);
        Mockito.when(target1.getSpec()).thenReturn(mockSpec);
        Mockito.when(target2.getSerializedIdentifyingFields()).thenReturn(targetIdentifier2);
        Mockito.when(target2.getProbeId()).thenReturn(probeId);
        Mockito.when(target2.getSpec()).thenReturn(mockSpec);
        Mockito.when(target3.getSerializedIdentifyingFields()).thenReturn(targetIdentifier3);
        Mockito.when(target3.getProbeId()).thenReturn(probeId);
        Mockito.when(target3.getSpec()).thenReturn(mockSpec);

    }

    /**
     * Tests getting the transport for different targets using round robin.
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testRoundRobinOrderForValidationRequest() throws ProbeException {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);

        final ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        // test that if the collection is a singleton, we always choose the same transport
        assertTrue(chooser.choose(target1, validationRequest) == transport1);
        assertTrue(chooser.choose(target1, validationRequest) == transport1);
        transportList.add(transport2);
        // test that if we choose more transports than exist in the list, we cycle back around
        final ITransport<MediationServerMessage, MediationClientMessage> transportChoice1 =
            chooser.choose(target1, validationRequest);
        final ITransport<MediationServerMessage, MediationClientMessage> transportChoice2 =
            chooser.choose(target1, validationRequest);
        final ITransport<MediationServerMessage, MediationClientMessage> transportChoice3 =
            chooser.choose(target1, validationRequest);

        assertNotEquals(transportChoice1, transportChoice2);
        assertEquals(transportChoice1, transportChoice3);

        assertTrue(transportChoice1 == transport1 || transportChoice1 == transport2);
        assertTrue(transportChoice2 == transport1 || transportChoice2 == transport2);
    }

    /**
     * Tests getting the transport for different targets, but one probe instance, for a discovery
     * request.
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testWithOneTransport() throws ProbeException {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);
        ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        // test that if the collection is a singleton, we always choose the same transport
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);
        assertEquals(chooser.choose(target2, discoveryRequest), transport1);
    }

    /**
     * Tests getting the transport for different targets, and different probe instances.
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testMultipleTransports() throws ProbeException {

        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();

        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1, transport2);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);
        ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        // Create new transports, for each target, until transports are available, then use round
        // robin
        assertEquals(chooser.choose(target1, discoveryRequest),
            transportList.get(0));
        assertEquals(chooser.choose(target2, discoveryRequest),
            transportList.get(1));
        assertEquals(chooser.choose(target3, discoveryRequest),
            transportList.get(0));

        // Make sure we can retrieve already created transport for a target
        assertEquals(chooser.choose(target3, discoveryRequest),
            transportList.get(0));
    }

    /**
     * Tests reassigning the transport after a probe restarts.
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testProbeRestart() throws ProbeException {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
                createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
                createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                transportList = Lists.newArrayList(transport1);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);
        ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);
        // If probe restarts, the available transports might be different than the previous ones
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                newTransportList = Lists.newArrayList(transport2);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(newTransportList);
        assertEquals(chooser.choose(target2, discoveryRequest), transport2);
    }

    /**
     * Tests that calling assignTargetToTransport overwrites existing assignments.
     *
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testAssignTargetToTransport() throws ProbeException {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
                createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
                createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                transportList = Lists.newArrayList(transport1);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);
        ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);
        // assert that when transport1 is now assigned to target1 even if other transports are
        // available
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                newTransportList = Lists.newArrayList(transport1, transport2);
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(newTransportList);
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);

        // now assign transport2 to target 1 and confirm that chooser now chooses transport2
        ContainerInfo info =
            ContainerInfo.newBuilder().putPersistentTargetIdMap(Probes.incrementalProbe.getProbeType(),
            TargetIdSet.newBuilder().addTargetId(targetIdentifier1).build()).build();
        chooser.parseContainerInfoWithTransport(info, transport2);
        assertEquals(chooser.choose(target1, discoveryRequest), transport2);
    }

    /**
     * Tests that given a target with a channel and no transports with that channel we throw a
     * {@link ProbeException}.
     *
     * @throws ProbeException if there are no transports available.
     */
    @Test
    public void testTransportWithChannel() throws ProbeException {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1, transport2);
        final String channel2 = "channel2";

        final TargetSpec specWithChannel2 =
            TargetSpec.newBuilder().setProbeId(probeId).setCommunicationBindingChannel(channel2).build();
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);
        Mockito.when(target2.getSpec()).thenReturn(specWithChannel2);
        ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        chooser.parseContainerInfoWithTransport(ContainerInfo.newBuilder()
            .setCommunicationBindingChannel(channel2).build(), transport2);
        assertEquals(chooser.choose(target2, discoveryRequest), transport2);
    }

    /**
     * Tests that given a target with a channel and no transports with that channel we throw a
     * {@link ProbeException}.
     *
     * @throws ProbeException if there are no transports available.
     */
    @Test(expected = ProbeException .class)
    public void testNoAvailableTransportsWithChannel() throws ProbeException {
        final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        final String channel1 = "channel1";
        final String channel2 = "channel2";

        final TargetSpec specWithChannel =
            TargetSpec.newBuilder().setProbeId(probeId).setCommunicationBindingChannel(channel1).build();
        Mockito.when(probeStore.getTransport(probeId)).thenReturn(transportList);
        Mockito.when(target1.getSpec()).thenReturn(specWithChannel);

        ProbeContainerChooser chooser = new ProbeContainerChooserImpl(probeStore);
        final ContainerInfo cInfo =
            ContainerInfo.newBuilder().setCommunicationBindingChannel(channel2)
                .putPersistentTargetIdMap(Probes.incrementalProbe.getProbeType(),
            TargetIdSet.newBuilder().addTargetId(targetIdentifier1).build()).build();

        chooser.parseContainerInfoWithTransport(cInfo, transport1);
        chooser.choose(target1, discoveryRequest);
    }

    @SuppressWarnings("unchecked")
    private static ITransport<MediationServerMessage, MediationClientMessage> createTransport() {
        return (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(ITransport.class);
    }

}
