package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo.TargetIdSet;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Class that tests a PersistentListenerProbeContainerChooser.
 */
public class ProbeContainerChooserTest {

    private ProbeStore probeStore;
    private TargetStore targetStore;

    private ProbeContainerChooser chooser;

    private final ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
    private static final  Long probeId = 1L;
    private static final String targetIdentifier1 = "targetId1";
    private static final String targetIdentifier2 = "targetId2";
    private static final String targetIdentifier3 = "targetId3";

    private static final String targetIdentifier4 = "targetId4";

    private TargetSpec mockSpec = TargetSpec.getDefaultInstance();
    private final Target target1 = mockTarget(targetIdentifier1);
    private final Target target2 = mockTarget(targetIdentifier2);
    private final Target target3 = mockTarget(targetIdentifier3);

    private final Target target4 = mockTarget(targetIdentifier4);

    private final ITransport<MediationServerMessage, MediationClientMessage> transport1 =
            createTransport();
    private final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            createTransport();
    private final ITransport<MediationServerMessage, MediationClientMessage> transport3 =
            createTransport();

    private final MediationServerMessage validationRequest =
        MediationServerMessage.newBuilder().setValidationRequest(ValidationRequest.newBuilder()
            .setProbeType("pType").build()).build();

    private final MediationServerMessage discoveryRequest =
        MediationServerMessage.newBuilder().setDiscoveryRequest(DiscoveryRequest.newBuilder()
            .setProbeType("pType").setDiscoveryType(DiscoveryType.FULL).build()).build();
    private static final ContainerInfo info = ContainerInfo.newBuilder().addProbes(Probes.incrementalProbe).build();

    /**
     * Initialize variables and mocks.
     *
     * @throws ProbeException if one encountered.
     */
    @Before
    public void setup() throws ProbeException {
        probeStore = Mockito.mock(ProbeStore.class);
        targetStore = Mockito.mock(TargetStore.class);

        when(probeStore.getProbe(probeId)).thenReturn(Optional.of(Probes.incrementalProbe));
        when(probeStore.getTransportsForTarget(any(Target.class))).thenAnswer(
                (Answer<Collection<ITransport<MediationServerMessage, MediationClientMessage>>>)invocation -> {
                    Object[] args = invocation.getArguments();
                    Target target = (Target)args[0];
                    return probeStore.getTransport(target.getProbeId());
                });
        when(probeStore.getProbeInfoForType(Probes.incrementalProbe.getProbeType()))
                .thenReturn(Optional.of(Probes.incrementalProbe));
        when(probeStore.getProbeIdForType(Probes.incrementalProbe.getProbeType()))
                .thenReturn(Optional.of(probeId));
        when(probeStore.getChannel(any())).thenReturn(Optional.empty());
        when(targetStore.getProbeTargets(probeId)).thenReturn(Arrays.asList(target1, target2, target3));

        when(scheduledExecutorService.schedule(any(Runnable.class), any(Long.class), eq(TimeUnit.SECONDS)))
                .thenReturn(mock(ScheduledFuture.class));

        this.chooser = new ProbeContainerChooserImpl(probeStore, targetStore, scheduledExecutorService, 30);
    }

    /**
     * Tests getting the transport for different targets using round robin.
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testRoundRobinOrderForValidationRequest() throws ProbeException {
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        when(probeStore.getTransport(probeId)).thenReturn(transportList);

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
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1);
        when(probeStore.getTransport(probeId)).thenReturn(transportList);
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
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
            transportList = Lists.newArrayList(transport1, transport2);
        when(probeStore.getTransport(probeId)).thenReturn(transportList);
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
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                transportList = Lists.newArrayList(transport1);
        when(probeStore.getTransport(probeId)).thenReturn(transportList);
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);
        // If probe restarts, the available transports might be different than the previous ones
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                newTransportList = Lists.newArrayList(transport2);
        when(probeStore.getTransport(probeId)).thenReturn(newTransportList);
        assertEquals(chooser.choose(target2, discoveryRequest), transport2);
    }

    /**
     * Tests that calling assignTargetToTransport overwrites existing assignments.
     *
     * @throws ProbeException if probe can't be found.
     */
    @Test
    public void testAssignTargetToTransport() throws ProbeException {
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                transportList = Lists.newArrayList(transport1);
        when(probeStore.getTransport(probeId)).thenReturn(transportList);
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);
        // assert that when transport1 is now assigned to target1 even if other transports are
        // available
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                newTransportList = Lists.newArrayList(transport1, transport2);
        when(probeStore.getTransport(probeId)).thenReturn(newTransportList);
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);

        // now assign transport2 to target 1 and confirm that chooser now chooses transport2
        ContainerInfo info =
            ContainerInfo.newBuilder().putPersistentTargetIdMap(Probes.incrementalProbe.getProbeType(),
            TargetIdSet.newBuilder().addTargetId(targetIdentifier1).build()).build();
        chooser.onTransportRegistered(info, transport2);
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
        final String channel2 = "channel2";

        final TargetSpec specWithChannel2 =
            TargetSpec.newBuilder().setProbeId(probeId).setCommunicationBindingChannel(channel2).build();
        when(target2.getSpec()).thenReturn(specWithChannel2);
        when(probeStore.getTransportsForTarget(target2)).thenReturn(
                Collections.singletonList(transport2));
        assertEquals(chooser.choose(target2, discoveryRequest), transport2);
    }

    /**
     * Tests that given a target with a channel and no transports with that channel we throw a
     * {@link ProbeException}.
     *
     * @throws ProbeException if there are no transports available.
     */
    @Test(expected = ProbeException.class)
    public void testNoAvailableTransportsWithChannel() throws ProbeException {
        final String channel1 = "channel1";

        final TargetSpec specWithChannel =
            TargetSpec.newBuilder().setProbeId(probeId).setCommunicationBindingChannel(channel1).build();
        when(target1.getSpec()).thenReturn(specWithChannel);
        when(probeStore.getTransportsForTarget(target1)).thenReturn(Collections.emptySet());

        chooser.choose(target1, discoveryRequest);
    }

    /**
     * Test that targets are rebalanced evenly among transports. If we have 20 targets and 10
     * transports, we expect each transport to be assigned 2 targets.
     *
     * @throws ProbeException if probe can't be found
     */
    @Test
    public void testRebalanceTargetsEvenlyAmongTransports() throws ProbeException {
        final int numTargets = 20;
        final int numTransports = 10;
        List<Target> targets = new ArrayList<>();
        for (int i = 0; i < numTargets; i++) {
            targets.add(mockTarget(String.valueOf(i)));
        }
        List<ITransport<MediationServerMessage, MediationClientMessage>> transports = new ArrayList<>();
        for (int i = 0; i < numTransports; i++) {
            transports.add(createTransport());
        }

        when(targetStore.getProbeTargets(probeId)).thenReturn(targets);
        when(probeStore.getTransport(probeId)).thenReturn(transports);

        transports.forEach(transport -> chooser.onTransportRegistered(info, transport));
        captureLastRebalanceTaskAndExecute(numTransports);

        // verify that 20 targets are spread evenly across 10 transport, 2 for each
        Map<ITransport<MediationServerMessage, MediationClientMessage>, List<Target>>
                targetsByTransport = targets.stream().collect(Collectors.groupingBy(
                target -> chooser.getTransportByTargetId(createTargetId(target))));
        assertTrue(targetsByTransport.keySet().containsAll(transports));
        assertTrue(targetsByTransport.values().stream().flatMap(List::stream)
                .collect(Collectors.toList()).containsAll(targets));
        targetsByTransport.values().forEach(values -> assertEquals(numTargets / numTransports, values.size()));

        // if we manually do another rebalance, the mapping should stay the same since it's already balanced
        ((ProbeContainerChooserImpl)chooser).rebalance(Probes.incrementalProbe.getProbeType());
        Map<ITransport<MediationServerMessage, MediationClientMessage>, List<Target>>
                newTargetsByTransport = targets.stream().collect(Collectors.groupingBy(
                target -> chooser.getTransportByTargetId(createTargetId(target))));
        assertEquals(targetsByTransport, newTargetsByTransport);
    }

    /**
     * Test rebalance results after each transport register, step by step.
     *
     * @throws ProbeException if probe can't be found
     */
    @Test
    public void testRebalanceTargetsOnTransportRegisterOneByOne() throws ProbeException {
        // initial transport has 3 targets
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1));
        assertEquals(chooser.choose(target1, discoveryRequest), transport1);
        assertEquals(chooser.choose(target2, discoveryRequest), transport1);
        assertEquals(chooser.choose(target3, discoveryRequest), transport1);

        // register a new transport
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2));
        chooser.onTransportRegistered(info, transport2);
        captureLastRebalanceTaskAndExecute(1);
        // confirm that one transport have 2 targets while the other one has 1
        List<ITransport<MediationServerMessage, MediationClientMessage>> result = Arrays.asList(
                chooser.choose(target1, discoveryRequest),
                chooser.choose(target2, discoveryRequest),
                chooser.choose(target3, discoveryRequest));
        assertTrue(result.containsAll(Arrays.asList(transport1, transport1, transport2))
                || result.containsAll(Arrays.asList(transport1, transport2, transport2)));

        // register another new transport
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2, transport3));
        chooser.onTransportRegistered(info, transport3);
        captureLastRebalanceTaskAndExecute(2);
        // confirm each transport has one target
        result = Arrays.asList(chooser.choose(target1, discoveryRequest),
                chooser.choose(target2, discoveryRequest),
                chooser.choose(target3, discoveryRequest));
        assertTrue(result.containsAll(Arrays.asList(transport1, transport2, transport3)));
    }

    /**
     * Test that old rebalance tasks are canceled if new transport registers immediately, and new
     * rebalance task is scheduled, so we don't rebalance multiple times if multiple registers
     * within short time.
     *
     * @throws ProbeException if probe can't be found
     */
    @Test
    public void testRebalanceTargetsOnTransportRegisterWithDelay() throws ProbeException {
        final ScheduledFuture scheduledFuture1 = mock(ScheduledFuture.class);
        when(scheduledExecutorService.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS)))
                .thenReturn(scheduledFuture1);
        // first register
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1));
        chooser.onTransportRegistered(info, transport1);
        // initial transport has 3 targets
        assertEquals(transport1, chooser.choose(target1, discoveryRequest));
        assertEquals(transport1, chooser.choose(target2, discoveryRequest));
        assertEquals(transport1, chooser.choose(target3, discoveryRequest));

        final ScheduledFuture scheduledFuture2 = mock(ScheduledFuture.class);
        when(scheduledExecutorService.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS)))
                .thenReturn(scheduledFuture2);
        // two more register immediately thus previous scheduled rebalance task should be canceled
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2));
        chooser.onTransportRegistered(info, transport2);
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2, transport3));
        chooser.onTransportRegistered(info, transport3);
        // verify the old tasks were canceled
        verify(scheduledFuture1).cancel(false);
        verify(scheduledFuture2).cancel(false);
        captureLastRebalanceTaskAndExecute(3);
        // verify final rebalance task is executed and targets are balanced now
        List<ITransport<MediationServerMessage, MediationClientMessage>> result = Arrays.asList(
                chooser.choose(target1, discoveryRequest),
                chooser.choose(target2, discoveryRequest),
                chooser.choose(target3, discoveryRequest));
        assertTrue(result.containsAll(Arrays.asList(transport1, transport2, transport3)));
    }

    /**
     * Test that rebalance task respects target channel binding. Given existing mapping:
     * - transport1 (channel1) --> 2 targets
     * - transport2 (channel2) --> 4 targets
     * - transport3 (no channel) --> 6 targets
     * Then 3 more transport register: transport4 (channel1), transport5 (channel5) and transport6 (no channel).
     * After a rebalance, we expect the mapping to be:
     * - transport1 (channel1) --> 2 targets
     * - transport4 (channel1) --> 2 targets
     * - transport2 (channel2) --> 2 targets
     * - transport5 (channel2) --> 2 targets
     * - transport3 (no channel) --> 2 targets
     * - transport6 (no channel) --> 2 targets
     */
    @Test
    public void testRebalanceTargetsWithMixedChannels() throws ProbeException {
        final List<Target> targets = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            targets.add(mockTarget(String.valueOf(i)));
        }

        // transport1 (channel1) has 2 targets
        when(probeStore.getChannel(transport1)).thenReturn(Optional.of("channel1"));
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1));
        when(targetStore.getProbeTargets(probeId)).thenReturn(targets.subList(0, 2));
        TargetSpec specWithChannel1 = TargetSpec.newBuilder().setProbeId(probeId)
                .setCommunicationBindingChannel("channel1").build();
        targets.subList(0, 2).forEach(target -> when(target.getSpec()).thenReturn(specWithChannel1));
        chooser.onTransportRegistered(info, transport1);
        captureLastRebalanceTaskAndExecute(1);

        // transport2 (channel2) has 4 targets
        when(probeStore.getChannel(transport2)).thenReturn(Optional.of("channel2"));
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2));
        when(targetStore.getProbeTargets(probeId)).thenReturn(targets.subList(0, 6));
        TargetSpec specWithChannel2 = TargetSpec.newBuilder().setProbeId(probeId)
                .setCommunicationBindingChannel("channel2").build();
        targets.subList(2, 6).forEach(target -> when(target.getSpec()).thenReturn(specWithChannel2));
        chooser.onTransportRegistered(info, transport2);
        captureLastRebalanceTaskAndExecute(2);

        // transport3 (no channel) has 6 targets
        when(probeStore.getChannel(transport3)).thenReturn(Optional.empty());
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2, transport3));
        when(targetStore.getProbeTargets(probeId)).thenReturn(targets.subList(0, 12));
        chooser.onTransportRegistered(info, transport3);
        captureLastRebalanceTaskAndExecute(3);

        // register 3 new transports
        ITransport<MediationServerMessage, MediationClientMessage> transport4 = createTransport();
        when(probeStore.getChannel(transport4)).thenReturn(Optional.of("channel1"));
        ITransport<MediationServerMessage, MediationClientMessage> transport5 = createTransport();
        when(probeStore.getChannel(transport5)).thenReturn(Optional.of("channel2"));
        ITransport<MediationServerMessage, MediationClientMessage> transport6 = createTransport();
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2,
                transport3, transport4, transport5, transport6));
        chooser.onTransportRegistered(info, transport4);
        chooser.onTransportRegistered(info, transport5);
        chooser.onTransportRegistered(info, transport6);
        captureLastRebalanceTaskAndExecute(6);

        // verify
        final Map<ITransport<MediationServerMessage, MediationClientMessage>, List<Target>>
                transportToTargets = targets.stream().collect(Collectors.groupingBy(
                        target -> chooser.getTransportByTargetId(createTargetId(target))));
        assertEquals(6, transportToTargets.size());
        transportToTargets.forEach((key, value) -> assertEquals(2, value.size()));
        // check channel binding are respected
        // targets on these transport should have no channel
        transportToTargets.get(transport3).forEach(t -> assertFalse(t.getSpec().hasCommunicationBindingChannel()));
        transportToTargets.get(transport6).forEach(t -> assertFalse(t.getSpec().hasCommunicationBindingChannel()));

        // targets on these transport should have same channel or no channel
        transportToTargets.get(transport1).forEach(t -> assertTrue(
                !t.getSpec().hasCommunicationBindingChannel()
                        || t.getSpec().getCommunicationBindingChannel().equals("channel1")));
        transportToTargets.get(transport4).forEach(t -> assertTrue(
                !t.getSpec().hasCommunicationBindingChannel()
                        || t.getSpec().getCommunicationBindingChannel().equals("channel1")));
        transportToTargets.get(transport2).forEach(t -> assertTrue(
                !t.getSpec().hasCommunicationBindingChannel()
                        || t.getSpec().getCommunicationBindingChannel().equals("channel2")));
        transportToTargets.get(transport5).forEach(t -> assertTrue(
                !t.getSpec().hasCommunicationBindingChannel()
                        || t.getSpec().getCommunicationBindingChannel().equals("channel2")));
    }

    /**
     * Test the targets on the removed transport are rebalanced to other available transports.
     *
     * @throws ProbeException if probe can't be found
     */
    @Test
    public void testRebalanceTargetsOnTransportRemoved() throws ProbeException {
        // mock two targets, one on each transport
        when(targetStore.getProbeTargets(probeId)).thenReturn(Arrays.asList(target1, target2));
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1));
        chooser.onTargetAdded(target1);
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2));
        chooser.onTargetAdded(target2);
        // verify before removing transport
        assertEquals(transport1, chooser.getTransportByTargetId(createTargetId(target1)));
        assertEquals(transport2, chooser.getTransportByTargetId(createTargetId(target2)));

        // remove transport2, then target2 should go to transport1
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1));
        chooser.onTransportRemoved(transport2);
        captureLastRebalanceTaskAndExecute(1);

        assertEquals(transport1, chooser.getTransportByTargetId(createTargetId(target1)));
        assertEquals(transport1, chooser.getTransportByTargetId(createTargetId(target2)));
    }

    /**
     * Test that new target always goes to the transport with least targets.
     *
     * @throws ProbeException if probe can't be found
     */
    @Test
    public void testChooseTransportOnTargetAdded() throws ProbeException {
        // first target goes to transport1, since only one available
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1));
        chooser.onTargetAdded(target1);
        assertEquals(transport1, chooser.getTransportByTargetId(createTargetId(target1)));

        // second target goes to transport2, since first transport already has one
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2));
        chooser.onTargetAdded(target2);
        assertEquals(transport2, chooser.getTransportByTargetId(createTargetId(target2)));

        // third target goes to either transport1 or transport2, since they have same number of targets
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2));
        chooser.onTargetAdded(target3);
        assertTrue(transport1 == chooser.getTransportByTargetId(createTargetId(target3))
                || transport2 == chooser.getTransportByTargetId(createTargetId(target3)));

        // fourth target goes to transport3, since it has least (0) targets
        when(probeStore.getTransport(probeId)).thenReturn(Arrays.asList(transport1, transport2, transport3));
        chooser.onTargetAdded(target4);
        assertEquals(transport3, chooser.getTransportByTargetId(createTargetId(target4)));
    }

    @SuppressWarnings("unchecked")
    private static ITransport<MediationServerMessage, MediationClientMessage> createTransport() {
        return (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(ITransport.class);
    }

    private Target mockTarget(String targetIdentifier) {
        Target target = mock(Target.class);
        when(target.getSerializedIdentifyingFields()).thenReturn(targetIdentifier);
        when(target.getDisplayName()).thenReturn(targetIdentifier);
        when(target.getProbeId()).thenReturn(probeId);
        when(target.getSpec()).thenReturn(mockSpec);
        when(target.getProbeInfo()).thenReturn(Probes.incrementalProbe);
        return target;
    }

    private Pair<String, String> createTargetId(Target target) {
        return new Pair<>(Probes.incrementalProbe.getProbeType(),
                target.getSerializedIdentifyingFields());
    }

    private void captureLastRebalanceTaskAndExecute(int numInvocations) {
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduledExecutorService, times(numInvocations)).schedule(runnableCaptor.capture(), anyLong(), eq(TimeUnit.SECONDS));
        runnableCaptor.getValue().run();
    }
}
