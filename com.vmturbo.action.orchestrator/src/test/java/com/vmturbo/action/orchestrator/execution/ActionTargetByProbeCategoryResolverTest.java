package com.vmturbo.action.orchestrator.execution;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.action.orchestrator.store.ActionCapabilitiesStore;
import com.vmturbo.action.orchestrator.store.ProbeActionCapabilitiesStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * Tests for resolving action execution conflicts by probe category priority.
 */
public class ActionTargetByProbeCategoryResolverTest {

    private TopologyProcessor topologyProcessor;

    private ActionCapabilitiesStore actionCapabilitiesStore;

    private ActionTargetResolver targetResolver;

    private final long ID_1 = 1l;
    private final long ID_2 = 2l;
    private final long ID_3 = 3l;
    private final long NOT_EXISTTING_TARGET = 999l;
    private final long UNKNOWN_CATEGORY_PROBE = 4l;

    @Before
    public void setup() throws CommunicationException, TopologyProcessorException {
        actionCapabilitiesStore = Mockito.mock(ProbeActionCapabilitiesStore.class);
        topologyProcessor = Mockito.mock(TopologyProcessor.class);
        targetResolver = new ActionTargetByProbeCategoryResolver(topologyProcessor, actionCapabilitiesStore);
        final TargetInfo target1 = createTarget(ID_1);
        final TargetInfo target2 = createTarget(ID_2);
        final TargetInfo target3 = createTarget(ID_3);
        final TargetInfo unknownCategoryTarget = createTarget(UNKNOWN_CATEGORY_PROBE);
        // Curently STORAGE has priority 2
        final ProbeInfo probe1 = createProbe("STORAGE", ID_1);
        // It has 1 priority
        final ProbeInfo probe2 = createProbe("LOAD BALANCER", ID_2);
        // It has 8 priority
        final ProbeInfo probe3 = createProbe("FLOW", ID_3);
        final ProbeInfo unknownCategoryProbe = createProbe("UNKNOWN", 0); // We haven't this
        // category
        Mockito.when(topologyProcessor.getTarget(ID_1)).thenReturn(target1);
        Mockito.when(topologyProcessor.getTarget(ID_2)).thenReturn(target2);
        Mockito.when(topologyProcessor.getTarget(ID_3)).thenReturn(target3);
        Mockito.when(topologyProcessor.getTarget(NOT_EXISTTING_TARGET))
                .thenThrow(TopologyProcessorException.class);
        Mockito.when(topologyProcessor.getTarget(UNKNOWN_CATEGORY_PROBE)).thenReturn(unknownCategoryTarget);
        Mockito.when(topologyProcessor.getProbe(ID_1)).thenReturn(probe1);
        Mockito.when(topologyProcessor.getProbe(ID_2)).thenReturn(probe2);
        Mockito.when(topologyProcessor.getProbe(ID_3)).thenReturn(probe3);
        Mockito.when(topologyProcessor.getProbe(UNKNOWN_CATEGORY_PROBE)).thenReturn(unknownCategoryProbe);
    }

    @Nonnull
    private TargetInfo createTarget(@Nonnull long probeId) {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        Mockito.when(targetInfo.getProbeId()).thenReturn(probeId);
        return targetInfo;
    }

    @Nonnull
    private ProbeInfo createProbe(@Nonnull String category, @Nonnull long id) {
        final ProbeInfo probeInfo = Mockito.mock(ProbeInfo.class);
        Mockito.when(probeInfo.getCategory()).thenReturn(category);
        Mockito.when(probeInfo.getId()).thenReturn(id);
        return probeInfo;
    }

    /**
     * Tests resolving target for the conflict when three different targets can execute it.
     *
     * @throws TargetResolutionException when action was null of provided targets set was null or
     * empty
     */
    @Test
    public void testResolveTarget() throws TargetResolutionException {
        final ActionDTO.Action action = createAction();
        final long resolvedTarget = targetResolver.resolveExecutantTarget(action,
                ImmutableSet.of(ID_1, ID_2, ID_3));
        Assert.assertEquals(ID_2, resolvedTarget);
    }

    /**
     * Tests resolving the target for the conflict when provided not existed targetId.
     *
     * @throws TargetResolutionException when action was null of provided targets set was null or
     * empty
     */
    @Test
    public void testResolveTargetWithNotExistingTarget() throws TargetResolutionException {
        final ActionDTO.Action action = createAction();
        final long resolvedTarget = targetResolver.resolveExecutantTarget(action,
                ImmutableSet.of(ID_1, ID_2, ID_3, NOT_EXISTTING_TARGET));
        Assert.assertEquals(ID_2, resolvedTarget);
    }

    /**
     * Tests resolving the target when provided provided the target with unknown probe category
     *
     * @throws TargetResolutionException when action was null of provided targets set was null or
     * empty
     */
    @Test
    public void testResolveTargetOfUnknownProbe() throws TargetResolutionException {
        final ActionDTO.Action action = createAction();
        final long resolvedTarget = targetResolver.resolveExecutantTarget(action,
                ImmutableSet.of(UNKNOWN_CATEGORY_PROBE, ID_3));
        Assert.assertEquals(ID_3, resolvedTarget);
    }

    /**
     * Tests resolving target for the conflict when three different targets can execute it but one
     * of them doesn't support this kind of action.
     *
     * @throws TargetResolutionException when action was null of provided targets set was null or
     * empty
     */
    @Test
    public void testResolveTargetOneNotSupportedTarget() throws TargetResolutionException {
        final Map<Long, List<ProbeActionCapability>> probesCapabilities = ImmutableMap.of(
                ID_1, ImmutableList.of(createProbeActionCapability(ActionCapability.SUPPORTED)),
                ID_2, ImmutableList.of(createProbeActionCapability(ActionCapability.NOT_SUPPORTED)),
                ID_3, ImmutableList.of(createProbeActionCapability(ActionCapability.SUPPORTED)));
        Mockito.when(actionCapabilitiesStore.getCapabilitiesForProbes(
                ImmutableSet.of(ID_1, ID_2, ID_3))).thenReturn(probesCapabilities);
        final ActionDTO.Action action = createAction();
        final long resolvedTarget = targetResolver.resolveExecutantTarget(action,
                ImmutableSet.of(ID_1, ID_2, ID_3));
        Assert.assertEquals(ID_1, resolvedTarget);
    }

    /**
     * Tests resolving target for the conflict when three different targets can execute it but only
     * one of them supports this kind of action.
     *
     * @throws TargetResolutionException when action was null of provided targets set was null or
     * empty
     */
    @Test
    public void testResolveTargetOnlyOneSupportsAction() throws TargetResolutionException {
        final Map<Long, List<ProbeActionCapability>> probesCapabilities = ImmutableMap.of(
                ID_1, ImmutableList.of(createProbeActionCapability(ActionCapability.NOT_SUPPORTED)),
                ID_2, ImmutableList.of(createProbeActionCapability(ActionCapability.NOT_SUPPORTED)),
                ID_3, ImmutableList.of(createProbeActionCapability(ActionCapability.SUPPORTED)));
        Mockito.when(actionCapabilitiesStore.getCapabilitiesForProbes(
                ImmutableSet.of(ID_1, ID_2, ID_3))).thenReturn(probesCapabilities);
        final ActionDTO.Action action = createAction();
        final long resolvedTarget = targetResolver.resolveExecutantTarget(action,
                ImmutableSet.of(ID_1, ID_2, ID_3));
        Assert.assertEquals(ID_3, resolvedTarget);
    }

    /**
     * Tests that for each {@link ActionTypeCase} there is probe priorities.
     */
    @Test
    public void testAllActionTypesHaveProbePriorities() {
        for (ActionTypeCase actionTypeCase : ActionTypeCase.values()) {
            Assert.assertNotNull(ActionTargetByProbeCategoryResolver
                    .getProbePrioritiesFor(actionTypeCase));
        }
    }

    @Nonnull
    private ProbeActionCapability createProbeActionCapability(@Nonnull ActionCapability capability) {
        return ProbeActionCapability.newBuilder().addCapabilityElement(ActionCapabilityElement
                .newBuilder().setActionType(ActionType.RECONFIGURE)
                .setActionCapability(capability)).build();
    }

    private Action createAction() {
        return Action.newBuilder().setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder().setTargetId(1).build())
                .build()).setImportance(1).setExplanation(Explanation.newBuilder().build())
                .setId(1).build();
    }
}
