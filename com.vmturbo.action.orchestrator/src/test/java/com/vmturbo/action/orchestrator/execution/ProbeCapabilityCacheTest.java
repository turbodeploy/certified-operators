package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilities;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilitiesFactory;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CapabilityMatcher;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesResponse;
import com.vmturbo.common.protobuf.topology.Probe.ListProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapabilities;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.MoveParameters;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeMoles.ProbeActionCapabilitiesServiceMole;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;

public class ProbeCapabilityCacheTest {

    private ProbeActionCapabilitiesServiceMole capabilitiesBackendMole =
        spy(new ProbeActionCapabilitiesServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(capabilitiesBackendMole);

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private CachedCapabilitiesFactory cachedCapabilitiesFactory = mock(CachedCapabilitiesFactory.class);

    private ProbeCapabilityCache capabilityCache;

    @Captor
    private ArgumentCaptor<Map<Long, ProbeCategory>> probeCategoryCaptor;

    @Captor
    private ArgumentCaptor<Map<Long, List<ProbeActionCapability>>> probeCapabilityCaptor;

    @Captor
    private ArgumentCaptor<Map<Long, Long>> targetToProbeCaptor;

    private TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        capabilityCache = new ProbeCapabilityCache(topologyProcessor,
            ProbeActionCapabilitiesServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            cachedCapabilitiesFactory);
    }

    @Test
    public void testAddsListeners() {
        verify(topologyProcessor).addProbeListener(capabilityCache);
    }

    @Nonnull
    private ProbeInfo probeInfo(final long probeId, final ProbeCategory probeCategory) {
        final ProbeInfo probeInfo = mock(ProbeInfo.class);
        when(probeInfo.getId()).thenReturn(probeId);
        when(probeInfo.getCategory()).thenReturn(probeCategory.getCategory());
        return probeInfo;
    }

    @Nonnull
    private ProbeActionCapabilities capabilities(final long probeId, final int seed) {
        return ProbeActionCapabilities.newBuilder()
            .setProbeId(probeId)
            .addActionCapabilities(ProbeActionCapability.newBuilder()
                .setEntityType(seed))
            .build();
    }

    @Nonnull
    private TargetInfo targetInfo(final long targetId, final long probeId) {
        final TargetInfo targetInfo = mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        return targetInfo;
    }

    @Test
    public void testFullRefreshError() throws CommunicationException {
        final CachedCapabilities cachedCapabilities = mock(CachedCapabilities.class);
        // An attempt to get cached capabilities should trigger another refresh, since we shouldn't
        // have saved the empty capabilities (because we hit an error trying to refresh).
        final CachedCapabilities cachedCapabilities2 = mock(CachedCapabilities.class);
        when(cachedCapabilitiesFactory.newCapabilities(any(), any(), any()))
            .thenReturn(cachedCapabilities)
            .thenReturn(cachedCapabilities2);
        when(topologyProcessor.getAllProbes()).thenThrow(CommunicationException.class);

        assertThat(capabilityCache.fullRefresh(), is(cachedCapabilities));

        verify(cachedCapabilitiesFactory)
            .newCapabilities(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

        assertThat(capabilityCache.getCachedCapabilities(), is(cachedCapabilities2));

        verify(cachedCapabilitiesFactory, times(2))
            .newCapabilities(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testFirstGetDoesRefresh() throws CommunicationException {
        final CachedCapabilities cachedCapabilities = mock(CachedCapabilities.class);
        when(cachedCapabilitiesFactory.newCapabilities(any(), any(), any()))
            .thenReturn(cachedCapabilities);

        final ProbeInfo probeInfo = probeInfo(2, ProbeCategory.CLOUD_MANAGEMENT);
        final ProbeActionCapabilities capabilities = capabilities(2, 10);
        final TargetInfo targetInfo = targetInfo(3, probeInfo.getId());
        when(topologyProcessor.getAllProbes())
            .thenReturn(ImmutableSet.of(probeInfo));
        when(topologyProcessor.getAllTargets())
            .thenReturn(ImmutableSet.of(targetInfo));
        doReturn(Collections.singletonList(capabilities))
            .when(capabilitiesBackendMole).listProbeActionCapabilities(any());

        assertThat(capabilityCache.getCachedCapabilities(), is(cachedCapabilities));

        // First time there should be a request from all backends.
        verify(topologyProcessor).getAllProbes();
        verify(capabilitiesBackendMole)
            .listProbeActionCapabilities(ListProbeActionCapabilitiesRequest.getDefaultInstance());

        verify(cachedCapabilitiesFactory).newCapabilities(probeCategoryCaptor.capture(),
            probeCapabilityCaptor.capture(), targetToProbeCaptor.capture());

        assertThat(probeCapabilityCaptor.getValue(),
            is(ImmutableMap.of(2L, capabilities.getActionCapabilitiesList())));
        assertThat(probeCategoryCaptor.getValue(),
            is(ImmutableMap.of(2L, ProbeCategory.CLOUD_MANAGEMENT)));
        assertThat(targetToProbeCaptor.getValue(),
            is(ImmutableMap.of(3L, probeInfo.getId())));

        // Second time, we should retrieve the cached ones. No full refresh.
        assertThat(capabilityCache.getCachedCapabilities(), is(cachedCapabilities));

        verifyNoMoreInteractions(cachedCapabilitiesFactory);
    }


    @Test
    public void onProbeRegistered() throws CommunicationException {
        final TopologyProcessorDTO.ProbeInfo probeInfo = TopologyProcessorDTO.ProbeInfo.newBuilder()
            .setId(123L)
            .setCategory(ProbeCategory.CLOUD_NATIVE.getCategory())
            .build();
        final ProbeActionCapabilities probeCapabilities = capabilities(probeInfo.getId(), 1);

        // Return empty sets for ease of checking.
        when(topologyProcessor.getAllTargets())
            .thenReturn(Collections.emptySet());
        when(topologyProcessor.getAllProbes())
            .thenReturn(Collections.emptySet());
        doReturn(Collections.emptyList())
            .when(capabilitiesBackendMole).listProbeActionCapabilities(any());

        final CachedCapabilities cachedCapabilities1 = mock(CachedCapabilities.class);
        when(cachedCapabilitiesFactory.newCapabilities(any(), any(), any()))
            .thenReturn(cachedCapabilities1);

        // First time probe gets registered we do a full refresh (since refresh hasn't been done yet).
        capabilityCache.onProbeRegistered(probeInfo);

        verify(cachedCapabilitiesFactory).newCapabilities(Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap());

        assertThat(capabilityCache.getCachedCapabilities(), is(cachedCapabilities1));

        // Next time a probe gets registered we get the probes capabilities and rebuild the
        // capability cache.
        final CachedCapabilities cachedCapabilities2 = mock(CachedCapabilities.class);
        when(cachedCapabilitiesFactory.newCapabilities(any(), any(), any()))
            .thenReturn(cachedCapabilities2);
        doReturn(GetProbeActionCapabilitiesResponse.newBuilder()
            .addAllActionCapabilities(probeCapabilities.getActionCapabilitiesList())
            .build()).when(capabilitiesBackendMole).getProbeActionCapabilities(any());

        capabilityCache.onProbeRegistered(probeInfo);

        verify(capabilitiesBackendMole).getProbeActionCapabilities(GetProbeActionCapabilitiesRequest.newBuilder()
            .setProbeId(probeInfo.getId())
            .build());
        // Two invocations, because we verified the first one earlier.
        verify(cachedCapabilitiesFactory, times(2)).newCapabilities(
            ImmutableMap.of(probeInfo.getId(), ProbeCategory.CLOUD_NATIVE),
            ImmutableMap.of(probeInfo.getId(), probeCapabilities.getActionCapabilitiesList()),
            Collections.emptyMap());

        assertThat(capabilityCache.getCachedCapabilities(), is(cachedCapabilities2));
    }

    @Test
    public void testCachedCapabilitiesProbeCategory() {
        final CapabilityMatcher capabilityMatcher = mock(CapabilityMatcher.class);
        final CachedCapabilitiesFactory cachedCapabilitiesFactory = new CachedCapabilitiesFactory(capabilityMatcher);
        final long probeId = 1;
        final Map<Long, ProbeCategory> probeCatMap =
            ImmutableMap.of(probeId, ProbeCategory.DATABASE_SERVER);
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
            ImmutableMap.of(probeId, capabilities(probeId, 10).getActionCapabilitiesList());

        final CachedCapabilities capabilities =
            cachedCapabilitiesFactory.newCapabilities(probeCatMap, probeCapabilities,
                Collections.emptyMap());
        assertThat(capabilities.getProbeCategory(probeId).get(), is(ProbeCategory.DATABASE_SERVER));
    }

    @Test
    public void testCachedCapabilitiesUnknownProbe() throws UnsupportedActionException {
        final CapabilityMatcher capabilityMatcher = mock(CapabilityMatcher.class);
        final CachedCapabilitiesFactory cachedCapabilitiesFactory = new CachedCapabilitiesFactory(capabilityMatcher);
        final Map<Long, ProbeCategory> probeCatMap =
            ImmutableMap.of(1L, ProbeCategory.DATABASE_SERVER);
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
            ImmutableMap.of(1L, capabilities(1L, 10).getActionCapabilitiesList());

        final CachedCapabilities capabilities =
            cachedCapabilitiesFactory.newCapabilities(probeCatMap, probeCapabilities,
                Collections.emptyMap());
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);

        assertThat(capabilities.getSupportLevel(
            testAction,
            ActionDTOUtil.getPrimaryEntity(testAction),
            // Non-existing probe.
            123123), is(SupportLevel.UNSUPPORTED));

        // Shouldn't have gotten to the capability matcher.
        verifyZeroInteractions(capabilityMatcher);
    }

    @Test
    public void testCachedCapabilitiesNoApplicableCapabilityElements() throws UnsupportedActionException {
        final CapabilityMatcher capabilityMatcher = mock(CapabilityMatcher.class);
        final CachedCapabilitiesFactory cachedCapabilitiesFactory = new CachedCapabilitiesFactory(capabilityMatcher);
        final long probeId = 1;
        final Map<Long, ProbeCategory> probeCatMap =
            ImmutableMap.of(probeId, ProbeCategory.DATABASE_SERVER);
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
            ImmutableMap.of(probeId, capabilities(probeId, 10).getActionCapabilitiesList());

        final CachedCapabilities capabilities =
            cachedCapabilitiesFactory.newCapabilities(probeCatMap, probeCapabilities, Collections.emptyMap());
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);

        // No matched capabilities
        when(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities.get(1L)))
            .thenReturn(Collections.emptyList());

        // Default support level when no capabilities are provided is show-only
        assertThat(capabilities.getSupportLevel(
            testAction,
            ActionDTOUtil.getPrimaryEntity(testAction),
            probeId), is(SupportLevel.SHOW_ONLY));

        verify(capabilityMatcher).getApplicableCapabilities(testAction, primaryEntity, probeCapabilities.get(probeId));
    }

    @Test
    public void testCachedCapabilitiesUseApplicableElement() throws UnsupportedActionException {
        final CapabilityMatcher capabilityMatcher = mock(CapabilityMatcher.class);
        final CachedCapabilitiesFactory cachedCapabilitiesFactory = new CachedCapabilitiesFactory(capabilityMatcher);
        final long probeId = 1;
        final Map<Long, ProbeCategory> probeCatMap =
            ImmutableMap.of(probeId, ProbeCategory.DATABASE_SERVER);
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
            ImmutableMap.of(probeId, capabilities(probeId, 10).getActionCapabilitiesList());

        final CachedCapabilities capabilities =
            cachedCapabilitiesFactory.newCapabilities(probeCatMap, probeCapabilities,
                Collections.emptyMap());
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);

        // One matched capability.
        when(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities.get(1L)))
            .thenReturn(ImmutableList.of(
                ActionCapabilityElement.newBuilder()
                    .setActionType(ActionType.MOVE)
                    .setActionCapability(ActionCapability.SUPPORTED)
                    .build()));

        assertThat(capabilities.getSupportLevel(
            testAction,
            ActionDTOUtil.getPrimaryEntity(testAction),
            probeId), is(SupportLevel.SUPPORTED));

        verify(capabilityMatcher).getApplicableCapabilities(testAction, primaryEntity, probeCapabilities.get(probeId));
    }

    @Test
    public void testCachedCapabilitiesChooseMinApplicableElement() throws UnsupportedActionException {
        final CapabilityMatcher capabilityMatcher = mock(CapabilityMatcher.class);
        final CachedCapabilitiesFactory cachedCapabilitiesFactory = new CachedCapabilitiesFactory(capabilityMatcher);
        final long probeId = 1;
        final Map<Long, ProbeCategory> probeCatMap =
            ImmutableMap.of(probeId, ProbeCategory.APPLICATION_SERVER);
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
            ImmutableMap.of(probeId, capabilities(probeId, 10).getActionCapabilitiesList());

        final CachedCapabilities capabilities =
            cachedCapabilitiesFactory.newCapabilities(probeCatMap, probeCapabilities,
                Collections.emptyMap());
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);

        // Two matched capabilities - one says not executable, one says supported.
        when(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities.get(probeId)))
            .thenReturn(ImmutableList.of(
                ActionCapabilityElement.newBuilder()
                    .setActionType(ActionType.MOVE)
                    .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                    .build(),
                ActionCapabilityElement.newBuilder()
                    .setActionType(ActionType.MOVE)
                    .setActionCapability(ActionCapability.SUPPORTED)
                    .build()));

        // We should use the minimum of the two.
        assertThat(capabilities.getSupportLevel(
            testAction,
            ActionDTOUtil.getPrimaryEntity(testAction),
            probeId), is(SupportLevel.SHOW_ONLY));

        verify(capabilityMatcher).getApplicableCapabilities(testAction, primaryEntity, probeCapabilities.get(probeId));
    }

    @Test
    public void testCachedCapabilitiesFactoryNoProbeCategoryForTargetProbe() throws UnsupportedActionException {
        final CapabilityMatcher capabilityMatcher = mock(CapabilityMatcher.class);
        final CachedCapabilitiesFactory cachedCapabilitiesFactory = new CachedCapabilitiesFactory(capabilityMatcher);
        final long probeId = 1;
        final long targetId = 11;
        // No probe category information.
        final Map<Long, ProbeCategory> probeCatMap = Collections.emptyMap();
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
            ImmutableMap.of(probeId, capabilities(probeId, 10).getActionCapabilitiesList());

        final CachedCapabilities capabilities =
            cachedCapabilitiesFactory.newCapabilities(probeCatMap, probeCapabilities, Collections.emptyMap());
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);

        assertFalse(capabilities.getProbeCategory(targetId).isPresent());

        // Missing probe category should mean the target didn't get added.
        assertThat(capabilities.getSupportLevel(testAction, primaryEntity, targetId),
            is(SupportLevel.UNSUPPORTED));

        verifyZeroInteractions(capabilityMatcher);
    }

    @Test
    public void testCapabilityMatcherEntityTypeNoMatch() throws UnsupportedActionException {
        CapabilityMatcher capabilityMatcher = new CapabilityMatcher();
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);
        final List<ProbeActionCapability> probeCapabilities = ImmutableList.of(
            ProbeActionCapability.newBuilder()
                // Type won't match
                .setEntityType(primaryEntity.getType() + 1)
                .build());

        assertTrue(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities)
            .isEmpty());
    }

    @Test
    public void testCapabilityMatcherActionTypeNoMatch() throws UnsupportedActionException {
        CapabilityMatcher capabilityMatcher = new CapabilityMatcher();
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);
        final List<ProbeActionCapability> probeCapabilities = ImmutableList.of(
            ProbeActionCapability.newBuilder()
                // Type matches.
                .setEntityType(primaryEntity.getType())
                // Capability element for different action type.
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                    .setActionType(ActionType.SUSPEND)
                    .setActionCapability(ActionCapability.SUPPORTED))
                .build());

        assertTrue(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities)
            .isEmpty());
    }

    @Test
    public void testCapabilityMatcherMoveActionNoMoveSpecificInfoNoMatch() throws UnsupportedActionException {
        CapabilityMatcher capabilityMatcher = new CapabilityMatcher();
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);
        final List<ProbeActionCapability> probeCapabilities = ImmutableList.of(
            ProbeActionCapability.newBuilder()
                // Type matches.
                .setEntityType(primaryEntity.getType())
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                    .setActionType(ActionType.MOVE)
                    .setActionCapability(ActionCapability.SUPPORTED))
                    // No move-specific info - no match!
                .build());

        assertTrue(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities)
            .isEmpty());
    }

    @Test
    public void testCapabilityMatcherMoveActionTargetTypesNoMatch() throws UnsupportedActionException {
        CapabilityMatcher capabilityMatcher = new CapabilityMatcher();
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);
        final List<ProbeActionCapability> probeCapabilities = ImmutableList.of(
            ProbeActionCapability.newBuilder()
                // Type matches.
                .setEntityType(primaryEntity.getType())
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                    .setActionType(ActionType.MOVE)
                    .setActionCapability(ActionCapability.SUPPORTED)
                    .setMove(MoveParameters.newBuilder()
                        // Different target entity type - no match!
                        .addTargetEntityType(3)))
                .build());

        assertTrue(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities)
            .isEmpty());
    }

    @Test
    public void testCapabilityMatcherMoveActionTargetTypesMatch() throws UnsupportedActionException {
        CapabilityMatcher capabilityMatcher = new CapabilityMatcher();
        final ActionDTO.Action testAction =
            testActionBuilder.buildMoveAction(1L, 2L, 2, 3L, 2);
        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);

        final ActionCapabilityElement matchingElement = ActionCapabilityElement.newBuilder()
            .setActionType(ActionType.MOVE)
            .setActionCapability(ActionCapability.SUPPORTED)
            .setMove(MoveParameters.newBuilder()
                .addTargetEntityType(3)
                // This is the right type! Should match!
                .addTargetEntityType(2))
            .build();

        final List<ProbeActionCapability> probeCapabilities = ImmutableList.of(
            ProbeActionCapability.newBuilder()
                // Type matches.
                .setEntityType(primaryEntity.getType())
                .addCapabilityElement(matchingElement)
                .build());

        assertThat(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities),
            is(Collections.singletonList(matchingElement)));
    }

    @Test
    public void testCapabilityMatcherCompoundMoveActionTargetTypesPartialMatch() throws UnsupportedActionException {
        CapabilityMatcher capabilityMatcher = new CapabilityMatcher();
        final int destType1 = 2;
        final int destType2 = 3;
        final ActionDTO.Action testAction = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionOrchestratorTestUtils.createActionEntity(10))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(22)
                            .setType(destType1))
                        .setDestination(ActionEntity.newBuilder()
                            .setId(23)
                            .setType(destType1)))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(32)
                            .setType(destType2))
                        .setDestination(ActionEntity.newBuilder()
                            .setId(33)
                            .setType(destType2)))))
            .build();

        final ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(testAction);

        final ActionCapabilityElement matchingElement = ActionCapabilityElement.newBuilder()
            .setActionType(ActionType.MOVE)
            .setActionCapability(ActionCapability.SUPPORTED)
            .setMove(MoveParameters.newBuilder()
                // Match one of the destination types.
                .addTargetEntityType(destType2))
            .build();

        final List<ProbeActionCapability> probeCapabilities = ImmutableList.of(
            ProbeActionCapability.newBuilder()
                // Type matches.
                .setEntityType(primaryEntity.getType())
                .addCapabilityElement(matchingElement)
                .build());

        assertThat(capabilityMatcher.getApplicableCapabilities(testAction, primaryEntity, probeCapabilities),
            is(Collections.singletonList(matchingElement)));
    }
}
