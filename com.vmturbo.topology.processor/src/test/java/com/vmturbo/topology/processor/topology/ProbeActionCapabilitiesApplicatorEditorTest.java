package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor.EditorSummary;

public class ProbeActionCapabilitiesApplicatorEditorTest {

    private static final long DEFAULT_TARGET_ID = 1L;
    private ProbeActionCapabilitiesApplicatorEditor editor;
    private TargetStore targetStore = mock(TargetStore.class);
    private final Target target = mock(Target.class);

    @Before
    public void setup() {
        editor = new ProbeActionCapabilitiesApplicatorEditor(targetStore);
        when(target.getId()).thenReturn(DEFAULT_TARGET_ID);
        when(targetStore.getAll()).thenReturn(Collections.singletonList(target));
    }

    /**
     * Verify movable is disabled for Container.
     *
     * <p>Scenario:
     *   Target: Kubernetes
     *     CONTAINER: MOVE -> NOT_SUPPORTED
     *   Entity:
     *     Container (id: 1)
     *
     * <p>Result: Movable is disabled for Container.
     */
    @Test
    public void testEditOneCapability() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER, ActionType.MOVE,
                        ActionCapability.NOT_SUPPORTED, "Kubernetes"));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE, 2L));

        final TopologyGraph<TopologyEntity> graph =
                TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        validateCommodityMovable(graph,
                getTopologyEntityPredicate(EntityType.CONTAINER_VALUE),
                builder -> !builder.getMovable());
        assertEquals(0, editorSummary.getMovableToTrueCounter());
        assertEquals(1, editorSummary.getMovableToFalseCounter());
    }

    /**
     * Verify cloneable and suspendable are set to true for ContainerPod.
     *
     * <p>Scenario:
     *   Target: Kubernetes:
     *     CONTAINER_POD: PROVISION -> SUPPORTED
     *                    SUSPEND -> SUPPORTED
     *   Entity:
     *     ContainerPod (id: 1)
     *
     * <p>Result: Cloneable and suspendable are enabled for ContainerPod.
     */
    @Test
    public void testEditMultipleCapabilities() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER_POD, ActionType.PROVISION,
                        ActionCapability.SUPPORTED, ActionType.SUSPEND,
                        ActionCapability.SUPPORTED, "Kubernetes"));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.VCPU_REQUEST.getNumber(),
                EntityType.CONTAINER_POD_VALUE, 2L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.CONTAINER_POD_VALUE,
                AnalysisSettings::getCloneable, AnalysisSettings::getSuspendable);
        assertEquals(1, editorSummary.getCloneableToTrueCounter());
        assertEquals(1, editorSummary.getSuspendableToTrueCounter());
    }

    /**
     * Verify movable is enabled for Virtual Machine when no action capability for VM is provided
     * by the probe.
     *
     * <p>Scenario:
     *   Target: Kubernetes:
     *     CONTAINER: MOVE -> NOT_SUPPORTED
     *   Entity:
     *     VirtualMachine (id: 2)
     *
     * <p>Result: Movable is enabled for Virtual Machine. (No action capabilities for Virtual
     *   Machine are set by the probe, the editor treats VM Move action as NOT_EXECUTABLE, and
     *   enables the action for market analysis)
     */
    @Test
    public void testEditMovableWithNoRelatedActionCapability() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER, ActionType.MOVE,
                        ActionCapability.NOT_SUPPORTED, "Kubernetes"));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(2L, buildTopologyEntity(2L, CommodityDTO.CommodityType.CLUSTER.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE, 3L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
                getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
                CommoditiesBoughtFromProvider::getMovable);
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(0, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify movable is disabled for VM's ST when movable action capability is unsupported for VV in probe
     *
     * <p>Scenario:
     *   Target: AWS:
     *     VIRTUAL_VOLUME: MOVE -> NOT_SUPPORTED
     *   Entities:
     *     VIRTUAL_VOLUME (id: 4)
     *     VIRTUAL_MACHINE (id: 2)
     *
     * <p>Result: Movable is disabled for storage tier under the VM.
     */
    @Ignore("Re-Enable when AWS Target is ready")
    @Test
    public void testEditMovableVolumeForAWSTarget() {
        final long vvOid = 4L;
        final long vmOid = 2L;
        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.VIRTUAL_VOLUME, ActionType.MOVE, ActionCapability.NOT_SUPPORTED, SDKProbeType.AWS.getProbeType()));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(vmOid, buildVMTopologyEntityWithConnectedVV(vmOid, vvOid, 3L));
        topology.put(vvOid, buildVVTopologyEntity(vvOid));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            CommoditiesBoughtFromProvider::getMovable);
        validateSpecificCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            EntityType.STORAGE_TIER,
            vvOid,
            provider -> !provider.getMovable());

        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify movable is disabled for VM's ST when movable action capability is unsupported for VV in probe
     *
     * <p>Scenario:
     *   Target: AWS:
     *     VIRTUAL_VOLUME: MOVE -> SUPPORTED
     *   Entities:
     *     VIRTUAL_VOLUME (id: 4)
     *     VIRTUAL_MACHINE (id: 2)
     *
     * <p>Result: Movable is enabled for storage tier under the VM.
     */
    @Ignore("Re-Enable when AWS Target is ready")
    @Test
    public void testEditMovableVolumeForAWSTargetWithSupport() {
        final long vvOid = 4L;
        final long vmOid = 2L;
        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.VIRTUAL_VOLUME, ActionType.MOVE, ActionCapability.SUPPORTED, SDKProbeType.AWS.getProbeType()));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(vmOid, buildVMTopologyEntityWithConnectedVV(vmOid, vvOid, 3L));
        topology.put(vvOid, buildVVTopologyEntity(vvOid));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            CommoditiesBoughtFromProvider::getMovable);
        validateSpecificCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            EntityType.STORAGE_TIER,
            vvOid,
            CommoditiesBoughtFromProvider::getMovable);
    }

    /**
     * Verify not executable action capabilities are treated as enabled for analysis.
     *
     * <p>Scenario:
     *   Target: Kubernetes:
     *     APPLICATION: PROVISION -> NOT_EXECUTABLE
     *     CONTAINER: SUSPEND -> NOT_EXECUTABLE
     *   Entities:
     *     Application (id: 1)
     *     Container (id: 2)
     *
     * <p>Result: Cloneable and suspendable are enabled for both Application and Container (When
     *   action capabilities are either not set, or set to NOT_EXECUTABLE for an entity type by the
     *   probe, the action will be enabled for market analysis)
     */
    @Test
    public void testEditNotExecutableAreTreatedAsEnabledForAnalysis() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.APPLICATION, ActionType.PROVISION,
                        EntityType.CONTAINER, ActionType.SUSPEND,
                        ActionCapability.NOT_EXECUTABLE, "Kubernetes"));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.RESPONSE_TIME.getNumber(),
                EntityType.APPLICATION_VALUE, 2L));
        topology.put(2L, buildTopologyEntity(2L, CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE, 3L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.APPLICATION_VALUE,
                AnalysisSettings::getCloneable, AnalysisSettings::getSuspendable);
        verifyAnalysisSettingProperty(graph, EntityType.CONTAINER_VALUE,
                AnalysisSettings::getCloneable, AnalysisSettings::getSuspendable);
        assertEquals(2, editorSummary.getCloneableToTrueCounter());
        assertEquals(2, editorSummary.getSuspendableToTrueCounter());
    }

    /**
     * Verify if an action capability is enabled by probe, but disabled by user policy setting,
     * the user setting takes precedence.
     *
     * <p>Scenario:
     *   Target: Kubernetes:
     *     CONTAINER_POD: PROVISION -> SUPPORTED
     *                    SUSPEND -> SUPPORTED
     *   Entity:
     *     ContainerPod (id: 1) with user policy settings:
     *       Cloneable: Disabled
     *       Suspendable: Disabled
     *
     * <p>Result: Cloneable and suspendable are disabled for ContainerPod.
     */
    @Test
    public void testUserSettingDisabledOverwritesProbeSettingEnabled() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER_POD, ActionType.PROVISION,
                        ActionCapability.SUPPORTED, ActionType.SUSPEND,
                        ActionCapability.SUPPORTED, "Kubernetes"));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.VCPU_REQUEST.getNumber(),
                EntityType.CONTAINER_POD_VALUE, false));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.CONTAINER_POD_VALUE,
                builder -> !builder.getCloneable(), builder -> !builder.getSuspendable());
        assertEquals(0, editorSummary.getCloneableToTrueCounter());
        assertEquals(0, editorSummary.getSuspendableToTrueCounter());
        assertEquals(0, editorSummary.getCloneableToFalseCounter());
        assertEquals(0, editorSummary.getSuspendableToFalseCounter());
    }

    /**
     * Verify when an entity is discovered by two probes, where one probe has action capability set,
     * the other does not have action capability set, the editor ignores the entities discovered by
     * the probe that does not have action capability set.
     *
     * <p>Scenario:
     *   Target 1: Kubernetes (has proper action capability set)
     *     CONTAINER: MOVE -> NOT_SUPPORTED
     *   Target 2: VCenter (does not have proper action capability set)
     *     VIRTUAL_MACHINE: MOVE -> NOT_SUPPORTED
     *   Entities:
     *     Container (id: 1), discovered by Kubernetes target
     *     VirtualMachine (id: 2), discovered by both Kubernetes and VCenter target
     *     Database (id: 3), discovered by VCenter target
     *
     * <p>Result:
     *   Container entity: Movable is disabled (Discovered by Kubernetes target only)
     *   VirtualMachine entity: Movable is enabled (Discovered by both Kubernetes and VCenter
     *     target, ignore action capabilities from VCenter target. Kubernetes target does not set
     *     action capability for VM. Defaults to NOT_EXECUTABLE, thus enabled for analysis)
     *   Database entity: Movable is not set (Discovered by VCenter target only, which does not
     *     have proper action capability set, do not set Movable property)
     *
     */
    @Test
    public void testEditWithTwoProbes() {
        final Target target1 = mock(Target.class);
        final Target target2 = mock(Target.class);
        when(target1.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER, ActionType.MOVE,
                        ActionCapability.NOT_SUPPORTED, "Kubernetes"));
        when(target1.getId()).thenReturn(13L);
        when(target2.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.VIRTUAL_MACHINE, ActionType.MOVE,
                        ActionCapability.NOT_SUPPORTED, "VCenter"));
        when(target2.getId()).thenReturn(14L);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target1, target2));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE, 2L, ImmutableSet.of(13L)));
        topology.put(2L, buildTopologyEntity(2L, CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE, 3L, ImmutableSet.of(13L, 14L)));
        topology.put(3L, buildTopologyEntity(3L, CommodityDTO.CommodityType.CLUSTER.getNumber(),
                EntityType.DATABASE_VALUE, 4L, ImmutableSet.of(14L)));
        final TopologyGraph<TopologyEntity> graph =
                TopologyEntityTopologyGraphCreator.newGraph(topology);
        final EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        validateCommodityMovable(graph,
                getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
                builder -> !builder.hasMovable());
        validateCommodityMovable(graph,
                getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
                builder -> builder.hasMovable() && builder.getMovable());
        validateCommodityMovable(graph,
                getTopologyEntityPredicate(EntityType.CONTAINER_VALUE),
                builder -> builder.hasMovable() && !builder.getMovable());
        assertEquals(1, editorSummary.getMovableToTrueCounter());
        assertEquals(1, editorSummary.getMovableToFalseCounter());
    }

    private void verifyAnalysisSettingProperty(final TopologyGraph<TopologyEntity> graph, final int entityTypeValue,
                                               final Predicate<AnalysisSettings> cloneablePredicate,
                                               final Predicate<AnalysisSettings> suspendablePredicate) {
        graph.entities().filter(getTopologyEntityPredicate(entityTypeValue)).forEach(entity -> {
            final AnalysisSettings settings = entity
                    .getTopologyEntityDtoBuilder()
                    .getAnalysisSettings();
            assertTrue(cloneablePredicate.test(settings));
            assertTrue(suspendablePredicate.test(settings));
        });
    }

    private void validateCommodityMovable(final TopologyGraph<TopologyEntity> graph,
                                          final Predicate<TopologyEntity> predicate,
                                          final Predicate<CommoditiesBoughtFromProvider> movable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
                assertTrue(entity
                        .getTopologyEntityDtoBuilder()
                        .getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .allMatch(movable)));
    }

    private void validateSpecificCommodityMovable(final TopologyGraph<TopologyEntity> graph,
                                                  final Predicate<TopologyEntity> predicate,
                                                  final EntityType providerEntityType,
                                                  final long volumeId,
                                                  final Predicate<CommoditiesBoughtFromProvider> commoditiesMovable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
            assertTrue(entity
                .getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .filter(provider -> provider.getProviderEntityType() == providerEntityType.getNumber())
                .filter(provider -> provider.getVolumeId() == volumeId)
                .allMatch(commoditiesMovable)
            )
        );
    }

    @Nonnull
    private Predicate<TopologyEntity> getTopologyEntityPredicate(int entityTypeValue) {
        return topologyEntity -> topologyEntity.getEntityType() == entityTypeValue;
    }

    @Nonnull
    private TopologyEntity.Builder buildVVTopologyEntity(long vvOid) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        Collections.singleton(DEFAULT_TARGET_ID).forEach(id -> origin.putDiscoveredTargetData(id,
            PerTargetEntityInformation.getDefaultInstance()));
        return TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder()
                .setAnalysisSettings(AnalysisSettings.newBuilder().build())
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(origin)
                    .build())
                .setOid(vvOid)
        );
    }

    @Nonnull
    private TopologyEntity.Builder buildVMTopologyEntityWithConnectedVV(long vmOid, long vvOid, long providerId) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        Collections.singleton(DEFAULT_TARGET_ID).forEach(id -> origin.putDiscoveredTargetData(id,
            PerTargetEntityInformation.getDefaultInstance()));
        return TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder()
                .setAnalysisSettings(AnalysisSettings.newBuilder().build())
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(origin)
                    .build())
                .setOid(vmOid)
                .addConnectedEntityList(
                    ConnectedEntity.newBuilder()
                        .setConnectedEntityId(vvOid)
                        .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .build()
                )
                .addCommoditiesBoughtFromProviders(
                    CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(providerId)
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setVolumeId(vvOid)
                )
        );
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType, long providerId) {
        return buildTopologyEntity(oid, type, entityType, providerId, Collections.singleton(DEFAULT_TARGET_ID));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType,
                                                       long providerId, final Collection<Long> targetIds) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        targetIds.forEach(id -> origin.putDiscoveredTargetData(id,
                          PerTargetEntityInformation.getDefaultInstance()));
        return TopologyEntityUtils.topologyEntityBuilder(
                TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().build())
                        .setEntityType(entityType)
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(origin)
                                .build())
                        .setOid(oid).addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(providerId)
                                .addCommodityBought(
                                        CommodityBoughtDTO.newBuilder()
                                                .setCommodityType(
                                                        CommodityType.newBuilder().setType(type).setKey("").build()
                                                ).setActive(true)
                                )
                ));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType, boolean isEnabled) {
        return TopologyEntityUtils.topologyEntityBuilder(
                TopologyEntityDTO.newBuilder()
                        .setEntityType(entityType)
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(DEFAULT_TARGET_ID,
                                            PerTargetEntityInformation.getDefaultInstance())
                                        .build())
                                .build())
                        .setAnalysisSettings(AnalysisSettings.newBuilder()
                                .setSuspendable(isEnabled)
                                .setCloneable(isEnabled)
                                .build())
                        .setOid(oid).addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                                .addCommodityBought(
                                        CommodityBoughtDTO.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder().setType(type).setKey("").build()
                                                ).setActive(true)
                                )
                ));
    }

    private ProbeInfo getProbeInfo(final EntityType entityType,
                                   final ActionType change,
                                   final ActionCapability capability,
                                   final String probeType
    ) {
        return ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType(probeType)
                .addTargetIdentifierField("field")
                .addActionPolicy(ActionPolicyDTO.newBuilder()
                        .addPolicyElement(ActionPolicyElement.newBuilder()
                                .setActionType(change)
                                .setActionCapability(capability).build())
                        .setEntityType(entityType).build())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("name")
                                .setDisplayName("displayName")
                                .setDescription("description")
                                .setIsSecret(true)))
                .build();
    }

    private ProbeInfo getProbeInfo(final EntityType firstEntityType,
                                   final ActionType firstActionType,
                                   final EntityType secondEntityType,
                                   final ActionType sendActionType,
                                   final ActionCapability capability,
                                   final String probeType) {
        return ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType(probeType)
                .addTargetIdentifierField("field")
                .addActionPolicy(ActionPolicyDTO.newBuilder()
                        .addPolicyElement(ActionPolicyElement.newBuilder()
                                .setActionType(firstActionType)
                                .setActionCapability(capability).build())
                        .setEntityType(firstEntityType).build())
                .addActionPolicy(ActionPolicyDTO.newBuilder()
                        .addPolicyElement(ActionPolicyElement.newBuilder()
                                .setActionType(sendActionType)
                                .setActionCapability(capability).build())
                        .setEntityType(secondEntityType).build())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("name")
                                .setDisplayName("displayName")
                                .setDescription("description")
                                .setIsSecret(true)))
                .build();
    }

    private ProbeInfo getProbeInfo(final EntityType firstEntityType,
                                   final ActionType firstActionType,
                                   final ActionCapability firstCapability,
                                   final ActionType sendActionType,
                                   final ActionCapability secondCapability,
                                   final String probeType
    ) {
        return ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType(probeType)
                .addTargetIdentifierField("field")
                .addActionPolicy(ActionPolicyDTO.newBuilder()
                        .addPolicyElement(ActionPolicyElement.newBuilder()
                                .setActionType(firstActionType)
                                .setActionCapability(firstCapability).build())
                        .addPolicyElement(ActionPolicyElement.newBuilder()
                                .setActionType(sendActionType)
                                .setActionCapability(secondCapability).build())
                        .setEntityType(firstEntityType).build())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("name")
                                .setDisplayName("displayName")
                                .setDescription("description")
                                .setIsSecret(true)))
                .build();
    }
}
