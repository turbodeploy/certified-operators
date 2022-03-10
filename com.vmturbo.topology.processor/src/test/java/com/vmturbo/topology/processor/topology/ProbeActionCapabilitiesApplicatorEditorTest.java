package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
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
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor.EditorSummary;

public class ProbeActionCapabilitiesApplicatorEditorTest {

    private static final long DEFAULT_TARGET_ID = 1L;

    /**
     * Storage Commodity Type ID.
     */
    private static final int STORAGE_COMMODITY_TYPE_ID = 1234;

    /**
     * Storage Commodity Type.
     */
    private static final CommodityTypeImpl STORAGE_COMMODITY_TYPE =
        new CommodityTypeImpl().setType(STORAGE_COMMODITY_TYPE_ID);

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
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
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
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.VCPU_REQUEST.getNumber(),
                EntityType.CONTAINER_POD_VALUE, 2L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.CONTAINER_POD_VALUE,
                AnalysisSettingsView::getCloneable, AnalysisSettingsView::getSuspendable);
        assertEquals(1, editorSummary.getCloneableToTrueCounter());
        assertEquals(1, editorSummary.getSuspendableToTrueCounter());
    }

    /**
     * Verify movable is not set for Virtual Machine when no action capability for VM is provided
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
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(2L, buildTopologyEntity(2L, CommodityDTO.CommodityType.CLUSTER.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE, 4L));
        topology.put(4L, buildTopologyEntity(4L, CommodityDTO.CommodityType.CLUSTER.getNumber(),
            EntityType.VIRTUAL_VOLUME_VALUE, 3L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
                    getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
                    builder -> !builder.hasMovable());
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            builder -> !builder.hasMovable());
                    //CommoditiesBoughtFromProvider::hasMovable);
        assertEquals(0, movableEditSummary.getMovableToTrueCounter());
        assertEquals(0, movableEditSummary.getMovableToFalseCounter());

    }

    /**
     * Verify movable is disabled for VV's ST when movable action capability is unsupported for VV in probe.
     *
     * <p>Scenario:
     *   Target: AWS:
     *     VIRTUAL_VOLUME: MOVE -> NOT_SUPPORTED
     *   Entities:
     *     VIRTUAL_VOLUME (id: 4)
     *     STORAGE_TIER (id: 3)
     *     VIRTUAL_MACHINE (id: 2)
     *
     * <p>Result: Movable is disabled for storage tier under the VV.
     */
    @Test
    public void testEditMovableVolumeForAWSTarget() {
        final long vvOid = 4L;
        final long stOid = 3L;
        final long vmOid = 2L;
        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.VIRTUAL_VOLUME, ActionType.MOVE, ActionCapability.NOT_SUPPORTED, SDKProbeType.AWS.getProbeType()));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmOid, buildVMTopologyEntityWithVvProvider(vmOid, vvOid));
        topology.put(vvOid, buildVVTopologyEntityWithStProvider(vvOid, stOid));
        topology.put(stOid, buildStTopologyEntity(stOid));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.STORAGE_TIER_VALUE),
            CommoditiesBoughtFromProviderView::getMovable);
        validateSpecificCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            EntityType.STORAGE_TIER,
            provider -> !provider.getMovable());

        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify movable is disabled for VV's ST when movable action capability is supported for VV in probe
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
    @Test
    public void testEditMovableVolumeForAWSTargetWithSupport() {
        final long vvOid = 4L;
        final long stOid = 3L;
        final long vmOid = 2L;
        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.VIRTUAL_VOLUME, ActionType.MOVE, ActionCapability.SUPPORTED, SDKProbeType.AWS.getProbeType()));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmOid, buildVMTopologyEntityWithVvProvider(vmOid, vvOid));
        topology.put(vvOid, buildVVTopologyEntityWithStProvider(vvOid, stOid));
        topology.put(stOid, buildStTopologyEntity(stOid));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.STORAGE_TIER_VALUE),
            CommoditiesBoughtFromProviderView::getMovable);
        validateSpecificCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            EntityType.STORAGE_TIER,
                CommoditiesBoughtFromProviderView::getMovable);
    }

    /**
     * Verify movable is disabled for VV's ST when movable action capability is supported for VV in probe
     *
     * <p>Scenario:
     *   Target: AWS:
     *     STORAGE_TIER: MOVE -> NOT_SUPPORTED
     *   Entities:
     *     VIRTUAL_VOLUME (id: 4)
     *     VIRTUAL_MACHINE (id: 2)
     *     STORAGE_TIER (id: 3)
     *
     * <p>Result: Movable is enabled for storage tier under the VM.
     */
    @Test
    public void testStForAWSTarget() {
        final long vmOid = 2L;
        final long vvOid = 4L;
        final long stOid = 3L;

        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.STORAGE_TIER, ActionType.MOVE, ActionCapability.NOT_SUPPORTED, SDKProbeType.AWS.getProbeType()));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmOid, buildVMTopologyEntityWithVvProvider(vmOid, vvOid));
        topology.put(vvOid, buildVVTopologyEntityWithStProvider(vvOid, stOid));
        topology.put(stOid, buildStTopologyEntity(stOid));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        editor.applyPropertiesEdits(graph);

        validateSpecificCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            EntityType.STORAGE_TIER,
            provider -> !provider.getMovable());
    }

    /**
     * Verify movable is disabled for VV's ST when movable action capability is supported for VV in probe
     *
     * <p>Scenario:
     *   Target: AWS:
     *     STORAGE_TIER: MOVE -> SUPPORTED
     *   Entities:
     *     VIRTUAL_VOLUME (id: 4)
     *     VIRTUAL_MACHINE (id: 2)
     *     STORAGE_TIER (id: 3)
     *
     * <p>Result: Movable is enabled for storage tier under the VM.
     */
    @Test
    public void testStForAWSTarget2() {
        final long vmOid = 2L;
        final long vvOid = 4L;
        final long stOid = 3L;

        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.STORAGE_TIER, ActionType.MOVE, ActionCapability.SUPPORTED, SDKProbeType.AWS.getProbeType()));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmOid, buildVMTopologyEntityWithVvProvider(vmOid, vvOid));
        topology.put(vvOid, buildVVTopologyEntityWithStProvider(vvOid, stOid));
        topology.put(stOid, buildStTopologyEntity(stOid));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        editor.applyPropertiesEdits(graph);

        validateSpecificCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_VOLUME_VALUE),
            EntityType.STORAGE_TIER,
            provider -> !provider.getMovable());
    }

    @Test
    public void testEditScalableDisabledForCloudVMs() {
        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.VIRTUAL_MACHINE,
                    ActionType.SCALE, ActionCapability.NOT_SUPPORTED, "Kubernetes"));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(2L, buildTopologyEntityWithCommBought(2L,  EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.VCPU.getNumber(), true, true,
                Collections.singleton(DEFAULT_TARGET_ID)));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary resizeableEditSummary = editor.applyPropertiesEdits(graph);

        validateCommodityScalable(graph,
                getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
                provider -> !provider.getScalable());
        validateSpecificCommodityScalable(graph,
                getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
                EntityType.PHYSICAL_MACHINE,
                provider -> !provider.getScalable());

        assertEquals(1, resizeableEditSummary.getScalableToFalseCounter());
    }

    @Test
    public void testEditResizeableDisabledForCloudNativeVMs() {
        when(target.getProbeInfo()).thenReturn(getProbeInfo(EntityType.VIRTUAL_MACHINE,
                ActionType.RIGHT_SIZE, ActionCapability.NOT_SUPPORTED, "Kubernetes"));
        boolean defaultEntityLevelResizeable = true;
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(2L, buildTopologyEntityWithCommSold(2L,  EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.VCPU.getNumber(), defaultEntityLevelResizeable,
                Collections.singleton(DEFAULT_TARGET_ID)));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary resizeableEditSummary = editor.applyPropertiesEdits(graph);

        validateCommodityResizeable(graph,
                getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
                builder -> builder.hasIsResizeable() && !builder.getIsResizeable());

        assertEquals(1, resizeableEditSummary.getResizeableToFalseCounter());
    }

    /**
     * Verify not executable action capabilities are treated as enabled for analysis.
     *
     * <p>Scenario:
     *   Target: Kubernetes:
     *     APPLICATION COMPONENT: PROVISION -> NOT_EXECUTABLE
     *   Entities:
     *     Application Component (id: 1)
     *
     * <p>Result: Cloneable and suspendable are enabled for Application and Container (When
     *   action capabilities are either not set, or set to NOT_EXECUTABLE for an entity type by the
     *   probe, the action will be enabled for market analysis)
     */
    @Test
    public void testEditNotExecutableForApplicationsAreTreatedAsEnabledForAnalysis() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.APPLICATION_COMPONENT,
                        ActionType.PROVISION, ActionCapability.NOT_EXECUTABLE,
                        ActionType.SUSPEND, ActionCapability.NOT_EXECUTABLE,
                        "Kubernetes"));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.RESPONSE_TIME.getNumber(),
                EntityType.APPLICATION_COMPONENT_VALUE, 2L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.APPLICATION_COMPONENT_VALUE,
                AnalysisSettingsView::getCloneable, AnalysisSettingsView::getSuspendable);
        assertEquals(1, editorSummary.getCloneableToTrueCounter());
        assertEquals(1, editorSummary.getSuspendableToTrueCounter());
    }

    /**
     * Verify not executable action capabilities are treated as enabled for analysis.
     *
     * <p>Scenario:
     *   Target: Kubernetes:
     *     CONTAINER: SUSPEND -> NOT_EXECUTABLE
     *     CONTAINER: PROVISION -> NOT_EXECUTABLE
     *   Entities:
     *     Container (id: 2)
     *
     * <p>Result: Cloneable and suspendable are enabled for both Application and Container (When
     *   action capabilities are either not set, or set to NOT_EXECUTABLE for an entity type by the
     *   probe, the action will be enabled for market analysis)
     */
    @Test
    public void testEditNotExecutableForContainersAreTreatedAsEnabledForAnalysis() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER, ActionType.PROVISION,
                        ActionCapability.NOT_EXECUTABLE, ActionType.SUSPEND,
                        ActionCapability.NOT_EXECUTABLE, "Kubernetes"));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(2L, buildTopologyEntity(2L, CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE, 3L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.CONTAINER_VALUE,
                AnalysisSettingsView::getCloneable, AnalysisSettingsView::getSuspendable);
        assertEquals(1, editorSummary.getCloneableToTrueCounter());
        assertEquals(1, editorSummary.getSuspendableToTrueCounter());
    }

    /**
     * Verify that the provision and suspend settings in a container pod
     * are not impacted if the policy is not specified at the entity or at the probe level.
     */
    @Test
    public void testUnsetActionsForContainerPodsAreUnsetForAnalysis() {
        when(target.getProbeInfo())
                .thenReturn(getProbeInfo(EntityType.CONTAINER_POD,
                        ActionType.RESIZE, ActionCapability.NOT_EXECUTABLE,
                        ActionType.MOVE, ActionCapability.NOT_EXECUTABLE,
                        "Kubernetes"));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(2L, buildTopologyEntity(2L, CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_POD_VALUE, 3L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary editorSummary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.CONTAINER_POD_VALUE,
                analysisSettingsBuilder -> !analysisSettingsBuilder.hasCloneable(),
                analysisSettingsBuilder -> !analysisSettingsBuilder.hasSuspendable());
        assertEquals(0, editorSummary.getCloneableToTrueCounter());
        assertEquals(0, editorSummary.getSuspendableToTrueCounter());
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
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
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
     *   Target 2: VCenter (does not have proper action capability set for VIRTAUL_MACHINE)
     *     VIRTUAL_MACHINE: MOVE -> not specified
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
                .thenReturn(getProbeInfo(EntityType.VIRTUAL_DATACENTER, ActionType.MOVE,
                        ActionCapability.NOT_SUPPORTED, "VCenter"));
        when(target2.getId()).thenReturn(14L);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target1, target2));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
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
                builder -> !builder.hasMovable());
        validateCommodityMovable(graph,
                getTopologyEntityPredicate(EntityType.CONTAINER_VALUE),
                builder -> builder.hasMovable() && !builder.getMovable());
        assertEquals(0, editorSummary.getMovableToTrueCounter());
        assertEquals(1, editorSummary.getMovableToFalseCounter());
    }

    private void verifyAnalysisSettingProperty(final TopologyGraph<TopologyEntity> graph, final int entityTypeValue,
                                               final Predicate<AnalysisSettingsView> cloneablePredicate,
                                               final Predicate<AnalysisSettingsView> suspendablePredicate) {
        graph.entities().filter(getTopologyEntityPredicate(entityTypeValue)).forEach(entity -> {
            final AnalysisSettingsView settings = entity
                    .getTopologyEntityImpl()
                    .getAnalysisSettings();
            assertTrue(cloneablePredicate.test(settings));
            assertTrue(suspendablePredicate.test(settings));
        });
    }

    private void validateCommodityMovable(final TopologyGraph<TopologyEntity> graph,
                                            final Predicate<TopologyEntity> predicate,
                                            final Predicate<CommoditiesBoughtFromProviderView> movable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
                assertTrue(entity
                        .getTopologyEntityImpl()
                        .getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .allMatch(movable)));
    }

    private void validateCommodityScalable(final TopologyGraph<TopologyEntity> graph,
                                          final Predicate<TopologyEntity> predicate,
                                          final Predicate<CommoditiesBoughtFromProviderView> scalable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
                assertTrue(entity
                        .getTopologyEntityImpl()
                        .getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .allMatch(scalable)));
    }

    private void validateCommodityResizeable(final TopologyGraph<TopologyEntity> graph,
                                           final Predicate<TopologyEntity> predicate,
                                           final Predicate<CommoditySoldView> resizeable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
                assertTrue(entity
                        .getTopologyEntityImpl()
                        .getCommoditySoldListList()
                        .stream()
                        .allMatch(resizeable)));
    }

    private void validateSpecificCommodityScalable(final TopologyGraph<TopologyEntity> graph,
                                                  final Predicate<TopologyEntity> predicate,
                                                  final EntityType providerEntityType,
                                                  final Predicate<CommoditiesBoughtFromProviderView> commoditiesScalable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
                assertTrue(entity
                        .getTopologyEntityImpl()
                        .getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .filter(CommoditiesBoughtFromProviderView::hasProviderEntityType)
                        .filter(provider -> provider.getProviderEntityType() == providerEntityType.getNumber())
                        .allMatch(commoditiesScalable)
                )
        );
    }

    private void validateSpecificCommodityMovable(final TopologyGraph<TopologyEntity> graph,
                                                  final Predicate<TopologyEntity> predicate,
                                                  final EntityType providerEntityType,
                                                  final Predicate<CommoditiesBoughtFromProviderView> commoditiesMovable) {
        assertTrue(graph.entities().anyMatch(predicate));
        graph.entities().filter(predicate).forEach(entity ->
            assertTrue(entity
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(CommoditiesBoughtFromProviderView::hasProviderEntityType)
                .filter(provider -> provider.getProviderEntityType() == providerEntityType.getNumber())
                .allMatch(commoditiesMovable)
            )
        );
    }

    @Nonnull
    private Predicate<TopologyEntity> getTopologyEntityPredicate(int entityTypeValue) {
        return topologyEntity -> topologyEntity.getEntityType() == entityTypeValue;
    }

    @Nonnull
    private TopologyEntity.Builder buildVVTopologyEntityWithStProvider(long vvOid, long stOid) {
        DiscoveryOriginImpl origin = new DiscoveryOriginImpl();
        Collections.singleton(DEFAULT_TARGET_ID).forEach(id -> origin.putDiscoveredTargetData(id,
            new PerTargetEntityInformationImpl()));
        return buildTopologyEntity(vvOid, EntityType.VIRTUAL_VOLUME_VALUE,
            Optional.of(new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setProviderId(stOid)
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(STORAGE_COMMODITY_TYPE))),
            Optional.of(new CommoditySoldImpl().setCommodityType(STORAGE_COMMODITY_TYPE))
        );
    }

    @Nonnull
    private TopologyEntity.Builder buildVMTopologyEntityWithVvProvider(long vmOid, long vvOid) {
        return buildTopologyEntity(vmOid, EntityType.VIRTUAL_MACHINE_VALUE,
            Optional.of(new CommoditiesBoughtFromProviderImpl()
                .setProviderId(vvOid)
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(STORAGE_COMMODITY_TYPE))),
            Optional.empty());
    }

    @Nonnull
    private TopologyEntity.Builder buildStTopologyEntity(long stOid) {
        final CommoditySoldImpl commoditySoldDTO = new CommoditySoldImpl()
            .setCommodityType(STORAGE_COMMODITY_TYPE);
        return buildTopologyEntity(stOid, EntityType.STORAGE_TIER_VALUE,
            Optional.empty(), Optional.of(commoditySoldDTO));
    }

    /**
     * Build {@link TopologyEntity} with the provided type and oid and create commodities.
     *
     * @param oid oid of the entity
     * @param entityTypeValue entity type oid
     * @param commoditiesBoughtFromProviderOpt optional, {@link CommoditiesBoughtFromProviderImpl}
     * @param commoditySoldOpt optional, {@link CommoditySoldImpl}
     * @return {@link TopologyEntity} created
     */
    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int entityTypeValue,
                                                       Optional<CommoditiesBoughtFromProviderImpl> commoditiesBoughtFromProviderOpt,
                                                       Optional<CommoditySoldImpl> commoditySoldOpt) {
        DiscoveryOriginImpl origin = new DiscoveryOriginImpl();
        Collections.singleton(DEFAULT_TARGET_ID).forEach(id -> origin.putDiscoveredTargetData(id, new PerTargetEntityInformationImpl()));
        TopologyEntityImpl topologyEntityDTOBuilder = new TopologyEntityImpl()
            .setAnalysisSettings(new AnalysisSettingsImpl())
            .setEntityType(entityTypeValue)
            .setOrigin(new OriginImpl()
                .setDiscoveryOrigin(origin))
            .setOid(oid);

        commoditiesBoughtFromProviderOpt.ifPresent(commoditiesBoughtFromProvider -> topologyEntityDTOBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider));
        commoditySoldOpt.ifPresent(commoditySold -> topologyEntityDTOBuilder.addCommoditySoldList(commoditySold));

        return TopologyEntityUtils.topologyEntityBuilder(topologyEntityDTOBuilder);
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType, long providerId) {
        return buildTopologyEntity(oid, type, entityType, providerId, Collections.singleton(DEFAULT_TARGET_ID));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType,
                                                       long providerId, final Collection<Long> targetIds) {
       DiscoveryOriginImpl origin = new DiscoveryOriginImpl();
        targetIds.forEach(id -> origin.putDiscoveredTargetData(id,
                new PerTargetEntityInformationImpl()));
        return TopologyEntityUtils.topologyEntityBuilder(
                new TopologyEntityImpl()
                        .setAnalysisSettings(new AnalysisSettingsImpl())
                        .setEntityType(entityType)
                        .setOrigin(new OriginImpl()
                                .setDiscoveryOrigin(origin))
                        .setOid(oid).addCommoditiesBoughtFromProviders(
                        new CommoditiesBoughtFromProviderImpl()
                                .setProviderId(providerId)
                                .addCommodityBought(
                                        new CommodityBoughtImpl()
                                                .setCommodityType(
                                                        new CommodityTypeImpl().setType(type).setKey(""))
                                                .setActive(true)
                                )
                ));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType, boolean isEnabled) {
        return TopologyEntityUtils.topologyEntityBuilder(
                new TopologyEntityImpl()
                        .setEntityType(entityType)
                        .setOrigin(new OriginImpl()
                                .setDiscoveryOrigin(new DiscoveryOriginImpl()
                                        .putDiscoveredTargetData(DEFAULT_TARGET_ID,
                                            new PerTargetEntityInformationImpl())))
                        .setAnalysisSettings(new AnalysisSettingsImpl()
                                .setSuspendable(isEnabled)
                                .setCloneable(isEnabled))
                        .setOid(oid).addCommoditiesBoughtFromProviders(
                        new CommoditiesBoughtFromProviderImpl()
                                .addCommodityBought(
                                        new CommodityBoughtImpl()
                                                .setCommodityType(new CommodityTypeImpl().setType(type).setKey(""))
                                                .setActive(true)
                                )
                ));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntityWithCommSold(long oid, int entityType, int commType,
                                                                   boolean isResizeable,
                                                        final Collection<Long> targetIds) {
        DiscoveryOriginImpl origin = new DiscoveryOriginImpl();
        targetIds.forEach(id -> origin.putDiscoveredTargetData(id,
                new PerTargetEntityInformationImpl()));
        return TopologyEntityUtils.topologyEntityBuilder(
                    new TopologyEntityImpl()
                        .setAnalysisSettings(new AnalysisSettingsImpl())
                        .setEntityType(entityType)
                        .setOid(oid)
                        .setOrigin(new OriginImpl()
                                .setDiscoveryOrigin(origin))
                        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                                new CommodityTypeImpl().setType(commType).setKey(""))
                                .setIsResizeable(isResizeable))
                );
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntityWithCommBought(long oid, int entityType, int commType,
                                                                   boolean isMovable, boolean isScalable,
                                                                   final Collection<Long> targetIds) {
        DiscoveryOriginImpl origin = new DiscoveryOriginImpl();
        targetIds.forEach(id -> origin.putDiscoveredTargetData(id,
                new PerTargetEntityInformationImpl()));
        return TopologyEntityUtils.topologyEntityBuilder(
                new TopologyEntityImpl()
                        .setAnalysisSettings(new AnalysisSettingsImpl())
                        .setEntityType(entityType)
                        .setOrigin(new OriginImpl()
                                .setDiscoveryOrigin(origin))
                        .setOid(oid)
                        .addCommoditiesBoughtFromProviders(
                            new CommoditiesBoughtFromProviderImpl()
                                .addCommodityBought(
                                        new CommodityBoughtImpl()
                                                .setCommodityType(new CommodityTypeImpl().setType(commType).setKey(""))
                                ).setMovable(isMovable).setScalable(isScalable)
                        )
            );
    }

    private ProbeInfo getProbeInfo(final EntityType entityType,
                                   final ActionType change,
                                   final ActionCapability capability,
                                   final String probeType
    ) {
        return ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setUiProbeCategory("cat")
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
                .setUiProbeCategory(probeType)
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
