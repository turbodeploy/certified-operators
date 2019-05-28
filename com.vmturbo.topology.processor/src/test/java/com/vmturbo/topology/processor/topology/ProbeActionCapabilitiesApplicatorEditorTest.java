package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
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
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor.EditorSummary;

public class ProbeActionCapabilitiesApplicatorEditorTest {

    public static final long DEFAULT_TARGET_ID = 1L;
    private ProbeActionCapabilitiesApplicatorEditor editor;

    private ProbeStore probeStore = mock(ProbeStore.class);

    private TargetStore targetStore = mock(TargetStore.class);

    @Before
    public void setup() {
        editor = new ProbeActionCapabilitiesApplicatorEditor(probeStore, targetStore);
        final Target target = mock(Target.class);
        when(target.getId()).thenReturn(DEFAULT_TARGET_ID);
        when(targetStore.getAll()).thenReturn(Collections.singletonList(target));
    }

    /**
     * Verify editing movable to true for DATABASE with VDC as provider types.
     * Action capabilities:
     * VIRTUAL_MACHINE: MOVE -> NOT_SUPPORTED
     * movable is set to true for Database.
     */
    @Test
    public void testEditMovableWithNoRelatedActionCapability() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.VIRTUAL_MACHINE, ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_DATACENTER_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);
        // movable is set to true for Database.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> builder.getMovable());
        assertEquals(2, movableEditSummary.getMovableToTrueCounter());
        assertEquals(0, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify editing movable to for DATABASE with VDC as provider with two targets
     * Action capabilities:
     * Target one:
     * DATABASE: MOVE -> NOT_SUPPORTED
     * Target two:
     * DATABASE: MOVE -> SUPPORTED
     * Movable is set to true.
     * <p>
     * Explanation: If both netapp and vc are discovering a storage. vc cannot move it,
     * but netapp will, so we should mark it movable.
     */
    @Test
    public void testEditMovableWithTwoTargetsCase1() {
        final long firstTargetId = 2L;
        final long secondTargetId = 3L;
        final long firstProbeId = 4L;
        final long secondProbeId = 5L;
        when(probeStore.getProbe(firstProbeId))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.DATABASE, ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));
        when(probeStore.getProbe(secondProbeId))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.DATABASE, ActionType.MOVE, ActionCapability.SUPPORTED)));

        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));
        final ImmutableSet<Long> targetIds = ImmutableSet.of(firstTargetId, secondTargetId);
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L, targetIds));
        final Target target1 = mock(Target.class);
        final Target target2 = mock(Target.class);
        when(target1.getProbeId()).thenReturn(firstProbeId);
        when(target2.getProbeId()).thenReturn(secondProbeId);
        when(target1.getId()).thenReturn(firstTargetId);
        when(target2.getId()).thenReturn(secondTargetId);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target1, target2));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        // Movable is set to true.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> builder.getMovable());
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify editing movable for DATABASE with VDC as provider with two target, one has action capability
     * and the other hasn't.
     * <p>
     * Action capabilities:
     * Target one:
     * DATABASE: MOVE -> NOT_SUPPORTED
     * Target two:
     * Empty
     * <p>
     * Movable is set to true.
     */
    @Test
    public void testEditMovableWithTwoTargetsCase2() {
        final long firstTargetId = 2L;
        final long secondTargetId = 3L;
        final long firstProbeId = 4L;
        final long secondProbeId = 5L;
        when(probeStore.getProbe(firstProbeId))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.DATABASE, ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));
        when(probeStore.getProbe(secondProbeId))
            .thenReturn(Optional.empty());

        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));
        final ImmutableSet<Long> targetIds = ImmutableSet.of(firstTargetId, secondTargetId);
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L, targetIds));
        final Target target1 = mock(Target.class);
        final Target target2 = mock(Target.class);
        when(target1.getProbeId()).thenReturn(firstProbeId);
        when(target2.getProbeId()).thenReturn(secondProbeId);
        when(target1.getId()).thenReturn(firstTargetId);
        when(target2.getId()).thenReturn(secondTargetId);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target1, target2));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);
        // Movable is set to true.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> builder.getMovable());
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }


    /**
     * Verify editing movable for DATABASE with VDC as provider types.
     * Action capabilities:
     * DATABASE
     * MOVE -> NOT_SUPPORTED
     * <p>
     * movable are set to false.
     */
    @Test
    public void testEditMovableWithActionCapability() {
        final ProbeInfo probeInfo = getProbeInfo(EntityType.DATABASE_SERVER,
            ActionType.MOVE, EntityType.DATABASE, ActionType.MOVE, ActionCapability.NOT_SUPPORTED);
        when(probeStore.getProbe(anyLong())).thenReturn(Optional.of(probeInfo));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_DATACENTER_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        // movable are set to false to both
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify if the entity type is general entity type (VDC in this case), movable will
     * be set according to probe action capability.
     * Action capabilities:
     * VIRTUAL_MACHINE: MOVE -> NOT_SUPPORTED
     * <p>
     * movable is set true for entity with VIRTUAL_DATACENTER_VALUE type.
     */
    @Test
    public void testEditMovableWithVDC() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.VIRTUAL_MACHINE, ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_DATACENTER_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);
        // verify movable is true.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_DATACENTER_VALUE),
            builder -> builder.getMovable());
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(0, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify editing movable when the entity type is VM and provider is PM.
     * Action capabilities:
     * VIRTUAL_MACHINE: MOVE -> NOT_SUPPORTED
     * Movable is set to false.
     */
    @Test
    public void testEditMovableWithVMAndProviderPMCase1() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(Optional.of(getProbeInfo(ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.PHYSICAL_MACHINE_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        // Movable is set to false.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify editing movable when the entity type is VM and provider is PM.
     * Action capabilities:
     * VIRTUAL_MACHINE: MOVE -> SUPPORTED
     * Movable is set to true.
     */
    @Test
    public void testEditMovableWithVMAndProviderPMCase2() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(Optional.of(getProbeInfo(ActionType.MOVE, ActionCapability.SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.PHYSICAL_MACHINE_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);
        // Movable is set to true.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> builder.getMovable());
        assertEquals(2, movableEditSummary.getMovableToTrueCounter());
        assertEquals(0, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify editing movable when the entity type is VM and provider is Storage.
     * Action capabilities:
     * VIRTUAL_MACHINE: CHANGE -> NOT_SUPPORTED
     * Movable is set to false.
     */
    @Test
    public void testEditMovableWithVMAndProviderStorageCase1() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(Optional.of(getProbeInfo(ActionType.CHANGE, ActionCapability.NOT_SUPPORTED)));

        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.STORAGE_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);

        // Movable is set to false.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());
        assertEquals(1, movableEditSummary.getMovableToTrueCounter());
        assertEquals(1, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify editing movable when the entity type is VM and provider is Storage.
     * Action capabilities:
     * VIRTUAL_MACHINE: CHANGE -> SUPPORTED
     * Movable is set to true.
     */
    @Test
    public void testEditMovableWithVMAndProviderStorageCase2() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(Optional.of(getProbeInfo(ActionType.CHANGE, ActionCapability.SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1L, buildTopologyEntity(1L, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.STORAGE_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());

        EditorSummary movableEditSummary = editor.applyPropertiesEdits(graph);
        // Movable is set to true.
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> builder.getMovable());
        assertEquals(2, movableEditSummary.getMovableToTrueCounter());
        assertEquals(0, movableEditSummary.getMovableToFalseCounter());
    }

    /**
     * Verify if properties values are set by user, the editor will:
     * 1. if all probes don't allow the actions, set them to false.
     * 2. if at least one probe allow the action, check if the property is set.
     * If not set, set to true, and leave it otherwise.
     * <p>
     * Action capabilities:
     * VIRTUAL_MACHINE: MOVE -> NOT_SUPPORTED
     * <p>
     * User set:
     * movable -> false
     * cloneable -> false
     * suspendable -> true
     * <p>
     * For entity type DATABASE_VALUE:
     * movable is set to false (since user want it disable it, although probe support it) .
     * cloneable is set to false (since user wants it to be false, although probe support it).
     * suspendable is set to true (since user wants it to be true and probe support it).
     */
    @Test
    public void testEditMovableClonableAndSuspendableWithValuesPreSet() {
        // probe only have policy for VM.
        when(probeStore.getProbe(anyLong()))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.VIRTUAL_MACHINE, ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));

        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, false));
        // user set 1. cloneable to false 2. suspendable to true
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, false));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary summary = editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());


        verifyAnalysisSettingProperty(graph, EntityType.DATABASE_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());

        assertEquals(1, summary.getMovableToTrueCounter());
        assertEquals(0, summary.getMovableToFalseCounter());
        assertEquals(0, summary.getCloneableToTrueCounter());
        assertEquals(0, summary.getCloneableToFalseCounter());
        assertEquals(0, summary.getSuspendableToTrueCounter());
        assertEquals(0, summary.getSuspendableToFalseCounter());
    }


    /**
     * Verify editing movable, cloneable and suspendable to for DATABASE with VM as provider
     * Action capabilities:
     * VIRTUAL_DATACENTER: MOVE -> NOT_SUPPORTED
     * <p>
     * movable is set to false.
     * cloneable is set to true.
     * suspendable is set to true.
     */
    @Test
    public void testEditMovableWithCapabilityCloneableAndSuspendableWithoutActionCapability() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.VIRTUAL_DATACENTER, ActionType.MOVE, ActionCapability.NOT_SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_DATACENTER_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        EditorSummary summary = editor.applyPropertiesEdits(graph);

        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_DATACENTER_VALUE),
            builder -> !builder.getMovable());
        verifyAnalysisSettingProperty(graph, EntityType.VIRTUAL_DATACENTER_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());
        verifyAnalysisSettingProperty(graph, EntityType.DATABASE_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());

        assertEquals(1, summary.getMovableToTrueCounter());
        assertEquals(1, summary.getMovableToFalseCounter());
        assertEquals(2, summary.getCloneableToTrueCounter());
        assertEquals(0, summary.getCloneableToFalseCounter());
        assertEquals(2, summary.getSuspendableToTrueCounter());
        assertEquals(0, summary.getSuspendableToFalseCounter());
    }

    /**
     * Verify editing cloneable and suspendable properties for DATABASE with VDC as provider types.
     * Action capabilities:
     * DATABASE:
     * PROVISION -> NOT_SUPPORTED
     * SUSPEND -> NOT_SUPPORTED
     * Both cloneable and suspendable are set to false.
     */
    @Test
    public void testEditCloneableAndSuspendableWithActionCapability() {
        when(probeStore.getProbe(anyLong()))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.DATABASE, ActionType.PROVISION,
                    ActionCapability.NOT_SUPPORTED, ActionType.SUSPEND, ActionCapability.NOT_SUPPORTED)));
        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_DATACENTER_VALUE, 1L));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_DATACENTER_VALUE),
            builder -> !builder.getMovable());
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());
        verifyAnalysisSettingProperty(graph, EntityType.VIRTUAL_DATACENTER_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());
        verifyAnalysisSettingProperty(graph, EntityType.DATABASE_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());

        EditorSummary summary = editor.applyPropertiesEdits(graph);
        verifyAnalysisSettingProperty(graph, EntityType.VIRTUAL_DATACENTER_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());
        // Both cloneable and suspendable are set to false.
        verifyAnalysisSettingProperty(graph, EntityType.DATABASE_VALUE, s -> !s.getCloneable(), s -> !s.getSuspendable());

        assertEquals(2, summary.getMovableToTrueCounter());
        assertEquals(0, summary.getMovableToFalseCounter());
        assertEquals(1, summary.getCloneableToTrueCounter());
        assertEquals(1, summary.getCloneableToFalseCounter());
        assertEquals(1, summary.getSuspendableToTrueCounter());
        assertEquals(1, summary.getSuspendableToFalseCounter());
    }


    /**
     * Verify editing cloneable and suspendable for DATABASE with VDC as provider with two targets
     * Action capabilities:
     * Target one:
     * STORAGE: PROVISION -> NOT_SUPPORTED, SUSPEND -> NOT_SUPPORTED
     * Target two:
     * STORAGE: PROVISION -> SUPPORTED, SUSPEND -> SUPPORTED
     * <p>
     * both are set to true.
     * <p>
     * Explanation: If both netapp and vc are discovering a storage. vc cannot move it,
     * but netapp will, so we should mark it movable.
     */
    @Test
    public void testEditCloneableAndSuspendableWithTwoTargetsCase1() {

        final long firstTargetId = 2L;
        final long secondTargetId = 3L;
        final long firstProbeId = 4L;
        final long secondProbeId = 5L;
        when(probeStore.getProbe(firstProbeId))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.STORAGE, ActionType.PROVISION,
                    ActionCapability.NOT_SUPPORTED, ActionType.SUSPEND, ActionCapability.NOT_SUPPORTED)));
        when(probeStore.getProbe(secondProbeId))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.STORAGE, ActionType.PROVISION,
                    ActionCapability.SUPPORTED, ActionType.SUSPEND, ActionCapability.SUPPORTED)));

        final Map<Long, Builder> topology = new HashMap<>();
        // Setting VM itself as provider. And since a special case say, if the provider is not PM or storage
        // set the movable to false.
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.PHYSICAL_MACHINE_VALUE, 1L));
        final ImmutableSet<Long> targetIds = ImmutableSet.of(firstTargetId, secondTargetId);
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.STORAGE_VALUE, 1L, targetIds));
        final Target target1 = mock(Target.class);
        final Target target2 = mock(Target.class);
        when(target1.getProbeId()).thenReturn(firstProbeId);
        when(target2.getProbeId()).thenReturn(secondProbeId);
        when(target1.getId()).thenReturn(firstTargetId);
        when(target2.getId()).thenReturn(secondTargetId);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target1, target2));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.PHYSICAL_MACHINE_VALUE),
            builder -> !builder.getMovable());
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.STORAGE_VALUE),
            builder -> !builder.getMovable());
        verifyAnalysisSettingProperty(graph, EntityType.PHYSICAL_MACHINE_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());
        verifyAnalysisSettingProperty(graph, EntityType.STORAGE_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());

        EditorSummary summary = editor.applyPropertiesEdits(graph);
        // both are set to true.
        verifyAnalysisSettingProperty(graph, EntityType.PHYSICAL_MACHINE_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());
        verifyAnalysisSettingProperty(graph, EntityType.STORAGE_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());

        assertEquals(2, summary.getMovableToTrueCounter());
        assertEquals(0, summary.getMovableToFalseCounter());
        assertEquals(2, summary.getCloneableToTrueCounter());
        assertEquals(0, summary.getCloneableToFalseCounter());
        assertEquals(2, summary.getSuspendableToTrueCounter());
        assertEquals(0, summary.getSuspendableToFalseCounter());
    }

    /**
     * Verify editing cloneable and suspendable for DATABASE with VDC as provider with two targets
     * Action capabilities:
     * Target one:
     * DATABASE: PROVISION -> NOT_SUPPORTED, SUSPEND -> NOT_SUPPORTED
     * Target two:
     * empty
     * <p>
     * Both are set to true.
     */
    @Test
    public void testEditCloneableAndSuspendableWithTwoTargetsCase2() {
        final long firstTargetId = 2L;
        final long secondTargetId = 3L;
        final long firstProbeId = 4L;
        final long secondProbeId = 5L;
        when(probeStore.getProbe(firstProbeId))
            .thenReturn(
                Optional.of(getProbeInfo(EntityType.DATABASE, ActionType.PROVISION,
                    ActionCapability.NOT_SUPPORTED, ActionType.SUSPEND, ActionCapability.NOT_SUPPORTED)));
        when(probeStore.getProbe(secondProbeId))
            .thenReturn(Optional.empty());

        final Map<Long, Builder> topology = new HashMap<>();
        // Setting VM itself as provider. And since a special case say, if the provider is not PM or storage
        // set the movable to false.
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.VIRTUAL_MACHINE_VALUE, 1L));
        final ImmutableSet<Long> targetIds = ImmutableSet.of(firstTargetId, secondTargetId);
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()
            , EntityType.DATABASE_VALUE, 1L, targetIds));
        final Target target1 = mock(Target.class);
        final Target target2 = mock(Target.class);
        when(target1.getProbeId()).thenReturn(firstProbeId);
        when(target2.getProbeId()).thenReturn(secondProbeId);
        when(target1.getId()).thenReturn(firstTargetId);
        when(target2.getId()).thenReturn(secondTargetId);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target1, target2));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // validate the movable are false before this stage
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.VIRTUAL_MACHINE_VALUE),
            builder -> !builder.getMovable());
        validateCommodityMovable(graph,
            getTopologyEntityPredicate(EntityType.DATABASE_VALUE),
            builder -> !builder.getMovable());
        verifyAnalysisSettingProperty(graph, EntityType.VIRTUAL_MACHINE_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());
        verifyAnalysisSettingProperty(graph, EntityType.DATABASE_VALUE, s -> !s.getCloneable(), s -> s.getSuspendable());

        EditorSummary summary = editor.applyPropertiesEdits(graph);
        // both are set to true
        verifyAnalysisSettingProperty(graph, EntityType.VIRTUAL_MACHINE_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());
        verifyAnalysisSettingProperty(graph, EntityType.DATABASE_VALUE, s -> s.getCloneable(), s -> s.getSuspendable());

        assertEquals(1, summary.getMovableToTrueCounter());
        assertEquals(1, summary.getMovableToFalseCounter());
        assertEquals(2, summary.getCloneableToTrueCounter());
        assertEquals(0, summary.getCloneableToFalseCounter());
        assertEquals(2, summary.getSuspendableToTrueCounter());
        assertEquals(0, summary.getSuspendableToFalseCounter());
    }

    public void verifyAnalysisSettingProperty(final TopologyGraph<TopologyEntity> graph, final int entityTypeValue,
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

    @Nonnull
    private void validateCommodityMovable(final TopologyGraph<TopologyEntity> graph,
                                          final Predicate<TopologyEntity> predicate,
                                          final Predicate<CommoditiesBoughtFromProvider> movable) {
        assertTrue(graph.entities().filter(predicate).count() > 0);
        graph.entities().filter(predicate).forEach(entity ->
            assertTrue(entity
                .getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersList()
                .stream()
                .allMatch(movable)));
    }

    @Nonnull
    private Predicate<TopologyEntity> getTopologyEntityPredicate(int entityTypeValue) {
        return topologyEntity -> topologyEntity.getEntityType() == entityTypeValue;
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType, long providerId) {
        return buildTopologyEntity(oid, type, entityType, providerId, Collections.singleton(DEFAULT_TARGET_ID));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType,
                                                       long providerId, final Collection<Long> targetIds) {
        return TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder()
                .setAnalysisSettings(AnalysisSettings.newBuilder().build())
                .setEntityType(entityType)
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .addAllDiscoveringTargetIds(targetIds)
                        .build())
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
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type, int entityType, boolean movable) {
        return TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder()
                .setEntityType(entityType)
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .addAllDiscoveringTargetIds(Collections.singleton(DEFAULT_TARGET_ID))
                        .build())
                    .build())
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setSuspendable(true)
                    .setCloneable(false)
                    .build())
                .setOid(oid).addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setMovable(movable)
                    .addCommodityBought(
                        CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(type).setKey("").build()
                            ).setActive(true)
                    )
            ));
    }

    private ProbeInfo getProbeInfo(final ActionType change, final ActionCapability capability) {
        return ProbeInfo.newBuilder()
            .setProbeCategory("cat")
            .setProbeType("type")
            .addTargetIdentifierField("field")
            .addActionPolicy(ActionPolicyDTO.newBuilder()
                .addPolicyElement(ActionPolicyElement.newBuilder()
                    .setActionType(change)
                    .setActionCapability(capability).build())
                .setEntityType(EntityType.VIRTUAL_MACHINE).build())
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setMandatory(true)
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName("name")
                    .setDisplayName("displayName")
                    .setDescription("description")
                    .setIsSecret(true)))
            .build();
    }

    private ProbeInfo getProbeInfo(final EntityType entityType,
                                   final ActionType change,
                                   final ActionCapability capability) {
        return ProbeInfo.newBuilder()
            .setProbeCategory("cat")
            .setProbeType("type")
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
                                   final ActionCapability capability) {
        return ProbeInfo.newBuilder()
            .setProbeCategory("cat")
            .setProbeType("type")
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
                                   final ActionCapability secondCapability
    ) {
        return ProbeInfo.newBuilder()
            .setProbeCategory("cat")
            .setProbeType("type")
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
