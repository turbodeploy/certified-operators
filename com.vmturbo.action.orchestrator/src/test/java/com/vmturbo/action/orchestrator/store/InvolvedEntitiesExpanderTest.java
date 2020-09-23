package com.vmturbo.action.orchestrator.store;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Verifies {@link InvolvedEntitiesExpander} handles to responses from repository and supply chain
 * services to expand involved entities.
 */
public class InvolvedEntitiesExpanderTest {

    private InvolvedEntitiesExpander involvedEntitiesExpander;

    private static final long BUSINESS_APP_OID = 1L;
    private static final long BUSINESS_TRANS_OID = 2L;
    private static final long SERVICE_OID = 3L;
    private static final long WORKLOAD_CONTROLLER_OID = 5L;
    private static final long VM_OID = 7L;

    private ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);

    private SupplyChainCalculator supplyChainCalculator = mock(SupplyChainCalculator.class);

    /**
     * Initialize InvolvedEntitiesExpander with mocked repository and supply chain services.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        involvedEntitiesExpander = new InvolvedEntitiesExpander(actionTopologyStore, supplyChainCalculator);
    }

    /**
     * A an empty set and a set containing at least one non-arm entity should not be expanded.
     */
    @Test
    public void testNoExpandNeeded() {
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        InvolvedEntitiesFilter actualFilter =
            involvedEntitiesExpander.expandInvolvedEntitiesFilter(Collections.emptySet());
        Assert.assertEquals("empty list should not be expanded",
            Collections.emptySet(),
            actualFilter.getEntities());
        Assert.assertEquals("empty list should not be expanded",
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());

        final TopologyGraph<ActionGraphEntity> mockGraph = mock(TopologyGraph.class);
        makeGraphEntity(BUSINESS_APP_OID, EntityType.BUSINESS_APPLICATION, mockGraph);
        makeGraphEntity(BUSINESS_TRANS_OID, EntityType.BUSINESS_TRANSACTION, mockGraph);
        makeGraphEntity(SERVICE_OID, EntityType.SERVICE, mockGraph);
        makeGraphEntity(VM_OID, EntityType.VIRTUAL_MACHINE, mockGraph);
        final ActionRealtimeTopology realtimeTopology = mock(ActionRealtimeTopology.class);
        when(realtimeTopology.entityGraph()).thenReturn(mockGraph);
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(realtimeTopology));

        actualFilter =
            involvedEntitiesExpander.expandInvolvedEntitiesFilter(ImmutableSet.of(BUSINESS_APP_OID,
                    BUSINESS_TRANS_OID, SERVICE_OID, VM_OID));
        Assert.assertEquals("set with a vm should not be expanded",
            ImmutableSet.of(BUSINESS_APP_OID, BUSINESS_TRANS_OID, SERVICE_OID, VM_OID),
            actualFilter.getEntities());
        Assert.assertEquals("set with a vm should not be expanded",
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());

        Assert.assertEquals("set with a vm should not be expanded",
            ImmutableSet.of(BUSINESS_APP_OID, BUSINESS_TRANS_OID, SERVICE_OID, VM_OID),
            actualFilter.getEntities());
        Assert.assertEquals("set with a vm should not be expanded",
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());
    }

    /**
     * Expands ARM entities over multiple supply chain nodes and state maps.
     */
    @Test
    public void testExpandNeeded() {
        final TopologyGraph<ActionGraphEntity> mockGraph = mock(TopologyGraph.class);
        makeGraphEntity(BUSINESS_APP_OID, EntityType.BUSINESS_APPLICATION, mockGraph);
        makeGraphEntity(BUSINESS_TRANS_OID, EntityType.BUSINESS_TRANSACTION, mockGraph);
        makeGraphEntity(SERVICE_OID, EntityType.SERVICE, mockGraph);
        final ActionRealtimeTopology realtimeTopology = mock(ActionRealtimeTopology.class);
        when(realtimeTopology.entityGraph()).thenReturn(mockGraph);
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(realtimeTopology));

        Set<Long> inputOids = ImmutableSet.of(BUSINESS_APP_OID, BUSINESS_TRANS_OID, SERVICE_OID);
        when(supplyChainCalculator.getSupplyChainNodes(any(), any(), any(), any()))
                .thenReturn(Collections.emptyMap());
        InvolvedEntitiesFilter actualFilter =
                involvedEntitiesExpander.expandInvolvedEntitiesFilter(inputOids);
        Assert.assertEquals("when supply chain service returns empty response, return "
                        + "the original input involved entities",
            inputOids,
            actualFilter.getEntities());
        Assert.assertEquals(
            InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS,
            actualFilter.getCalculationType());

        final long ba2Oid = 123111;
        final long networkOid = 77123111177L;
        when(supplyChainCalculator.getSupplyChainNodes(any(), any(), any(), any()))
            .thenReturn(ImmutableMap.of(
                    EntityType.BUSINESS_TRANSACTION.getValue(), SupplyChainNode.newBuilder()
                        .putMembersByState(0, MemberList.newBuilder()
                            .addMemberOids(BUSINESS_TRANS_OID)
                            .buildPartial())
                        .build(),
                    EntityType.BUSINESS_APPLICATION.getValue(), SupplyChainNode.newBuilder()
                        .putMembersByState(0, MemberList.newBuilder()
                            .addMemberOids(BUSINESS_APP_OID)
                            .buildPartial())
                        .putMembersByState(1, MemberList.newBuilder()
                            .addMemberOids(WORKLOAD_CONTROLLER_OID)
                            .buildPartial())
                        .build(),
                    EntityType.NETWORK.getValue(), SupplyChainNode.newBuilder()
                            .putMembersByState(0, MemberList.newBuilder()
                                    .addMemberOids(networkOid)
                                    .buildPartial())
                            .build(),
                    3, SupplyChainNode.newBuilder()
                        // empty state map
                        .build()));

        actualFilter = involvedEntitiesExpander.expandInvolvedEntitiesFilter(inputOids);
        Assert.assertEquals("Supply chain response should be filtered by entity type.",
            ImmutableSet.of(BUSINESS_APP_OID, BUSINESS_TRANS_OID, WORKLOAD_CONTROLLER_OID),
            actualFilter.getEntities());

        verify(supplyChainCalculator, times(2))
                .getSupplyChainNodes(eq(mockGraph), eq(inputOids), isA(Predicate.class), isA(TraversalRulesLibrary.class));

        Assert.assertEquals(
            InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS,
            actualFilter.getCalculationType());
    }

    private ActionGraphEntity makeGraphEntity(long entityOid, EntityType type, TopologyGraph<ActionGraphEntity> graph) {
        ActionGraphEntity e = mock(ActionGraphEntity.class);
        when(e.getOid()).thenReturn(entityOid);
        when(e.getEntityType()).thenReturn(type.getValue());
        when(graph.getEntity(entityOid)).thenReturn(Optional.of(e));
        return e;
    }
}
