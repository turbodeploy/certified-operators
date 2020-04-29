package com.vmturbo.action.orchestrator.store;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Verifies {@link InvolvedEntitiesExpander} handles to responses from repository and supply chain
 * services to expand involved entities.
 */
public class InvolvedEntitiesExpanderTest {

    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());
    private final SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());

    private InvolvedEntitiesExpander involvedEntitiesExpander;

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(
        supplyChainServiceMole,
        repositoryServiceMole);

    /**
     * Initialize InvolvedEntitiesExpander with mocked repository and supply chain services.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        involvedEntitiesExpander = new InvolvedEntitiesExpander(
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * A an empty set and a set containing at least one non-arm entity should not be expanded.
     */
    @Test
    public void testNoExpandNeeded() {
        InvolvedEntitiesFilter actualFilter =
            involvedEntitiesExpander.expandInvolvedEntitiesFilter(Collections.emptySet());
        Assert.assertEquals("empty list should not be expanded",
            Collections.emptySet(),
            actualFilter.getEntities());
        Assert.assertEquals("empty list should not be expanded",
            InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());

        when(repositoryServiceMole.retrieveTopologyEntities(any()))
            .thenReturn(Arrays.asList(PartialEntityBatch.newBuilder()
                .addEntities(makeApiPartialEntity(1, EntityType.BUSINESS_APPLICATION))
                .addEntities(makeApiPartialEntity(2, EntityType.BUSINESS_TRANSACTION))
                .addEntities(makeApiPartialEntity(3, EntityType.SERVICE))
                .addEntities(makeApiPartialEntity(4, EntityType.VIRTUAL_MACHINE))
                .buildPartial()));
        actualFilter =
            involvedEntitiesExpander.expandInvolvedEntitiesFilter(ImmutableSet.of(1L, 2L, 3L, 4L));
        Assert.assertEquals("set with a vm should not be expanded",
            ImmutableSet.of(1L, 2L, 3L, 4L),
            actualFilter.getEntities());
        Assert.assertEquals("set with a vm should not be expanded",
            InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());

        when(repositoryServiceMole.retrieveTopologyEntities(any()))
            .thenReturn(Arrays.asList(PartialEntityBatch.newBuilder()
                .addEntities(makeApiPartialEntity(4, EntityType.VIRTUAL_MACHINE))
                .addEntities(makeApiPartialEntity(1, EntityType.BUSINESS_APPLICATION))
                .addEntities(makeApiPartialEntity(2, EntityType.BUSINESS_TRANSACTION))
                .addEntities(makeApiPartialEntity(3, EntityType.SERVICE))
                .buildPartial()));
        Assert.assertEquals("set with a vm should not be expanded",
            ImmutableSet.of(1L, 2L, 3L, 4L),
            actualFilter.getEntities());
        Assert.assertEquals("set with a vm should not be expanded",
            InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());
    }

    /**
     * Expands ARM entities over multiple supply chain nodes and state maps.
     */
    @Test
    public void testExpandNeeded() {
        when(repositoryServiceMole.retrieveTopologyEntities(any()))
            .thenReturn(Arrays.asList(PartialEntityBatch.newBuilder()
                .addEntities(makeApiPartialEntity(1, EntityType.BUSINESS_APPLICATION))
                .addEntities(makeApiPartialEntity(2, EntityType.BUSINESS_TRANSACTION))
                .addEntities(makeApiPartialEntity(3, EntityType.SERVICE))
                .buildPartial()));

        Set<Long> inputOids = ImmutableSet.of(1L, 2L, 3L);
        when(supplyChainServiceMole.getSupplyChain(any()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .buildPartial());
        InvolvedEntitiesFilter actualFilter =
            involvedEntitiesExpander.expandInvolvedEntitiesFilter(inputOids);
        Assert.assertEquals("when supply chain service returns empty response, return the" +
                "the original input involved entities",
            inputOids,
            actualFilter.getEntities());
        Assert.assertEquals(
            InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS,
            actualFilter.getCalculationType());

        when(supplyChainServiceMole.getSupplyChain(any()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .putMembersByState(0, MemberList.newBuilder()
                            .addMemberOids(1L)
                            .addMemberOids(2L)
                            .addMemberOids(4L)
                            .buildPartial())
                        .buildPartial())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .putMembersByState(0, MemberList.newBuilder()
                            // purposely duplicated
                            .addMemberOids(1L)
                            .addMemberOids(3L)
                            .buildPartial())
                        .putMembersByState(1, MemberList.newBuilder()
                            .addMemberOids(5L)
                            .buildPartial())
                        .buildPartial())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        // empty state map
                        .buildPartial())
                    .buildPartial())
                .buildPartial());

        actualFilter =
            involvedEntitiesExpander.expandInvolvedEntitiesFilter(inputOids);
        Assert.assertEquals("when supply chain service returns empty response, return the" +
                "the original input involved entities",
            ImmutableSet.of(1L, 2L, 3L, 4L, 5L),
            actualFilter.getEntities());
        Assert.assertEquals(
            InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS,
            actualFilter.getCalculationType());
    }

    @Nonnull
    private PartialEntity makeApiPartialEntity(long entityOid, EntityType type) {
        return PartialEntity.newBuilder()
            .setMinimal(MinimalEntity.newBuilder()
                .setOid(entityOid)
                .setEntityType(type.getValue())
                .buildPartial())
            .buildPartial();
    }
}
