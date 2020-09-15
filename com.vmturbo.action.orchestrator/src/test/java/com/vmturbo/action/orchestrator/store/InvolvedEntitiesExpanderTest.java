package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
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

    private static final long BUSINESS_APP_OID = 1L;
    private static final long BUSINESS_TRANS_OID = 2L;
    private static final long SERVICE_OID = 3L;
    private static final long APP_COMPONENT_OID = 4L;
    private static final long CONTAINER_OID = 5L;
    private static final long VM_OID = 7L;

    private static final List<String> ENTITY_TYPES = Arrays.asList(
            ApiEntityType.BUSINESS_APPLICATION.apiStr(),
            ApiEntityType.BUSINESS_TRANSACTION.apiStr(),
            ApiEntityType.SERVICE.apiStr(),
            ApiEntityType.APPLICATION_COMPONENT.apiStr(),
            ApiEntityType.CONTAINER.apiStr(),
            ApiEntityType.CONTAINER_POD.apiStr(),
            ApiEntityType.VIRTUAL_MACHINE.apiStr(),
            ApiEntityType.DATABASE_SERVER.apiStr(),
            ApiEntityType.VIRTUAL_VOLUME.apiStr(),
            ApiEntityType.STORAGE.apiStr(),
            ApiEntityType.PHYSICAL_MACHINE.apiStr());

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
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES,
            actualFilter.getCalculationType());

        when(repositoryServiceMole.retrieveTopologyEntities(any()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(makeApiPartialEntity(BUSINESS_APP_OID, EntityType.BUSINESS_APPLICATION))
                        .addEntities(makeApiPartialEntity(BUSINESS_TRANS_OID, EntityType.BUSINESS_TRANSACTION))
                        .addEntities(makeApiPartialEntity(SERVICE_OID, EntityType.SERVICE))
                        .addEntities(makeApiPartialEntity(VM_OID, EntityType.VIRTUAL_MACHINE))
                        .buildPartial()));
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
        when(repositoryServiceMole.retrieveTopologyEntities(any()))
            .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                    .addEntities(makeApiPartialEntity(BUSINESS_APP_OID, EntityType.BUSINESS_APPLICATION))
                    .addEntities(makeApiPartialEntity(BUSINESS_TRANS_OID, EntityType.BUSINESS_TRANSACTION))
                    .addEntities(makeApiPartialEntity(SERVICE_OID, EntityType.SERVICE))
                    .buildPartial()));

        Set<Long> inputOids = ImmutableSet.of(BUSINESS_APP_OID, BUSINESS_TRANS_OID, SERVICE_OID);
        when(supplyChainServiceMole.getSupplyChain(any()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .buildPartial());
        InvolvedEntitiesFilter actualFilter =
                involvedEntitiesExpander.expandInvolvedEntitiesFilter(inputOids);
        Assert.assertEquals("when supply chain service returns empty response, return "
                        + "the original input involved entities",
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
                            .addMemberOids(BUSINESS_APP_OID)
                            .addMemberOids(BUSINESS_TRANS_OID)
                            .addMemberOids(APP_COMPONENT_OID)
                            .buildPartial())
                        .buildPartial())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .putMembersByState(0, MemberList.newBuilder()
                            // purposely duplicated
                            .addMemberOids(BUSINESS_APP_OID)
                            .addMemberOids(SERVICE_OID)
                            .buildPartial())
                        .putMembersByState(1, MemberList.newBuilder()
                            .addMemberOids(CONTAINER_OID)
                            .buildPartial())
                        .buildPartial())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        // empty state map
                        .buildPartial())
                    .buildPartial())
                .buildPartial());

        actualFilter = involvedEntitiesExpander.expandInvolvedEntitiesFilter(inputOids);
        Assert.assertEquals("when supply chain service returns empty response, return "
                        + "the original input involved entities",
            ImmutableSet.of(BUSINESS_APP_OID, BUSINESS_TRANS_OID, SERVICE_OID, APP_COMPONENT_OID,
                    CONTAINER_OID),
            actualFilter.getEntities());

        ArgumentCaptor<GetSupplyChainRequest> argument =
                ArgumentCaptor.forClass(GetSupplyChainRequest.class);
        verify(supplyChainServiceMole, times(2))
                .getSupplyChain(argument.capture());
        SupplyChainScope supplyChainScope = argument.getValue().getScope();
        Assert.assertNotNull(supplyChainScope);
        Assert.assertTrue(supplyChainScope.getStartingEntityOidList().containsAll(inputOids));
        Assert.assertTrue(supplyChainScope.getEntityTypesToIncludeList().containsAll(ENTITY_TYPES));

        Assert.assertEquals(
            InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS,
            actualFilter.getCalculationType());
        Assert.assertThat(actualFilter.getEntities(), containsInAnyOrder(BUSINESS_APP_OID,
                BUSINESS_TRANS_OID, SERVICE_OID, APP_COMPONENT_OID, CONTAINER_OID));
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
