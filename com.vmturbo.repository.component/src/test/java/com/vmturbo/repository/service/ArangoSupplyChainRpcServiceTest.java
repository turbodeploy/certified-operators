package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javaslang.control.Either;
import reactor.core.publisher.Mono;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ArangoSupplyChainRpcServiceTest {

    private GraphDBService graphDBService = mock(GraphDBService.class);
    private SupplyChainService supplyChainService = mock(SupplyChainService.class);
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private ArangoSupplyChainRpcService supplyChainBackend =
        Mockito.spy(new ArangoSupplyChainRpcService(graphDBService, supplyChainService, userSessionContext, 1234L));

    private SupplyChainServiceBlockingStub supplyChainStub;

    private final SupplyChainNode pmNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                .addMemberOids(1L)
                .addMemberOids(2L)
                .build())
            .setEntityType("PhysicalMachine")
            .build();
    private final SupplyChainNode vmNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(3L)
                    .addMemberOids(4L)
                    .addMemberOids(5L)
                    .build())
            .setEntityType("VirtualMachine")
            .build();

    private final SupplyChainNode cloudVMNode1 = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(11L)
                    .build())
            .addConnectedProviderTypes(ApiEntityType.AVAILABILITY_ZONE.apiStr())
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr())
            .build();

    private final SupplyChainNode cloudVMNode2 = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(12L)
                    .build())
            .addConnectedProviderTypes(ApiEntityType.AVAILABILITY_ZONE.apiStr())
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr())
            .build();

    private final SupplyChainNode cloudVolumeNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(21L)
                    .build())
            .addConnectedConsumerTypes(ApiEntityType.VIRTUAL_MACHINE.apiStr())
            .addConnectedProviderTypes(ApiEntityType.AVAILABILITY_ZONE.apiStr())
            .setEntityType(ApiEntityType.VIRTUAL_VOLUME.apiStr())
            .build();

    private final SupplyChainNode cloudZoneNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(31L)
                    .build())
            .addConnectedConsumerTypes(ApiEntityType.VIRTUAL_MACHINE.apiStr())
            .addConnectedConsumerTypes(ApiEntityType.VIRTUAL_VOLUME.apiStr())
            .setEntityType(ApiEntityType.AVAILABILITY_ZONE.apiStr())
            .build();

    private final SupplyChainNode cloudRegionNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(41L)
                    .build())
            .addConnectedProviderTypes(ApiEntityType.AVAILABILITY_ZONE.apiStr())
            .setEntityType(ApiEntityType.REGION.apiStr())
            .build();

    private final SupplyChainNode cloudAccountNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(51)
                    .build())
            .addConnectedProviderTypes(ApiEntityType.VIRTUAL_MACHINE.apiStr())
            .addConnectedProviderTypes(ApiEntityType.VIRTUAL_VOLUME.apiStr())
            .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.apiStr())
            .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(supplyChainBackend);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        supplyChainStub = SupplyChainServiceGrpc.newBlockingStub(server.getChannel());

        Mockito.when(userSessionContext.getUserAccessScope())
                .thenReturn(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE);

        doReturn(Optional.of(ApiEntityType.VIRTUAL_MACHINE)).when(
                supplyChainBackend).getUIEntityType(5678L);
        doReturn(Optional.of(ApiEntityType.VIRTUAL_MACHINE)).when(
                supplyChainBackend).getUIEntityType(91011L);
        doReturn(Optional.of(ApiEntityType.VIRTUAL_MACHINE)).when(
                supplyChainBackend).getUIEntityType(11L);
        doReturn(Optional.of(ApiEntityType.VIRTUAL_MACHINE)).when(
                supplyChainBackend).getUIEntityType(12L);
        doReturn(Optional.of(ApiEntityType.AVAILABILITY_ZONE)).when(
                supplyChainBackend).getUIEntityType(31L);
        doReturn(Optional.of(ApiEntityType.REGION)).when(
                supplyChainBackend).getUIEntityType(41L);
        doReturn(Optional.of(ApiEntityType.BUSINESS_ACCOUNT)).when(
                supplyChainBackend).getUIEntityType(51L);
    }

    @Test
    public void testGetMultiSupplyChainsSuccess() throws Exception {
        final long contextId = 123L;
        final SupplyChainSeed seed1 = SupplyChainSeed.newBuilder()
            .setSeedOid(1L)
            .setScope(SupplyChainScope.newBuilder()
                .addStartingEntityOid(1L))
            .build();
        final SupplyChainNode node1 = SupplyChainNode.newBuilder()
                .setEntityType("foo")
                .build();
        final SupplyChainSeed seed2 = SupplyChainSeed.newBuilder()
            .setSeedOid(2L)
            .setScope(SupplyChainScope.newBuilder()
                .addStartingEntityOid(2L))
            .build();
        final SupplyChainNode node2 = SupplyChainNode.newBuilder()
                .setEntityType("bar")
                .build();

        // Right now the multi-supply-chains request just calls the regular supply chains request.
        // Override the behaviour to return the nodes we want.
        doAnswer(invocation -> {
            final GetSupplyChainRequest request = invocation.getArgumentAt(0, GetSupplyChainRequest.class);
            final StreamObserver<GetSupplyChainResponse> nodeObserver =
                    invocation.getArgumentAt(1, StreamObserver.class);
            if (request.getScope().getStartingEntityOidList().equals(seed1.getScope().getStartingEntityOidList())) {
                nodeObserver.onNext(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(SupplyChain.newBuilder()
                        .addSupplyChainNodes(node1))
                    .build());
                nodeObserver.onCompleted();;
            } else if (request.getScope().getStartingEntityOidList().equals(seed2.getScope().getStartingEntityOidList())) {
                nodeObserver.onNext(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(SupplyChain.newBuilder()
                        .addSupplyChainNodes(node2))
                    .build());
                nodeObserver.onCompleted();;
            }
            return null;
        }).when(supplyChainBackend).getSupplyChain(any(), any());


        final Map<Long, GetMultiSupplyChainsResponse> responseBySeedOid = new HashMap<>();
        supplyChainStub.getMultiSupplyChains(GetMultiSupplyChainsRequest.newBuilder()
                .setContextId(contextId)
                .addSeeds(seed1)
                .addSeeds(seed2)
                .build()).forEachRemaining(resp -> responseBySeedOid.put(resp.getSeedOid(), resp));
        assertThat(responseBySeedOid.size(), is(2));
        assertThat(responseBySeedOid.get(seed1.getSeedOid()).getSupplyChain().getSupplyChainNodesList(), contains(node1));
        assertThat(responseBySeedOid.get(seed2.getSeedOid()).getSupplyChain().getSupplyChainNodesList(), contains(node2));
    }

    /**
     * Test the case that cloud supply chain starts from multiple VMs. Verify that it will query
     * another supply chain based on the returned zone, and the final supply chain contains VM,
     * Volume, Zone and Region. It also verifies that it only queries ArangoDB once for the zone,
     * although these two VMs (11L and 12L) are connected to same Zone.
     */
    @Test
    public void testGetMultiSourceSupplyChainForCloudVMs() throws Exception {
        doReturn(Either.right(Stream.of(cloudVMNode1, cloudVolumeNode, cloudZoneNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("11"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudVMNode2, cloudZoneNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("12"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response =
            supplyChainStub.getSupplyChain(GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(11L, 12L)))
                .build());

        final SupplyChainNode vmNode1And2combined = cloudVMNode1.toBuilder().putMembersByState(0,
                MemberList.newBuilder()
                        .addMemberOids(11L)
                        .addMemberOids(12L)
                        .build())
                .build();

        // verify that it only query ArangoDB once for the zone, although the supply chain for
        // two starting vertices (11L and 12L) return same zone (31)
        verify(graphDBService, times(1)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
            eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(4, nodes.size());
        findAndCompareSupplyChainNode(nodes, vmNode1And2combined);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudZoneNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    @Test
    public void testGetMultiSupplyChainsError() throws Exception {
        final long contextId = 123L;
        final SupplyChainSeed seed1 = SupplyChainSeed.newBuilder()
            .setSeedOid(1L)
            .setScope(SupplyChainScope.newBuilder()
                .addStartingEntityOid(1L))
            .build();
        final SupplyChainSeed seed2 = SupplyChainSeed.newBuilder()
            .setSeedOid(2L)
            .setScope(SupplyChainScope.newBuilder()
                .addStartingEntityOid(2L))
            .build();
        final SupplyChain sc2 = SupplyChain.newBuilder()
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                .setEntityType("bar"))
            .build();

        final Throwable error = Status.INVALID_ARGUMENT.withDescription("one two three").asException();

        // Right now the multi-supply-chains request just calls the regular supply chains request.
        // Override the behaviour to return the nodes we want.
        doAnswer(invocation -> {
            final GetSupplyChainRequest request = invocation.getArgumentAt(0, GetSupplyChainRequest.class);
            final StreamObserver<GetSupplyChainResponse> nodeObserver =
                    invocation.getArgumentAt(1, StreamObserver.class);
            // The first seed will result in an error, and the second seed will work fine.
            // But we shouldn't make it to the second seed - we should error out as soon as we
            // encounter an error!
            if (request.getScope().getStartingEntityOidList().equals(seed1.getScope().getStartingEntityOidList())) {
                nodeObserver.onError(error);
            } else if (request.getScope().getStartingEntityOidList().equals(seed2.getScope().getStartingEntityOidList())) {
                nodeObserver.onNext(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(sc2)
                    .build());
                nodeObserver.onCompleted();;
            }
            return null;
        }).when(supplyChainBackend).getSupplyChain(any(), any());

        final Map<Long, GetMultiSupplyChainsResponse> responseBySeedOid = new HashMap<>();
        supplyChainStub.getMultiSupplyChains(GetMultiSupplyChainsRequest.newBuilder()
            .setContextId(contextId)
            .addSeeds(seed1)
            .addSeeds(seed2)
            .build()).forEachRemaining(resp -> responseBySeedOid.put(resp.getSeedOid(), resp));

        assertThat(responseBySeedOid.get(seed1.getSeedOid()).getError(), is(error.getMessage()));
        assertThat(responseBySeedOid.get(seed2.getSeedOid()).getSupplyChain(), is(sc2));
    }

    @Test
    public void testGetSingleSourceSupplyChainSuccess() throws Exception {
        doReturn(Either.right(Stream.of(pmNode, vmNode)))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()), eq("5678"),
                eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                        eq(Collections.emptySet()),
                        eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(5678L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(2, nodes.size());
        compareSupplyChainNode(pmNode, nodes.get(0));
        compareSupplyChainNode(vmNode, nodes.get(1));
    }

    @Test
    public void testGetSingleSourceSupplyChainFiltered() throws Exception {
        doReturn(Either.right(Stream.of(pmNode, vmNode)))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllEntityTypesToInclude(Lists.newArrayList("VirtualMachine"))
                    .addAllStartingEntityOid(Lists.newArrayList(5678L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(1, nodes.size());
        compareSupplyChainNode(vmNode, nodes.get(0));
    }

    /**
     * Test the case that cloud supply chain starts from single VM. Verify that it will query
     * another supply chain based on the returned zone, and the final supply chain contains VM,
     * Volume, Zone and Region.
     */
    @Test
    public void testGetSingleSourceSupplyChainForCloudVM() throws Exception {
        doReturn(Either.right(Stream.of(cloudVMNode1, cloudVolumeNode, cloudZoneNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("11"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(11L)))
                .build());

        // verify that it requested another supply chain starting from zone to only traverse the
        // path containing only region and zone
        verify(graphDBService, times(1)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
            eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(4, nodes.size());
        findAndCompareSupplyChainNode(nodes, cloudVMNode1);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudZoneNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    /**
     * Test the case that cloud supply chain starts from single AvailabilityZone. Verify that it
     * only requested once the complete supply chain starting from zone (without the limit to only
     * traverse the path containing Region and Zone). Verify that the final supply chain contains
     * expected entities: VM, Volume, Zone, Region. And verify that it didn't request another
     * supply chain starting from zone to get Region like other cases (only traverse the path
     * containing only Region and Zone).
     */
    @Test
    public void testGetSingleSourceSupplyChainForCloudZone() throws Exception {
        doReturn(Either.right(Stream.of(cloudVMNode1, cloudVolumeNode, cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(31L)))
                .build());

        // verify that it only requested once the complete supply chain starting from zone
        verify(graphDBService, times(1)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Collections.emptySet()),
            eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        // verify that it didn't request another supply chain starting from zone
        verify(graphDBService, times(0)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
            eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(4, nodes.size());
        findAndCompareSupplyChainNode(nodes, cloudVMNode1);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudZoneNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    @Test
    public void testGetSingleSourceSupplyChainForCloudRegion() throws Exception {
        doReturn(Either.right(Stream.of(cloudVMNode1, cloudVolumeNode, cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("41"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(41L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(4, nodes.size());
        findAndCompareSupplyChainNode(nodes, cloudVMNode1);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudZoneNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    @Test
    public void testGetSingleSourceSupplyChainForAWSAccount() throws Exception {
        // there is zone in returned supply chain nodes
        doReturn(Either.right(Stream.of(cloudVMNode1, cloudVolumeNode, cloudZoneNode,
                    cloudRegionNode, cloudAccountNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("51"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(51L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        // check account node is removed
        assertEquals(4, nodes.size());
        findAndCompareSupplyChainNode(nodes, cloudVMNode1);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudZoneNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    @Test
    public void testGetSingleSourceSupplyChainForAzureAccount() throws Exception {
        // there is no zone in returned supply chain nodes
        doReturn(Either.right(Stream.of(cloudVMNode1, cloudVolumeNode, cloudRegionNode, cloudAccountNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("51"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(Lists.newArrayList(51L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        // check account node is removed
        assertEquals(3, nodes.size());
        findAndCompareSupplyChainNode(nodes, cloudVMNode1);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    @Test
    public void testGetSingleSourceSupplyChainFailure() throws Exception {
        doReturn(Either.left(new IllegalStateException("failed")))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(EnvironmentType.CLOUD)),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        expectedException.expect(GrpcRuntimeExceptionMatcher
            .hasCode(Code.INTERNAL)
            .descriptionContains("failed"));

        // Force evaluation of the stream
        supplyChainStub.getSupplyChain(GetSupplyChainRequest.newBuilder()
            .setContextId(1234L)
            .setScope(SupplyChainScope.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addAllStartingEntityOid(Lists.newArrayList(5678L)))
            .build());
    }

    @Test
    public void testGetSingleSourceSupplyChainNotFound() throws Exception {
        doReturn(Either.left(new NoSuchElementException("foo")))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(EnvironmentType.CLOUD)),
            eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Collections.emptySet()),
            eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(GetSupplyChainRequest.newBuilder()
            .setContextId(1234L)
            .setScope(SupplyChainScope.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addAllStartingEntityOid(Lists.newArrayList(5678L)))
            .build());

        assertThat(response.getSupplyChain().getMissingStartingEntitiesList(), contains(5678L));
    }

    @Test
    public void testMergedSupplyChain() throws Exception {

        final SupplyChainNode pmNode2 = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(1L)
                        .addMemberOids(5L)
                        .build())
                .setEntityType("PhysicalMachine")
                .build();
        final SupplyChainNode pmMergedNode = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(1L)
                        .addMemberOids(5L)
                        .addMemberOids(2L)
                        .build())
                .setEntityType("PhysicalMachine")
                .build();
        final SupplyChainNode vmNode2 = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(3L)
                        .addMemberOids(4L)
                        .build())
                .setEntityType("VirtualMachine")
                .build();
        final SupplyChainNode vmMergedNode = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(3L)
                        .addMemberOids(4L)
                        .addMemberOids(5L)
                        .build())
                .setEntityType("VirtualMachine")
                .build();

        doReturn(Either.right(Stream.of(pmNode, vmNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(EnvironmentType.CLOUD)),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(EnvironmentType.CLOUD)),
                eq("91011"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .setEnvironmentType(EnvironmentType.CLOUD)
                    .addAllStartingEntityOid(Lists.newArrayList(5678L, 91011L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(2, nodes.size());
        compareSupplyChainNode(pmMergedNode, nodes.get(0));
        compareSupplyChainNode(vmMergedNode, nodes.get(1));
    }

    @Test
    public void testMergedSupplyChainFiltered() throws Exception {

        final SupplyChainNode pmNode2 = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(1L)
                        .addMemberOids(5L)
                        .build())
                .setEntityType("PhysicalMachine")
                .build();
        final SupplyChainNode pmMergedNode = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(1L)
                        .addMemberOids(5L)
                        .addMemberOids(2L)
                        .build())
                .setEntityType("PhysicalMachine")
                .build();
        final SupplyChainNode vmNode2 = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(3L)
                        .addMemberOids(4L)
                        .build())
                .setEntityType("VirtualMachine")
                .build();
        final SupplyChainNode vmMergedNode = SupplyChainNode.newBuilder()
                .putMembersByState(0, MemberList.newBuilder()
                        .addMemberOids(3L)
                        .addMemberOids(4L)
                        .addMemberOids(5L)
                        .build())
                .setEntityType("VirtualMachine")
                .build();

        doReturn(Either.right(Stream.of(pmNode, vmNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(EnvironmentType.CLOUD)),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(EnvironmentType.CLOUD)),
                eq("91011"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .setEnvironmentType(EnvironmentType.CLOUD)
                    .addAllEntityTypesToInclude(Lists.newArrayList("PhysicalMachine"))
                    .addAllStartingEntityOid(Lists.newArrayList(5678L, 91011L)))
                .build());

        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertEquals(1, nodes.size());
        compareSupplyChainNode(pmMergedNode, nodes.get(0));
    }

    @Test
    public void testGetGlobalSupplyChainSuccess() throws Exception {
        final Map<String, SupplyChainNode> inputNodes = ImmutableMap.of(
            "PhysicalMachine", pmNode,
            "VirtualMachine", vmNode);
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of(1234L)),
                eq(Optional.of(EnvironmentType.CLOUD)),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN)))
            .thenReturn(Mono.just(inputNodes));

        // Force evaluation of the stream
        final GetSupplyChainResponse response = supplyChainStub.getSupplyChain(
            GetSupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setScope(SupplyChainScope.newBuilder()
                    .setEnvironmentType(EnvironmentType.CLOUD))
                .build());
        final List<SupplyChainNode> nodes = response.getSupplyChain().getSupplyChainNodesList();
        assertThat(nodes, containsInAnyOrder(pmNode, vmNode));
    }

    @Test
    public void testGetGlobalSupplyChainFailure() throws Exception {
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of(1234L)),
                eq(Optional.of(EnvironmentType.CLOUD)),
                eq(ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN)))
            .thenReturn(Mono.error(new RuntimeException("failed")));

        expectedException.expect(GrpcRuntimeExceptionMatcher
            .hasCode(Code.INTERNAL)
            .anyDescription());

        // Force evaluation of the stream
        Lists.newArrayList(supplyChainStub.getSupplyChain(GetSupplyChainRequest.newBuilder()
            .setContextId(1234L)
            .setScope(SupplyChainScope.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD))
            .build()));
    }

    /**
     * Find the same type of supply chain node from the list and compare them.
     */
    private void findAndCompareSupplyChainNode(List<SupplyChainNode> list, SupplyChainNode node) {
        Optional<SupplyChainNode> optionalSupplyChainNode = list.stream()
                .filter(supplyChainNode -> supplyChainNode.getEntityType().equals(node.getEntityType()))
                .findAny();
        assertTrue(optionalSupplyChainNode.isPresent());
        compareSupplyChainNode(node, optionalSupplyChainNode.get());
    }

    /**
     * Compare two {@link SupplyChainNode}s field-by-field since 'repeated' fields
     * may not have the same order.
     *
     * @param node1 first {@link SupplyChainNode} to compare
     * @param node2 second {@link SupplyChainNode} to compare
     */
    private void compareSupplyChainNode(SupplyChainNode node1, SupplyChainNode node2) {
        assertEquals(node1.getEntityType(), node2.getEntityType());
        assertEquals(node1.getSupplyChainDepth(), node2.getSupplyChainDepth());
        assertThat(node1.getMembersByStateCount(), equalTo(node2.getMembersByStateCount()));
        node1.getMembersByStateMap().forEach((state, membersForState) -> {
            assertThat(node2.getMembersByStateMap().get(state).getMemberOidsList(),
                    containsInAnyOrder(membersForState.getMemberOidsList().toArray()));
        });
        assertThat(node1.getConnectedConsumerTypesList().size(),
                equalTo(node2.getConnectedConsumerTypesList().size()));
        assertThat(node1.getConnectedConsumerTypesList(),
                containsInAnyOrder(node2.getConnectedConsumerTypesList().toArray()));
    }

}
