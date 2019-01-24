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
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;
import reactor.core.publisher.Mono;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;

public class SupplyChainRpcServiceTest {

    private GraphDBService graphDBService = mock(GraphDBService.class);
    private SupplyChainService supplyChainService = mock(SupplyChainService.class);
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private SupplyChainRpcService supplyChainBackend =
        Mockito.spy(new SupplyChainRpcService(graphDBService, supplyChainService, userSessionContext));

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
            .addConnectedProviderTypes(RepoEntityType.AVAILABILITY_ZONE.getValue())
            .setEntityType(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .build();

    private final SupplyChainNode cloudVMNode2 = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(12L)
                    .build())
            .addConnectedProviderTypes(RepoEntityType.AVAILABILITY_ZONE.getValue())
            .setEntityType(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .build();

    private final SupplyChainNode cloudVolumeNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(21L)
                    .build())
            .addConnectedConsumerTypes(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .addConnectedProviderTypes(RepoEntityType.AVAILABILITY_ZONE.getValue())
            .setEntityType(RepoEntityType.VIRTUAL_VOLUME.getValue())
            .build();

    private final SupplyChainNode cloudZoneNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(31L)
                    .build())
            .addConnectedConsumerTypes(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .addConnectedConsumerTypes(RepoEntityType.VIRTUAL_VOLUME.getValue())
            .setEntityType(RepoEntityType.AVAILABILITY_ZONE.getValue())
            .build();

    private final SupplyChainNode cloudRegionNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(41L)
                    .build())
            .addConnectedProviderTypes(RepoEntityType.AVAILABILITY_ZONE.getValue())
            .setEntityType(RepoEntityType.REGION.getValue())
            .build();

    private final SupplyChainNode cloudAccountNode = SupplyChainNode.newBuilder()
            .putMembersByState(0, MemberList.newBuilder()
                    .addMemberOids(51)
                    .build())
            .addConnectedProviderTypes(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .addConnectedProviderTypes(RepoEntityType.VIRTUAL_VOLUME.getValue())
            .setEntityType(RepoEntityType.BUSINESS_ACCOUNT.getValue())
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

        doReturn(Optional.of(RepoEntityType.VIRTUAL_MACHINE.getValue())).when(
                supplyChainBackend).getRepoEntityType(5678L);
        doReturn(Optional.of(RepoEntityType.VIRTUAL_MACHINE.getValue())).when(
                supplyChainBackend).getRepoEntityType(91011L);
        doReturn(Optional.of(RepoEntityType.VIRTUAL_MACHINE.getValue())).when(
                supplyChainBackend).getRepoEntityType(11L);
        doReturn(Optional.of(RepoEntityType.VIRTUAL_MACHINE.getValue())).when(
                supplyChainBackend).getRepoEntityType(12L);
        doReturn(Optional.of(RepoEntityType.AVAILABILITY_ZONE.getValue())).when(
                supplyChainBackend).getRepoEntityType(31L);
        doReturn(Optional.of(RepoEntityType.REGION.getValue())).when(
                supplyChainBackend).getRepoEntityType(41L);
        doReturn(Optional.of(RepoEntityType.BUSINESS_ACCOUNT.getValue())).when(
                supplyChainBackend).getRepoEntityType(51L);
    }

    @Test
    public void testGetMultiSupplyChainsSuccess() throws Exception {
        final long contextId = 123L;
        final SupplyChainSeed seed1 = SupplyChainSeed.newBuilder()
                .setSeedOid(1L)
                .addStartingEntityOid(1L)
                .build();
        final SupplyChainNode node1 = SupplyChainNode.newBuilder()
                .setEntityType("foo")
                .build();
        final SupplyChainSeed seed2 = SupplyChainSeed.newBuilder()
                .setSeedOid(2L)
                .addStartingEntityOid(2L)
                .build();
        final SupplyChainNode node2 = SupplyChainNode.newBuilder()
                .setEntityType("bar")
                .build();

        // Right now the multi-supply-chains request just calls the regular supply chains request.
        // Override the behaviour to return the nodes we want.
        doAnswer(invocation -> {
            final SupplyChainRequest request = invocation.getArgumentAt(0, SupplyChainRequest.class);
            final StreamObserver<SupplyChainNode> nodeObserver =
                    invocation.getArgumentAt(1, StreamObserver.class);
            if (request.getStartingEntityOidList().equals(seed1.getStartingEntityOidList())) {
                nodeObserver.onNext(node1);
                nodeObserver.onCompleted();;
            } else if (request.getStartingEntityOidList().equals(seed2.getStartingEntityOidList())) {
                nodeObserver.onNext(node2);
                nodeObserver.onCompleted();;
            }
            return null;
        }).when(supplyChainBackend).getSupplyChain(any(), any());


        final Map<Long, MultiSupplyChainsResponse> responseBySeedOid = new HashMap<>();
        supplyChainStub.getMultiSupplyChains(MultiSupplyChainsRequest.newBuilder()
                .setContextId(contextId)
                .addSeeds(seed1)
                .addSeeds(seed2)
                .build()).forEachRemaining(resp -> responseBySeedOid.put(resp.getSeedOid(), resp));
        assertThat(responseBySeedOid.size(), is(2));
        assertThat(responseBySeedOid.get(seed1.getSeedOid()).getSupplyChainNodesList(), contains(node1));
        assertThat(responseBySeedOid.get(seed2.getSeedOid()).getSupplyChainNodesList(), contains(node2));
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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudVMNode2, cloudZoneNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("12"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(11L, 12L))
                        .build()));

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
            eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

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
                .addStartingEntityOid(1L)
                .build();
        final SupplyChainSeed seed2 = SupplyChainSeed.newBuilder()
                .setSeedOid(2L)
                .addStartingEntityOid(2L)
                .build();
        final SupplyChainNode node2 = SupplyChainNode.newBuilder()
                .setEntityType("bar")
                .build();

        final String errorDescription = "one two three";

        // Right now the multi-supply-chains request just calls the regular supply chains request.
        // Override the behaviour to return the nodes we want.
        doAnswer(invocation -> {
            final SupplyChainRequest request = invocation.getArgumentAt(0, SupplyChainRequest.class);
            final StreamObserver<SupplyChainNode> nodeObserver =
                    invocation.getArgumentAt(1, StreamObserver.class);
            // The first seed will result in an error, and the second seed will work fine.
            // But we shouldn't make it to the second seed - we should error out as soon as we
            // encounter an error!
            if (request.getStartingEntityOidList().equals(seed1.getStartingEntityOidList())) {
                nodeObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorDescription).asException());
            } else if (request.getStartingEntityOidList().equals(seed2.getStartingEntityOidList())) {
                nodeObserver.onNext(node2);
                nodeObserver.onCompleted();;
            }
            return null;
        }).when(supplyChainBackend).getSupplyChain(any(), any());

        final Map<Long, MultiSupplyChainsResponse> responseBySeedOid = new HashMap<>();
        try {
            supplyChainStub.getMultiSupplyChains(MultiSupplyChainsRequest.newBuilder()
                    .setContextId(contextId)
                    .addSeeds(seed1)
                    .addSeeds(seed2)
                    .build()).forEachRemaining(resp -> responseBySeedOid.put(resp.getSeedOid(), resp));
        } catch (StatusRuntimeException e) {
            // We shouldn't have gotten ANY responses.
            assertThat(responseBySeedOid.size(), is(0));
            assertTrue(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(errorDescription)
                .matches(e));
        }
    }

    @Test
    public void testGetSingleSourceSupplyChainSuccess() throws Exception {
        doReturn(Either.right(Stream.of(pmNode, vmNode)))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()), eq("5678"),
                eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                        eq(Collections.emptySet()),
                        eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
            supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .addAllStartingEntityOid(Lists.newArrayList(5678L))
                .build()));

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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
            supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .addAllEntityTypesToInclude(Lists.newArrayList("VirtualMachine"))
                .addAllStartingEntityOid(Lists.newArrayList(5678L))
                .build()));

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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(11L))
                        .build()));

        // verify that it requested another supply chain starting from zone to only traverse the
        // path containing only region and zone
        verify(graphDBService, times(1)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
            eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(31L))
                        .build()));

        // verify that it only requested once the complete supply chain starting from zone
        verify(graphDBService, times(1)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Collections.emptySet()),
            eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        // verify that it didn't request another supply chain starting from zone
        verify(graphDBService, times(0)).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
            eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
            eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
            eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("41"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(41L))
                        .build()));

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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN));

        doReturn(Either.right(Stream.of(cloudZoneNode, cloudRegionNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()),
                eq("31"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE)),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(51L))
                        .build()));

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
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(51L))
                        .build()));

        // check account node is removed
        assertEquals(3, nodes.size());
        findAndCompareSupplyChainNode(nodes, cloudVMNode1);
        findAndCompareSupplyChainNode(nodes, cloudVolumeNode);
        findAndCompareSupplyChainNode(nodes, cloudRegionNode);
    }

    @Test
    public void testGetSingleSourceSupplyChainFailure() throws Exception {
        doReturn(Either.left("failed"))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        expectedException.expect(GrpcRuntimeExceptionMatcher
            .hasCode(Code.INTERNAL)
            .descriptionContains("failed"));

        // Force evaluation of the stream
        Lists.newArrayList(supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
            .setContextId(1234L)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addAllStartingEntityOid(Lists.newArrayList(5678L))
            .build()));
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
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq("91011"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .addAllStartingEntityOid(Lists.newArrayList(5678L, 91011L))
                        .build()));

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
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq("5678"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq("91011"), eq(Optional.of(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE)),
                eq(Collections.emptySet()),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .addAllEntityTypesToInclude(Lists.newArrayList("PhysicalMachine"))
                        .addAllStartingEntityOid(Lists.newArrayList(5678L, 91011L))
                        .build()));

        assertEquals(1, nodes.size());
        compareSupplyChainNode(pmMergedNode, nodes.get(0));
    }

    @Test
    public void testGetGlobalSupplyChainSuccess() throws Exception {
        final Map<String, SupplyChainNode> inputNodes = ImmutableMap.of(
            "PhysicalMachine", pmNode,
            "VirtualMachine", vmNode);
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of(1234L)),
                eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN)))
            .thenReturn(Mono.just(inputNodes));

        // Force evaluation of the stream
        final List<SupplyChainNode> nodes = Lists.newArrayList(
            supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build()));
        assertThat(nodes, containsInAnyOrder(pmNode, vmNode));
    }

    @Test
    public void testGetGlobalSupplyChainFailure() throws Exception {
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of(1234L)),
                eq(Optional.of(UIEnvironmentType.CLOUD)),
                eq(SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN)))
            .thenReturn(Mono.error(new RuntimeException("failed")));

        expectedException.expect(GrpcRuntimeExceptionMatcher
            .hasCode(Code.INTERNAL)
            .anyDescription());

        // Force evaluation of the stream
        Lists.newArrayList(supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
            .setContextId(1234L)
            .setEnvironmentType(EnvironmentType.CLOUD)
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