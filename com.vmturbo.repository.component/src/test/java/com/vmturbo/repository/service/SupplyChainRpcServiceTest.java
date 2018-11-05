package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import javaslang.control.Either;
import reactor.core.publisher.Mono;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.mapping.UIEnvironmentType;

public class SupplyChainRpcServiceTest {

    private GraphDBService graphDBService = mock(GraphDBService.class);
    private SupplyChainService supplyChainService = mock(SupplyChainService.class);

    private SupplyChainRpcService supplyChainBackend =
        new SupplyChainRpcService(graphDBService, supplyChainService);

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

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(supplyChainBackend);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        supplyChainStub = SupplyChainServiceGrpc.newBlockingStub(server.getChannel());
    }

    @Test
    public void testGetSingleSourceSupplyChainSuccess() throws Exception {
        doReturn(Either.right(Stream.of(pmNode, vmNode)))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()), eq("5678"));

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
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.empty()), eq("5678"));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
            supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .addAllEntityTypesToInclude(Lists.newArrayList("VirtualMachine"))
                .addAllStartingEntityOid(Lists.newArrayList(5678L))
                .build()));

        assertEquals(1, nodes.size());
        compareSupplyChainNode(vmNode, nodes.get(0));
    }

    @Test
    public void testGetSingleSourceSupplyChainFailure() throws Exception {
        doReturn(Either.left("failed"))
            .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)), eq("5678"));

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
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)), eq("5678"));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),eq("91011"));

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
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)), eq("5678"));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD)),eq("91011"));

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
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD))))
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
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of(1234L)), eq(Optional.of(UIEnvironmentType.CLOUD))))
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