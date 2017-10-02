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

import org.junit.After;
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

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;

public class SupplyChainRpcServiceTest {

    private GraphDBService graphDBService = mock(GraphDBService.class);
    private SupplyChainService supplyChainService = mock(SupplyChainService.class);

    private SupplyChainRpcService supplyChainBackend =
        new SupplyChainRpcService(graphDBService, supplyChainService);

    private GrpcTestServer server;

    private SupplyChainServiceBlockingStub supplyChainStub;

    private final SupplyChainNode pmNode = SupplyChainNode.newBuilder()
            .addAllMemberOids(Lists.newArrayList(1L, 2L))
            .setEntityType("PhysicalMachine")
            .build();
    private final SupplyChainNode vmNode = SupplyChainNode.newBuilder()
            .addAllMemberOids(Lists.newArrayList(3L, 4L, 5L))
            .setEntityType("VirtualMachine")
            .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        server = GrpcTestServer.withServices(supplyChainBackend);
        supplyChainStub = SupplyChainServiceGrpc.newBlockingStub(server.getChannel());
    }

    @After
    public void teardown() {
        server.close();
    }

    @Test
    public void testGetSingleSourceSupplyChainSuccess() throws Exception {
        doReturn(Either.right(Stream.of(pmNode, vmNode)))
            .when(graphDBService).getSupplyChain(eq(Optional.of("1234")), eq("5678"));

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
    public void testGetSingleSourceSupplyChainFailure() throws Exception {
        doReturn(Either.left("failed"))
            .when(graphDBService).getSupplyChain(eq(Optional.of("1234")), eq("5678"));

        expectedException.expect(GrpcExceptionMatcher
            .code(Code.INTERNAL)
            .descriptionContains("failed"));

        // Force evaluation of the stream
        Lists.newArrayList(supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
            .setContextId(1234L)
                .addAllStartingEntityOid(Lists.newArrayList(5678L))
            .build()));
    }

    @Test
    public void testMergedSupplyChain() throws Exception {

        final SupplyChainNode pmNode2 = SupplyChainNode.newBuilder()
                .addAllMemberOids(Lists.newArrayList(1L, 5L))
                .setEntityType("PhysicalMachine")
                .build();
        final SupplyChainNode pmMergedNode = SupplyChainNode.newBuilder()
                .addAllMemberOids(Lists.newArrayList(1L, 5L, 2L))
                .setEntityType("PhysicalMachine")
                .build();
        final SupplyChainNode vmNode2 = SupplyChainNode.newBuilder()
                .addAllMemberOids(Lists.newArrayList(3L, 4L))
                .setEntityType("VirtualMachine")
                .build();
        final SupplyChainNode vmMergedNode = SupplyChainNode.newBuilder()
                .addAllMemberOids(Lists.newArrayList(3L, 4L, 5L))
                .setEntityType("VirtualMachine")
                .build();

        doReturn(Either.right(Stream.of(pmNode, vmNode)))
                .when(graphDBService).getSupplyChain(eq(Optional.of("1234")), eq("5678"));
        doReturn(Either.right(Stream.of(pmNode2, vmNode2)))
                .when(graphDBService).getSupplyChain(eq(Optional.of("1234")), eq("91011"));

        final List<SupplyChainNode> nodes = Lists.newArrayList(
                supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                        .setContextId(1234L)
                        .addAllStartingEntityOid(Lists.newArrayList(5678L, 91011L))
                        .build()));

        assertEquals(2, nodes.size());
        compareSupplyChainNode(pmMergedNode, nodes.get(0));
        compareSupplyChainNode(vmMergedNode, nodes.get(1));
    }

    @Test
    public void testGetGlobalSupplyChainSuccess() throws Exception {
        final Map<String, SupplyChainNode> inputNodes = ImmutableMap.of(
            "PhysicalMachine", pmNode,
            "VirtualMachine", vmNode);
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of("1234"))))
            .thenReturn(Mono.just(inputNodes));

        // Force evaluation of the stream
        final List<SupplyChainNode> nodes = Lists.newArrayList(
            supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
                .setContextId(1234L)
                .build()));
        assertThat(nodes, containsInAnyOrder(pmNode, vmNode));
    }

    @Test
    public void testGetGlobalSupplyChainFailure() throws Exception {
        when(supplyChainService.getGlobalSupplyChain(eq(Optional.of("1234"))))
            .thenReturn(Mono.error(new RuntimeException("failed")));

        expectedException.expect(GrpcExceptionMatcher
            .code(Code.INTERNAL)
            .withCause(IOException.class));

        // Force evaluation of the stream
        Lists.newArrayList(supplyChainStub.getSupplyChain(SupplyChainRequest.newBuilder()
            .setContextId(1234L)
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
        assertThat(node1.getMemberOidsList().size(),
                equalTo(node2.getMemberOidsList().size()));
        assertThat(node1.getMemberOidsList(),
                containsInAnyOrder(node2.getMemberOidsList().toArray()));
        assertThat(node1.getConnectedConsumerTypesList().size(),
                equalTo(node2.getConnectedConsumerTypesList().size()));
        assertThat(node1.getConnectedConsumerTypesList(),
                containsInAnyOrder(node2.getConnectedConsumerTypesList().toArray()));
    }

}