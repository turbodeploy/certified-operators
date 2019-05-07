package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.da;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.lp;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.nodeMapFor;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.storage;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.vm;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Type;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.SupplyChainNodeBuilder;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.SupplyChainVertex;

public class SupplyChainSubgraphTest {

    @Test
    public void testLoadingStitchedSupplyChain() throws Exception {
        final List<ResultVertex> consumers = graphResultFromFile("protobuf/messages/supply-chain-consumers.json");
        final List<ResultVertex> providers = graphResultFromFile("protobuf/messages/supply-chain-providers.json");

        final SupplyChainSubgraph subgraph =
            new SupplyChainSubgraph(providers, consumers);

        final Map<String, SupplyChainNode> supplyChainNodes = subgraph.toSupplyChainNodes().stream()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        assertThat(supplyChainNodes.keySet(), containsInAnyOrder(
            RepoEntityType.APPLICATION.getValue(),
            RepoEntityType.VIRTUAL_MACHINE.getValue(),
            RepoEntityType.PHYSICAL_MACHINE.getValue(),
            RepoEntityType.VIRTUAL_DATACENTER.getValue(),
            RepoEntityType.DATACENTER.getValue(),
            RepoEntityType.STORAGE.getValue(),
            RepoEntityType.DISKARRAY.getValue()
        ));

        supplyChainNodes.values().stream()
            .map(RepositoryDTOUtil::getMemberCount)
            .forEach(nodeMemberCount -> assertEquals(1L, (long)nodeMemberCount));

        assertThat(supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .getConnectedConsumerTypesList(), contains(RepoEntityType.APPLICATION.getValue()));
        assertThat(supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .getConnectedProviderTypesList(),
            containsInAnyOrder(RepoEntityType.PHYSICAL_MACHINE.getValue(),
                RepoEntityType.STORAGE.getValue(),
                RepoEntityType.VIRTUAL_DATACENTER.getValue()));
    }

    @Test
    public void testMultipleEntitiesConnectedToSameProvider() {
        /**
         *   1
         *  / \
         * 22 33
         *  \ /
         *  444
         */
        ResultVertex da1 = da("444", "", "22");
        ResultVertex da2 = da("444", "", "33");
        ResultVertex storage1 = storage("22", "444", "1");
        ResultVertex storage2 = storage("33", "444", "1");
        ResultVertex vm1 = vm("1", "22", "");
        ResultVertex vm2 = vm("1", "33", "");

        List<ResultVertex> vertices =
                ImmutableList.of(vm1, vm2, storage1, storage2, da1, da2);
        final SupplyChainSubgraph subgraph =
                new SupplyChainSubgraph(vertices, vertices);
        final Map<String, SupplyChainNode> supplyChainNodes = nodeMapFor(subgraph);

        assertEquals(1, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.DISKARRAY.getValue())));
    }


    @Test
    public void testUnconnectedEntitySupplyChain() {
        ResultVertex vm = vm("1", "", "");

        List<ResultVertex> vertices = ImmutableList.of(vm);
        final SupplyChainSubgraph subgraph =
                new SupplyChainSubgraph(vertices, vertices);
        final Map<String, SupplyChainNode> supplyChainNodes = subgraph.toSupplyChainNodes().stream()
                .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        final SupplyChainNode vmNode = supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue());
        assertEquals(1, RepositoryDTOUtil.getMemberCount(vmNode));
        assertThat(vmNode.getConnectedConsumerTypesList(), is(empty()));
        assertThat(vmNode.getConnectedProviderTypesList(), is(empty()));
    }

    /**
     * Ensure self-loops in the supply chain introduced by VDC's that buy from other VDC's can be correctly
     * handled. If not properly handled, the supply chain does not contain any links below VDC (ie host,
     * datacenter, etc.)
     *
     * @throws Exception If the test files can't be loaded.
     */
    @Test
    public void testVdcBuyingFromOtherVdc() throws Exception {
        final List<ResultVertex> consumers = graphResultFromFile("protobuf/messages/supply-chain-vdc-consumers.json");
        final List<ResultVertex> providers = graphResultFromFile("protobuf/messages/supply-chain-vdc-providers.json");

        final SupplyChainSubgraph subgraph =
                new SupplyChainSubgraph(providers, consumers);
        final Map<String, SupplyChainNode> supplyChainNodes = subgraph.toSupplyChainNodes().stream()
                .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        assertThat(supplyChainNodes.keySet(), containsInAnyOrder(
                RepoEntityType.APPLICATION.getValue(),
                RepoEntityType.VIRTUAL_MACHINE.getValue(),
                RepoEntityType.PHYSICAL_MACHINE.getValue(),
                RepoEntityType.VIRTUAL_DATACENTER.getValue(),
                RepoEntityType.DATACENTER.getValue(),
                RepoEntityType.STORAGE.getValue(),
                RepoEntityType.DISKARRAY.getValue()
        ));

        assertEquals(2, RepositoryDTOUtil.getMemberCount(supplyChainNodes.get(RepoEntityType.VIRTUAL_DATACENTER.getValue())));
        assertThat(supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())
                .getConnectedConsumerTypesList(), contains(RepoEntityType.APPLICATION.getValue()));
        assertThat(supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())
                .getConnectedProviderTypesList(), contains(RepoEntityType.VIRTUAL_DATACENTER.getValue()));
    }

    /**
     * This tests the situation, in which an entity type can be found in two (or more) different depths.
     * Let A, B, and C represent entity types.  Let A1, A2 be entities of type A, B1 entity of type B,
     * and C1 entity of type C.  Let A1 consume from B1 and A2 from C2.  Consider the supply chain validation
     * graph generated with C1 as a starting point.
     *
     * Since C1 has a direct consumer A2, the indirect consumer A1 must be ignored.
     * In this test, we should see in the final graph all 3 entity types each having exactly one entity.
     */
    @Test
    public void testOneEntityTypeInMultipleDepths() {
        /*                 A (2 entities; one consumes from C and one from B)
         *               / |
         *   (1 entity) B  |
         *               \ |
         *                 C (1 entity)
         */

        ResultVertex da1 = da("3", "", "11");
        ResultVertex da2 = da("3", "", "2");
        ResultVertex storage1 = storage("2", "3", "1");
        ResultVertex vm1 = vm("1", "2", "");
        ResultVertex vm2 = vm("11", "3", "");

        List<ResultVertex> vertices =
                ImmutableList.of(da1, da2, storage1, vm1, vm2);
        final SupplyChainSubgraph subgraph =
                new SupplyChainSubgraph(vertices, vertices);
        final Map<String, SupplyChainNode> supplyChainNodes = nodeMapFor(subgraph);

        assertEquals(1, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.DISKARRAY.getValue())));
        assertEquals(1, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.STORAGE.getValue())));
        assertEquals(1, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())));
    }

    /**
     * This test is similar to {@link #testOneEntityTypeInMultipleDepths()}, but with a twist.  The
     * entities are now as follows: C=DiskArray, B=LogicalPool, A=Storage.  Since edges between
     * LogicalPool and DiskArray are mandatory, neither the direct consumer A2 nor the indirect consumer
     * A1 must be ignored.  The graph must contain 1 disk array, 1 logical pool, and 2 storage.
     */
    @Test
    public void testMandatoryEdges() {
        /*                 Storage (2 entities; one consumes from LogicalPool and one from DiskArray)
         *               /           |
         *(1 entity) LogicalPool     |
         *               \           |
         *              DiskArray (1 entity)
         */

        ResultVertex da1 = da("3", "", "11");
        ResultVertex da2 = da("3", "", "2");
        ResultVertex logicalPool1 = lp("2", "3", "");
        ResultVertex storage1 = storage("1", "2", "1");
        ResultVertex storage2 = storage("11", "3", "");

        List<ResultVertex> vertices =
                ImmutableList.of(da1, da2, logicalPool1, storage1, storage2);
        final SupplyChainSubgraph subgraph =
                new SupplyChainSubgraph(vertices, vertices);
        final Map<String, SupplyChainNode> supplyChainNodes = nodeMapFor(subgraph);

        assertEquals(1, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.DISKARRAY.getValue())));
        assertEquals(1, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.LOGICALPOOL.getValue())));
        assertEquals(2, RepositoryDTOUtil.getMemberCount(
                supplyChainNodes.get(RepoEntityType.STORAGE.getValue())));
    }

    @Test
    public void testNodeBuilder() {
        final String activeState = UIEntityState.ACTIVE.getValue();
        final String idleState = UIEntityState.IDLE.getValue();
        final int activeStateInt = UIEntityState.ACTIVE.toEntityState().getNumber();
        final int idleStateInt = UIEntityState.IDLE.toEntityState().getNumber();

        final SupplyChainNodeBuilder nodeBuilder = new SupplyChainNodeBuilder();
        nodeBuilder.setEntityType("VirtualMachine");
        nodeBuilder.setSupplyChainDepth(7);
        nodeBuilder.addMember("1", activeState);
        nodeBuilder.addMember("2", idleState);

        final SupplyChainVertex vertex1 = mock(SupplyChainVertex.class);
        final SupplyChainVertex hostVertex = mock(SupplyChainVertex.class);
        final SupplyChainVertex appVertex = mock(SupplyChainVertex.class);
        when(hostVertex.getEntityType()).thenReturn("PhysicalMachine");
        when(appVertex.getEntityType()).thenReturn("Application");

        when(vertex1.getProviders()).thenReturn(Collections.singletonList(hostVertex));
        when(vertex1.getConsumers()).thenReturn(Collections.singletonList(appVertex));

        final Map<String, SupplyChainVertex> graph = ImmutableMap.of("1", vertex1);
        final SupplyChainNode node = nodeBuilder.buildNode(graph);
        assertThat(node.getEntityType(), is("VirtualMachine"));
        assertThat(node.getSupplyChainDepth(), is(7));
        assertThat(RepositoryDTOUtil.getAllMemberOids(node), containsInAnyOrder(1L, 2L));
        assertThat(node.getConnectedConsumerTypesList(), containsInAnyOrder("Application"));
        assertThat(node.getConnectedProviderTypesList(), containsInAnyOrder("PhysicalMachine"));

        final Map<Integer, MemberList> membersByStateMap = node.getMembersByStateMap();
        assertThat(membersByStateMap.keySet(), containsInAnyOrder(activeStateInt, idleStateInt));
        assertThat(membersByStateMap.get(activeStateInt).getMemberOidsList(),
                containsInAnyOrder(1L));
        assertThat(membersByStateMap.get(idleStateInt).getMemberOidsList(),
                containsInAnyOrder(2L));
    }

    /**
     * Read {@link com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex} from a file.
     *
     * @param fileName The name of the file containing the entities.
     * @return
     * @throws Exception
     */
    private static List<ResultVertex> graphResultFromFile(@Nonnull final String fileName) throws IOException {
        final Enumeration<URL> urlEnumeration =
                SupplyChainSubgraphTest.class.getClassLoader().getResources(fileName);
        final URL url = urlEnumeration.nextElement();

        StringWriter writer = new StringWriter();
        IOUtils.copy(url.openStream(), writer, "UTF-8");
        final String resultString = writer.toString();

        Type listType = new TypeToken<ArrayList<ResultVertex>>(){}.getType();
        return new Gson().fromJson(resultString, listType);
    }

}