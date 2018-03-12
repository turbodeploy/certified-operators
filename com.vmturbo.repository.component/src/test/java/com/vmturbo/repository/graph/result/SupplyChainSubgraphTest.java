package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.da;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.nodeMapFor;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.storage;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.subgraphFor;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.vm;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultEdge;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.SubgraphResult;

public class SupplyChainSubgraphTest {
    @Test
    public void testLoadingStitchedSupplyChain() throws Exception {
        final SubgraphResult consumers = graphResultFromFile("protobuf/messages/supply-chain-consumers.json");
        final SubgraphResult providers = graphResultFromFile("protobuf/messages/supply-chain-providers.json");

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
            .map(node -> node.getMemberOidsList().size())
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
        SubgraphResult providersResult = subgraphFor(1)
            .providerEdges(
                vm(1).consumesFrom(storage(22)),
                vm(1).consumesFrom(storage(33))
            ).providerEdges(
                storage(22).consumesFrom(da(444)),
                storage(33).consumesFrom(da(444))
            ).build();

        final ResultVertex origin = vm(1).vertex;
        final SupplyChainSubgraph subgraph =
            new SupplyChainSubgraph(providersResult, new SubgraphResult(origin, Collections.emptyList()));
        final Map<String, SupplyChainNode> supplyChainNodes = nodeMapFor(subgraph);

        assertEquals(1, supplyChainNodes.get(RepoEntityType.DISKARRAY.getValue())
            .getMemberOidsList().size());
    }

    @Test
    public void testUnconnectedEntitySupplyChain() {
        final ResultVertex origin = vm(1).vertex;
        final SubgraphResult providersResult = new SubgraphResult(origin, Collections.emptyList());
        final SubgraphResult consumersResult = new SubgraphResult(origin, Collections.emptyList());

        final SupplyChainSubgraph subgraph =
            new SupplyChainSubgraph(providersResult, consumersResult);
        final Map<String, SupplyChainNode> supplyChainNodes = subgraph.toSupplyChainNodes().stream()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));

        final SupplyChainNode vmNode = supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue());
        assertEquals(1, vmNode.getMemberOidsList().size());
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
        final SubgraphResult consumers = graphResultFromFile("protobuf/messages/supply-chain-vdc-consumers.json");
        final SubgraphResult providers = graphResultFromFile("protobuf/messages/supply-chain-vdc-providers.json");

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

        assertEquals(2, supplyChainNodes.get(RepoEntityType.VIRTUAL_DATACENTER.getValue()).getMemberOidsCount());
        assertThat(supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())
            .getConnectedConsumerTypesList(), contains(RepoEntityType.APPLICATION.getValue()));
        assertThat(supplyChainNodes.get(RepoEntityType.VIRTUAL_MACHINE.getValue())
                .getConnectedProviderTypesList(), contains(RepoEntityType.VIRTUAL_DATACENTER.getValue()));
    }

    private static class TypeAndEdges {
        private String type;
        private List<ResultEdge> edges;

        public TypeAndEdges() {
            this.type = "";
            this.edges = Collections.emptyList();
        }

        public String getType() {
            return type;
        }

        public List<ResultEdge> getEdges() {
            return edges;
        }
    }

    /**
     * Read {@link com.vmturbo.repository.graph.result.SupplyChainSubgraphTest.TypeAndEdges} from a file.
     *
     * @param fileName The name of the file containing the entities.
     * @return
     * @throws Exception
     */
    private static SubgraphResult graphResultFromFile(@Nonnull final String fileName) throws IOException {
        final Enumeration<URL> urlEnumeration = SupplyChainSubgraphTest.class.getClassLoader().getResources(fileName);
        final URL url = urlEnumeration.nextElement();

        StringWriter writer = new StringWriter();
        IOUtils.copy(url.openStream(), writer, "UTF-8");
        final String resultString = writer.toString();

        return new Gson().fromJson(resultString, SubgraphResult.class);
    }
}