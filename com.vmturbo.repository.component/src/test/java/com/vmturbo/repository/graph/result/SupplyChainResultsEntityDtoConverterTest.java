package com.vmturbo.repository.graph.result;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import reactor.core.publisher.Flux;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.UIEntityState;

@RunWith(MockitoJUnitRunner.class)
public class SupplyChainResultsEntityDtoConverterTest {

    private Multimap<String, String> providerRels;

    @Before
    public void setUp() {
        providerRels = HashMultimap.create();
        providerRels.put("Application", "VirtualMachine");
        providerRels.putAll("VirtualMachine", ImmutableList.of("PhysicalMachine", "Storage"));
        providerRels.put("PhysicalMachine", "DataCenter");
    }

    @Test
    public void testGlobalSupplyChainStateMap() {
        final List<Long> vmIds = ImmutableList.of(1L, 2L);
        final List<Long> idleVmIds = ImmutableList.of(3L, 4L);
        final Flux<SupplyChainOidsGroup> entities = Flux.fromIterable(ImmutableList.of(
                new SupplyChainOidsGroup("VirtualMachine", "ACTIVE", vmIds),
                new SupplyChainOidsGroup("VirtualMachine", "IDLE", idleVmIds)));

        final GlobalSupplyChainFluxResult globalResults = ImmutableGlobalSupplyChainFluxResult.builder()
                .entities(entities)
                .build();

        final Map<String, SupplyChainNode> supplyChain = SupplyChainResultsConverter
                .toSupplyChainNodes(globalResults, providerRels)
                .block();

        final SupplyChainNode vmNode = supplyChain.get("VirtualMachine");
        assertThat(vmNode, Matchers.notNullValue());
        final int idleState = UIEntityState.IDLE.toEntityState().getNumber();
        final int activeState = UIEntityState.ACTIVE.toEntityState().getNumber();
        assertThat(vmNode.getMembersByStateMap().keySet(), containsInAnyOrder(
                idleState, activeState));
        assertThat(vmNode.getMembersByStateOrThrow(activeState).getMemberOidsList(),
                contains(vmIds.toArray()));
        assertThat(vmNode.getMembersByStateOrThrow(idleState).getMemberOidsList(),
                contains(idleVmIds.toArray()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(vmNode),
                containsInAnyOrder(Stream.concat(vmIds.stream(), idleVmIds.stream()).toArray()));
    }

    @Test
    public void testGlobalSupplyChainConversion() {
        final List<Long> dcIds = ImmutableList.of(100L);
        final List<Long> pmIds = ImmutableList.of(1L, 2L, 3L);
        final List<Long> vmIds = ImmutableList.of(4L, 5L, 6L, 7L);
        final List<Long> networkIds = ImmutableList.of(111L);


        final Flux<SupplyChainOidsGroup> entities = Flux.fromIterable(ImmutableList.of(
                new SupplyChainOidsGroup("DataCenter", "ACTIVE", dcIds),
                new SupplyChainOidsGroup("PhysicalMachine", "ACTIVE", pmIds),
                new SupplyChainOidsGroup("VirtualMachine", "ACTIVE", vmIds),
                new SupplyChainOidsGroup("Network", "ACTIVE", networkIds)
        ));

        final GlobalSupplyChainFluxResult globalResults = ImmutableGlobalSupplyChainFluxResult.builder()
                .entities(entities)
                .build();

        final Map<String, SupplyChainNode> supplyChain = SupplyChainResultsConverter
                .toSupplyChainNodes(globalResults, providerRels)
                .block();

        assertThat(supplyChain.size(), is(4));
        assertThat(supplyChain.keySet(), containsInAnyOrder("DataCenter", "PhysicalMachine",
                "VirtualMachine", "Network"));
        assertThat(RepositoryDTOUtil.getMemberCount(supplyChain.get("DataCenter")), is(dcIds.size()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplyChain.get("PhysicalMachine")), is(pmIds.size()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplyChain.get("VirtualMachine")), is(vmIds.size()));
        assertThat(RepositoryDTOUtil.getMemberCount(supplyChain.get("Network")), is(networkIds.size()));

        assertThat(supplyChain.get("DataCenter").getConnectedProviderTypesList(), Matchers.empty());
        assertThat(supplyChain.get("DataCenter").getConnectedConsumerTypesList(),
                containsInAnyOrder("PhysicalMachine"));

        assertThat(supplyChain.get("PhysicalMachine").getConnectedProviderTypesList(),
                containsInAnyOrder("DataCenter"));
        assertThat(supplyChain.get("PhysicalMachine").getConnectedConsumerTypesList(),
                containsInAnyOrder("VirtualMachine"));

        assertThat(supplyChain.get("VirtualMachine").getConnectedProviderTypesList(),
                containsInAnyOrder("PhysicalMachine", "Storage"));
        assertThat(supplyChain.get("VirtualMachine").getConnectedConsumerTypesList(),
                containsInAnyOrder("Application"));

        assertThat(supplyChain.get("Network").getConnectedProviderTypesList(), Matchers.empty());
        assertThat(supplyChain.get("Network").getConnectedConsumerTypesList(), Matchers.empty());
    }
}
