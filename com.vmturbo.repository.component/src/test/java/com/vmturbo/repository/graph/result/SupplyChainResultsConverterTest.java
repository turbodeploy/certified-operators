package com.vmturbo.repository.graph.result;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import reactor.core.publisher.Flux;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;

@RunWith(MockitoJUnitRunner.class)
public class SupplyChainResultsConverterTest {

    private Multimap<String, String> providerRels;

    @Before
    public void setUp() {
        providerRels = HashMultimap.create();
        providerRels.put("Application", "VirtualMachine");
        providerRels.putAll("VirtualMachine", ImmutableList.of("PhysicalMachine", "Storage"));
        providerRels.put("PhysicalMachine", "DataCenter");
    }

    @Test
    public void testGlobalSupplyChainConversion() {
        final List<Long> dcIds = ImmutableList.of(100L);
        final List<Long> pmIds = ImmutableList.of(1L, 2L, 3L);
        final List<Long> vmIds = ImmutableList.of(4L, 5L, 6L, 7L);
        final List<Long> networkIds = ImmutableList.of(111L);


        final Flux<TypeAndOids> entities = Flux.fromIterable(ImmutableList.of(
                new TypeAndOids("DataCenter", dcIds),
                new TypeAndOids("PhysicalMachine", pmIds),
                new TypeAndOids("VirtualMachine", vmIds),
                new TypeAndOids("Network", networkIds)
        ));

        final GlobalSupplyChainFluxResult globalResults = ImmutableGlobalSupplyChainFluxResult.builder()
                .entities(entities)
                .build();

        final Map<String, SupplyChainNode> supplyChain = SupplyChainResultsConverter
                .toSupplyChainNodes(globalResults, providerRels)
                .block();

        assertThat(supplyChain).hasSize(4);
        assertThat(supplyChain).containsKeys("DataCenter", "PhysicalMachine", "VirtualMachine");
        assertThat(supplyChain.get("DataCenter").getMemberOidsCount()).isEqualTo(dcIds.size());
        assertThat(supplyChain.get("PhysicalMachine").getMemberOidsCount()).isEqualTo(pmIds.size());
        assertThat(supplyChain.get("VirtualMachine").getMemberOidsCount()).isEqualTo(vmIds.size());
        assertThat(supplyChain.get("Network").getMemberOidsCount()).isEqualTo(networkIds.size());

        assertThat(supplyChain.get("DataCenter")).extracting(SupplyChainNode::getConnectedProviderTypesList)
                .containsExactly(Collections.emptyList());
        assertThat(supplyChain.get("DataCenter")).extracting(SupplyChainNode::getConnectedConsumerTypesList)
                .containsExactly(Collections.singletonList("PhysicalMachine"));

        assertThat(supplyChain.get("PhysicalMachine")).extracting(SupplyChainNode::getConnectedProviderTypesList)
                .containsExactly(Collections.singletonList("DataCenter"));
        assertThat(supplyChain.get("PhysicalMachine")).extracting(SupplyChainNode::getConnectedConsumerTypesList)
                .containsExactly(Collections.singletonList("VirtualMachine"));

        assertThat(supplyChain.get("VirtualMachine")).extracting(SupplyChainNode::getConnectedProviderTypesList)
                .containsExactly(Arrays.asList("PhysicalMachine", "Storage"));
        assertThat(supplyChain.get("VirtualMachine")).extracting(SupplyChainNode::getConnectedConsumerTypesList)
                .containsExactly(Collections.singletonList("Application"));

        assertThat(supplyChain.get("Network")).extracting(SupplyChainNode::getConnectedProviderTypesList)
                .containsExactly(Collections.emptyList());
        assertThat(supplyChain.get("Network")).extracting(SupplyChainNode::getConnectedConsumerTypesList)
                .containsExactly(Collections.emptyList());
    }
}
