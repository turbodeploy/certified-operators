package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.ResultsFixture.APP_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DA_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DC_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.ST_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.fill;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.parameter.GraphCmd.SupplyChainDirection;

public class ResultsConverterTest {

    private Map<String, SupplyChainNode> supplyChainNodes;

    @Test
    public void testNodeSingleGlobalSupplyChain() {
        Map<String, Set<Long>> typesAndOids = new HashMap<>();
        addTypesAndOids(typesAndOids, fill(1, DC_TYPE));
        addTypesAndOids(typesAndOids, fill(5, PM_TYPE));
        addTypesAndOids(typesAndOids, fill(10, VM_TYPE));
        addTypesAndOids(typesAndOids, fill(10, APP_TYPE));
        addTypesAndOids(typesAndOids, fill(20, ST_TYPE));
        addTypesAndOids(typesAndOids, fill(20, DA_TYPE));

        Multimap<String, String> providerRels = HashMultimap.create();
        providerRels.put(PM_TYPE, DC_TYPE);
        providerRels.put(VM_TYPE, PM_TYPE);
        providerRels.put(VM_TYPE, ST_TYPE);
        providerRels.put(APP_TYPE, VM_TYPE);
        providerRels.put(ST_TYPE, DA_TYPE);

        whenConvertToSupplyChainNodes(typesAndOids, providerRels);

        thenNodesHaveSizeAndKeys(DC_TYPE, PM_TYPE, VM_TYPE, APP_TYPE, ST_TYPE, DA_TYPE);

        thenNodeTypeHasOids(DC_TYPE, typesAndOids.get(DC_TYPE));
        thenNodeTypeHasOids(PM_TYPE, typesAndOids.get(PM_TYPE));
        thenNodeTypeHasOids(VM_TYPE, typesAndOids.get(VM_TYPE));
        thenNodeTypeHasOids(APP_TYPE, typesAndOids.get(APP_TYPE));
        thenNodeTypeHasOids(ST_TYPE, typesAndOids.get(ST_TYPE));
        thenNodeTypeHasOids(DA_TYPE, typesAndOids.get(DA_TYPE));

        thenNodeTypeHasProviders(PM_TYPE, DC_TYPE);
        thenNodeTypeHasConsumers(PM_TYPE, VM_TYPE);
        thenNodeTypeHasConsumers(VM_TYPE, APP_TYPE);
        thenNodeTypeHasConsumers(ST_TYPE, VM_TYPE);
        thenNodeTypeHasConsumers(DA_TYPE, ST_TYPE);
    }

    private void addTypesAndOids(@Nonnull final Map<String, Set<Long>> typesAndOids,
                                 @Nonnull final List<ServiceEntityRepoDTO> entities) {
        entities.forEach(entity -> {
            Set<Long> oids = typesAndOids.computeIfAbsent(entity.getEntityType(), (entityType) -> new HashSet<Long>());
            oids.add(Long.parseLong(entity.getOid()));
        });
    }

    private void whenConvertToSupplyChainNodes(final Map<String, Set<Long>> globalResults,
                                                final Multimap<String, String> providerRelationships) {
        Map<String, SupplyChainNode.Builder> builders = SupplyChainResultsConverter.toSupplyChainNodeBuilders(globalResults);
        Multimap<String, String> consumerRelationships = HashMultimap.create();
        Multimaps.invertFrom(providerRelationships, consumerRelationships);

        ResultsConverter.fillNodeRelationships(builders, providerRelationships,
            consumerRelationships, SupplyChainDirection.PROVIDER);
        ResultsConverter.fillNodeRelationships(builders, consumerRelationships,
            providerRelationships, SupplyChainDirection.CONSUMER);

        supplyChainNodes = builders.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build()));
    }

    private void thenNodesHaveSizeAndKeys(final String... expectedKeys) {
        assertThat(supplyChainNodes).hasSize(expectedKeys.length);
        assertThat(supplyChainNodes).containsOnlyKeys(expectedKeys);
    }

    private void thenNodeTypeHasOids(final String entityType, final Collection<Long> oids) {
        assertThat(supplyChainNodes.get(entityType).getMemberOidsList())
                .hasSameElementsAs(oids);
    }

    private void thenNodeTypeHasConsumers(final String entityType, final String... consumers) {
        assertThat(supplyChainNodes.get(entityType).getConnectedConsumerTypesList())
                .hasSameElementsAs(Arrays.asList(consumers));
    }

    private void thenNodeTypeHasProviders(final String entityType, final String... providers) {
        assertThat(supplyChainNodes.get(entityType).getConnectedProviderTypesList())
            .hasSameElementsAs(Arrays.asList(providers));
    }
}