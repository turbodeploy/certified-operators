package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.ResultsFixture.APP_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.CONTAINER_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DA_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DC_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.ST_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.fill;
import static com.vmturbo.repository.graph.result.ResultsFixture.supplyChainQueryResultFor;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.parameter.GraphCmd.SupplyChainDirection;

public class ResultsConverterTest {

    private SupplyChainExecutorResult supplyChainExecutorResult;
    private Map<String, SupplyChainNode> supplyChainNodes;

    @Test
    public void testNodeEmptyExecutorResults() {
        givenSupplyChainExecutorResult(Collections.emptyList(), Collections.emptyList());
        whenConvertToSupplyChainNodes();
        assertThat(supplyChainNodes).isEmpty();
    }

    /**
     * PM -> VM
     */
    @Test
    public void testNode_PM_VM() {
        final int numPMs = 5;
        final int numVMs = 2;
        final List<ServiceEntityRepoDTO> pmInstances = fill(numPMs, PM_TYPE);
        final List<ServiceEntityRepoDTO> vmInstances = fill(numVMs, VM_TYPE);
        final SupplyChainQueryResult consumerQueryResult = supplyChainQueryResultFor(
                PM_TYPE, pmInstances, vmInstances);

        givenSupplyChainExecutorResult(Collections.emptyList(),
                                       Collections.singletonList(consumerQueryResult));
        whenConvertToSupplyChainNodes();

        thenNodesHaveSizeAndKeys(PM_TYPE, VM_TYPE);
        thenNodeTypeHasInstances(VM_TYPE, vmInstances);
        thenNodeTypeHasInstances(PM_TYPE, pmInstances);
        thenNodeTypeHasConsumers(PM_TYPE, VM_TYPE);
    }

    /**
     * PM -> VM
     *  `--> CONTAINER
     */
    @Test
    public void testNode_PM_VM_CONTAINER() {
        final int numPMs = 5;
        final int numVMs = 2;
        final int numContainers = 2;

        final List<ServiceEntityRepoDTO> pmInstances = fill(numPMs, PM_TYPE);
        final List<ServiceEntityRepoDTO> vmInstances = fill(numVMs, VM_TYPE);
        final List<ServiceEntityRepoDTO> containerInstances = fill(numContainers, CONTAINER_TYPE);

        final SupplyChainQueryResult consumerQueryResult = supplyChainQueryResultFor(
                PM_TYPE, pmInstances,
                new ImmutableList.Builder<ServiceEntityRepoDTO>().addAll(vmInstances).addAll(containerInstances).build());

        givenSupplyChainExecutorResult(Collections.emptyList(),
                                       Collections.singletonList(consumerQueryResult));
        whenConvertToSupplyChainNodes();

        thenNodesHaveSizeAndKeys(PM_TYPE, VM_TYPE, CONTAINER_TYPE);
        thenNodeTypeHasInstances(VM_TYPE, vmInstances);
        thenNodeTypeHasInstances(PM_TYPE, pmInstances);
        thenNodeTypeHasInstances(CONTAINER_TYPE, containerInstances);
        thenNodeTypeHasConsumers(PM_TYPE, VM_TYPE, CONTAINER_TYPE);
    }

    /**
     * PM -> VM -> APP
     */
    @Test
    public void testNode_PM_VM_APP() {
        final int numPMs = 3;
        final int numVMs = 5;
        final int numApps = 5;

        final List<ServiceEntityRepoDTO> pmInstances = fill(numPMs, PM_TYPE);
        final List<ServiceEntityRepoDTO> vmInstances = fill(numVMs, VM_TYPE);
        final List<ServiceEntityRepoDTO> appInstances = fill(numApps, APP_TYPE);

        List<SupplyChainQueryResult> consumerQueryResults = Lists.newArrayList(
                supplyChainQueryResultFor(PM_TYPE,  pmInstances, vmInstances),
                supplyChainQueryResultFor(VM_TYPE,  vmInstances, appInstances),
                supplyChainQueryResultFor(APP_TYPE, appInstances, Collections.emptyList())
        );

        givenSupplyChainExecutorResult(Collections.emptyList(), consumerQueryResults);
        whenConvertToSupplyChainNodes();

        thenNodesHaveSizeAndKeys(PM_TYPE, VM_TYPE, APP_TYPE);
        thenNodeTypeHasInstances(VM_TYPE, vmInstances);
        thenNodeTypeHasInstances(PM_TYPE, pmInstances);
        thenNodeTypeHasInstances(APP_TYPE, appInstances);
        thenNodeTypeHasConsumers(PM_TYPE, VM_TYPE);
        thenNodeTypeHasConsumers(VM_TYPE, APP_TYPE);
    }


    /**
     * DC -> PM -> VM
     * Start from PM.
     */
    @Test
    public void testNode_DC_PM_VM() {
        final int numDCs = 1;
        final int numPMs = 3;
        final int numVMs = 5;

        final List<ServiceEntityRepoDTO> dcInstances = fill(numDCs, DC_TYPE);
        final List<ServiceEntityRepoDTO> pmInstances = fill(numPMs, PM_TYPE);
        final List<ServiceEntityRepoDTO> vmInstances = fill(numVMs, VM_TYPE);

        List<SupplyChainQueryResult> consumerQueryResults = Lists.newArrayList(
                supplyChainQueryResultFor(PM_TYPE, pmInstances, vmInstances),
                supplyChainQueryResultFor(VM_TYPE, vmInstances, Collections.emptyList())
        );

        List<SupplyChainQueryResult> providerQueryResults = Lists.newArrayList(
                supplyChainQueryResultFor(PM_TYPE, pmInstances, dcInstances)
        );

        givenSupplyChainExecutorResult(providerQueryResults, consumerQueryResults);
        whenConvertToSupplyChainNodes();

        thenNodesHaveSizeAndKeys(PM_TYPE, VM_TYPE, DC_TYPE);
        thenNodeTypeHasInstances(VM_TYPE, vmInstances);
        thenNodeTypeHasInstances(PM_TYPE, pmInstances);
        thenNodeTypeHasInstances(DC_TYPE, dcInstances);
        thenNodeTypeHasConsumers(PM_TYPE, VM_TYPE);
        thenNodeTypeHasProviders(PM_TYPE, DC_TYPE);
    }


    /**
     * DC -> PM -> VM -> APP
     *        `--> CONTAINER
     */
    @Test
    public void testNode_DC_PM_VM_CONTAINER_APP() {
        final int numDCs = 1;
        final int numPMs = 3;
        final int numVMs = 5;
        final int numApps = 10;
        final int numContainers = 10;

        final List<ServiceEntityRepoDTO> dcInstances = fill(numDCs, DC_TYPE);
        final List<ServiceEntityRepoDTO> pmInstances = fill(numPMs, PM_TYPE);
        final List<ServiceEntityRepoDTO> vmInstances = fill(numVMs, VM_TYPE);
        final List<ServiceEntityRepoDTO> appInstances = fill(numApps, APP_TYPE);
        final List<ServiceEntityRepoDTO> containerInstances = fill(numContainers, CONTAINER_TYPE);

        final List<ServiceEntityRepoDTO> pmConsumers = new ImmutableList.Builder<ServiceEntityRepoDTO>()
                .addAll(vmInstances)
                .addAll(containerInstances)
                .build();

        List<SupplyChainQueryResult> consumerQueryResults = Lists.newArrayList(
                supplyChainQueryResultFor(PM_TYPE, pmInstances, pmConsumers),
                supplyChainQueryResultFor(VM_TYPE, vmInstances, appInstances),
                supplyChainQueryResultFor(CONTAINER_TYPE, containerInstances, Collections.emptyList()),
                supplyChainQueryResultFor(APP_TYPE, appInstances, Collections.emptyList())
        );

        List<SupplyChainQueryResult> providerQueryResults = Lists.newArrayList(
                supplyChainQueryResultFor(PM_TYPE, pmInstances, dcInstances)
        );

        givenSupplyChainExecutorResult(providerQueryResults, consumerQueryResults);
        whenConvertToSupplyChainNodes();

        thenNodesHaveSizeAndKeys(PM_TYPE, VM_TYPE, DC_TYPE, APP_TYPE, CONTAINER_TYPE);

        thenNodeTypeHasInstances(PM_TYPE, pmInstances);
        thenNodeTypeHasInstances(VM_TYPE, vmInstances);
        thenNodeTypeHasInstances(DC_TYPE, dcInstances);
        thenNodeTypeHasInstances(APP_TYPE, appInstances);
        thenNodeTypeHasInstances(CONTAINER_TYPE, containerInstances);

        thenNodeTypeHasConsumers(PM_TYPE, VM_TYPE, CONTAINER_TYPE);
        thenNodeTypeHasConsumers(VM_TYPE, APP_TYPE);
        thenNodeTypeHasProviders(PM_TYPE, DC_TYPE);
    }

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

    private void givenSupplyChainExecutorResult(final List<SupplyChainQueryResult> providers,
                                                final List<SupplyChainQueryResult> consumers) {
        supplyChainExecutorResult = new SupplyChainExecutorResult(providers, consumers);
    }

    private void whenConvertToSupplyChainNodes() {
        supplyChainNodes = ResultsConverter.toSupplyChainNodes(supplyChainExecutorResult)
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));
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

    private void thenNodeTypeHasInstances(final String entityType, final Collection<ServiceEntityRepoDTO> entities) {
        thenNodeTypeHasOids(entityType, entities.stream()
            .map(entity -> Long.parseLong(entity.getOid()))
            .collect(Collectors.toList()));
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