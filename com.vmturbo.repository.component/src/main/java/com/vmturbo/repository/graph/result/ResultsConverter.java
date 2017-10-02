package com.vmturbo.repository.graph.result;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;

/**
 * The converter is responsible for converting data returned from the GraphDB to
 * data expected by the UI.
 */
public class ResultsConverter {

    private static final String PHYSICAL_MACHINE = "PhysicalMachine";

    /**
     * Convert {@link SupplyChainExecutorResult} returned the {@link GraphDBExecutor} to
     * a list of {@link SupplyChainNode}s.
     *
     * @param results The results returned by the {@link GraphDBExecutor}.
     * @return {@link SupplyChainNode}s.
     */
    @Nonnull
    public static java.util.stream.Stream<SupplyChainNode> toSupplyChainNodes(final SupplyChainExecutorResult results) {
        Map<String, SupplyChainNode.Builder> nodes = new HashMap<>(
            Math.max(results.getConsumers().size(), results.getProviders().size()));

        results.getConsumers().forEach(consumer -> {
            SupplyChainNode.Builder nodeBuilder = nodes.computeIfAbsent(
                consumer.getType(), (nodeEntityType) -> nodeBuilderFor(consumer));
            nodeBuilder.addAllConnectedConsumerTypes(consumer.getNeighbourTypes());
            addNeighbors(nodes, consumer.getNeighbourInstances());
        });
        results.getProviders().forEach(provider -> {
            SupplyChainNode.Builder nodeBuilder = nodes.computeIfAbsent(
                provider.getType(), (nodeEntityType) -> nodeBuilderFor(provider));
            nodeBuilder.addAllConnectedProviderTypes(provider.getNeighbourTypes());
            addNeighbors(nodes, provider.getNeighbourInstances());
        });

        return nodes.values().stream()
            .map(SupplyChainNode.Builder::build);
    }

    private static void addNeighbors(@Nonnull Map<String, SupplyChainNode.Builder> nodes,
                                     @Nonnull Map<String, List<SupplyChainNeighbour>> neighbourInstances) {
        neighbourInstances.forEach((type, entities) -> {
            nodes.computeIfAbsent(type, (k) -> nodeBuilderFor(k, entities));
        });
    }

    @Nonnull
    private static SupplyChainNode.Builder nodeBuilderFor(@Nonnull final SupplyChainQueryResult queryResult) {
        SupplyChainNode.Builder nodeBuilder = SupplyChainNode.newBuilder()
            .setEntityType(queryResult.getType())
            .setSupplyChainDepth(0);

        queryResult.getInstances().forEach(oid -> nodeBuilder.addMemberOids(Long.parseLong(oid)));

        return nodeBuilder;
    }

    @Nonnull
    private static SupplyChainNode.Builder nodeBuilderFor(@Nonnull final String type,
                                                          @Nonnull final List<SupplyChainNeighbour> neighborType) {
        SupplyChainNode.Builder nodeBuilder = SupplyChainNode.newBuilder()
            .setEntityType(type)
            .setSupplyChainDepth(0);

        neighborType.stream()
            .map(SupplyChainNeighbour::getId)
            .distinct()
            .map(Long::parseLong)
            .forEach(nodeBuilder::addMemberOids);

        return nodeBuilder;
    }

    /**
     * Convert a {@link ServiceEntityRepoDTO} into a {@link ServiceEntityApiDTO}.
     *
     * The {@link ServiceEntityRepoDTO} contains similar set of fields as {@link ServiceEntityApiDTO}.
     *
     * @param repoDTO The {@link ServiceEntityRepoDTO} to be converted.
     * @return A {@link ServiceEntityApiDTO}.
     */
    public static ServiceEntityApiDTO toServiceEntityApiDTO(final ServiceEntityRepoDTO repoDTO) {
        final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();

        serviceEntityApiDTO.setDisplayName(repoDTO.getDisplayName());
        serviceEntityApiDTO.setUuid(repoDTO.getUuid());
        serviceEntityApiDTO.setClassName(repoDTO.getEntityType());
        serviceEntityApiDTO.setState(repoDTO.getState());

        return serviceEntityApiDTO;
    }

    /**
     * Fill relationships for the global supply chain.
     *
     * @param supplyChainNodeBuilders The different entity types in the supply chain.
     * @param relationships The provider or consumer relations between those entity types.
     * @param direction Either PROVIDER or CONSUMER.
     * @return The entries inside the supply chain.
     */
    public static void fillNodeRelationships(
            @Nonnull final Map<String, SupplyChainNode.Builder> supplyChainNodeBuilders,
            @Nonnull final Multimap<String, String> relationships,
            @Nonnull final Multimap<String, String> inverseRelationships,
            @Nonnull final GraphCmd.SupplyChainDirection direction) {
        final LinkedList<String> queue = new LinkedList<>();
        final Set<String> processed = new HashSet<>();

        // The result needs to be scoped to PHYSICAL_MACHINE, that is how Legacy works.
        queue.push(PHYSICAL_MACHINE);
        while (!queue.isEmpty()) {
            final String currentType = queue.pop();

            if (processed.contains(currentType)) {
                continue;
            }

            final Collection<String> rels = relationships.get(currentType);
            final Optional<SupplyChainNode.Builder> supplyChainNodeBuilder =
                Optional.ofNullable(supplyChainNodeBuilders.get(currentType));

            supplyChainNodeBuilder.ifPresent(nodeBuilder -> {
                if (direction == GraphCmd.SupplyChainDirection.PROVIDER) {
                    nodeBuilder.addAllConnectedProviderTypes(rels);
                } else if (direction == GraphCmd.SupplyChainDirection.CONSUMER) {
                    nodeBuilder.addAllConnectedConsumerTypes(rels);
                }
            });

            processed.add(currentType);

            queue.addAll(rels);

            // Check and create sublinks
            // Current UI does not perform this step.
            if (!currentType.equals(PHYSICAL_MACHINE)) {
                final List<String> inverseRels = inverseRelationships.get(currentType).stream()
                        .filter(r -> !processed.contains(r)).collect(Collectors.toList());
                queue.addAll(inverseRels);
            }
        }
    }
}
