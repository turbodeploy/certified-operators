package com.vmturbo.repository.graph.result;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.parameter.GraphCmd;

/**
 * The converter is responsible for converting data returned from the GraphDB to
 * data expected by the UI.
 */
public class ResultsConverter {


    private ResultsConverter() {
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
        serviceEntityApiDTO.setTags(repoDTO.getTags());

        return serviceEntityApiDTO;
    }

    /**
     * Fill relationships for the global supply chain.
     *
     * @param supplyChainNodeBuilders The different entity types in the supply chain.
     * @param relationships The provider or consumer relations between those entity types.
     * @param inverseRelationships The inverse of relationships.
     * @param direction Either PROVIDER or CONSUMER.
     */
    public static void fillNodeRelationships(
            @Nonnull final Map<String, SupplyChainNode.Builder> supplyChainNodeBuilders,
            @Nonnull final Multimap<String, String> relationships,
            @Nonnull final Multimap<String, String> inverseRelationships,
            @Nonnull final GraphCmd.SupplyChainDirection direction) {
        final LinkedList<String> queue = new LinkedList<>();
        final Set<String> processed = new HashSet<>();

        for (String rel : relationships.keys()) {

            queue.push(rel);
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
                if (!currentType.equals(rel)) {
                    final List<String> inverseRels = inverseRelationships.get(currentType).stream()
                            .filter(r -> !processed.contains(r)).collect(Collectors.toList());
                    queue.addAll(inverseRels);
                }
            }
            queue.clear();
        }
    }
}
