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
import javax.annotation.Nullable;

import com.google.common.collect.Multimap;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
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
     * This is a minimal subset of ServiceEntityApiDTO fields. In particular, Aspects are
     * not handled here.
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

        // TODO: ServiceEntityDTO should not contain an API DTO
        if (repoDTO.getEnvironmentType() != null) {
            serviceEntityApiDTO.setEnvironmentType(EnvironmentType.valueOf(repoDTO.getEnvironmentType()));
        }

        // set discoveredBy
        serviceEntityApiDTO.setDiscoveredBy(createDiscoveredBy(repoDTO.getTargetVendorIds()));

        return serviceEntityApiDTO;
    }

    /**
     * Create the discoveredBy field based on given targetIds of the se. Currently, only target id
     * is set, probe type will be handled separately in API component.
     *
     * @param targetIds associated target ids for the entity
     * @return TargetApiDTO representing the discoveredBy field of serviceEntityApiDTO
     */
    private static TargetApiDTO createDiscoveredBy(@Nullable Map<String, String> targetIds) {
        TargetApiDTO targetApiDTO = new TargetApiDTO();
        if (targetIds != null && targetIds.size() > 0) {
            // an entity may be discovered by multiple targets, just pick one of them.
            // if the entity is discovered by different probe types, it will be a random one
            targetApiDTO.setUuid(targetIds.keySet().iterator().next());
        }
        return targetApiDTO;
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
