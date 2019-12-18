package com.vmturbo.topology.graph.supplychain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * This component calculates global supply chains.
 */
public class GlobalSupplyChainCalculator {
    /**
     * Entity types that should not be included in the global supply chain.
     */
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN =
        ImmutableSet.of(EntityType.COMPUTE_TIER_VALUE, EntityType.STORAGE_TIER_VALUE,
                        EntityType.DATABASE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
                        EntityType.BUSINESS_ACCOUNT_VALUE, EntityType.CLOUD_SERVICE_VALUE,
                        EntityType.HYPERVISOR_SERVER_VALUE, EntityType.PROCESSOR_POOL_VALUE);

    /**
     * Compute the global supply chain.
     *
     * @param topology the topology
     * @param environmentType environment to filter entities against
     * @param <E> The type of {@link TopologyGraphEntity} in the graph
     * @return The {@link SupplyChainNode}s of the result,
     *         grouped by entity type
     */
    @Nonnull
    public <E extends TopologyGraphEntity<E>> Map<UIEntityType, SupplyChainNode> getSupplyChainNodes(
            @Nonnull TopologyGraph<E> topology, @Nonnull UIEnvironmentType environmentType) {
        return getSupplyChainNodes(topology,
                                   entity -> environmentType.matchesEnvType(entity.getEnvironmentType()));
    }

    /**
     * Compute the global supply chain.
     *
     * @param topology the topology
     * @param entityFilter filter for the entities to be included in
     *                     the result
     * @param <E> The type of {@link TopologyGraphEntity} in the graph
     * @return The {@link SupplyChainNode}s of the result,
     *         grouped by entity type
     */
    @Nonnull
    public <E extends TopologyGraphEntity<E>> Map<UIEntityType, SupplyChainNode> getSupplyChainNodes(
            @Nonnull TopologyGraph<E> topology, @Nonnull Predicate<E> entityFilter) {
        final Map<UIEntityType, SupplyChainNode> result = new HashMap<>(topology.entityTypes().size());

        for (Integer entityTypeId : topology.entityTypes()) {
            // filter out unwanted types
            if (IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(entityTypeId)) {
                continue;
            }

            // collect connected types and members for this type
            final Set<Integer> connectedConsumerTypes = new HashSet<>();
            final Set<Integer> connectedProviderTypes = new HashSet<>();
            final Map<EntityState, Set<Long>> membersByState = new HashMap<>();
            topology.entitiesOfType(entityTypeId)
                .filter(entityFilter)
                .forEach(entity -> {
                            SupplyChainCalculator.updateConsumerAndProviderTypeSets(
                                connectedConsumerTypes, connectedProviderTypes, entity);
                            membersByState.computeIfAbsent(entity.getEntityState(), k -> new HashSet<>())
                                          .add(entity.getOid());
                        });

            // if no members were found, ignore this type
            if (membersByState.isEmpty()) {
                continue;
            }

            // filter unwanted types from connected type sets
            connectedConsumerTypes.removeIf(IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN::contains);
            connectedProviderTypes.removeIf(IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN::contains);

            final UIEntityType entityType = UIEntityType.fromType(entityTypeId);

            // construct the supply chain node and add it to the result
            final SupplyChainNode.Builder supplyChainNodeBuilder =
                SupplyChainNode.newBuilder().setEntityType(entityType.apiStr());
            for (Integer consumerType : connectedConsumerTypes) {
                supplyChainNodeBuilder.addConnectedConsumerTypes(
                        UIEntityType.fromType(consumerType).apiStr());
            }
            for (Integer providerType : connectedProviderTypes) {
                supplyChainNodeBuilder.addConnectedProviderTypes(
                        UIEntityType.fromType(providerType).apiStr());
            }
            for (Entry<EntityState, Set<Long>> stateAndMembers : membersByState.entrySet()) {
                supplyChainNodeBuilder.putMembersByState(
                        stateAndMembers.getKey().getNumber(),
                        MemberList.newBuilder()
                                .addAllMemberOids(stateAndMembers.getValue())
                                .build());
            }
            result.put(entityType, supplyChainNodeBuilder.build());
        }

        return result;
    }
}
