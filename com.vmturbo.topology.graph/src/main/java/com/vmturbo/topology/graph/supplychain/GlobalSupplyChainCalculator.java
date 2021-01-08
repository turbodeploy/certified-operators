package com.vmturbo.topology.graph.supplychain;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
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
                        EntityType.CLOUD_SERVICE_VALUE, EntityType.HYPERVISOR_SERVER_VALUE,
                        EntityType.PROCESSOR_POOL_VALUE, EntityType.SERVICE_PROVIDER_VALUE);

    public static final Predicate<Integer> DEFAULT_ENTITY_TYPE_FILTER = IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN::contains;

    /**
     * Utility method. It takes two sets containing type entities
     * and an entity. It adds the consumers and the inbound connections
     * of the entity to the first list and the providers and outbound
     * connections of the entity to the second list.
     *
     * @param consumerTypes the list of entity types ids, in which to add
     *                      the entity types of consumers and ingoing
     *                      connections of the entity
     * @param providerTypes the list of entity types ids, in which to add
     *                      the entity types of providers and outgoing
     *                      connections of the entity
     * @param entity the entity
     * @param <E1> the subclass of {@link TopologyGraphEntity}
     *             that represents the entity
     */
    public static <E1 extends TopologyGraphEntity<E1>> void
            updateConsumerAndProviderTypeSets(@Nonnull Set<Integer> consumerTypes,
                                              @Nonnull Set<Integer> providerTypes,
                                              @Nonnull E1 entity) {
        providerTypes.addAll(getEntityTypesFromListOfEntities(entity.getProviders()));
        providerTypes.addAll(getEntityTypesFromListOfEntities(entity.getAggregatorsAndOwner()));
        providerTypes.addAll(getEntityTypesFromListOfEntities(entity.getControllers()));
        providerTypes.addAll(getEntityTypesFromListOfEntities(entity.getOutboundAssociatedEntities()));
        consumerTypes.addAll(getEntityTypesFromListOfEntities(entity.getConsumers()));
        consumerTypes.addAll(getEntityTypesFromListOfEntities(entity.getAggregatedAndOwnedEntities()));
        consumerTypes.addAll(getEntityTypesFromListOfEntities(entity.getControlledEntities()));
        consumerTypes.addAll(getEntityTypesFromListOfEntities(entity.getInboundAssociatedEntities()));
    }

    /**
     * Utility method to get a list of entity type ids
     * given a collection of entities.
     *
     * @param entities the entities
     * @param <E1> the subclass of {@link TopologyGraphEntity}
     *             that represents the entity
     * @return the entity type ids
     */
    private static <E1 extends TopologyGraphEntity<E1>> Set<Integer>
            getEntityTypesFromListOfEntities(@Nonnull Collection<E1> entities) {
        return entities.stream()
                .map(E1::getEntityType)
                .collect(Collectors.toSet());
    }

    /**
     * Compute the global supply chain.
     *
     * @param topology the topology
     * @param entityFilter filter for the entities to be included in
     *                     the result
     * @param entityTypesToSkip predicate used to determine if an entity type should be skipped during
     *                          traversal.
     * @param <E> The type of {@link TopologyGraphEntity} in the graph
     * @return The {@link SupplyChainNode}s of the result,
     *         grouped by entity type
     */
    @Nonnull
    public <E extends TopologyGraphEntity<E>> Map<ApiEntityType, SupplyChainNode> getSupplyChainNodes(
            @Nonnull TopologyGraph<E> topology, @Nonnull Predicate<E> entityFilter,
            @Nonnull final Predicate<Integer> entityTypesToSkip) {
        final Map<ApiEntityType, SupplyChainNode> result = new HashMap<>(topology.entityTypes().size());

        for (Integer entityTypeId : topology.entityTypes()) {
            // filter out unwanted types
            if (entityTypesToSkip.test(entityTypeId)) {
                continue;
            }

            // collect connected types and members for this type
            final Set<Integer> connectedConsumerTypes = new HashSet<>();
            final Set<Integer> connectedProviderTypes = new HashSet<>();
            final Map<EntityState, Set<Long>> membersByState = new HashMap<>();
            topology.entitiesOfType(entityTypeId)
                .filter(entityFilter)
                .forEach(entity -> {
                            updateConsumerAndProviderTypeSets(
                                connectedConsumerTypes, connectedProviderTypes, entity);
                            membersByState.computeIfAbsent(entity.getEntityState(), k -> new HashSet<>())
                                          .add(entity.getOid());
                        });

            // if no members were found, ignore this type
            if (membersByState.isEmpty()) {
                continue;
            }

            // filter unwanted types from connected type sets
            connectedConsumerTypes.removeIf(entityTypesToSkip);
            connectedProviderTypes.removeIf(entityTypesToSkip);

            final ApiEntityType entityType = ApiEntityType.fromType(entityTypeId);

            // construct the supply chain node and add it to the result
            final SupplyChainNode.Builder supplyChainNodeBuilder =
                SupplyChainNode.newBuilder().setEntityType(entityType.typeNumber());
            for (Integer consumerType : connectedConsumerTypes) {
                supplyChainNodeBuilder.addConnectedConsumerTypes(
                        ApiEntityType.fromType(consumerType).typeNumber());
            }
            for (Integer providerType : connectedProviderTypes) {
                supplyChainNodeBuilder.addConnectedProviderTypes(
                        ApiEntityType.fromType(providerType).typeNumber());
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
