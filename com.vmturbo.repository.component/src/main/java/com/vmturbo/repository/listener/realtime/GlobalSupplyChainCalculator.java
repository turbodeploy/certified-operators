package com.vmturbo.repository.listener.realtime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.repository.service.TopologyGraphSupplyChainRpcService;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainResolver;

/**
 * Utility class responsible for calculating the global supply chain of a topology represented
 * by a {@link TopologyGraph}.
 */
public class GlobalSupplyChainCalculator {

    // Use factory.
    private GlobalSupplyChainCalculator() {}

    /**
     * Compute the global supply chain for a {@link TopologyGraph}. The global supply chain
     * represents all the entity types and how they are connected to each other.
     *
     * @param entityGraph The {@link TopologyGraph}.
     * @return A map of (entity type) -> ({@link SupplyChainNode} for the entity type).
     */
    @Nonnull
    public Map<UIEntityType, SupplyChainNode> computeGlobalSupplyChain(@Nonnull final TopologyGraph<RepoGraphEntity> entityGraph,
                                                                       @Nonnull final UIEnvironmentType envType,
                                                                       @Nonnull final SupplyChainResolver<RepoGraphEntity> supplyChainResolver) {
        final Map<UIEntityType, SupplyChainNode> nodes = new HashMap<>(entityGraph.entityTypes().size());
        entityGraph.entityTypes().stream()
            .filter(type -> !TopologyGraphSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(type))
            .map(type -> {
                // Build up the sets of unique connected types and members by state.
                final Set<Integer> connectedConsumerTypes = new HashSet<>();
                final Set<Integer> connectedProviderTypes = new HashSet<>();
                final Map<EntityState, Set<Long>> membersByState = new HashMap<>();

                // Iterate over all entities of the non-ignored types.
                entityGraph.entitiesOfType(type)
                    // We should only have to filter the top-level entities by environment type,
                    // because there are no connections across environment types.
                    //
                    // i.e. if an entity is a CLOUD entity, we can assume all its providers and
                    // consumers are also CLOUD entities. Therefore we don't need to filter the
                    // connected entities by type as long as we filter the initial entities here.
                    .filter(entity -> envType.matchesEnvType(entity.getEnvironmentType()))
                    .forEach(entityOfType -> {
                        // Consumers and connected-from count as "connected consumer types"
                        supplyChainResolver.getConnectedConsumers(entityOfType)
                            .map(RepoGraphEntity::getEntityType)
                            .filter(consumerType ->
                                !TopologyGraphSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(consumerType))
                            .forEach(connectedConsumerTypes::add);
                        // Providers and connected-to count as "connected provider types"
                        supplyChainResolver.getConnectedProviders(entityOfType)
                            .map(RepoGraphEntity::getEntityType)
                            .filter(consumerType ->
                                !TopologyGraphSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(consumerType))
                            .forEach(connectedProviderTypes::add);
                        membersByState.computeIfAbsent(entityOfType.getEntityState(), k -> new HashSet<>())
                            .add(entityOfType.getOid());
                    });

                if (!membersByState.isEmpty()) {
                    final SupplyChainNode.Builder scNode = SupplyChainNode.newBuilder()
                        .setEntityType(UIEntityType.fromType(type).apiStr());

                    connectedConsumerTypes.forEach(consumerType ->
                        scNode.addConnectedConsumerTypes(UIEntityType.fromType(consumerType).apiStr()));
                    connectedProviderTypes.forEach(providerType ->
                        scNode.addConnectedProviderTypes(UIEntityType.fromType(providerType).apiStr()));

                    membersByState.forEach((state, members) -> {
                        scNode.putMembersByState(state.getNumber(), MemberList.newBuilder()
                            .addAllMemberOids(members)
                            .build());
                    });

                    return scNode.build();
                } else {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .forEach(scNode -> nodes.put(UIEntityType.fromString(scNode.getEntityType()), scNode));
        return nodes;
    }

    @Nonnull
    public static GlobalSupplyChainCalculatorFactory newFactory() {
        return GlobalSupplyChainCalculator::new;
    }

    /**
     * Factory for dependency injection.
     */
    @FunctionalInterface
    public interface GlobalSupplyChainCalculatorFactory {
        GlobalSupplyChainCalculator newCalculator();
    }
}
