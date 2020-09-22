package com.vmturbo.action.orchestrator.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Determine when expansion is needed. Also determines how to expand.
 */
public class InvolvedEntitiesExpander {

    /**
     * These are the entities that need to be retrieved underneath BusinessApp, BusinessTxn, and
     * Service.
     * <pre>
     * BApp -> BTxn -> Service -> AppComp -> Node
     *                                       VirtualMachine  --> VDC ----> Host  --------
     *                       DatabaseServer   \    \   \             ^                \
     *                                          \    \   \___________/                v
     *                                           \    -----> Volume   ------------->  Storage
     *                                            \                                   ^
     *                                             ----------------------------------/
     * </pre>
     */
    public static final List<Integer> PROPAGATED_ARM_ENTITY_TYPES = Arrays.asList(
        ApiEntityType.APPLICATION_COMPONENT.typeNumber(),
        ApiEntityType.WORKLOAD_CONTROLLER.typeNumber(),
        ApiEntityType.CONTAINER_POD.typeNumber(),
        ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
        ApiEntityType.DATABASE_SERVER.typeNumber(),
        ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
        ApiEntityType.STORAGE.typeNumber(),
        ApiEntityType.PHYSICAL_MACHINE.typeNumber());

    private static final Set<Integer> ENTITY_TYPES_BELOW_ARM = ImmutableSet.<Integer>builder()
        .addAll(PROPAGATED_ARM_ENTITY_TYPES)
        .add(ApiEntityType.BUSINESS_APPLICATION.typeNumber())
        .add(ApiEntityType.BUSINESS_TRANSACTION.typeNumber())
        .add(ApiEntityType.SERVICE.typeNumber())
        .build();

    private static final Set<Integer> ARM_ENTITY_TYPE = ImmutableSet.of(
        ApiEntityType.BUSINESS_APPLICATION.typeNumber(),
        ApiEntityType.BUSINESS_TRANSACTION.typeNumber(),
        ApiEntityType.SERVICE.typeNumber());

    private final Map<Integer, LongSet> expandedEntitiesPerARMEntityType = new HashMap<>();

    private static Logger logger = LogManager.getLogger();

    private final ActionTopologyStore actionTopologyStore;

    private final SupplyChainCalculator supplyChainCalculator;

    /**
     * Creates an instance that uses the provided services for calculating expansion.
     *
     * @param actionTopologyStore Retrieve small topology cache.
     * @param supplyChainCalculator Calculate the supply chain.
     */
    public InvolvedEntitiesExpander(@Nonnull final ActionTopologyStore actionTopologyStore,
            @Nonnull final SupplyChainCalculator supplyChainCalculator) {
        this.actionTopologyStore = actionTopologyStore;
        this.supplyChainCalculator = supplyChainCalculator;
    }

    /**
     * Check if an entity's actions should be propagated to the required ARM entity type.
     *
     * @param involvedEntityId the involved entity ID.
     * @param desiredEntityTypes the ARM entity types in the query.
     * @return true if this entity's actions should be propagated to the required ARM entity types.
     */
    public boolean isBelowARMEntityType(long involvedEntityId, Set<Integer> desiredEntityTypes) {
        return desiredEntityTypes.stream().anyMatch(entityType -> isARMEntityType(entityType)
                && expandedEntitiesPerARMEntityType.get(entityType).contains(involvedEntityId));
    }

    /**
     * Check if the given entity type is an ARM entity type.
     *
     * @param entityType the entity type.
     * @return true if this is an ARM entity type.
     */
    public boolean isARMEntityType(int entityType) {
        return ARM_ENTITY_TYPE.contains(entityType);
    }

    /**
     * Determines the filter needed for the input involvedEntities. If involvedEntities contains
     * at least one non-ARM entity, then they will not be expanded. If they are all ARM entities,
     * they will be expanded to include the entities below it in the supply chain.
     *
     * @param involvedEntities the entities to be expanded.
     * @return the filter needed for the input involvedEntities.
     */
    public InvolvedEntitiesFilter expandInvolvedEntitiesFilter(
            @Nonnull final Collection<Long> involvedEntities) {
        Optional<ActionRealtimeTopology> topology = actionTopologyStore.getSourceTopology();
        if (topology.isPresent()) {
            TopologyGraph<ActionGraphEntity> graph = topology.get().entityGraph();
            if (areAllARMEntities(involvedEntities, graph)) {
                return new InvolvedEntitiesFilter(expandARMEntities(involvedEntities, graph),
                        InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
            }
        }
        return new InvolvedEntitiesFilter(
            new HashSet<>(involvedEntities),
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
    }

    /**
     * Queries repositoryService to see if we need to expand these entities. If the entire list
     * contains ARM entities, then the involved entities will need to be expanded.
     *
     * @param involvedEntities the entities to search if we need expansion.
     * @param graph The topology graph to use to look up the entities.
     * @return true if we need expansion.
     */
    private boolean areAllARMEntities(@Nonnull final Collection<Long> involvedEntities,
                                      TopologyGraph<ActionGraphEntity> graph) {
        if (involvedEntities.isEmpty()) {
            return false;
        }

        return involvedEntities.stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(ActionGraphEntity::getEntityType)
                // Make sure all entities are ARM.
                .allMatch(ARM_ENTITY_TYPE::contains);
    }

    /**
     * Expand all ARM entities by type and save the mapping for later use.
     */
    public void expandAllARMEntities() {
        expandedEntitiesPerARMEntityType.clear();
        Optional<ActionRealtimeTopology> topology = actionTopologyStore.getSourceTopology();
        if (topology.isPresent()) {
            TopologyGraph<ActionGraphEntity> graph = topology.get().entityGraph();
            ARM_ENTITY_TYPE.forEach(armEntityType -> {
                final Set<Long> entitiesOfType = graph.entitiesOfType(armEntityType)
                        .map(ActionGraphEntity::getOid)
                        .collect(Collectors.toSet());
                expandedEntitiesPerARMEntityType.put(armEntityType,
                        expandARMEntities(entitiesOfType, graph));
            });
        }
    }

    /**
     * Expands the involved entities to include the entities below. This allows use to link actions
     * like move VM from PM1 to PM2 when a business app uses the VM.
     *
     * @param involvedEntities the entities to expand.
     * @param graph The topology graph to use to expand entities.
     * @return the expanded entities.
     */
    @Nonnull
    private LongSet expandARMEntities(Collection<Long> involvedEntities, TopologyGraph<ActionGraphEntity> graph) {
        final LongOpenHashSet retSet = new LongOpenHashSet();
        Map<Integer, SupplyChainNode> supplyChain = supplyChainCalculator.getSupplyChainNodes(graph, involvedEntities,
                e -> true, new TraversalRulesLibrary<>());
        if (supplyChain.isEmpty()) {
            logger.warn("Unable to expand supply chain of {} entities.", involvedEntities.size());
            return new LongOpenHashSet(involvedEntities);
        }
        supplyChain.forEach((type, node) -> {
            if (ENTITY_TYPES_BELOW_ARM.contains(type)) {
                retSet.addAll(RepositoryDTOUtil.getAllMemberOids(node));
            }
        });
        retSet.trim();
        return retSet;
    }

    /**
     * Structure for holding the result of expansion by
     * {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
     */
    public static class InvolvedEntitiesFilter {

        private final Set<Long> entities;
        private final InvolvedEntityCalculation calculationType;

        /**
         * Constructs the filter with the provided entities and calculation type.
         *
         * @param entities the expanded entities.
         * @param calculationType the calculation type for filtering involved entities.
         */
        public InvolvedEntitiesFilter(
                final @Nonnull Set<Long> entities,
                final @Nonnull InvolvedEntityCalculation calculationType) {
            this.entities = entities;
            this.calculationType = calculationType;
        }

        /**
         * Returns the entities expanded by
         * {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         *
         * @return the entities expanded by
         *         {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         */
        @Nonnull
        public Set<Long> getEntities() {
            return entities;
        }

        /**
         * Returns the calculation type determined by
         * {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         *
         * @return the calculation type determined by
         *         {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         */
        @Nonnull
        public InvolvedEntityCalculation getCalculationType() {
            return calculationType;
        }
    }
}
