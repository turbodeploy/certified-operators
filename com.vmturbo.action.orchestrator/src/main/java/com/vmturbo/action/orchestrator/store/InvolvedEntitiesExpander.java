package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.common.protobuf.action.InvolvedEntityExpansionUtil.EXPANSION_ALL_ENTITY_TYPES;
import static com.vmturbo.common.protobuf.action.InvolvedEntityExpansionUtil.EXPANSION_REQUIRED_ENTITY_TYPES;
import static com.vmturbo.common.protobuf.action.InvolvedEntityExpansionUtil.expansionRequiredEntityType;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Determine when expansion is needed. Also determines how to expand.
 */
public class InvolvedEntitiesExpander {

    private final Map<Integer, LongSet> expandedEntitiesByRequiredEntityType = new HashMap<>();

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
     * Check if an entity's actions should be propagated to the expansion-required entity type.
     *
     * @param involvedEntityId the involved entity ID.
     * @param desiredEntityTypes the entity types in the query that require expansion.
     * @return true if this entity's actions should be propagated to the required entity types.
     */
    public boolean shouldPropagateAction(long involvedEntityId, Set<Integer> desiredEntityTypes) {
        return desiredEntityTypes.stream().anyMatch(entityType -> expansionRequiredEntityType(entityType)
                && expandedEntitiesByRequiredEntityType.get(entityType).contains(involvedEntityId));
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
            if (areAllExpansionRequiredEntities(involvedEntities, graph)) {
                return new InvolvedEntitiesFilter(expandEntities(involvedEntities, graph),
                        InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
            }
        }
        return new InvolvedEntitiesFilter(
            new HashSet<>(involvedEntities),
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
    }

    /**
     * Queries repositoryService to see if we need to expand these entities. If the entire list
     * contains expansion-required entities, then the involved entities will need to be expanded.
     *
     * @param involvedEntities the entities to search if we need expansion.
     * @param graph The topology graph to use to look up the entities.
     * @return true if we need expansion.
     */
    private boolean areAllExpansionRequiredEntities(@Nonnull final Collection<Long> involvedEntities,
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
                .allMatch(EXPANSION_REQUIRED_ENTITY_TYPES::contains);
    }

    /**
     * Expand all required entities by type and save the mapping for later use.
     */
    public void expandAllRequiredEntities() {
        expandedEntitiesByRequiredEntityType.clear();
        Optional<ActionRealtimeTopology> topology = actionTopologyStore.getSourceTopology();
        if (topology.isPresent()) {
            TopologyGraph<ActionGraphEntity> graph = topology.get().entityGraph();
            EXPANSION_REQUIRED_ENTITY_TYPES.forEach(armEntityType -> {
                final Set<Long> entitiesOfType = graph.entitiesOfType(armEntityType)
                        .map(ActionGraphEntity::getOid)
                        .collect(Collectors.toSet());
                expandedEntitiesByRequiredEntityType.put(armEntityType,
                        expandEntities(entitiesOfType, graph));
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
    private LongSet expandEntities(Collection<Long> involvedEntities, TopologyGraph<ActionGraphEntity> graph) {
        final LongOpenHashSet retSet = new LongOpenHashSet();
        Map<Integer, SupplyChainNode> supplyChain = supplyChainCalculator.getSupplyChainNodes(graph, involvedEntities,
                e -> true, new TraversalRulesLibrary<>());
        if (supplyChain.isEmpty()) {
            logger.warn("Unable to expand supply chain of {} entities.", involvedEntities.size());
            return new LongOpenHashSet(involvedEntities);
        }
        supplyChain.forEach((type, node) -> {
            if (EXPANSION_ALL_ENTITY_TYPES.contains(type)) {
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
