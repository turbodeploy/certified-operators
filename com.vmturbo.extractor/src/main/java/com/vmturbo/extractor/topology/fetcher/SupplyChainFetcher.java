package com.vmturbo.extractor.topology.fetcher;

import static com.vmturbo.extractor.search.SearchMetadataUtils.SEARCH_FILTER_SPEC_BASED_ON_METADATA;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.utils.SearchFiltersMapper.SearchFilterSpec;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Calculate supply chain for each entity based on the constructed graph. It returns supply chain
 * data which is a mapping from entity id to a mapping of related entity ids grouped by related
 * entity type, like: Map&lt;entity oid, Map&lt;related entity type, Set&lt;related entity
 * oids&gt;&gt;&gt;
 */
public class SupplyChainFetcher extends DataFetcher<SupplyChain> {

    private static final TraversalRulesLibrary<SupplyChainEntity> ruleChain = new TraversalRulesLibrary<>();

    private static final Predicate<SupplyChainEntity> entityPredicate = e -> true;

    private static final SearchResolver<SupplyChainEntity> searchResolver = new SearchResolver<>(
            new TopologyFilterFactory<SupplyChainEntity>());

    private static final SupplyChainCalculator calc = new SupplyChainCalculator();

    private final TopologyGraph<SupplyChainEntity> graph;

    private final boolean requireFullSupplyChain;

    /**
     * Create a new instance.
     *
     * @param graph    topology graph
     * @param timer    timer
     * @param consumer fn to handle computed supply chain data
     * @param requireFullSupplyChain whether or not to calculate supply chain for all entities
     */
    public SupplyChainFetcher(@Nonnull TopologyGraph<SupplyChainEntity> graph,
            @Nonnull MultiStageTimer timer,
            @Nonnull Consumer<SupplyChain> consumer,
            boolean requireFullSupplyChain) {
        super(timer, consumer);
        this.graph = graph;
        this.requireFullSupplyChain = requireFullSupplyChain;
    }

    @Override
    protected String getName() {
        return "Compute Supply Chain";
    }

    @Override
    protected SupplyChain fetch() {
        // <entity id, <related entity type, <related entity ids>>
        final Map<Long, Map<Integer, Set<Long>>> entityToRelated = new Long2ObjectOpenHashMap<>();
        final Map<Long, Map<Integer, Set<Long>>> syncEntityToRelated =
                Collections.synchronizedMap(entityToRelated);
        if (requireFullSupplyChain) {
            graph.entities().parallel().forEach(e -> calculateFullSupplyChain(e, graph, syncEntityToRelated));
            if (logger.isDebugEnabled()) {
                logSupplyChainAsymmetries(entityToRelated);
            }
        } else {
            graph.entities().parallel().forEach(e -> {
                final Map<Integer, SearchFilterSpec> filterSpecByRelatedEntityType =
                        SEARCH_FILTER_SPEC_BASED_ON_METADATA.get(e.getEntityType());
                if (filterSpecByRelatedEntityType == null) {
                    return;
                }
                calculatePartialSupplyChain(e, graph, filterSpecByRelatedEntityType, syncEntityToRelated);
            });
        }
        return new SupplyChain(entityToRelated, requireFullSupplyChain);
    }


    /**
     * Calculate full supply chain for the given entity using {@link SupplyChainCalculator}.
     *
     * @param e entity
     * @param graph graph containing all entities
     * @param syncEntityToRelated the map to populate related entities
     */
    public static void calculateFullSupplyChain(SupplyChainEntity e,
            final TopologyGraph<SupplyChainEntity> graph,
            final Map<Long, Map<Integer, Set<Long>>> syncEntityToRelated) {
        final Map<Integer, SupplyChainNode> related = calc.getSupplyChainNodes(graph,
                Collections.singletonList(e.getOid()), entityPredicate, ruleChain);
        final Map<Integer, Set<Long>> relatedEntitiesByType = related.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue()
                        .getMembersByStateMap().values().stream()
                        .map(MemberList::getMemberOidsList)
                        .flatMap(Collection::stream)
                        .mapToLong(Long::longValue)
                        .collect(LongOpenHashSet::new, LongOpenHashSet::add, LongOpenHashSet::addAll)));
        syncEntityToRelated.put(e.getOid(), relatedEntitiesByType);
    }

    /**
     * Calculate partial supply chain for the given entity based on the provided search filters.
     *
     * @param e entity
     * @param graph graph containing all entities
     * @param filterSpecByRelatedEntityType search filters used to find related entities, only
     *                                      those provided related entity types are calculated
     * @param syncEntityToRelated the map to populate related entities
     */
    public static void calculatePartialSupplyChain(SupplyChainEntity e,
            final TopologyGraph<SupplyChainEntity> graph,
            final Map<Integer, SearchFilterSpec> filterSpecByRelatedEntityType,
            final Map<Long, Map<Integer, Set<Long>>> syncEntityToRelated) {
        final Map<Integer, Set<Long>> relatedEntitiesByType =
                syncEntityToRelated.computeIfAbsent(e.getOid(), k -> new HashMap<>());
        filterSpecByRelatedEntityType.forEach((relatedEntityType, searchFilterSpec) -> {
            final Set<Long> relatedEntities = new LongOpenHashSet();
            for (List<SearchFilter> searchFilters : searchFilterSpec.getSearchFiltersToCombine()) {
                // combine results of all possible search filters
                searchResolver.search(Stream.of(e), searchFilters, graph)
                        .forEach(relatedEntity -> relatedEntities.add(relatedEntity.getOid()));
            }
            if (!relatedEntities.isEmpty()) {
                relatedEntitiesByType.put(relatedEntityType, relatedEntities);
            }
        });
    }

    /**
     * Wrapper class around entityToRelatedEntities.
     */
    public static class SupplyChain {
        /**
         * Mapping from entity oid to another mapping from related entity type to related entity oids.
         */
        private final Map<Long, Map<Integer, Set<Long>>> entityToRelatedEntities;

        /**
         * Whether or not this is a full supply chain calculation, which means all related entities
         * are included in the map above.
         */
        private final boolean full;

        /**
         * Constructor.
         *
         * @param entityToRelatedEntities entity to related entities by related entity type
         * @param full whether or not this is a full supply chain calculation
         */
        public SupplyChain(Map<Long, Map<Integer, Set<Long>>> entityToRelatedEntities, boolean full) {
            this.entityToRelatedEntities = entityToRelatedEntities;
            this.full = full;
        }

        /**
         * Get the related entities for given entity.
         *
         * @param entityOid oid of entity
         * @return related entities by type
         */
        @Nonnull
        public Map<Integer, Set<Long>> getRelatedEntities(long entityOid) {
            Map<Integer, Set<Long>> relatedByType = entityToRelatedEntities.get(entityOid);
            return relatedByType != null ? relatedByType : Collections.emptyMap();
        }

        /**
         * Get the related entities for given entity and given related entity type.
         *
         * @param entityOid oid of entity
         * @param relatedEntityType type of related entity
         * @return related entities oids
         */
        @Nonnull
        public Set<Long> getRelatedEntitiesOfType(long entityOid, int relatedEntityType) {
            Map<Integer, Set<Long>> relatedEntitiesByType = getRelatedEntities(entityOid);
            if (relatedEntitiesByType.isEmpty()) {
                return Collections.emptySet();
            }
            Set<Long> relatedEntities = relatedEntitiesByType.get(relatedEntityType);
            return relatedEntities != null ? relatedEntities : Collections.emptySet();
        }

        public boolean isFull() {
            return full;
        }

        public Map<Long, Map<Integer, Set<Long>>> getEntityToRelatedEntities() {
            return entityToRelatedEntities;
        }
    }

    /**
     * We normally expect the supply chain relationship to be symmetric (if x is in y's supply
     * chain, then y is in x's).
     *
     * <p>As a precaution, we can perform an exhaustive check for asymmetries, and log whatever
     * entity type pairs are involved. This can be used across a variety of topologies to look for
     * potential problems with the supply chain traversal rules.
     *
     * @param entityToRelated supply chain relationships
     */
    private void logSupplyChainAsymmetries(final Map<Long, Map<Integer, Set<Long>>> entityToRelated) {
        // compute the basic (entity, entity) relationship without interposed type information
        Map<Long, Set<Long>> relatedEntities = new Long2ObjectOpenHashMap<>();
        entityToRelated.forEach((oid1, typeToRelated) ->
                typeToRelated.forEach((type, related) ->
                        related.forEach(oid2 ->
                                relatedEntities.computeIfAbsent(oid1, _oid1 -> new LongOpenHashSet())
                                        .add(oid2))));
        // now find all entity<->entity asymmetries and record corresponding type<->type asymmetries
        Map<Integer, Set<Integer>> asymmetries = new Int2ObjectOpenHashMap<>();
        relatedEntities.forEach((oid1, related) ->
                related.forEach(oid2 -> {
                    if (!relatedEntities.get(oid2).contains(oid1)) {
                        Optional<Integer> type1 = graph.getEntity(oid1)
                                .map(SupplyChainEntity::getEntityType);
                        Optional<Integer> type2 = graph.getEntity(oid2)
                                .map(SupplyChainEntity::getEntityType);
                        if (type1.isPresent() && type2.isPresent()) {
                            asymmetries.computeIfAbsent(type1.get(),
                                    _t -> new IntOpenHashSet()).add(type2.get());
                        }
                    }
                }));
        // log anything we found
        asymmetries.forEach((type, asymmTypes) ->
                asymmTypes.forEach(asymmType ->
                        logger.debug("Supply Chain asymmetries detected from entity type {} to {}",
                                EntityType.forNumber(type), EntityType.forNumber(asymmType))));
    }

}
