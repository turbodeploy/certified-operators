package com.vmturbo.extractor.topology.fetcher;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Calculate supply chain for each entity based on the constructed graph. It returns supply chain
 * data which is a mapping from entity id to a mapping of related entity ids grouped by related
 * entity type, like: Map&lt;entity oid, Map&lt;related entity type, Set&lt;related entity
 * oids&gt;&gt;&gt;
 */
public class SupplyChainFetcher extends DataFetcher<Map<Long, Map<Integer, Set<Long>>>> {

    private static final TraversalRulesLibrary<SupplyChainEntity> ruleChain = new TraversalRulesLibrary<>();

    private static final Predicate<SupplyChainEntity> entityPredicate = e -> true;

    private static final SupplyChainCalculator calc = new SupplyChainCalculator();

    private final TopologyGraph<SupplyChainEntity> graph;

    private final boolean requireSupplyChainForAllEntities;

    /**
     * Create a new instance.
     *
     * @param graph    topology graph
     * @param timer    timer
     * @param consumer fn to handle computed supply chain data
     * @param requireSupplyChainForAllEntities whether or not to calculate supply chain for all entities
     */
    public SupplyChainFetcher(@Nonnull TopologyGraph<SupplyChainEntity> graph,
            @Nonnull MultiStageTimer timer,
            @Nonnull Consumer<Map<Long, Map<Integer, Set<Long>>>> consumer,
            boolean requireSupplyChainForAllEntities) {
        super(timer, consumer);
        this.graph = graph;
        this.requireSupplyChainForAllEntities = requireSupplyChainForAllEntities;
    }

    @Override
    protected String getName() {
        return "Compute Supply Chain";
    }

    @Override
    protected Map<Long, Map<Integer, Set<Long>>> fetch() {
        // <entity id, <related entity type, <related entity ids>>
        final Map<Long, Map<Integer, Set<Long>>> entityToRelated = new Long2ObjectOpenHashMap<>();
        final Map<Long, Map<Integer, Set<Long>>> syncEntityToRelated =
                Collections.synchronizedMap(entityToRelated);

        graph.entities().parallel()
                .filter(e -> requireSupplyChainForAllEntities
                        || SearchMetadataUtils.shouldComputeSupplyChain(e.getEntityType()))
                .forEach(e -> {
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
                });
        if (logger.isDebugEnabled()) {
            logSupplyChainAsymmetries(entityToRelated);
        }
        return entityToRelated;
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
