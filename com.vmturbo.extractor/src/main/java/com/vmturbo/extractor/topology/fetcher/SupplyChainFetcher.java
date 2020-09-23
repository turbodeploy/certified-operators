package com.vmturbo.extractor.topology.fetcher;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.SupplyChainEntity;
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
        return entityToRelated;
    }
}
