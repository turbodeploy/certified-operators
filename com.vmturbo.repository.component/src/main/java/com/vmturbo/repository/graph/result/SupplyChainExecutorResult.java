package com.vmturbo.repository.graph.result;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

@Immutable
public class SupplyChainExecutorResult {

    private static final Logger logger = LogManager.getLogger();

    private final List<SupplyChainQueryResult> providers;
    private final List<SupplyChainQueryResult> consumers;

    public SupplyChainExecutorResult(final List<SupplyChainQueryResult> providers,
                                     final List<SupplyChainQueryResult> consumers) {
        this.providers = providers;
        this.consumers = consumers;
    }

    public List<SupplyChainQueryResult> getProviders() {
        return providers;
    }

    public List<SupplyChainQueryResult> getConsumers() {
        return consumers;
    }

    /**
     * Prune the results of a scoped (i.e. single-entity) supply chain query.
     * <p>
     * Pruning means removing any neighbours that are further away from the starting entity than
     * the first entity of the neighbours' type. For example, when scoping to a VM we may have:
     *      VM -> PM1 (PM1 provides memory to VM)
     *      VM -> Storage -> PM2 (PM2 provides some commodity to the storage)
     * We want to remove PM2 from the result. Why? Because at the time of this writing the supply
     * chain graph as presented in the UI has one node per entity type. This means we can't
     * distinguish between entities at different "depths" w.r.t. the starting entity, and showing
     * them all in a single node would be misleading.
     * <p>
     * TODO (roman, July 5 2017): This pruning doesn't really belong in the repository. We would be
     * better off returning the depths of all neighbours to the API, and doing the pruning in the
     * API layer.
     *
     * @param startId The OID of the starting entity.
     * @return A new, pruned {@link SupplyChainExecutorResult}.
     */
    public SupplyChainExecutorResult prune(@Nonnull final String startId) {
        // The provider and consumer queries, if they return results, should contain the start ID.
        //
        // The provider and consumer query results may be empty at the edges of the supply chain.
        // In that case, we don't expect them to have the start ID.
        if (!((providers.isEmpty() || providers.stream().anyMatch(res ->
                    res.getInstances().contains(startId))) &&
              (consumers.isEmpty() || consumers.stream().anyMatch(res ->
                      res.getInstances().contains(startId))))) {
            logger.warn("Start ID {} not found in a non-empty supply chain query result." +
                    " This shouldn't happen! Skipping supply chain pruning.", startId);
            return this;
        }

        final Set<String> retainOids =
                getRetainOids(Stream.concat(providers.stream(), consumers.stream()));
        // Make sure the start entity is retained no matter what.
        retainOids.add(startId);

        return new SupplyChainExecutorResult(
            providers.stream().map(res -> res.prune(retainOids)).collect(Collectors.toList()),
            consumers.stream().map(res -> res.prune(retainOids)).collect(Collectors.toList()));
    }

    /**
     * Get the OIDs that should be retained given a set of query results.
     */
    @VisibleForTesting
    static Set<String> getRetainOids(final Stream<SupplyChainQueryResult> resultStream) {
        // High-level explanation:
        //
        // Suppose we have the following supply chain, starting with VM1:
        //   --- VM1
        // /    /   \   \
        // |  VDC1  DS1 DS2
        // | /  \
        // PM1  PM2
        //
        // The query result should return (ommitting entities with no neighbours):
        //
        // VM
        //  Neighbours:
        //    PhysicalMachine:
        //       PM1, Depth: 2
        //    VDC:
        //       VDC, Depth: 2
        //    Datastore:
        //       DS1, Depth: 2
        //       DS2, Depth: 2
        // VDC
        //  Neighbours:
        //    PhysicalMachine:
        //       PM1, Depth: 3
        //       PM2, Depth: 3
        //
        // Then we arrange them by type and depth, like so:
        //
        // PhysicalMachine:
        //   2 : PM1
        //   3 : PM1, PM2
        // Datastore:
        //   2: DS1, DS2
        // VDC:
        //   2 : VDC1
        //
        // And then for each entity type we will take the lowest key (i.e. smallest depth),
        // so the "retainOids" will contain:
        //    PM1, DS1, DS2, VDC1

        // Neighbour type -> depth -> neighbours
        final Map<String, NavigableMap<Integer, Set<String>>> neighboursByDepth =
                new HashMap<>();

        // Go through the neighbours of all providers and consumers, and collect them into
        // the neighboursByDepth map.
        //
        // This logic makes no distinction between depth in providers and consumers because
        // the consumer and provider neighbours must be distinct, so an entity type encountered
        // in providers will not be encountered in consumers (and vice versa). It is illegal because
        // if there is an entity that is both a provider and a consumer of some entity then there
        // is a cycle in the supply chain, which is illegal.
        resultStream.forEach(queryResult ->
            queryResult.getNeighbourInstances().forEach((type, neighbourInfos) -> {
                final Map<Integer, Set<String>> typeNeighboursByDepth =
                        neighboursByDepth.computeIfAbsent(type, k -> new TreeMap<>());
                neighbourInfos.forEach(neighbour -> {
                    final Set<String> neighbours = typeNeighboursByDepth.computeIfAbsent(
                            neighbour.getDepth(), k -> new HashSet<>());
                    neighbours.add(neighbour.getId());
                });
            })
        );

        // For each type, take the set of neighbours with the smallest depth (i.e. the shortest
        // distance from the starting entity). Disregard all others.
        return neighboursByDepth.values().stream()
                .map(NavigableMap::firstEntry)
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("providers", getProviders())
                .add("consumers", getConsumers())
                .toString();
    }

}
