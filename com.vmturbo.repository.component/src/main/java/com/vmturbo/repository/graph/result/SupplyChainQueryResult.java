package com.vmturbo.repository.graph.result;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

@Immutable
public class SupplyChainQueryResult {

    private String type;

    private Set<String> instances;

    private Map<String, List<SupplyChainNeighbour>> neighbourInstances;

    /**
     * Default constructor required for initialization via ArangoDB's java driver.
     */
    public SupplyChainQueryResult() {
        this("", new HashSet<>(), new HashMap<>());
    }

    @VisibleForTesting
    SupplyChainQueryResult(String type,
                           Set<String> instances,
                           Map<String, List<SupplyChainNeighbour>> neighbourInstances) {
        this.type = type;
        this.instances = instances;
        this.neighbourInstances = neighbourInstances;
    }

    /**
     * Return a pruned version of this query result. The pruned result will only contain
     * OID's in the provided set.
     *
     * @param retainOids The OIDs to retain. All other OIDs will be not be present in the result.
     * @return A new {@link SupplyChainQueryResult} containing only OIDs in retainOids. The
     *   map of neighbour instances in the new result returned by
     *   {@link SupplyChainQueryResult#getNeighbourInstances()} may contain empty entries.
     */
    SupplyChainQueryResult prune(@Nonnull final Set<String> retainOids) {
        final Set<String> newInstances = Sets.intersection(this.instances, retainOids);
        final Map<String, List<SupplyChainNeighbour>> newNeighbourInstances = new HashMap<>();
        getNeighbourInstances().forEach((type, instanceIds) ->
            newNeighbourInstances.put(type,
                instanceIds.stream()
                    .filter(neighbour -> retainOids.contains(neighbour.getId()))
                    .collect(Collectors.toList())));

        return new SupplyChainQueryResult(this.type, newInstances, newNeighbourInstances);
    }

    public Set<String> getInstances() {
        return instances;
    }

    public String getType() {
        return type;
    }

    public Set<String> getNeighbourTypes() {
        return Collections.unmodifiableSet(neighbourInstances.keySet());
    }

    public Map<String, List<SupplyChainNeighbour>> getNeighbourInstances() {
        return Collections.unmodifiableMap(neighbourInstances);
    }
}
