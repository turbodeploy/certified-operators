package com.vmturbo.repository.graph.result;

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;

/**
 * A collection of entity OIDs in the supply chain, partitioned
 * by entity type and state.
 *
 * e.g. The operation to fetch the global supply chain runs a query
 * to get all entities, grouped by type and state. Each unique
 * combination of type and state would be stored in a {@link SupplyChainOidsGroup}.
 */
public class SupplyChainOidsGroup {
    private String entityType;
    private String state;
    private List<Long> oids;

    public SupplyChainOidsGroup(String entityType, String state, List<Long> oids) {
        this.entityType = entityType;
        this.state = state;
        this.oids = oids;
    }

    public SupplyChainOidsGroup() { }

    public String getEntityType() {
        return entityType;
    }

    public String getState() {
        return state;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<Long> getOids() {
        return oids;
    }

    public void setOids(List<Long> oids) {
        this.oids = oids;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("entityType", entityType)
                .add("oids", oids)
                .add("state", state)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SupplyChainOidsGroup)) return false;
        SupplyChainOidsGroup that = (SupplyChainOidsGroup) o;
        return Objects.equals(entityType, that.entityType) &&
                Objects.equals(state, that.state) &&
                Objects.equals(oids, that.oids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, state, oids);
    }
}
