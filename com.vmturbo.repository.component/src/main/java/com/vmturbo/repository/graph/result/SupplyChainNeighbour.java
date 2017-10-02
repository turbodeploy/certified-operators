package com.vmturbo.repository.graph.result;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

@Immutable
public class SupplyChainNeighbour {

    private String id;

    /**
     * The number of vertices between the start node in the supply chain query and the neighbour.
     * This will be 1 for the start node, and at least 2 for any other node.
     */
    private int depth;

    /**
     * Default constructor required for initialization with ArangoDB's java driver.
     */
    public SupplyChainNeighbour() {
        this("", -1);
    }

    @VisibleForTesting
    SupplyChainNeighbour(@Nonnull final String id, final int depth) {
        this.id = id;
        this.depth = depth;
    }

    public String getId() {
        return id;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("depth", depth)
                .toString();
    }
}
