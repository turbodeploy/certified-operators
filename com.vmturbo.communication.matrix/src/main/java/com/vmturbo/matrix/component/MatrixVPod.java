package com.vmturbo.matrix.component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link MatrixVPod} represents a fully connected graph of the endpoints.
 */
@NotThreadSafe
public class MatrixVPod {
    /**
     * The edges.
     */
    private Map<OverlayEdge, Entry> edges_ = new HashMap<>();

    /**
     * The total flow.
     */
    private double podFlow_;

    /**
     * Constructs the VPoD with a single edge.
     *
     * @param edge         The edge.
     * @param ipsToExclude The IP addresses to make edge be excluded from the VPod.
     */
    MatrixVPod(final @Nonnull OverlayEdge edge, final @Nullable Set<String> ipsToExclude) {
        Objects.requireNonNull(edge);
        // Don't bother with the excluded IP addresses.
        if (ipsToExclude != null &&
            (ipsToExclude.contains(edge.getSource().getNormalizedIpAddress())
             || ipsToExclude.contains(edge.getSink().getNormalizedIpAddress()))) {
            return;
        }
        podFlow_ = edge.getFlow();
        edges_.put(edge, new Entry(podFlow_, edge));
    }

    /**
     * Adds/Modifies the edge.
     *
     * @param edge         The edge.
     * @param ipsToExclude The IP addresses to make edge be excluded from the VPod.
     * @return {@code this}, so that it can be chained.
     */
    public MatrixVPod modify(final @Nonnull OverlayEdge edge,
                             final @Nullable Set<String> ipsToExclude) {
        Objects.requireNonNull(edge);
        // Don't bother with the excluded IP addresses.
        if (ipsToExclude != null &&
            (ipsToExclude.contains(edge.getSource().getNormalizedIpAddress())
             || ipsToExclude.contains(edge.getSink().getNormalizedIpAddress()))) {
            return this;
        }
        // If are updating the VPoD, we need to recalculate the total latency.
        Entry entry = edges_.get(edge);
        if (entry != null) {
            podFlow_ -= entry.flow;
        }
        // Update the latencies.
        // Update the flows.
        double edgeFlow = edge.getFlow();
        podFlow_ += edgeFlow;
        // Avoid re-allocating entry object if possible.
        if (entry != null) {
            entry.flow = edgeFlow;
            entry.edge = edge;
        } else {
            entry = new Entry(edgeFlow, edge);
        }
        edges_.put(edge, entry);
        return this;
    }

    /**
     * Returns the total flow of the VPoD.
     *
     * @return The total flow of the VPoD.
     */
    public double getPodFlow() {
        return podFlow_;
    }

    /**
     * Returns the edges for this VPoD.
     *
     * @return The set of edges.
     */
    Set<OverlayEdge> getEdges() {
        return Collections.unmodifiableSet(edges_.keySet());
    }

    /**
     * The entry structure.
     */
    private static class Entry {
        double flow;

        OverlayEdge edge;

        Entry(final double flow, final OverlayEdge edge) {
            this.flow = flow;
            this.edge = edge;
        }
    }
}
