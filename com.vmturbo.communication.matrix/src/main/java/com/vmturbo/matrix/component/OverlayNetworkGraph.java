package com.vmturbo.matrix.component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.ncm.MatrixDTO;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentityData;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The {@link OverlayNetworkGraph} represents an overlay network graph for VMs or Containers.
 */
@ThreadSafe class OverlayNetworkGraph {
    /**
     * The logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(OverlayNetworkGraph.class);

    /**
     * The empty double array.
     */
    static final double[] EMPTY_FLOWS = new double[]{};

    /**
     * The start/finish update cycle.
     * The default is 10 minutes.
     * We do the actual finish if we were told to finish and the update cycle lasted at least as
     * long.
     */
    private final long updateCycle = Long.getLong("vmt.matrix.updatecycle",
                                                  TimeUnit.MINUTES.toMillis(10));

    /**
     * The maximum neighbors we can have before we remove the vertex and all its associated flows.
     */
    private static final int MAX_NEIGHBORS = Integer.getInteger("vmt.matrix.neighbors.max", 100);

    /**
     * The edge fader.
     * The fader will reduce the flow amount for the edges that weren't freshly updated.
     * When a threshold is hit, that edge will be removed from the graph.
     */
    private final EdgeFader edgeFader = new EdgeFader();

    /**
     * The current state.
     */
    @VisibleForTesting
    StateHolder state_;

    /**
     * The update state.
     */
    private StateHolder updateState_;

    /**
     * Constructs the communication matrix.
     */
    OverlayNetworkGraph() {
        state_ = new StateHolder(null);
    }

    /**
     * Restores the graph.
     *
     * @param edges The edges to update from.
     */
    void restore(final @Nonnull Collection<OverlayEdge> edges) {
        edges.forEach(edge -> modifyFlow(state_, edge.getSource(), edge.getSink(), edge));
    }

    /**
     * Export {@link OverlayVertex} to the DTO.
     *
     * @param vertex The vertex to be converted.
     * @return The DTO.
     */
    private MatrixDTO.OverlayVertex vertexToDto(final @Nonnull OverlayVertex vertex) {
        MatrixDTO.OverlayVertex.Builder dto = MatrixDTO.OverlayVertex.newBuilder();
        dto.setOid(vertex.oid_.getValue());
        dto.setIp(vertex.getNormalizedIpAddress());
        dto.setPort(vertex.getPort());
        dto.setUnderlayOid(vertex.underlayOid_.getValue());
        // For neighbours, we need oid, so really it is an oid to flow amount.
        vertex.getNeighbours().forEach((k, v) -> {
            // We use that for transfer only (mapping), and will reset to null later.
            dto.putNeighbors(k.getOid().getValue(), v);
        });
        return dto.build();
    }

    /**
     * Exports the overlay network to the DTO.
     *
     * @param exporter The exporter.
     */
    void exportGraph(final @Nonnull MatrixInterface.Codec exporter) {
        final Map<OverlayVertex, MatrixDTO.OverlayVertex> cache = new HashMap<>();
        long invalidOIDGenerator = -128L;
        // Prep work for unplaced vertices.
        for (OverlayEdge e : state_.edges_) {
            // Set dummy oids
            if (e.getSource().getOid() == null) {
                e.getSource().setOid(new VolatileLong(--invalidOIDGenerator));
            }
            if (e.getSource().underlayOid_ == null) {
                e.getSource().setUnderlayOid(new VolatileLong(-1L));
            }
            if (e.getSink().getOid() == null) {
                e.getSink().setOid(new VolatileLong(--invalidOIDGenerator));
            }
        }
        for (OverlayEdge e : state_.edges_) {
            MatrixDTO.OverlayEdge.Builder edge = MatrixDTO.OverlayEdge.newBuilder();
            edge.setFlow(e.getFlow());
            edge.setLatency(e.getLatency());
            edge.setRx(e.getRx());
            edge.setTx(e.getTx());
            if (e.getSink().underlayOid_ == null) {
                e.getSink().setUnderlayOid(new VolatileLong(-1L));
            }
            edge.setSource(cache.computeIfAbsent((OverlayVertex)e.source_,
                                                 k -> vertexToDto((OverlayVertex)e.source_)));
            edge.setSink(cache.computeIfAbsent((OverlayVertex)e.sink_,
                                               k -> vertexToDto((OverlayVertex)e.sink_)));
            MatrixDTO.OverlayEdge finalEdge = edge.build();
            exporter.next(finalEdge);
        }
    }

    /**
     * Convert vertex DTO to a vertex.
     *
     * @param dto The vertex DTO and neighbours map.
     * @return The vertex.
     */
    private Pair<OverlayVertex, Map<Long, Double>> dtoToVertex(
        final @Nonnull MatrixDTO.OverlayVertex dto) {
        OverlayVertex v = new OverlayVertex(dto.getIp(), dto.getPort());
        v.setOid(new VolatileLong(dto.getOid()));
        v.setUnderlayOid(new VolatileLong(dto.getUnderlayOid()));
        return new Pair<>(v, dto.getNeighborsMap());
    }

    /**
     * Converts the DTO to an edge.
     *
     * @param dto The edge DTO.
     * @param map The oid to vertex map.
     * @return The edge.
     */
    private OverlayEdge dtoToEdge(final @Nonnull MatrixDTO.OverlayEdge dto,
                                  Map<Long, Pair<OverlayVertex, Map<Long, Double>>> map) {
        return new OverlayEdge(dto.getLatency(), dto.getFlow(), dto.getTx(), dto.getRx(),
                               map.get(dto.getSource().getOid()).getFirst(),
                               map.get(dto.getSink().getOid()).getFirst());
    }

    /**
     * Imports the overlay graph.
     *
     * @param dtoEdges The edges.
     */
    void importGraph(final @Nonnull List<MatrixDTO.OverlayEdge> dtoEdges) {
        // Lets reconstruct vertices first.
        Map<Long, Pair<OverlayVertex, Map<Long, Double>>> map = new HashMap<>();
        dtoEdges.forEach(e -> {
            Pair<OverlayVertex, Map<Long, Double>> x = dtoToVertex(e.getSource());
            map.put(x.getFirst().getOid().getValue(), x);
            Pair<OverlayVertex, Map<Long, Double>> y = dtoToVertex(e.getSink());
            map.put(y.getFirst().getOid().getValue(), y);
        });
        // Now, we fix the neighbours for each vertex.
        // Our map becomes fully populated.
        for (Pair<OverlayVertex, Map<Long, Double>> pair : map.values()) {
            final OverlayVertex ov = pair.getFirst();
            for (Map.Entry<Long, Double> e : pair.getSecond().entrySet()) {
                Pair<OverlayVertex, Map<Long, Double>> v = map.get(e.getKey());
                // We are very likely in the middle of multiple updates
                // to the matrix, and neighbour is still in the map, but has
                // no active vertex.
                if (v == null) {
                    continue;
                }
                ov.getNeighbours().put(v.getFirst(), e.getValue());
            }
        }
        // Restore the dummies.
        map.values().stream().map(Pair::getFirst).forEach(OverlayVertex::restoreInvalidOids);
        // Now, edges.
        Set<OverlayEdge> edges = new HashSet<>();
        dtoEdges.forEach(e -> edges.add(dtoToEdge(e, map)));
        restore(edges);
        edges.forEach(edge -> {
            setEndpointOID(edge.getSource().getOid(),
                           edge.getSource().getNormalizedIpAddress());
            setEndpointOID(edge.getSink().getOid(),
                           edge.getSink().getNormalizedIpAddress());
        });
    }

    /**
     * Add all flows.
     * We first add all the new flows, then re-apply the old flows which we don't have now.
     * We identify the flow by pair of its vertices.
     * When we re-apply a previous flow, we fade its flow amount.
     * This code is equivalent to:
     * {@code beginUpdateFlows().updateFlows(flows).finishUpdateFlows();}.
     *
     * @param flows The flow DTOs.
     */
    void update(final @Nonnull Collection<FlowDTO> flows) {
        // We use update state, so that we have a coherent state of affairs in case of an invalid
        // data, which would throw an exception.
        updateState_ = new StateHolder(state_.edges_);
        flows.forEach(f -> modifyFlow(updateState_, f));
        // Get the edges that are kept, so that we can continue accumulating the total
        // transferred data amount per edge.
        for (OverlayEdge edge : updateState_.edges_) {
            OverlayEdge previous = updateState_.previousEdges_.get(edge);
            if (previous != null) {
                edge.accumulateData(previous.getAccumulatedData());
            }
        }
        // Remove all the ones that just appeared and are the same as old ones.
        // We do that because the current snapshot(flows) is what we want to have as far as flow
        // amounts are concerned.
        updateState_.previousEdges_.keySet().removeAll(updateState_.edges_);
        // Fade in the edges that weren't present in the current snapshot.
        updateState_.previousEdges_.keySet().forEach(f -> fade(updateState_, f));
        state_ = updateState_;
        state_.previousEdges_ = null;
        updateState_ = null;
    }

    /**
     * Reapply the edges from previous Matrix for each VPoD, but fade them.
     * The edges we end up re-applying are those for which:
     * 1. One of the vertices is present in the current map.
     * 2. The faded flow is still above the watermark.                               ø
     * The reason we require at least one vertex to be present, and not both, lies in the fact
     * that one of the vertices may help create a bridge between different vertices in the
     * currently supplied map, which otherwise could be lost.
     * <pre>
     * For example:
     *
     * Now:
     * V1 -> V2
     * V3 -> V4
     *
     * Was:
     * V2 -> V5
     * V5 -> V3
     * V5 -> V6
     *
     * So, if we don't include V2 -> V5 and V5 -> V3, we will lose V2 -> V5 -> V3 link,
     * which allows us to form V1 -> V2 -> V5 -> V3 -> V4 VPoD.
     * The V5 -> V6 will be kept, since we must be able handle the edge case of different probes
     * handling different network segments
     * </pre>
     * So essentially, we will keep the flows for as long as at least on of its vertices is still
     * communicating. We will discard the edges (flows) neither of whose vertices are
     * communicating any longer.
     *
     * @param state The state to be used.
     * @param edge  The edge to be faded.
     */
    private void fade(final @Nonnull StateHolder state, final @Nonnull OverlayEdge edge) {
        // Calculate the faded flow and see if we need to even apply the edge.
        // If the edge has expired, then calculate new flow amount and update the last update
        // timestamp for the edge.
        // If the edge has not expired, just add it as is.
        // In case edge has expired long ago (as in multiple of updateCycle), we assume that
        // we had a problem with probes, so that we treat that extended interval as if it were
        // a single interval (which it is).
        if (!edge.isExpired(updateCycle) || !edgeFader.fade(edge)) {
            modifyFlow(state, edge.getSource(), edge.getSink(), edge);
        }
    }

    /**
     * Retrieves the actual vertex (containing all the incoming/outgoing neighbors).
     * If the vertex corresponding to the passed in DTO does not exist in {@code pods_},
     * return the newly constructed vertex key.
     *
     * @param state       The state to use.
     * @param endpointDTO The endpoint DTO being turned into a key.
     * @return The existing or new matrix vertex.
     */
    private @Nonnull OverlayVertex getOrCreateVertex(final StateHolder state,
                                                     final EntityIdentityData endpointDTO) {
        OverlayVertex key = new OverlayVertex(endpointDTO.getIpAddress(), endpointDTO.getPort());
        OverlayVertex value = state.vertices_.get(key);
        return (value == null) ? key : value;
    }

    /**
     * Updates the VPoD's vertices.
     * Updates the {@code state.pods_} map for source and sink as keys, while adding the pair or
     * source/sink and VPoD as a value.
     * We need to keep the actual source/sink object as we require its internal data structures
     * when computing its neighbors.
     *
     * @param state  The state to use.
     * @param source The source vertex.
     * @param sink   The sinke vertex.
     * @param vPod   The VPoD.
     */
    private void updateVPodVertices(final @Nonnull StateHolder state,
                                    final @Nonnull OverlayVertex source,
                                    final @Nonnull OverlayVertex sink,
                                    final @Nonnull MatrixVPod vPod) {
        state.vertices_.put(source, source);
        state.vertices_.put(sink, sink);
        final String sourceIP = source.getNormalizedIpAddress();
        final String sinkIP = sink.getNormalizedIpAddress();
        if (state.ipsToExclude_ == null || !state.ipsToExclude_.contains(sourceIP)) {
            state.pods_.put(sourceIP, vPod);
        }
        if (state.ipsToExclude_ == null || !state.ipsToExclude_.contains(sinkIP)) {
            state.pods_.put(sinkIP, vPod);
        }
        state.sameIP_.computeIfAbsent(sourceIP, k -> new HashSet<>()).add(source);
        state.sameIP_.computeIfAbsent(sinkIP, k -> new HashSet<>()).add(sink);
    }

    /**
     * Modifies the flow.
     *
     * @param state The state.
     * @param flow  The flow representing the flow.
     */
    @VisibleForTesting
    void modifyFlow(final @Nonnull StateHolder state, final @Nonnull FlowDTO flow) {
        // Need to have an actual vertex with all its neighbours present in its data structures.
        try {
            OverlayVertex source = getOrCreateVertex(state, flow.getSourceEntityIdentityData());
            OverlayVertex sink = getOrCreateVertex(state, flow.getDestEntityIdentityData());
            // Compute the rest.
            OverlayEdge edge = new OverlayEdge(flow.getLatency(),
                                               flow.getFlowAmount(), flow.getTransmittedAmount(),
                                               flow.getReceivedAmount(), source, sink);
            modifyFlow(state, source, sink, edge);
        } catch (IllegalArgumentException e) {
            // We change the stance a bit here.
            // We allow incomplete network topology to be populated.
            // The reason behind that decision is as follows:
            // We become more resilient to a potential corrupt data, while allowing majority in.
            // With probes filtering things properly, this code will hopefully never execute.
            LOGGER.warn("Invalid Flow DTO(s). Skipping", e);
        }
    }

    /**
     * Modifies the flow.
     *
     * @param state  The state to be used.
     * @param source The source vertex.
     * @param sink   The sink edge.
     * @param edge   The edge.
     */
    private void modifyFlow(final @Nonnull StateHolder state,
                            final @Nonnull OverlayVertex source,
                            final @Nonnull OverlayVertex sink,
                            final @Nonnull OverlayEdge edge) {
        final MatrixVPod vPodSource = state.pods_.get(source.getNormalizedIpAddress());
        final MatrixVPod vPodSink = state.pods_.get(sink.getNormalizedIpAddress());
        source.addOutgoing(edge);
        sink.addIncoming(edge);
        state.edges_.add(edge);
        // We will handle the GC on the next full reconstruction cycle.
        // We assume that if the edge exists between two vertices, then the VPoD is set up.
        if (vPodSource == null && vPodSink == null) {
            updateVPodVertices(state, source, sink, new MatrixVPod(edge, state.ipsToExclude_));
        } else if (vPodSource != null && Objects.equals(vPodSource, vPodSink)) {
            // We have two VPoDs which are the same and non-null. Update one, that's enough.
            // The vPodSource will not be null here, as it won't be equals to vPodSink,
            // since at least one of them must be non-null.
            updateVPodVertices(state, source, sink, vPodSource.modify(edge, state.ipsToExclude_));
        } else if (vPodSource != null && vPodSink != null) {
            // Both VPoDs exist, but are not the same.
            // We have to merge them.
            // For all edges in sink, we move them to the source and
            // re-register all affected vertices with the source VPoD.
            // NB! This is a fairly expensive operation, so we might want to consider
            //     meta-VPoD in the future, whereby we link multiple VPoDs instead of
            //     outright merging them.
            vPodSink.getEdges().forEach(e -> updateVPodVertices(state, e.getSource(), e.getSink(),
                                                                vPodSource.modify(e,
                                                                                  state.ipsToExclude_)));
            state.vertices_.put(sink, sink);
            state.pods_.put(sink.getNormalizedIpAddress(), vPodSource);
            vPodSource.modify(edge, state.ipsToExclude_);
            state.sameIP_.computeIfAbsent(sink.getNormalizedIpAddress(), k -> new HashSet<>())
                         .add(sink);
        } else {
            // One of them is null, another isn't.
            // We need to set up the relationship.
            // Create/Update the pod.
            MatrixVPod vPod = (vPodSource == null) ? vPodSink : vPodSource;
            updateVPodVertices(state, source, sink, vPod.modify(edge, state.ipsToExclude_));
        }
    }

    /**
     * Returns the collection of all VPoD.
     * Each VPoD is represented by a collection of the IP addresses.
     *
     * @return The collection of VPoDs.
     */
    @Nonnull Collection<Collection<String>> getVpods() {
        Collection<Collection<String>> result = new ArrayList<>(state_.pods_.size());
        for (MatrixVPod vpod : new HashSet<>(state_.pods_.values())) {
            Set<OverlayEdge> edges = vpod.getEdges();
            Set<String> ips = new HashSet<>();
            for (OverlayEdge edge : edges) {
                ips.add(edge.getSource().getNormalizedIpAddress());
                ips.add(edge.getSink().getNormalizedIpAddress());
            }
            result.add(ips);
        }
        return result;
    }

    /**
     * Modifies flow in the graph.
     * We create shallow copies of the edge and its vertices.
     * We create vertices first.
     * Since we rely on the fact that the edge vertices will be consistent with
     * the vertices held in the state (same Java objects), we must construct the
     * edge later.
     *
     * @param graph The graph to modify the flow in.
     * @param edge  The flow.
     */
    private void modifyCopiedGraphFlow(final @Nonnull OverlayNetworkGraph graph,
                                       final @Nonnull OverlayEdge edge) {
        OverlayVertex src = edge.getSource().shallowCopy();
        src = graph.state_.vertices_.getOrDefault(src, src);
        OverlayVertex dst = edge.getSink().shallowCopy();
        dst = graph.state_.vertices_.getOrDefault(dst, dst);
        graph.modifyFlow(graph.state_, src, dst, edge.shallowCopy(src, dst));
    }

    /**
     * Obtains deep copy of the Matrix while excluding the supplied IP addresses.
     *
     * @param ips The set of IP addresses to be excluded.
     * @return The deep copy of this matrix.
     */
    @Nonnull OverlayNetworkGraph copyExcluding(final @Nonnull Set<String> ips) {
        final OverlayNetworkGraph graph = new OverlayNetworkGraph();

        graph.state_.ipsToExclude_ = ips.stream().map(OverlayVertex::normalizeIPAddress)
                                        .collect(Collectors.toSet());
        // Lets also remove the IPs with too many neighbors.
        state_.vertices_.keySet().stream()
                        .filter(v -> v.getNeighbours().size() > MAX_NEIGHBORS)
                        .map(OverlayVertex::getNormalizedIpAddress)
                        .forEach(graph.state_.ipsToExclude_::add);
        state_.edges_.forEach(edge -> modifyCopiedGraphFlow(graph, edge));
        graph.state_.ipsToExclude_ = null;
        return graph;
    }

    /**
     * Obtains deep copy of the Matrix.
     *
     * @return The deep copy of this matrix.
     */
    @Nonnull OverlayNetworkGraph copy() {
        final OverlayNetworkGraph graph = new OverlayNetworkGraph();
        state_.edges_.forEach(edge -> modifyCopiedGraphFlow(graph, edge));
        return graph;
    }

    /**
     * Links the Service Entity represented by an OID with the endpoint.
     *
     * @param oid The consumer oid.
     * @param ip  The consumer IP address.
     */
    void setEndpointOID(final @Nullable VolatileLong oid, final @Nonnull String ip) {
        // In case we don't have a placed vertex.
        if (oid == null) {
            return;
        }
        final Set<OverlayVertex> set = state_.sameIP_.get(ip);
        if (set != null) {
            set.forEach(v -> {
                v.setOid(oid);
                state_.oidLookup_.computeIfAbsent(oid, k -> new VertexSet()).add(v);
            });
        }
        // Merge VPods for all collected vertices.
        final VertexSet vertices = state_.oidLookup_.get(oid);
        if (vertices != null && !vertices.isEmpty()) {
            mergeVPods(vertices.toSet());
        }
    }

    /**
     * Checks whether OID is present in the OID to set of vertices map.
     * We will have it when we set the endpoint.
     *
     * @param oid The oid to check.
     * @return whether oid is present
     */
    boolean isOidPresent(final VolatileLong oid) {
        return !state_.oidLookup_.isEmpty() && state_.oidLookup_.containsKey(oid);
    }

    /**
     * Merges VPods for all vertices.
     *
     * @param vertices The vertices.
     */
    private void mergeVPods(final @Nonnull Set<OverlayVertex> vertices) {
        Set<MatrixVPod> vPods = vertices.stream()
                                        .map(v -> state_.pods_.get(v.getNormalizedIpAddress()))
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toSet());
        // None or one - NOOP.
        if (vPods.size() <= 1) {
            return;
        }
        // Merge VPods
        final Iterator<MatrixVPod> iterator = vPods.iterator();
        final MatrixVPod vPodSource = iterator.next();
        while (iterator.hasNext()) {
            // Update VPod vertices.
            final MatrixVPod vPod = iterator.next();
            for (OverlayEdge edge : vPod.getEdges()) {
                updateVPodVertices(state_, edge.getSource(), edge.getSink(),
                                   vPodSource.modify(edge, state_.ipsToExclude_));
            }
        }
        // Update the pods.
        // The vPodSource won't be null, as we have a VPoD set of size > 1, and the vPodSource is
        // the first one there.
        for (OverlayVertex vertex : vertices) {
            state_.pods_.put(vertex.getNormalizedIpAddress(), vPodSource);
        }
    }

    /**
     * Returns the flows per weight class for a give endpoint.
     *
     * @param oid             The OID of the entity.
     * @param provider        The projected provider.
     * @param underlayNetwork The underlay network. Due to the formatting restrictions, no
     *                        attributes specified.
     * @return result. Empty if no flows for an endpoint.
     */
    @Nonnull double[] getEndpointFlows(final VolatileLong oid,
                                       final VolatileLong provider,
                                       UnderlayNetworkGraph underlayNetwork) {
        // Self defence. We won't get much of anything anyways.
        if (oid == null || provider == null) {
            return EMPTY_FLOWS;
        }
        final VertexSet vertices = state_.oidLookup_.get(oid);
        if (vertices == null) {
            return EMPTY_FLOWS;
        }
        final double[] result = new double[OverlayVertex.WEIGHT_CLASSES_SIZE];
        for (OverlayVertex vertex : vertices) {
            double[] flows = vertex.getFlows(provider, underlayNetwork);
            result[0] += flows[0];
            result[1] += flows[1];
            result[2] += flows[2];
            result[3] += flows[3];
        }
        return result;
    }

    /**
     * Updates the Vertex underlay OID.
     * The check for whether oid is present is done before this call is made.
     *
     * @param oid         The vertex OID.
     * @param underlayOid The underlay OID.
     */
    void updateVertexUnderlay(final VolatileLong oid, final VolatileLong underlayOid) {
        final VertexSet vertices = state_.oidLookup_.get(oid);
        if (vertices.size() == 1) {
            vertices.single.setUnderlayOid(underlayOid);
        } else {
            for (OverlayVertex v : vertices) {
                v.setUnderlayOid(underlayOid);
            }
        }
    }

    /**
     * Checks whether graph is empty.
     *
     * @return {@code true} iff the graph is empty.
     */
    boolean isEmpty() {
        return state_.vertices_.isEmpty();
    }

    /**
     * Clears the graph.
     */
    void reset() {
        state_ = new StateHolder(null);
    }

    /**
     * The vertex set.
     */
    static final class VertexSet implements Iterable<OverlayVertex> {
        /**
         * The single instance.
         */
        OverlayVertex single;

        /**
         * The set if we have too many.
         */
        Set<OverlayVertex> set;

        /**
         * Constructs new VertexSet.
         */
        VertexSet() {
            set = new HashSet<>();
        }

        /**
         * Adds the vertex.
         *
         * @param v The vertex to be added.
         */
        void add(OverlayVertex v) {
            if (single == null) {
                if (!set.isEmpty()) {
                    set.add(v);
                } else {
                    single = v;
                }
            } else if (!single.equals(v)) {
                set.add(single);
                set.add(v);
                single = null;
            }
        }

        /**
         * Returns {@code true} iff VertexSet is empty.
         *
         * @return {@code true} iff VertexSet is empty.
         */
        boolean isEmpty() {
            return single == null && set.isEmpty();
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Iterator<OverlayVertex> iterator() {
            if (single != null) {
                return Collections.singletonList(single).iterator();
            }
            return set.iterator();
        }

        /**
         * Returns the {@link Set} representation.
         *
         * @return The {@link Set} representation.
         */
        Set<OverlayVertex> toSet() {
            if (single != null) {
                return Sets.newHashSet(single);
            }
            return set;
        }

        /**
         * Returns the size.
         *
         * @return The size.
         */
        int size() {
            if (single != null) {
                return 1;
            } else {
                return set.size();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof VertexSet)) {
                return false;
            }
            VertexSet vs = (VertexSet)o;
            return Objects.equals(single, vs.single) && set.equals(vs.set);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return toSet().hashCode();
        }
    }

    /**
     * The state holder. Contains the current state.
     */
    @VisibleForTesting
    static final class StateHolder {
        /**
         * The IP address to Vertex lookup.
         */
        Map<String, Set<OverlayVertex>> sameIP_ = new HashMap<>();

        /**
         * The OID to Vertex lookup.
         */
        Map<VolatileLong, VertexSet> oidLookup_ = new HashMap<>();

        /**
         * The set of vertices.
         */
        Map<OverlayVertex, OverlayVertex> vertices_ = new HashMap<>();

        /**
         * The set of pods associated with the vertex.
         * We need a pair so that we can both get the neighbours and VPoD for a vertex.
         */
        Map<String, MatrixVPod> pods_ = new HashMap<>();

        /**
         * The set of all edges.
         */
        Set<OverlayEdge> edges_ = new HashSet<>();

        /**
         * The temporary variable to hold the previous edges during flow update.
         */
        Map<OverlayEdge, OverlayEdge> previousEdges_;

        /**
         * The temporary excusion list.
         */
        private transient Set<String> ipsToExclude_;

        /**
         * Constructs the state holder.
         *
         * @param edges The current state edges.
         */
        StateHolder(final @Nullable Set<OverlayEdge> edges) {
            previousEdges_ = new HashMap<>();
            if (edges != null) {
                for (OverlayEdge edge : edges) {
                    previousEdges_.put(edge, edge);
                }
            }
        }
    }

    /**
     * The fade function.
     */
    private static class EdgeFader {
        /**
         * The fade watermark property.
         */
        private static final String WATERMARK_PROP = "matrix.overlay.fade.watermark";

        /**
         * The complete fade threshold in KBPS.
         */
        private static final double WATERMARK = Long.getLong(WATERMARK_PROP, 1L).doubleValue();

        /**
         * The fade watermark property.
         */
        private static final String RATIO_PROP = "matrix.overlay.fade.ratio";

        /**
         * The complete fade threshold in KBPS.
         */
        private static final double FADE_RATIO = Double.parseDouble(System.getProperty(RATIO_PROP,
                                                                                       ".3"));

        /**
         * Fades an edge.
         *
         * @param edge The edge to fade.
         * @return {@code true} iff the edge has faded.
         */
        boolean fade(final @Nonnull OverlayEdge edge) {
            final double flow = edge.getFlow() * FADE_RATIO;
            if (flow <= WATERMARK) {
                return true;
            }
            edge.setFlow(flow);
            edge.updateTimestamp();
            return false;
        }
    }
}
