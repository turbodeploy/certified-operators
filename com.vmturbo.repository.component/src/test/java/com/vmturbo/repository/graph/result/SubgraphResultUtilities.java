package com.vmturbo.repository.graph.result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.google.common.base.Preconditions;

import sun.security.provider.certpath.Vertex;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.constant.RepoObjectState.RepoEntityState;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.EdgeCollectionResult;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultEdge;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.SubgraphResult;
import com.vmturbo.stitching.StitchingEntity;

public class SubgraphResultUtilities {
    public static class SubgraphResultBuilder {
        private final String startingVertexOid;
        private final Optional<String> startingVertexEntityType;
        private final List<EdgeCollectionResult> edges = new ArrayList<>();

        private SubgraphResultBuilder(final long startingVertexOid) {
            this.startingVertexOid = Long.toString(startingVertexOid);
            startingVertexEntityType = Optional.empty();
        }

        private SubgraphResultBuilder(final long startingVertexOid,
                                      @Nonnull final String startingVertexEntityTYpe) {
            this.startingVertexOid = Long.toString(startingVertexOid);
            startingVertexEntityType = Optional.of(startingVertexEntityTYpe);
        }

        public SubgraphResultBuilder providerEdges(@Nonnull final ResultEdge... edges) {
            Preconditions.checkArgument(edges.length > 0);
            Preconditions.checkArgument(Stream.of(edges) // All consumers should be of same entity type
                .map(e -> e.getConsumer().getEntityType())
                .distinct()
                .count() == 1);

            this.edges.add(new EdgeCollectionResult(edges[0].getConsumer().getEntityType(),
                Arrays.asList(edges)));

            return this;
        }

        public SubgraphResultBuilder consumerEdges(@Nonnull final ResultEdge... edges) {
            Preconditions.checkArgument(edges.length > 0);
            Preconditions.checkArgument(Stream.of(edges) // All providers should be of same entity type
                .map(e -> e.getProvider().getEntityType())
                .distinct()
                .count() == 1);

            this.edges.add(new EdgeCollectionResult(edges[0].getProvider().getEntityType(),
                Arrays.asList(edges)));

            return this;
        }

        public SubgraphResult build() {
            final String entityType = startingVertexEntityType
                .orElse(findVertexEntityType(startingVertexOid, edges));

            return new SubgraphResult(new ResultVertex(startingVertexOid, entityType,
                    RepoEntityState.ACTIVE.getValue()), edges);
        }

        private String findVertexEntityType(@Nonnull final String oid,
                                            @Nonnull final List<EdgeCollectionResult> edges) {
            for (ResultEdge edge : edges.stream().flatMap(e -> e.getEdges().stream()).collect(Collectors.toList())) {
                if (edge.getConsumer().getId().equals(oid)) {
                    return edge.getConsumer().getEntityType();
                }
                if (edge.getProvider().getId().equals(oid)) {
                    return edge.getProvider().getEntityType();
                }
            }

            throw new IllegalArgumentException("No entity " + oid + " found.");
        }
    }

    public static SubgraphResultBuilder subgraphFor(final long startingVertexOid) {
        return new SubgraphResultBuilder(startingVertexOid);
    }

    public static SubgraphResultBuilder subgraphFor(final long startingVertexOid,
                                                    @Nonnull final String startingVertexEntityType) {
        return new SubgraphResultBuilder(startingVertexOid, startingVertexEntityType);
    }

    public static SubgraphResult emptySubgraphFor(@Nonnull final ResultVertexBuilder startingEntity) {
        return new SubgraphResult(startingEntity.vertex, Collections.emptyList());
    }

    /**
     * Builder for a SubgraphResult edge.
     */
    public static class ResultVertexBuilder {
        public final ResultVertex vertex;

        ResultVertexBuilder(@Nonnull final ResultVertex vertex) {
            this.vertex = Objects.requireNonNull(vertex);
        }

        public ResultEdge consumesFrom(@Nonnull final ResultVertexBuilder provider) {
            return new ResultEdge(provider.vertex, vertex);
        }

        public ResultEdge providesTo(@Nonnull final ResultVertexBuilder consumer) {
            return new ResultEdge(vertex, consumer.vertex);
        }
    }

    public static ResultVertexBuilder app(@Nonnull final long oid) {
        return new ResultVertexBuilder(new ResultVertex(Long.toString(oid),
                RepoEntityType.APPLICATION.getValue(),
                RepoEntityState.ACTIVE.getValue()));
    }

    public static ResultVertexBuilder vm(@Nonnull final long oid) {
        return new ResultVertexBuilder(new ResultVertex(Long.toString(oid),
                RepoEntityType.VIRTUAL_MACHINE.getValue(),
                RepoEntityState.ACTIVE.getValue()));
    }

    public static ResultVertexBuilder host(@Nonnull final long oid) {
        return new ResultVertexBuilder(new ResultVertex(Long.toString(oid),
                RepoEntityType.PHYSICAL_MACHINE.getValue(),
                RepoEntityState.ACTIVE.getValue()));
    }

    public static ResultVertexBuilder dc(@Nonnull final long oid) {
        return new ResultVertexBuilder(new ResultVertex(Long.toString(oid),
                RepoEntityType.DATACENTER.getValue(),
                RepoEntityState.ACTIVE.getValue()));
    }

    public static ResultVertexBuilder storage(@Nonnull final long oid) {
        return new ResultVertexBuilder(new ResultVertex(Long.toString(oid),
                RepoEntityType.STORAGE.getValue(),
                RepoEntityState.ACTIVE.getValue()));
    }

    public static ResultVertexBuilder da(@Nonnull final long oid) {
        return new ResultVertexBuilder(new ResultVertex(Long.toString(oid),
                RepoEntityType.DISKARRAY.getValue(),
                RepoEntityState.ACTIVE.getValue()));
    }

    public static Map<String, SupplyChainNode> nodeMapFor(@Nonnull final SupplyChainSubgraph subgraph) {
        return subgraph.toSupplyChainNodes().stream()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));
    }
}
