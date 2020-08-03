package com.vmturbo.repository.listener.realtime;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity.Builder;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
import com.vmturbo.topology.graph.util.BaseSearchableTopology;

/**
 * Represents an in-memory "source" topology for the realtime (live) context.
 * Each topology broadcast will result in a different {@link SourceRealtimeTopology}.
 * The {@link LiveTopologyStore} is responsible for keeping a reference to the most up-do-date
 * topology.
 */
@Immutable
@ThreadSafe
public class SourceRealtimeTopology extends BaseSearchableTopology<RepoGraphEntity> {

    /**
     * A calculator of the global chain.
     */
    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    /**
     * Lazily-computed global supply chain.
     * The overhead on a topology of 200k is about 4MB.
     * It's lazily computed because we only need it if a user logs in and wants to see
     * the global supply chain (which doesn't happen for most topologies with a 10-min update
     * interval).
     */
    private final Map<EnvironmentType, SetOnce<Map<ApiEntityType, SupplyChainNode>>> globalSupplyChain = new HashMap<>();

    /**
     * (tag key) -> (tag value) -> (oids of entities that have the key,value combination)
     * The (value) -> (oids) map is stored as a patricia trie to save memory.
     *
     */
    private final DefaultTagIndex tags;

    private SourceRealtimeTopology(@Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final TopologyGraph<RepoGraphEntity> entityGraph,
                                   @Nonnull final DefaultTagIndex tags,
                                   @Nonnull final GlobalSupplyChainCalculator globalSupplyChainCalculator) {
        super(topologyInfo, entityGraph);
        this.tags = tags;
        this.globalSupplyChainCalculator = globalSupplyChainCalculator;
    }

    /**
     * Get the {@link DefaultTagIndex} index for this topology. Can be used to look up all tags.
     *
     * @return The {@link DefaultTagIndex}.
     */
    @Nonnull
    public DefaultTagIndex globalTags() {
        return tags;
    }

    /**
     * Get the global supply chain. This is lazily cached, so the first call will take longer
     * than the subsequent ones.
     *
     * @param optEnvType possibly restrict results to a specific environment type
     * @param entityPredicate An optional predicate to apply to entities in the global supply chain.
     *                        Note - if this is set, we do not use a cached supply chain.
     * @param entityTypesToSkip a predicate used to determine if an entity type should be skipped
     *                          during traversal or not.
     * @return (entity type) -> ({@link SupplyChainNode} for the entity type)
     */
    @Nonnull
    public synchronized Map<ApiEntityType, SupplyChainNode> globalSupplyChainNodes(
            @Nonnull final Optional<EnvironmentType> optEnvType,
            @Nonnull final Optional<Predicate<RepoGraphEntity>> entityPredicate,
            @Nonnull final Predicate<Integer> entityTypesToSkip) {
        final EnvironmentType environmentType = optEnvType.orElse(EnvironmentType.HYBRID);

        final Predicate<EnvironmentType> environmentTypePredicate =
                EnvironmentTypeUtil.matchingPredicate(environmentType);
        if (entityPredicate.isPresent()) {
            // Can't use a cached supply chain because there is a custom predicate on the entities.
            Predicate<RepoGraphEntity> combinedPredicate  = e -> entityPredicate.get().test(e)
                && environmentTypePredicate.test(e.getEnvironmentType());
            return globalSupplyChainCalculator.getSupplyChainNodes(entityGraph(),
                    combinedPredicate,
                    entityTypesToSkip);
        } else {
            // Use the cached supply chain for the environment.
            final SetOnce<Map<ApiEntityType, SupplyChainNode>> envTypeNodes =
                    globalSupplyChain.computeIfAbsent(environmentType, k -> new SetOnce<>());
            return envTypeNodes.ensureSet(() ->
                    globalSupplyChainCalculator.getSupplyChainNodes(entityGraph(),
                            e -> environmentTypePredicate.test(e.getEnvironmentType()),
                            entityTypesToSkip));
        }
    }

    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        for (RepoGraphEntity entity: entityGraph().entities().collect(Collectors.toList())) {
            final String string = gson.toJson(entity.getTopologyEntity());
            appender.appendString(string);
        }
    }

    /**
     * Builder class for the {@link SourceRealtimeTopology}.
     * <p/>
     * Use: {@link SourceRealtimeTopologyBuilder#addEntities(Collection)} to add entities,
     * and {@link SourceRealtimeTopologyBuilder#finish()} when done.
     */
    public static class SourceRealtimeTopologyBuilder extends
            BaseSearchableTopologyBuilder<SourceRealtimeTopology, RepoGraphEntity, Builder> {

        private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

        SourceRealtimeTopologyBuilder(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final Consumer<SourceRealtimeTopology> onFinish,
                @Nonnull final GlobalSupplyChainCalculator globalSupplyChainCalculator) {
            super(Metrics.CONSTRUCTION_TIME_SUMMARY, topologyInfo, onFinish);
            this.globalSupplyChainCalculator = globalSupplyChainCalculator;
        }

        @Override
        protected Builder newBuilder(TopologyEntityDTO entity, DefaultTagIndex tagIndex,
                SharedByteBuffer compressionBuffer) {
            return new RepoGraphEntity.Builder(entity, tagIndex, compressionBuffer);
        }

        @Override
        protected SourceRealtimeTopology newTopology(TopologyInfo topologyInfo,
                TopologyGraph<RepoGraphEntity> graph) {
            return new SourceRealtimeTopology(topologyInfo, graph, getTagIndex(), globalSupplyChainCalculator);
        }
    }

    private static class Metrics {
        private static final DataMetricSummary CONSTRUCTION_TIME_SUMMARY = DataMetricSummary.builder()
            .withName("repo_source_realtime_construction_seconds")
            .withHelp("Total time taken to build the source realtime topology.")
            .build()
            .register();
    }
}
