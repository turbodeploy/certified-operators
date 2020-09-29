package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.ResultsConverter.fillNodeRelationships;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.repository.graph.parameter.GraphCmd;

/**
 * Converts {@link GlobalSupplyChainFluxResult} to a map of entity type -> {@link SupplyChainNode}
 */
public class SupplyChainResultsConverter {

    /**
     * Convert a {@link GlobalSupplyChainFluxResult} to a {@link Flux} of {@link SupplyChainNode}s.
     *
     * @param results The global results.
     * @param providerRels The provider relationships.
     * @return A map of entity types to {@link SupplyChainNode}..
     */
    public static Mono<Map<String, SupplyChainNode>> toSupplyChainNodes(final GlobalSupplyChainFluxResult results,
                                                                        final Multimap<String, String> providerRels) {
        final Multimap<String, String> consumerRels = Multimaps.invertFrom(providerRels, HashMultimap.create());

        return results.entities()
               .collect(new SupplyChainFluxCollector())
               .map(nodesMap -> {
                   fillNodeRelationships(nodesMap, providerRels, consumerRels, GraphCmd.SupplyChainDirection.PROVIDER);
                   fillNodeRelationships(nodesMap, consumerRels, providerRels, GraphCmd.SupplyChainDirection.CONSUMER);

                   return nodesMap.entrySet().stream()
                       .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build()));
               });
    }


    /**
     * A collector of database results.
     */
    public static class SupplyChainFluxCollector implements
            Collector<SupplyChainOidsGroup, MergedSupplyChainOidsGroup, Map<String, SupplyChainNode.Builder>> {

        @Override
        public Supplier<MergedSupplyChainOidsGroup> supplier() {
            return MergedSupplyChainOidsGroup::new;
        }

        @Override
        public BiConsumer<MergedSupplyChainOidsGroup, SupplyChainOidsGroup> accumulator() {
            return MergedSupplyChainOidsGroup::addOidsGroup;
        }

        @Override
        public BinaryOperator<MergedSupplyChainOidsGroup> combiner() {
            return MergedSupplyChainOidsGroup::mergeOidsGroup;
        }

        @Override
        public Function<MergedSupplyChainOidsGroup, Map<String, SupplyChainNode.Builder>> finisher() {
            return MergedSupplyChainOidsGroup::toNodeBuilders;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }

    /**
     * A utility class to help merge {@link SupplyChainOidsGroup} into {@link SupplyChainNode}s.
     *
     * We use it because sometimes multiple {@link SupplyChainOidsGroup}s need to be combined into
     * a single {@link SupplyChainNode} (e.g. when there are entities of the same type, but
     * different states).
     */
    private static class MergedSupplyChainOidsGroup {
        final Map<String, Map<Integer, Set<Long>>> oidsByTypeAndState = new HashMap<>();

        /**
         * Add a new {@link SupplyChainOidsGroup} to this merged group.
         *
         * @param group The new {@link SupplyChainOidsGroup}.
         */
        public void addOidsGroup(@Nonnull final SupplyChainOidsGroup group) {
            if (group.getEntityType() == null || group.getState() == null || group.getOids() == null) {
                return;
            }
            addOids(group.getEntityType(),
                    UIEntityState.fromString(group.getState()).toEntityState().getNumber(),
                    group.getOids());
        }

        /**
         * Merge a {@link MergedSupplyChainOidsGroup} into this group.
         *
         * @param otherMergedGroup The group to merge. The group to merge will NOT be modified.
         * @return A reference to this {@link MergedSupplyChainOidsGroup}, with the other group
         *         merged in.
         */
        @Nonnull
        public MergedSupplyChainOidsGroup mergeOidsGroup(@Nullable final MergedSupplyChainOidsGroup otherMergedGroup) {
            if (otherMergedGroup != null) {
                otherMergedGroup.oidsByTypeAndState.forEach((entityType, oidsByState) -> {
                    oidsByState.forEach((state, oids) -> addOids(entityType, state, oids));
                });
            }
            return this;
        }

        /**
         * Create a map of {@link SupplyChainNode.Builder}s to represent this merged group.
         *
         * @return A map of entity type -> {@link SupplyChainNode.Builder} for that type.
         */
        @Nonnull
        public Map<String, SupplyChainNode.Builder> toNodeBuilders() {
            final Map<String, SupplyChainNode.Builder> retMap = new HashMap<>();
            oidsByTypeAndState.forEach((entityType, oidsByState) -> {
                final SupplyChainNode.Builder thisNodeBuilder = SupplyChainNode.newBuilder()
                        .setEntityType(ApiEntityType.fromStringToSdkType(entityType));
                oidsByState.forEach((state, oidsForState) -> {
                    thisNodeBuilder.putMembersByState(state,
                            MemberList.newBuilder().addAllMemberOids(oidsForState).build());
                });
                retMap.put(entityType, thisNodeBuilder);
            });
            return retMap;
        }

        private void addOids(@Nonnull final String entityType,
                             @Nonnull final Integer state,
                             @Nonnull final Collection<Long> newOids) {
            final Map<Integer, Set<Long>> oidsByState =
                    oidsByTypeAndState.computeIfAbsent(entityType, k -> new HashMap<>());
            final Set<Long> oidsToAddTo = oidsByState.computeIfAbsent(state, k -> new HashSet<>());
            oidsToAddTo.addAll(newOids);
        }
    }
}
