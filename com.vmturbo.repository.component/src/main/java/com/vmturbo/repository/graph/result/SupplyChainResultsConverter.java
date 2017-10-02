package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.ResultsConverter.fillNodeRelationships;

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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
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
               .map(SupplyChainResultsConverter::toSupplyChainNodeBuilders)
               .map(nodesMap -> {
                   fillNodeRelationships(nodesMap, providerRels, consumerRels, GraphCmd.SupplyChainDirection.PROVIDER);
                   fillNodeRelationships(nodesMap, consumerRels, providerRels, GraphCmd.SupplyChainDirection.CONSUMER);

                   return nodesMap.entrySet().stream()
                       .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build()));
               });
    }

    /**
     * Convert the result from the database to a map of entity types to {@link SupplyChainNode.Builder}s.
     *
     * @param typeOidsAgg A mapping of entity types and OIDs.
     * @return A map of entity types to {@link SupplyChainNode}.
     */
    public static Map<String, SupplyChainNode.Builder> toSupplyChainNodeBuilders(final Map<String, Set<Long>> typeOidsAgg) {
        return typeOidsAgg.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                entry -> SupplyChainNode.newBuilder()
                    .setEntityType(entry.getKey())
                    .addAllMemberOids(entry.getValue()))
        );
    }

    /**
     * A collector of database results.
     */
    public static class SupplyChainFluxCollector implements Collector<TypeAndOids, Map<String, Set<Long>>, Map<String, Set<Long>>> {

        @Override
        public Supplier<Map<String, Set<Long>>> supplier() {
            return HashMap::new;
        }

        @Override
        public BiConsumer<Map<String, Set<Long>>, TypeAndOids> accumulator() {
            return (map, typeAndOids) -> {
                final Set<Long> oidsForType = map.getOrDefault(typeAndOids.getEntityType(), new HashSet<>());
                oidsForType.addAll(typeAndOids.getOids());
                map.put(typeAndOids.getEntityType(), oidsForType);
            };
        }

        @Override
        public BinaryOperator<Map<String, Set<Long>>> combiner() {
            return (m1, m2) -> {
                final Map<String, Set<Long>> combined = new HashMap<>(m1);
                m2.forEach((type, oids) -> {
                    final Set<Long> oidsForType = combined.getOrDefault(type, new HashSet<>(oids.size()));
                    oidsForType.addAll(oids);
                    combined.put(type, oidsForType);
                });

                return combined;
            };
        }

        @Override
        public Function<Map<String, Set<Long>>, Map<String, Set<Long>>> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }
}
