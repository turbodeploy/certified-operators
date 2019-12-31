package com.vmturbo.topology.graph.supplychain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.TraversalMode;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.TraversalState;
import com.vmturbo.topology.graph.supplychain.TraversalRule.DefaultTraversalRule;

/**
 * A chain of {@link TraversalRule}s to be used during the traversal
 * for scoped supply chain generation, in a standard "chain or responsibility"
 * way: the first rule that applies is used.
 *
 * @param <E> The type of {@link TopologyGraphEntity} in the graph.
 */
public class TraversalRulesLibrary<E extends TopologyGraphEntity<E>> {
    /**
     * Chain of all the rules.
     */
    private List<TraversalRule<E>> ruleChain =
            ImmutableList.of(
                // when traversing a PM do not consider providers of type Storage
                    // Note that the rule TypeSpecificFiltering
                    // does not apply to the seed. So, if the seed
                    // is a PM, then the providing storage will be included
                    // in the result.
                new TypeSpecificFiltering<E>(EntityType.PHYSICAL_MACHINE_VALUE,
                                            Collections.emptySet(),
                                            Collections.singleton(EntityType.STORAGE_VALUE)) {
                    // patch for DCs, because aggregation
                        // is not yet introduced on-prem
                        // TODO: remove as part of OM-51365
                    @Override
                    protected List<E> getFilteredConsumers(@Nonnull E entity,
                                                           @Nonnull TraversalMode traversalMode) {
                        // add connected DCs, to be considered even when going
                        // up the supply chain
                        return Stream.concat(getFilteredProviders(entity, traversalMode).stream()
                                                .filter(e ->
                                                    e.getEntityType() == EntityType.DATACENTER_VALUE),
                                             super.getFilteredConsumers(entity, traversalMode).stream())
                                .collect(Collectors.toList());
                    }
                },

                // when traversing a Storage do not consider consumers of type PM
                    // As with the previous rule in the chain, if the seed is
                    // a storage device, the consuming PMs will be included in
                    // the result.
                new TypeSpecificFiltering<>(EntityType.STORAGE_VALUE,
                                            Collections.singleton(EntityType.PHYSICAL_MACHINE_VALUE),
                                            Collections.emptySet()),

                // patch for VDCs, because aggregation
                    // is not yet introduced on-prem
                    // TODO: remove (or rewrite) as part of OM-51365
                new VDCRule<>(),

                // patch for DCs, because aggregation
                    // is not yet introduced on-prem
                    // TODO: remove as part of OM-51365
                new DCRule<>(),

                // when starting from a Volume, add the connected VM
                    // and storage to the seed
                new VolumeRule<>(),

                // use default traversal rule in all other cases
                new DefaultTraversalRule<>());

    /**
     * Find and apply the correct rule.
     *
     * @param entity entity being traversed
     * @param traversalMode mode of traversal
     * @param depth depth of traversal
     * @param frontier the frontier to add new traversal states to.
     *                 We rely on the assumptions that rules only add
     *                 traversal states to the frontier and use it in
     *                 no other way
     */
    public void apply(@Nonnull E entity, @Nonnull TraversalMode traversalMode, int depth,
                      @Nonnull Queue<TraversalState> frontier) {
        for (TraversalRule<E> rule : ruleChain) {
            if (rule.isApplicable(entity, traversalMode)) {
                rule.apply(entity, traversalMode, depth, frontier);
                return;
            }
        }

        // this should not happen, as the last rule always applies
        throw new IllegalStateException(
            "Could not find an appropriate traversal rule for entity " + entity.getDisplayName()
                + "and traversal mode " + traversalMode);
    }

    /**
     * Restrict traversal based on type.  The rule applies when traversing
     * an non-seed entity of a specified entity type.  When the rule applies
     * it excludes entities of a specific entity types from its consumers
     * and its producers.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class TypeSpecificFiltering<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        /**
         * The rule applies when traversing non-seed entities of this type.
         */
        private final int applyForThisType;

        /**
         * The rule excludes consumers with entity types in this set.
         */
        private final Set<Integer> excludeTheseConsumerTypes;

        /**
         * The rule excludes providers with entity types in this set.
         */
        private final Set<Integer> excludeTheseProviderTypes;

        /**
         * Create a {@link TypeSpecificFiltering}.
         *
         * @param applyForThisType apply for non-seed entities of this type
         * @param excludeTheseConsumerTypes exclude consumers with entity types
         *                                  in this set
         * @param excludeTheseProviderTypes exclude providers with entity types
         *                                  in this set
         */
        TypeSpecificFiltering(int applyForThisType,
                              @Nonnull Set<Integer> excludeTheseConsumerTypes,
                              @Nonnull Set<Integer> excludeTheseProviderTypes) {
            this.applyForThisType = applyForThisType;
            this.excludeTheseConsumerTypes = excludeTheseConsumerTypes;
            this.excludeTheseProviderTypes = excludeTheseProviderTypes;
        }

        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == applyForThisType && traversalMode != TraversalMode.START;
        }

        @Override
        protected List<E> getFilteredConsumers(@Nonnull E entity,
                                               @Nonnull TraversalMode traversalMode) {
            return entity.getConsumers().stream()
                         .filter(e -> !excludeTheseConsumerTypes.contains(e.getEntityType()))
                         .collect(Collectors.toList());
        }

        @Override
        protected List<E> getFilteredProviders(@Nonnull E entity,
                                               @Nonnull TraversalMode traversalMode) {
            return entity.getProviders().stream()
                         .filter(e -> !excludeTheseProviderTypes.contains(e.getEntityType()))
                         .collect(Collectors.toList());
        }
    }

    /**
     * Rule specific to virtual volumes.  When starting from a Volume,
     * add the connected VM to the seed and traverse from there.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class VolumeRule<E extends TopologyGraphEntity<E>> extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE
                        && traversalMode == TraversalMode.START;
        }

        @Override
        protected List<E> getFilteredAssociatedEntities(@Nonnull E entity,
                                                        @Nonnull TraversalMode traversalMode) {
            final List<E> result = new ArrayList<>(entity.getOutboundAssociatedEntities());
            result.addAll(entity.getInboundAssociatedEntities().stream()
                                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                                .collect(Collectors.toList()));
            return result;
        }
    }

    // TODO: remove or revisit this class as part of OM51365
    /**
     * This rule is a patch for VDC traversals.  It may be rendered obsolete
     * when VDCs become aggregators (OM-51365)
     *
     * <p>The rule is:
     *      <li>if the VDC is in the seed, add all consumers/producers to the seed,
     *      except VDCs.  Keep neighboring VDCs in the traversal but prevent further
     *      traversal from them</li>
     *      <li>if the VDC is not in the seed, stop the traversal</li>
     * </p>
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class VDCRule<E extends TopologyGraphEntity<E>> implements TraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE;
        }

        @Override
        public void apply(@Nonnull E entity, @Nonnull TraversalMode traversalMode, int depth,
                          @Nonnull Queue<TraversalState> frontier) {
            if (traversalMode != TraversalMode.START) {
                return;
            }

            final int newDepth = depth + 1;
            final Set<E> vdcNeighbors =
                Stream.concat(entity.getProviders().stream(), entity.getConsumers().stream())
                    .filter(e -> e.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE)
                    .collect(Collectors.toSet());
            final Set<E> nonVDCProviders =
                entity.getProviders().stream()
                    .filter(e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE)
                    .collect(Collectors.toSet());
            final Set<E> nonVDCConsumers =
                entity.getConsumers().stream()
                    .filter(e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE)
                    .collect(Collectors.toSet());


            for (E e : nonVDCProviders) {
                frontier.add(new TraversalState(e.getOid(), TraversalMode.START, newDepth));
            }
            for (E e : nonVDCConsumers) {
                frontier.add(new TraversalState(e.getOid(), TraversalMode.START, newDepth));
            }
            for (E e: vdcNeighbors) {
                // hack to avoid further traversal
                frontier.add(new TraversalState(e.getOid(), TraversalMode.INCLUDED_BY, newDepth));
            }
        }
    }

    // TODO: remove this class as part of OM-51365
    /**
     * This rule is a patch for DC traversals.  It will be rendered obsolete
     * when VDCs become aggregators (OM-51365).
     *
     *<p>The rule is: when a DC is in the seed, then add all consuming PM's
     * in the seed.</p>
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class DCRule<E extends TopologyGraphEntity<E>> implements TraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.DATACENTER_VALUE
                    && traversalMode == TraversalMode.START;
        }

        @Override
        public void apply(@Nonnull E entity, @Nonnull TraversalMode traversalMode, int depth,
                          @Nonnull Queue<TraversalState> frontier) {
            final int newDepth = depth + 1;
            for (E e : entity.getConsumers()) {
                frontier.add(new TraversalState(e.getOid(), TraversalMode.START, newDepth));
            }
        }
    }
}
