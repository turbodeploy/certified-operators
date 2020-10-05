package com.vmturbo.topology.graph.supplychain;

import java.util.Queue;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.TraversalMode;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator.TraversalState;

/**
 * Given a traversal state encountered during scoped supply generation,
 * a {@link TraversalRule} decides which traversal entities will be inserted
 * to the frontier.
 *
 * @param <E> The type of {@link TopologyGraphEntity} in the graph.
 */
public interface TraversalRule<E extends TopologyGraphEntity<E>> {
    /**
     * Decides whether the rule is applicable in a specific traversal state.
     *
     * @param entity entity being traversed
     * @param traversalMode mode of traversal
     * @return true if and only if the rule is applicable
     */
    boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode);

    /**
     * Applies the traversal rule: adds to the traversal frontier
     * new traversal states according to the rule.
     *
     * @param entity entity being traversed
     * @param traversalMode mode of traversal
     * @param depth depth of traversal
     * @param frontier the frontier to add new traversal states to.
     *                 The method should only add traversal states to the
     *                 frontier
     */
    void apply(@Nonnull E entity, @Nonnull TraversalMode traversalMode, int depth,
               @Nonnull Queue<TraversalState> frontier);

    /**
     * This is the default traversal rule, to be used in almost all cases
     * of scoped supply chain generation. Other rules will bypass the default
     * rule to implement special cases.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    class DefaultTraversalRule<E extends TopologyGraphEntity<E>> implements TraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return true;
        }

        @Override
        public void apply(@Nonnull E entity, @Nonnull TraversalMode traversalMode, int depth,
                          @Nonnull Queue<TraversalState> frontier) {
            // if the traversal mode is STOP, then stop here
            if (traversalMode == TraversalMode.STOP) {
                return;
            }

            final int newDepth = depth + 1;

            // add to the frontier all traversal states
            // that subclasses explicitly include to the traversal
            include(entity, traversalMode).forEach(mode -> frontier.add(mode.withDepth(newDepth)));

            // traverse the inclusion chain outwards
            // Add all Controllers of the
            // traversed entity to the frontier.
            // For example, if the entity is a cloud VM,
            // this will add the VMSpec(Represending the AWS ASG for example)
            // for the new traversal state introduced in the frontier,
            // the traversal mode becomes CONTROLLED_BY
            // This ensures that further traversal from the
            // entities newly added to the frontier will only continue
            // in the same direction.
            entity.getControllers().forEach(e ->
                frontier.add(new TraversalState(e.getOid(), TraversalMode.CONTROLLED_BY, newDepth)));
            // In the case when a VMSpec controls multiple VM, when one of the VM is the seed,
            // we don't want to see other vms in the Supply Chain. In this case, when the mode
            // is CONTROLLED_BY, then nothing else should be added to the frontier
            if (traversalMode == TraversalMode.CONTROLLED_BY) {
                return;
            }
            // traverse the inclusion chain inwards
            // traversal mode remains the same
            // Like with the outwards traversal, when an entity
            // is traversed, then any other entity it controls
            // should also be traversed.
            entity.getControlledEntities().forEach(e ->
                frontier.add(new TraversalState(e.getOid(), traversalMode, newDepth)));

            // traverse the inclusion chain outwards
                // Add all aggregators and the owner of the
                // traversed entity to the frontier.
                // For example, if the entity is a cloud VM,
                // this will add the containing zone or region
                // and the owning account to the frontier.
            // for the new traversal state introduced in the frontier,
            // the traversal mode becomes AGGREGATED_BY
                // This ensures that further traversal from the
                // entities newly added to the frontier will only continue
                // in the same direction.
                // For example, if we now add a zone to the frontier,
                // the next step will add the region that owns the zone
                // but it will not add any other VMs that are contained
                // in the zone.
            entity.getAggregatorsAndOwner().stream()
                .filter(filter(entity, EdgeTraversalDescription.FROM_AGGREGATED_TO_AGGREGATOR))
                .forEach(e ->
                    frontier.add(new TraversalState(e.getOid(), TraversalMode.AGGREGATED_BY, newDepth)));

            // if the traversal mode is AGGREGATED_BY,
            // then nothing else should be added to the frontier
                // For example, if the current entity is a zone and
                // we have already added to the frontier the region
                // that owns it. If the traversal mode is AGGREGATED_BY,
                // we shouldn't add anything else.
            if (traversalMode == TraversalMode.AGGREGATED_BY) {
                return;
            }

            // traverse the inclusion chain inwards
            // traversal mode remains the same
                // Like with the outwards traversal, when an entity
                // is traversed, then any other entity it owns or aggregates
                // should also be traversed. Unlike outwards traversal
                // though, traversal should continue from those entities.
                // For example, if the seed is a zone (i.e., the
                // current entity is a zone and the current traversal mode
                // is START), then all VMs that the zone contains are added
                // to the frontier. When we traverse from those VMs,
                // we will treat them as parts of the seed (traversal direction
                // will be START), which will in turn bring consuming
                // applications, anything higher the supply chain, etc.
            entity.getAggregatedAndOwnedEntities().stream()
                .filter(filter(entity, EdgeTraversalDescription.FROM_AGGREGATOR_TO_AGGREGATED))
                .forEach(e ->
                    frontier.add(new TraversalState(e.getOid(), traversalMode, newDepth)));

            // downward traversal of the supply chain
                // For example, from VMs to PMs.
                // The downward traversal is marked by traversal mode
                // CONSUMES. The downward traversal is initiated in
                // the seed, which means that traversal direction START
                // should also be included in the conditional.
            if (traversalMode == TraversalMode.CONSUMES || traversalMode == TraversalMode.START) {
                Stream.concat(entity.getProviders().stream(),
                              entity.getOutboundAssociatedEntities().stream())
                    .filter(filter(
                                entity, EdgeTraversalDescription.DOWN, traversalMode == TraversalMode.START))
                    .forEach(e ->
                        frontier.add(new TraversalState(e.getOid(), TraversalMode.CONSUMES, newDepth)));
            }

            // upward traversal of the supply chain
                // For example, from PMs to VMs.
                // The upward traversal is marked by traversal mode
                // PRODUCES. The upward traversal is initiated in
                // the seed, which means that traversal direction START
                // should also be included in the conditional.
            if (traversalMode == TraversalMode.PRODUCES || traversalMode == TraversalMode.START) {
                // from traversal modes START and PRODUCES,
                // we start/continue our upward traversal
                // of the supply chain
                Stream.concat(entity.getConsumers().stream(), entity.getInboundAssociatedEntities().stream())
                    .filter(filter(entity, EdgeTraversalDescription.UP, traversalMode == TraversalMode.START))
                    .forEach(e ->
                        frontier.add(new TraversalState(e.getOid(), TraversalMode.PRODUCES, newDepth)));
            }
        }

        /**
         * Convenience method that allows custom filtering of related entities.
         * The method allows filtering of connected entities as well as
         * providers and consumers.
         *
         * @param entity the entity whose related entities will be filtered
         * @param edgeTraversalDescription the traversal mode for the related entity
         * @param seed true if and only if the current entity is in the seed
         * @return a predicate that filters entities
         */
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            return e -> true;
        }

        private Predicate<E> filter(@Nonnull E entity,
                                    @Nonnull EdgeTraversalDescription edgeTraversalDescription) {
            return filter(entity, edgeTraversalDescription, false);
        }

        /**
         * Convenience method that can be overridden by the subclasses
         * to add new entities to the traversal frontier.  In other words,
         * a new traversal rule defined as a subclass of
         * {@link DefaultTraversalRule} can use this method to specify
         * that certain entities must be added to the traversal, if this
         * rule applies.
         *
         * <p>The entities are returned as a stream of {@link TraversalState}
         *    builders.  We return a {@link TraversalState} builder to
         *    not only specify the entities, but also the traversal modes with
         *    which they should be included in the traversal.  We return a
         *    builder instead of an immutable {@link TraversalState} object,
         *    because the traversal depth should not be set by this method,
         *    but by the calling method {@link DefaultTraversalRule#apply}.
         * </p>
         *
         * @param entity the entity
         * @param traversalMode the traversal mode
         * @return a stream of traversal state builders.  These traversal states
         *         will be included in the frontier with the appropriate depth
         */
        protected Stream<TraversalState.Builder> include(
                @Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return Stream.empty();
        }
    }

    /**
     * This enum describes the relationship between an entity that is being
     * traversed (we will call it the "source" entity) and another entity
     * that is about to be traversed (we will call it the "target" entity).
     */
    enum EdgeTraversalDescription {
        /**
         * The target provides to the source.
         */
        DOWN,
        /**
         * The target consumes by the source.
         */
        UP,
        /**
         * The target is an aggregator of the source.
         */
        FROM_AGGREGATED_TO_AGGREGATOR,
        /**
         * The target is aggregated by the source.
         */
        FROM_AGGREGATOR_TO_AGGREGATED
    }
}
