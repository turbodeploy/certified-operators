package com.vmturbo.topology.graph.supplychain;

import java.util.List;
import java.util.Queue;

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
            final int newDepth = depth + 1;

            // traverse the inclusion chain outwards
                // Add all aggregators and the owner of the
                // traversed entity to the frontier.
                // For example, if the entity is a cloud VM,
                // this will add the containing zone or region
                // and the owning account to the frontier.
            // for the new traversal state introduced in the frontier,
            // the traversal mode becomes INCLUDED_BY
                // This ensures that further traversal from the
                // entities newly added to the frontier will only continue
                // in the same direction.
                // For example, if we now add a zone to the frontier,
                // the next step will add the region that owns the zone
                // but it will not add any other VMs that are contained
                // in the zone.
            for (E e : getFilteredIncludingEntities(entity, traversalMode)) {
                frontier.add(new TraversalState(e.getOid(), TraversalMode.INCLUDED_BY, newDepth));
            }

            // if the traversal mode is INCLUDED_BY,
            // then nothing else should be added to the frontier
                // For example, if the current entity is a zone and
                // we have already added to the frontier the region
                // that owns it. If the traversal mode is INCLUDED_BY,
                // we shouldn't add anything else.
            if (traversalMode == TraversalMode.INCLUDED_BY) {
                return;
            }

            // always include normal connections
            // traversal mode remains the same
                // For example, if a VM is included in the traversal,
                // so is any volume connected to it.
            for (E e : getFilteredAssociatedEntities(entity, traversalMode)) {
                frontier.add(new TraversalState(e.getOid(), traversalMode, newDepth));
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
            for (E e : getFilteredIncludedEntities(entity, traversalMode)) {
                frontier.add(new TraversalState(e.getOid(), traversalMode, newDepth));
            }

            // downward traversal of the supply chain
                // For example, from VMs to PMs.
                // The downward traversal is marked by traversal mode
                // CONSUMES. The downward traversal is initiated in
                // the seed, which means that traversal direction START
                // should also be included in the conditional.
            if (traversalMode == TraversalMode.CONSUMES
                    || traversalMode == TraversalMode.START) {
                for (E e : getFilteredProviders(entity, traversalMode)) {
                    frontier.add(new TraversalState(e.getOid(), TraversalMode.CONSUMES, newDepth));
                }
            }

            // upward traversal of the supply chain
                // For example, from PMs to VMs.
                // The upward traversal is marked by traversal mode
                // PRODUCES. The upward traversal is initiated in
                // the seed, which means that traversal direction START
                // should also be included in the conditional.
            if (traversalMode == TraversalMode.PRODUCES
                    || traversalMode == TraversalMode.START) {
                // from traversal modes START and PRODUCES,
                // we start/continue our upward traversal
                // of the supply chain
                for (E e : getFilteredConsumers(entity, traversalMode)) {
                    frontier.add(new TraversalState(e.getOid(), TraversalMode.PRODUCES, newDepth));
                }
            }
        }

        /**
         * Convenience method that allows filtering of consumers of an entity
         * when overriding this class. Examples of consumers include:
         * VMs consume from PMs, apps consume from VMs, storage consumes from
         * disk arrays etc.
         *
         * @param entity the entity
         * @param traversalMode the traversal mode
         * @return consumers of this entity to be considered
         *         in the next traversal
         */
        protected List<E> getFilteredConsumers(@Nonnull E entity,
                                               @Nonnull TraversalMode traversalMode) {
            return entity.getConsumers();
        }

        /**
         * Convenience method that allows filtering of providers of an entity
         * when overriding this class. Examples of providers include:
         * PMs provide to VMs, VMs provide to apps, disk arrays provide to
         * storage etc.
         *
         * @param entity the entity
         * @param traversalMode the traversal mode
         * @return providers of this entity to be considered
         *         in the next traversal
         */
        protected List<E> getFilteredProviders(@Nonnull E entity,
                                               @Nonnull TraversalMode traversalMode) {
            return entity.getProviders();
        }

        /**
         * Convenience method that allows filtering of aggregators and the
         * owner of an entity when overriding this class. Examples of aggregators
         * include: regions aggregate tiers, zones aggregate workloads.
         * Examples of owners include: regions own zones, accounts own
         * sub-accounts and workloads.
         *
         * @param entity the entity
         * @param traversalMode the traversal mode
         * @return aggregators and owner of this entity to be considered
         *         in the next traversal
         */
        protected List<E> getFilteredIncludingEntities(@Nonnull E entity,
                                                       @Nonnull TraversalMode traversalMode) {
            return entity.getOwnersOrAggregators();
        }

        /**
         * Convenience method that allows filtering of aggregated and
         * owned entities of an entity when overriding this class. Examples of
         * aggregated entities include: regions aggregate tiers, zones aggregate
         * workloads. Examples of owned entities include: regions own zones,
         * accounts own sub-accounts and workloads.
         *
         * @param entity the entity
         * @param traversalMode the traversal mode
         * @return aggregated and owned entities of this entity to be considered
         *         in the next traversal
         */
        protected List<E> getFilteredIncludedEntities(@Nonnull E entity,
                                                      @Nonnull TraversalMode traversalMode) {
            return entity.getOwnedOrAggregatedEntities();
        }

        /**
         * Convenience method that allows filtering of "normal" outbound connections
         * of an entity when overriding this class. Examples of such connections
         * are: VMs are connected to virtual volumes. Virtual volumes are connected
         * to storage and to storage tiers. Compute tiers are connected to storage tiers.
         *
         * @param entity the entity
         * @param traversalMode the traversal mode
         * @return normal outbound connections of this entity to be considered
         *         in the next traversal
         */
        protected List<E> getFilteredAssociatedEntities(@Nonnull E entity,
                                                        @Nonnull TraversalMode traversalMode) {
            return entity.getOutboundAssociatedEntities();
        }
    }
}
