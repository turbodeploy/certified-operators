package com.vmturbo.topology.graph.supplychain;

import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
    private final List<TraversalRule<E>> ruleChain =
            ImmutableList.of(
                // special rule for PMs
                    // if not in the seed, do not traverse to storage
                    // if going up, include VDCs, but do not traverse from them
                    // always include the DC, but do not traverse further from it
                new PMRule<>(),

                // special rule for storage
                    // traverse to consuming PMs and the related DC, but do not allow
                    // further traversal from them
                    // do *not* traverse to providing PMs (this accommodates vSAN topologies,
                    // in which PMs can be providers to storage)
                new StorageRule<>(),

                // special rule for VMs and Container Pods
                    // special treatment for related VDCs
                    // (traverse them, but do not continue traversal from them)
                new VMPodRule<>(),

                // when a DC is in the seed add all consuming PMs in the seed
                    // this makes DC an aggregator of PMs, even if this relation
                    // is not explicit in the topology graph
                new DCRule<>(),

                // never traverse from parent to child account
                new AccountRule<>(),

                // never traverse from a tier to an aggregating region or zone
                new TierRule<>(),

                // rule specific to volumes as seed
                    // ignore traversing further up from vm consumers if a pod is
                    // also consuming the volume.
                new VolumeToPodRule<>(),

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
     * Rule specific to traversal of PMs.
     *
     * <p>When a PM is traversed:
     * <ul>
     *     <li>Include all neighboring DCs
     *     </li>
     *     <li>Do not traverse to storage (unless the PM is in the seed)
     *     </li>
     *     <li>Traverse to VDCs only when going up.
     *         Do not allow any more traversals from those VDCs
     *     </li>
     * </ul>
     * </p>
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class PMRule<E extends TopologyGraphEntity<E>> extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull final E entity, @Nonnull final TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE;
        }

        @Override
        protected Stream<TraversalState.Builder> include(
                @Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            // a connected DC is an aggregator
            final Stream<TraversalState.Builder> dcAsAggregator =
                    entity.getProviders().stream()
                        .filter(e -> e.getEntityType() == EntityType.DATACENTER_VALUE)
                        .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.AGGREGATED_BY));

            // if going "up", we should include VDCs in the traversal,
            // but we should not continue traversing from them
            // the same happens with STORAGEs here because in the VSAN case,
            // we want to show the Storages consuming from a host but not the VMs
            // consuming from the Storages
            if (traversalMode == TraversalMode.PRODUCES || traversalMode == TraversalMode.START) {
                return Stream.concat(
                        dcAsAggregator,
                        entity.getConsumers().stream()
                            .filter(e -> e.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE
                                            || e.getEntityType() == EntityType.STORAGE_VALUE)
                            .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.STOP)));
            } else {
                return dcAsAggregator;
            }
        }

        @Override
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            switch (edgeTraversalDescription) {
                case DOWN:
                    // if PM is not in the seed, then ignore storage
                    if (seed) {
                        return e -> true;
                    } else {
                        return e -> e.getEntityType() != EntityType.STORAGE_VALUE;
                    }
                case UP:
                    return e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE
                                    && e.getEntityType() != EntityType.STORAGE_VALUE;
                default:
                    return e -> true;
            }
        }
    }

    /**
     * Entities that fall under this rule are VMs, container pods,
     * and desktop pools. They must treat related VDCs in a special way:
     * they should allow traversal to related VDCs but disallow any
     * further traversal from there on.
     * This rule also adds in a condition specific to traversal of VMs
     * which have consumer pods which also consume cloud volumes.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class VMPodRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull final E entity, @Nonnull final TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                        || entity.getEntityType() == EntityType.CONTAINER_POD_VALUE
                        || (entity.getEntityType() == EntityType.DESKTOP_POOL_VALUE
                                && traversalMode != TraversalMode.START);
        }

        @Override
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            // we want to filter only for edges going down the supply chain
            if (edgeTraversalDescription != EdgeTraversalDescription.DOWN) {
                return e -> true;
            }

            // ignore VDCs, because they will be added by the "include" method
            // with traversal mode = STOP
            final Predicate<E> filterOutVDCs = e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE;
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE && !seed) {
                // when traversing down the supply chain of a vm, traverse
                // only those volumes which do not have pods as consumers (volumes
                // connected only to the vm).
                // if this traversal is coming from a pod which has a volume also as
                // a provider, the pods traversal will pull in those volumes which
                // are connected to the pod apart from the ones selected here
                return filterOutVDCs.and(e ->
                        e.getEntityType() != EntityType.VIRTUAL_VOLUME_VALUE
                            || e.getConsumers().stream().noneMatch(c ->
                                                c.getEntityType() == EntityType.CONTAINER_POD_VALUE));
            } else {
                // ignore VDCs, because they will be added by the "traverse and then stop" method
                return filterOutVDCs;
            }
        }

        @Override
        protected Stream<TraversalState.Builder> include(
                @Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            if (traversalMode == TraversalMode.CONSUMES) {
                // if going down, traverse only producing VDCs, but stop immediately
                return entity.getProviders().stream()
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE)
                        .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.STOP));
            } else {
                // if at the seed or going up, traverse all VDCs, but stop immediately
                final Stream<TraversalState.Builder> includedEntities = Stream.concat(entity
                        .getProviders().stream(), entity.getConsumers().stream())
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE)
                        .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.STOP));

                // additionally include volume providers of pods if the traversal is from VM
                // as seed, and the VM itself does not have any volume providers (for example
                // in an unstitched environment)
                // TODO: This can be removed when the VM to VV relationship is established
                // in an unstitched environment also.
                if ((entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        && (traversalMode == TraversalMode.START) && entity.getProviders()
                        .stream().noneMatch(e -> e.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)) {
                    return Stream.concat(includedEntities, entity.getConsumers().stream()
                            .filter(e -> e.getEntityType() == EntityType.CONTAINER_POD_VALUE)
                            .flatMap(e -> e.getProviders().stream()
                                    .filter(provider -> provider.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE))
                            .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.STOP)));

                }
                return includedEntities;
            }
        }
    }

    /**
     * Rule specific to traversal of storage: traverse to consuming PMs and DCs,
     * but do not allow any more traversal from them. Also: do not traverse to
     * providing PMs, unless the storage is in the seed.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class StorageRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.STORAGE_VALUE;
        }

        @Override
        protected Stream<TraversalState.Builder> include(
                @Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            // add connected DCs when in seed
            final Stream<TraversalState.Builder> dcs;
            if (traversalMode == TraversalMode.START) {
                dcs = entity.getConsumers().stream()
                            .filter(e -> e.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                            .flatMap(e -> e.getProviders().stream()
                                    .filter(e1 -> e1.getEntityType() == EntityType.DATACENTER_VALUE))
                            .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.AGGREGATED_BY));
            } else {
                dcs = Stream.empty();
            }

            // a consuming PM should be traversed when going up
            // however no more traversal should happen from that PM
            final Stream<TraversalState.Builder> pms;
            if (traversalMode == TraversalMode.START || traversalMode == TraversalMode.PRODUCES) {
                pms = entity.getConsumers().stream()
                            .filter(e -> e.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                            .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.STOP));
            } else {
                pms = Stream.empty();
            }

            return Stream.concat(dcs, pms);
        }

        @Override
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            // filter out all related PMs
            // except: consuming PMs can appear, when the storage is in the seed
            if (!seed || edgeTraversalDescription == EdgeTraversalDescription.UP) {
                return e -> e.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE;
            } else {
                return e -> true;
            }
        }
    }

    /**
     * When a DC is in the seed, then add all consuming PMs in the seed.
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

    /**
     * Account-specific rule: never traverse to the sub-accounts.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class AccountRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE
                        && traversalMode == TraversalMode.START;
        }

        @Override
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            if (edgeTraversalDescription == EdgeTraversalDescription.FROM_AGGREGATOR_TO_AGGREGATED) {
                return e -> e.getEntityType() != EntityType.BUSINESS_ACCOUNT_VALUE;
            } else {
                return e -> true;
            }
        }
    }

    /**
     * Tier-specific rule: do not traverse to regions or availability zones.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class TierRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {

        /**
         * All cloud tiers.
         */
        private static final Set<Integer> TIER_VALUES = ImmutableSet.of(
                EntityType.STORAGE_TIER_VALUE,
                EntityType.COMPUTE_TIER_VALUE,
                EntityType.DATABASE_SERVER_TIER_VALUE,
                EntityType.DATABASE_TIER_VALUE
        );

        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return TIER_VALUES.contains(entity.getEntityType());
        }

        @Override
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            if (edgeTraversalDescription == EdgeTraversalDescription.FROM_AGGREGATED_TO_AGGREGATOR) {
                return e -> e.getEntityType() != EntityType.REGION_VALUE
                            && e.getEntityType() != EntityType.AVAILABILITY_ZONE_VALUE;
            }
            return e -> false;
        }
    }

    /**
     * Virtual volume specific rule: when in seed, ignore traversing beyond
     * the vm consumer if there is a pod which is also consuming this volume.
     * This assumes that a pod will always be connected to both the vm and
     * the volume.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class VolumeToPodRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE;
        }

        @Override
        protected Stream<TraversalState.Builder> include(
                @Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            if (entity.getConsumers().stream()
                    .anyMatch(e -> e.getEntityType() == EntityType.CONTAINER_POD_VALUE)) {
                return entity.getConsumers().stream()
                                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                                .map(e -> new TraversalState.Builder(e.getOid(), TraversalMode.STOP));
            } else {
                return Stream.empty();
            }
        }

        @Override
        protected Predicate<E> filter(@Nonnull E entity,
                                      @Nonnull EdgeTraversalDescription edgeTraversalDescription,
                                      boolean seed) {
            if (edgeTraversalDescription == EdgeTraversalDescription.UP
                    && entity.getConsumers().stream()
                            .anyMatch(e -> e.getEntityType() == EntityType.CONTAINER_POD_VALUE)) {
                return e -> e.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE;
            } else {
                return e -> true;
            }
        }
    }
}
