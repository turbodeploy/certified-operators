package com.vmturbo.topology.graph.supplychain;

import java.util.List;
import java.util.Queue;
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
                // special rule for PMs
                    // if not in the seed, do not traverse to storage
                    // if going up, include VDCs, but do not traverse from them
                    // always include the DC, but do not traverse further from it TODO: remove as part of OM-51365
                new PMRule<>(),

                // special rule for storage
                    // traverse to consuming PMs and the related DC, but do not allow
                    // further traversal from them
                    // do *not* traverse to providing PMs (this accomodates vSAN topologies,
                    // in which PMs can be providers to storage)
                new StorageRule<>(),

                // special rule for VMs and Container Pods
                    // special treatment for related VDCs
                    // (traverse them, but do not continue traversal from them)
                new VMPodAggregatorRule<>(),

                // patch for DCs, because aggregation
                    // is not yet introduced on-prem
                    // TODO: remove as part of OM-51365
                new DCRule<>(),

                // never traverse from parent to child account
                new AccountRule<>(),

                // Always traverse from workload controller to namespace
                // (Cloud native)
                new WorkloadControllerRule<>(),

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
     *     <li>Include all neighboring DCs (TODO: will not be needed after OM-51365)
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
        protected Stream<E> getFilteredAggregators(@Nonnull E entity,
                                                   @Nonnull TraversalMode traversalMode) {
            final Stream<E> dcsAndTrueAggregators = // TODO, remove as part of OM-51365
                    Stream.concat(super.getFilteredProviders(entity, traversalMode)
                                        .filter(e -> e.getEntityType() == EntityType.DATACENTER_VALUE),
                                  super.getFilteredAggregators(entity, traversalMode));

            // if going "up" treat VDCs as aggregators (i.e., include in the traversal
            // but do not keep traversing from them)
            // we also add STORAGEs here because in the VSAN case, we want to show the Storages
            // consuming from a host but not the VMs consuming from the Storages
            if (traversalMode == TraversalMode.PRODUCES || traversalMode == TraversalMode.START) {
                return Stream.concat(super.getFilteredConsumers(entity, traversalMode)
                                            .filter(e -> e.getEntityType()
                                                        == EntityType.VIRTUAL_DATACENTER_VALUE
                                                    || e.getEntityType()
                                                        == EntityType.STORAGE_VALUE),
                                     dcsAndTrueAggregators);
            } else {
                return dcsAndTrueAggregators;
            }
        }

        @Override
        protected Stream<E> getFilteredProviders(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            if (traversalMode == TraversalMode.START) {
                return super.getFilteredProviders(entity, traversalMode);
            }

            // if PM is not in the seed, then ignore storage
            return super.getFilteredProviders(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.STORAGE_VALUE);
        }

        @Override
        protected Stream<E> getFilteredConsumers(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            // ignore VDCs, because they are treated as aggregators
            // ignore Storage consumers because these only occur in VSAN and get treated as
            // aggregators
            return super.getFilteredConsumers(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE)
                        .filter(e -> e.getEntityType() != EntityType.STORAGE_VALUE);
        }
    }

    /**
     * Entities that fall under this rule are VMs and container pods.
     * They must treat related VDCs in a special way: they should
     * allow traversal to related VDCs but disallow any further traversal
     * from there on.
     * This rule also adds in a condition specific to traversal of VMs
     * which have consumer pods which also consume cloud volumes.
     *
     * <p>This effect is achieved by using {@link DefaultTraversalRule},
     *    but treating VDCs as aggregators.</p>
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class VMPodAggregatorRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull final E entity, @Nonnull final TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                        || entity.getEntityType() == EntityType.CONTAINER_POD_VALUE;
        }

        @Override
        protected Stream<E> getFilteredAggregators(@Nonnull E entity,
                                                   @Nonnull TraversalMode traversalMode) {
            // VDCs to be treated as aggregators
            final Stream<E> vdcAggregators;
            if (traversalMode == TraversalMode.CONSUMES) {
                // if going down, treat only producing VDCs as aggregators
                // (adding consumers would bring in unwanted VDCs:
                // see the topology in test
                //     SupplyChainCalculatorTest.testVdcInContainerTopology2
                // as an example)
                vdcAggregators = super.getFilteredProviders(entity, traversalMode)
                                    .filter(e -> e.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE);
            } else {
                // if at the seed or going up, treat all VDCs as aggregators
                vdcAggregators = Stream.concat(super.getFilteredProviders(entity, traversalMode),
                                               super.getFilteredConsumers(entity, traversalMode))
                                    .filter(e -> e.getEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE);
            }

            // combine with true aggregators
            return Stream.concat(vdcAggregators, super.getFilteredAggregators(entity, traversalMode));
        }

        @Override
        protected Stream<E> getFilteredProviders(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            if ((entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                    && (traversalMode == TraversalMode.CONSUMES)) {

                // when traversing down the supply chain of a vm, traverse
                // only those volumes which do not have pods as consumers (volumes
                // connected only to the vm).
                // if this traversal is coming from a pod which has a volume also as
                // a provider, the pods traversal will pull in those volumes which
                // are connected to the pod apart from the ones selected here
                return super.getFilteredProviders(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE)
                        .filter(e -> !((e.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                                && (e.getConsumers().stream()
                                .filter(c -> c.getEntityType()
                                        == EntityType.CONTAINER_POD_VALUE).count() > 0)));
            }
            // ignore VDCs, because they are treated as aggregators
            return super.getFilteredProviders(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE);
        }

        @Override
        protected Stream<E> getFilteredConsumers(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            // ignore VDCs, because they are treated as aggregators
            return super.getFilteredConsumers(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.VIRTUAL_DATACENTER_VALUE);
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
        protected Stream<E> getFilteredConsumers(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            // ignore PMs as consumers
            return super.getFilteredConsumers(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE);
        }

        @Override
        protected Stream<E> getFilteredProviders(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            final Stream<E> allProviders = super.getFilteredProviders(entity, traversalMode);
            if (traversalMode == TraversalMode.START) {
                return allProviders;
            } else {
                // ignore providing PMs
                // PMs that provide to Storage may happen in a vSAN topology
                return allProviders.filter(e -> e.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE);
            }
        }

        @Override
        protected Stream<E> getFilteredAggregators(@Nonnull E entity,
                                                   @Nonnull TraversalMode traversalMode) {
            // when traversing up, treat PMs as aggregators of storage
            // this is a hack that allows PMs and the DC to appear in the supply chain
            // but disallows any further traversals from the DC
            if (traversalMode == TraversalMode.START || traversalMode == TraversalMode.PRODUCES) {
                return Stream.concat(super.getFilteredConsumers(entity, traversalMode)
                                        .filter(e -> e.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE),
                                     super.getFilteredAggregators(entity, traversalMode));
            } else {
                return super.getFilteredAggregators(entity, traversalMode);
            }
        }
    }

    // TODO: remove this class as part of OM-51365
    /**
     * This rule is a patch for DC traversals.  It will be rendered obsolete
     * when DCs become aggregators (OM-51365).
     *
     *<p>The rule is: when a DC is in the seed, then add all consuming PMs
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
        protected Stream<E> getFilteredAggregatedEntities(@Nonnull E entity,
                                                          @Nonnull TraversalMode traversalMode) {
            return super.getFilteredAggregatedEntities(entity, traversalMode)
                        .filter(e -> e.getEntityType() != EntityType.BUSINESS_ACCOUNT_VALUE);
        }
    }

    /**
     * Rule specific to traversal of Workload Controller.
     *
     * <p>When a Workload Controller is traversed:
     * <ul>
     *     <li>Include all neighboring Namespaces
     *     </li>
     *     <li>When traversing from WorkloadController as a seed, ensure
     *         we include Nodes (VMs and Hosts) in the supply chain.
     *     </li>
     * </ul>
     * </p>
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class WorkloadControllerRule<E extends TopologyGraphEntity<E>> extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull final E entity, @Nonnull final TraversalMode traversalMode) {
            return entity.getEntityType() == EntityType.WORKLOAD_CONTROLLER_VALUE;
        }

        @Override
        protected Stream<E> getFilteredAggregators(@Nonnull E entity,
                                                   @Nonnull TraversalMode traversalMode) {
            if (traversalMode == TraversalMode.AGGREGATED_BY) {
                // Treat Namespaces as aggregators (i.e., include in the traversal
                // but do not keep traversing from them)
                return Stream.concat(super.getFilteredProviders(entity, traversalMode)
                        .filter(e -> e.getEntityType() == EntityType.NAMESPACE_VALUE),
                    super.getFilteredAggregators(entity, traversalMode));
            } else {
                return super.getFilteredAggregators(entity, traversalMode);
            }
        }

        @Override
        protected Stream<E> getFilteredAggregatedEntities(@Nonnull E entity,
                                                          @Nonnull TraversalMode traversalMode) {
            return (traversalMode == TraversalMode.AGGREGATED_BY)
                ? super.getFilteredAggregatedEntities(entity, traversalMode)
                : Stream.empty();
        }

        @Override
        protected Stream<E> getFilteredProviders(@Nonnull E entity,
                                                 @Nonnull TraversalMode traversalMode) {
            final Stream<E> allProviders = super.getFilteredProviders(entity, traversalMode);
            if (traversalMode == TraversalMode.START) {
                // Ensure that we pull in the nodes (VMs and PMs) associated
                // with the Workload Controller through its pods when the Workload
                // Controller is the seed
                return Stream.concat(allProviders,
                    super.getFilteredConsumers(entity, traversalMode)
                        .filter(e -> e.getEntityType() == EntityType.CONTAINER_POD_VALUE)
                        .flatMap(e -> super.getFilteredProviders(e, traversalMode)
                            .filter(podProvider ->
                                podProvider.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE ||
                                    podProvider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)));
            } else {
                return allProviders;
            }
        }
    }

    /**
     * Tier-specific rule: when not in seed, do not traverse to regions
     * or availability zones.
     *
     * @param <E> The type of {@link TopologyGraphEntity} in the graph.
     */
    private static class TierRule<E extends TopologyGraphEntity<E>>
            extends DefaultTraversalRule<E> {
        @Override
        public boolean isApplicable(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return (entity.getEntityType() == EntityType.STORAGE_TIER_VALUE
                                || entity.getEntityType() == EntityType.COMPUTE_TIER_VALUE
                                || entity.getEntityType() == EntityType.DATABASE_SERVER_TIER_VALUE
                                || entity.getEntityType() == EntityType.DATABASE_TIER_VALUE)
                        && traversalMode != TraversalMode.START;
        }

        @Override
        protected Stream<E> getFilteredAggregators(@Nonnull E entity, @Nonnull TraversalMode traversalMode) {
            return super.getFilteredAggregators(entity, traversalMode)
                            .filter(e -> e.getEntityType() != EntityType.REGION_VALUE)
                            .filter(e -> e.getEntityType() != EntityType.AVAILABILITY_ZONE_VALUE);
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
        protected Stream<E> getFilteredAggregators(@Nonnull E entity,
                                                   @Nonnull TraversalMode traversalMode) {
            final Stream<E> consumerPods = super.getFilteredConsumers(entity, traversalMode)
                    .filter(e -> e.getEntityType()
                            == EntityType.CONTAINER_POD_VALUE);

            if (consumerPods.count() > 0) {
                // VMs to be treated as aggregators, to stop traversing further
                // from them which otherwise will bring in all pod consumers.
                return Stream.concat(super.getFilteredAggregators(entity, traversalMode),
                        super.getFilteredConsumers(entity, traversalMode)
                                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE));
            }
            return super.getFilteredAggregators(entity, traversalMode);
        }
    }
}
