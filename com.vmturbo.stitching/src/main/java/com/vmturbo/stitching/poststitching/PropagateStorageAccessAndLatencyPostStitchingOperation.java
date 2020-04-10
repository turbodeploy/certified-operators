package com.vmturbo.stitching.poststitching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.utilities.AccessAndLatency;

/**
 * This post-stitching operation propagates discovered metrics for StorageAccess and StorageLatency
 * commodities up and down the storage supply chain. In Classic/Legacy, the equivalent logic is
 * implemented as side effects on the setters for the used values on these commodities. In XL, we
 * implement the same semantics as a post-stitching operation.
 *
 * The purpose behind the access/latency propagation is as follows: probes cannot always obtain the
 * value of these commodities from the target itself. Some times those values will not be set, and
 * it's up to the server side to fill them so that the user can see them and the market can act
 * on them.
 *
 * A common situation where this operation will be engaged is when a hypervisor probe is discovered
 * without any associated storage targets. Usually, the hypervisor probe cannot discover the
 * Access and Latency values for the Storage, DiskArray, and LogicalPool entities but it can discover
 * them for the VirtualMachines or PhysicalMachines connected to the Storage entities. This code
 * aggregates the Access and Latency values from the VirtualMachines and Physical Machines and pushes
 * them down the supply chain so that the usage values for these commodities reflect the usage
 * values of the entities that they support. For StorageAccess (IOPS), the values are aggregated
 * by taking their sum. For StorageLatency, they are aggregated by taking their IOPS-weighted
 * average (see {@link AccessAndLatency#latencyWeightedAveraged(Stream)} for additional details on
 * what this means).
 *
 * In more detail, because the source of the latency and access values is actually the disk array
 * hosting the Storage/LogicalPool entities, we need to ensure that all the Storage/LogicalPool
 * values have the same utilization numbers on the variants of these commodities that they sell
 * (note that Storage/LogicalPool may both buy and sell these commodities). If we permitted them
 * to have different values, the market might recommend a move between Storages or LogicalPools
 * hosted by the same DiskArray which would be a useless movement because the DiskArray is
 * actually the physical provider of these commodities. To address this issue, this operation,
 * after it calculates how much of the DiskArray's Access/Latency sold is being used, pushes that
 * same usage number up to the LogicalPools and Storages that reside on the DiskArray being
 * examined.
 *
 * For additional details and diagrams containing examples, see the wiki page:
 * https://vmturbo.atlassian.net/wiki/spaces/Home/pages/386564365/Storage+Access+and+Latency+propagation+across+supply+chain
 */
public class PropagateStorageAccessAndLatencyPostStitchingOperation implements PostStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
        @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        // This operation operates on DiskArrays.
        return stitchingScopeFactory.entityTypeScope(EntityType.DISK_ARRAY);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity>
    performOperation(@Nonnull Stream<TopologyEntity> entities,
                     @Nonnull EntitySettingsCollection settingsCollection,
                     @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
        // If a probe supplies Access/Latency measurements for any of the Storage entities in the Storage
        // supply chain, it must supply all of them. So if the disk array already has both Access and Latency
        // usage numbers, the entire Storage subtree reachable from that DiskArray can be skipped. But if
        // either commodity is missing usage numbers, propagate the commodity values through its subtree.
        entities.map(AccessAndLatencySold::new)
            .filter(AccessAndLatencySold::missingLatencyOrAccess)
            .forEach(accessAndLatencySold -> propagateFromDiskArray(accessAndLatencySold, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Starting from a DiskArray, propagate StorageAccess and/or StorageLatency numbers throughout
     * its Storage supply chain reachable from this DiskArray.
     *
     * @param accessAndLatencySold An object containing the DiskArray to be processed along with the
     *                             AccessAndLatency sold by that DiskArray.
     * @param resultBuilder A builder on which topological changes should be queued whenever we
     *                      wish to update an entity.
     */
    private void propagateFromDiskArray(@Nonnull final AccessAndLatencySold accessAndLatencySold,
                                        @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        final TopologyEntity diskArray = accessAndLatencySold.entity;
        Preconditions.checkArgument(diskArray.getEntityType() == EntityType.DISK_ARRAY_VALUE);

        // Build the tree of access and latency nodes.
        final AccessAndLatencyTreeNode rootNode = AccessAndLatencyTreeNode.buildTree(diskArray, Optional.empty());

        // Set and collect access and latency bought used values.
        final List<AccessAndLatency> accessAndLatencyList = new ArrayList<>();
        rootNode.collectAndSetAccessAndLatency(accessAndLatencyList, resultBuilder);

        // Calculate the sold used values from the accumulated collection and propagate those back up.
        final double accessSold = AccessAndLatency.accessSum(accessAndLatencyList.stream());
        final double latencySold = AccessAndLatency.latencyWeightedAveraged(accessAndLatencyList.stream());
        rootNode.propagateSoldValues(accessSold, latencySold, resultBuilder);
    }

    /**
     * An interface encapsulating a node inside the StorageTree that should be processed during Access/Latency
     * propagation. There are two variants of these nodes, internal nodes and leaf nodes, each of which
     * has slightly different behavior.
     */
    private interface AccessAndLatencyTreeNode {
        /**
         * Push sold usage values up the storage supply chain to Storage and LogicalPool entities
         * in the tree.
         *
         * @param accessSold The usage value for storage access (IOPS) sold.
         * @param latencySold The usage value for storage latency sold.
         * @param resultBuilder A builder on which topological changes should be queued whenever we
         *                      wish to update an entity.
         */
        void propagateSoldValues(final double accessSold, final double latencySold,
                                 @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder);

        /**
         * Collect {@link AccessAndLatency} values from Leaf nodes (VMs and PMs in the tree) and push
         * them down, setting the locally aggregated value on the bought commodities of each inner
         * node passed along the way until the root is reached.
         *
         * @param accessAndLatencies The list of {@link AccessAndLatency}s on which all leaf node
         *                           access and latencies reachable from this node should be added.
         * @param resultBuilder A builder on which topological changes should be queued whenever we
         *                      wish to update an entity.
         */
        void collectAndSetAccessAndLatency(@Nonnull final List<AccessAndLatency> accessAndLatencies,
                                           @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder);

        /**
         * Check if this node has a StorageAccess commodity bought associated with it that has a
         * non-zero used value.
         *
         * @return True if the above conditions are true, false otherwise.
         */
        boolean hasNonZeroAccessBoughtUsed();

        // Entities outside of the ones listed above do not participate in the tree
        Map<Integer, BiFunction<TopologyEntity, Optional<TopologyEntity>, AccessAndLatencyTreeNode>>
        FACTORY_FUNCTIONS = ImmutableMap.of(
            EntityType.DISK_ARRAY_VALUE, AccessAndLatencyTreeInnerNode::new,
            EntityType.STORAGE_VALUE, AccessAndLatencyTreeInnerNode::new,
            EntityType.LOGICAL_POOL_VALUE, AccessAndLatencyTreeInnerNode::new,

            EntityType.PHYSICAL_MACHINE_VALUE, AccessAndLatencyTreeLeafNode::new,
            EntityType.VIRTUAL_MACHINE_VALUE, AccessAndLatencyTreeLeafNode::new
        );

        /**
         * Check if an entity may participate in the storage tree. The entity is checked for
         * participation by its entity type.
         *
         * @param entity The entity to check for participation.
         * @return True if an entity with the given type may participate in the storage tree,
         *         false otherwise.
         */
        static boolean participatesInStorageTree(@Nonnull final TopologyEntity entity) {
            return FACTORY_FUNCTIONS.containsKey(entity.getEntityType());
        }

        /**
         * Build the tree of {@link AccessAndLatencyTreeNode} reachable from the consumer buying
         * commodities from the given provider. The provider is necessary because, for example,
         * a VM may be buying commodities from multiple storages and we only wish to include
         * in the tree those commodities being bought from the specific provider.
         *
         * Note that the tree is built recursively. When an inner node is created, that inner
         * node will automatically add its relevant consumers to itself. A leaf node does
         * not add any other consumers.
         *
         * @param consumer The consumer of storage commodities from the provider.
         * @param provider The provider of the storage commodities.
         * @return A reference to a {@link AccessAndLatencyTreeNode} which is the root of the
         *         subtree reachable from the given consumer.
         */
        static AccessAndLatencyTreeNode buildTree(@Nonnull final TopologyEntity consumer,
                                                  @Nonnull final Optional<TopologyEntity> provider) {
            Preconditions.checkArgument(participatesInStorageTree(consumer));
            return FACTORY_FUNCTIONS.get(consumer.getEntityType()).apply(consumer, provider);
        }
    }

    /**
     * A {@link AccessAndLatencyTreeNode} representing a node INSIDE the tree, that is a node
     * that may have children. These inner nodes are associated with DiskArray, LogicalPool,
     * and Storage entity types.
     *
     * Inner nodes may both buy and sell StorageAccess and StorageLatency.
     *
     * An inner node contrasts with a leaf node.
     */
    private static class AccessAndLatencyTreeInnerNode implements AccessAndLatencyTreeNode {
        private final AccessAndLatencySold sold;
        private final Optional<AccessAndLatencyBought> bought;
        private final Map<TopologyEntity, AccessAndLatencyTreeNode> children;

        /**
         * Create a new inner node. Note that this will recursively create the subtree reachable from
         * this inner node.
         *
         * @param consumer The consumer entity associated with this node. The {@link AccessAndLatencySold}
         *                 and {@link AccessAndLatencyBought} are obtained from the consumer's commodities
         *                 sold and bought respectively. Children are automatically added based on the
         *                 consumers of this consumer.
         * @param provider The provider specifying which commodities on the consumer to examine. Only
         *                 commodities bought from this provider should be examined when building the tree.
         */
        public AccessAndLatencyTreeInnerNode(@Nonnull final TopologyEntity consumer,
                                             @Nonnull final Optional<TopologyEntity> provider) {
            Optional<TopologyEntity> actualProvider = provider;
            if (isVSANEntity(consumer)) {
                //VSAN storage buys from hosts and not from DiskArray
                actualProvider = consumer.getProviders().stream().filter(cProvider ->
                    cProvider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst();
            }
            sold = new AccessAndLatencySold(consumer);
            bought = actualProvider.map(storageCommoditiesProvider ->
                new AccessAndLatencyBought(consumer, storageCommoditiesProvider));

            // Collect all children from consumers of the consumer.
            children = consumer.getConsumers().stream()
                .filter(AccessAndLatencyTreeNode::participatesInStorageTree)
                .collect(Collectors.toMap(
                    Function.identity(),
                    consumerOfNode -> AccessAndLatencyTreeNode.buildTree(consumerOfNode, Optional.of(consumer))
                ));

            // For all physical machines children that have StorageAccess values set, we need to ignore
            // virtual machine consumers of those physical machines that are also consumers of this node.
            // This is because we assume that the value measured on the physical machine already includes
            // the values for its child VMs plus some overhead of its own. Thus if we included both the
            // PM and its child VMs would double-count the values on the VMs.
            final List<TopologyEntity> physicalMachinesWithStorageAccessUsed =
                children.entrySet().stream()
                    .filter(entry -> entry.getKey().getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                    .filter(entry -> entry.getValue().hasNonZeroAccessBoughtUsed())
                    .map(Entry::getKey)
                    .collect(Collectors.toList()); // Collect to a list to avoid ConcurrentModification
                                                   // of the children below.

            physicalMachinesWithStorageAccessUsed.stream().flatMap(physicalMachine ->
                physicalMachine.getConsumers().stream()
                    .filter(pmConsumer -> pmConsumer.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE))
                .forEach(children::remove);
        }

        private static boolean isVSANEntity(@Nonnull final TopologyEntity entity)  {
            return entity.getEntityType() == EntityType.STORAGE_VALUE  &&
                            entity.getTypeSpecificInfo().hasStorage()  &&
                            entity.getTypeSpecificInfo().getStorage().getStorageType() ==
                                StorageType.VSAN;
        }

        @Override
        public void propagateSoldValues(double accessSold, double latencySold,
                                        @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            if (sold.shouldUpdateLatencyOrAccess(accessSold, latencySold)) {
                resultBuilder.queueUpdateEntityAlone(sold.entity, topologyEntity -> {
                    sold.updateIfUnset(sold.access, accessSold);
                    sold.updateIfUnset(sold.latency, latencySold);
                });
            }

            children.values().forEach(child -> child.propagateSoldValues(
                accessSold, latencySold, resultBuilder));
        }

        @Override
        public void collectAndSetAccessAndLatency(@Nonnull List<AccessAndLatency> accessAndLatencies,
                                                  @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            final List<AccessAndLatency> localAccessAndLatencies = new ArrayList<>();
            children.values().forEach(child -> child.collectAndSetAccessAndLatency(
                localAccessAndLatencies, resultBuilder));

            bought.ifPresent(accessAndLatencyBought ->
                resultBuilder.queueUpdateEntityAlone(sold.entity, topologyEntity -> {
                    accessAndLatencyBought.updateIfUnset(accessAndLatencyBought.access,
                        () -> AccessAndLatency.accessSum(localAccessAndLatencies.stream()));
                    accessAndLatencyBought.updateIfUnset(accessAndLatencyBought.latency,
                        () -> AccessAndLatency.latencyWeightedAveraged(localAccessAndLatencies.stream()));
                })
            );

            // Add the local access and latencies to the collection.
            accessAndLatencies.addAll(localAccessAndLatencies);
        }

        @Override
        public boolean hasNonZeroAccessBoughtUsed() {
            return bought
                .flatMap(accessAndLatencyBought -> accessAndLatencyBought.access
                    .map(builder -> builder.hasUsed() && builder.getUsed() != 0))
                .orElse(false);
        }
    }

    /**
     * A {@link AccessAndLatencyTreeLeafNode} representing a node at a leaf of the tree (a
     * terminal nod), that is a node that may have children. These leaf nodes are associated
     * with PhysicalMachine and VirtualMachine entity types.
     *
     * Leaf nodes only buy StorageAccess and StorageLatency, they do not resell it.
     *
     * A leaf node contrasts with an inner node.
     */
    private static class AccessAndLatencyTreeLeafNode implements AccessAndLatencyTreeNode {
        final AccessAndLatencyBought bought;

        /**
         * Construct a new leaf node containing {@link AccessAndLatencyBought}.
         *
         * @param consumer The consumer whose Access and Latency commodities bought from the given
         *                 provider should be set on this leaf node.
         * @param provider The provider of the Access and Latency commodities. Must be present.
         */
        public AccessAndLatencyTreeLeafNode(@Nonnull final TopologyEntity consumer,
                                            @Nonnull final Optional<TopologyEntity> provider) {
            Preconditions.checkArgument(provider.isPresent());
            bought = new AccessAndLatencyBought(consumer, provider.get());
        }

        @Override
        public void propagateSoldValues(double accessSold, double latencySold,
                                        @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            // Leaf nodes do not sell access and latency and propagation terminates here.
            // Nothing to do.
        }

        @Override
        public void collectAndSetAccessAndLatency(@Nonnull final List<AccessAndLatency> accessAndLatencies,
                                                  @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            // Leaf nodes should not write new access and latency values.
            // However, we do need to collect our local {@link AccessAndLatency} into the list.
            accessAndLatencies.add(bought.buildAccessAndLatency());
        }

        @Override
        public boolean hasNonZeroAccessBoughtUsed() {
            return bought.access
                .map(builder -> builder.hasUsed() && builder.getUsed() != 0)
                .orElse(false);
        }
    }

    /**
     * The {@link AccessAndLatencySold} class encapsulates StorageAccess and StorageLatency
     * values sold by a particular entity.
     */
    @VisibleForTesting
    static class AccessAndLatencySold {
        public final Optional<CommoditySoldDTO.Builder> latency;
        public final Optional<CommoditySoldDTO.Builder> access;
        public final TopologyEntity entity;

        /**
         * Create a new {@link AccessAndLatencySold} containing the StorageAccess and StorageLatency commodities
         * sold by a given entity.
         *
         * @param entity The entity potentially selling Access and Latency.
         */
        public AccessAndLatencySold(@Nonnull final TopologyEntity entity) {
            final List<CommoditySoldDTO.Builder> commoditiesSold =
                entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList();
            latency = commoditiesSold.stream()
                .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.STORAGE_LATENCY_VALUE)
                .findFirst();
            access = commoditiesSold.stream()
                .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst();

            this.entity = Objects.requireNonNull(entity);
        }

        /**
         * Check whether the {@link AccessAndLatencySold} is missing the latency or access commodities
         * and that those commodities have usage values set.
         *
         * @return True if the access and latency values are present and have usage values, false
         *         otherwise.
         */
        public boolean missingLatencyOrAccess() {
            return !(
                latency.isPresent() && latency.get().hasUsed() && latency.get().getUsed() != 0 &&
                access.isPresent() && access.get().hasUsed() && access.get().getUsed() != 0
            );
        }

        /**
         * Tests whether EITHER the latency or access values should be updated.
         *
         * @param accessValue The value to set to access if it should be updated.
         * @param latencyValue The value to set to latency if it should be updated.
         *
         * @return True if EITHER the latency or access values should be updated.
         *         Also returns true if they both need to be updated.
         *         Returns false only when neither need to be updated.
         */
        public boolean shouldUpdateLatencyOrAccess(final double accessValue,
                                                   final double latencyValue) {
            // Note that we overwrite used values of 0 because probes (in particular, the VC probe)
            // send a value of 0 even when they want the value updated. Ideally, probes should only
            // send values that they want retained but until they put this into practice we
            // must also overwrite 0.
            return shouldUpdateValue(latency, latencyValue) || shouldUpdateValue(access, accessValue);
        }

        private boolean shouldUpdateValue(@Nonnull final Optional<CommoditySoldDTO.Builder> commodity,
                                          final double value) {
            return (commodity.isPresent() && (!commodity.get().hasUsed() ||
                (commodity.map(c -> c.getUsed() == 0 && value != 0).orElse(false))));
        }

        /**
         * Update the commodity sold builder used and peak values if they are unset.
         * We consider a used value unset if the commodity is present but either has no used value or the used
         * value is 0.
         *
         * @param accessOrLatency The access or latency commodity to update if unset.
         * @param value The value that the commodity used and peak will be updated to if the used is unset.
         */
        public void updateIfUnset(@Nonnull final Optional<CommoditySoldDTO.Builder> accessOrLatency,
                                  final double value) {
            accessOrLatency.ifPresent(commodity -> {
                if (!commodity.hasUsed() || commodity.getUsed() == 0) {
                    commodity.setUsed(value);

                    // Note that we set peak equal to used.
                    // Per Pankaj: "It is done this way to avoid excessive storage related
                    // actions based on transient values."
                    commodity.setPeak(value);
                }
            });
        }
    }

    /**
     * The {@link AccessAndLatencyBought} class encapsulates StorageAccess and StorageLatency
     * values bought by a particular entity from a given provider.
     */
    @VisibleForTesting
    static class AccessAndLatencyBought {
        public final Optional<CommodityBoughtDTO.Builder> latency;
        public final Optional<CommodityBoughtDTO.Builder> access;

        /**
         * Create a new {@link AccessAndLatencyBought} containing the StorageAccess and StorageLatency commodities
         * bought by the given consumer from the given provider are
         *
         * @param consumer The consumer potentially buying Access and Latency from the provider.
         * @param provider The provider potentially selling Access and Latency to the consumer.
         */
        public AccessAndLatencyBought(@Nonnull final TopologyEntity consumer,
                                      @Nonnull final TopologyEntity provider) {
            final Optional<CommoditiesBoughtFromProvider.Builder> boughtFromProvider =
                consumer.getTopologyEntityDtoBuilder()
                    .getCommoditiesBoughtFromProvidersBuilderList().stream()
                    .filter(commoditiesBought -> commoditiesBought.getProviderId() == provider.getOid())
                    .findFirst();

            access = boughtFromProvider
                .flatMap(fromProvider -> fromProvider.getCommodityBoughtBuilderList().stream()
                    .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                    .findFirst());
            latency = boughtFromProvider
                .flatMap(fromProvider -> fromProvider.getCommodityBoughtBuilderList().stream()
                    .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.STORAGE_LATENCY_VALUE)
                    .findFirst());
        }

        /**
         * Update the commodity bought builder used and peak values if they are unset.
         * We consider a used value unset if the commodity is present but either has no used value or the used
         * value is 0.
         *
         * @param accessOrLatency The access or latency commodity to update if unset.
         * @param valueSupplier A supplier for the value. Will not be evaluated if the value is not needed.
         */
        public void updateIfUnset(@Nonnull final Optional<CommodityBoughtDTO.Builder> accessOrLatency,
                                  final Supplier<Double> valueSupplier) {
            accessOrLatency.ifPresent(commodity -> {
                if (!commodity.hasUsed() || commodity.getUsed() == 0) {
                    final double value = valueSupplier.get();
                    commodity.setUsed(value);

                    // Note that we set peak equal to used.
                    // Per Pankaj: "It is done this way to avoid excessive storage related
                    // actions based on transient values."
                    commodity.setPeak(value);
                }
            });
        }

        /**
         * Build the {@link AccessAndLatency} associated with this {@link AccessAndLatencyBought}.
         * {@link AccessAndLatency}s can be used for calculating access and latency aggregations.
         *
         * @return The {@link AccessAndLatency} associated with this {@link AccessAndLatencyBought}.
         */
        @Nonnull
        public AccessAndLatency buildAccessAndLatency() {
            return new AccessAndLatency(
                access
                    .filter(Builder::hasUsed)
                    .map(Builder::getUsed),
                latency
                    .filter(Builder::hasUsed)
                    .map(Builder::getUsed)
            );
        }
    }
}
