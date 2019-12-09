package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;

/**
 * Specifies how to apply a particular set {@link PlacementPolicy}.
 *
 * Each {@link PlacementPolicy} implementation should have an accompanying
 * {@link PlacementPolicyApplication} implementation. The main reason to split the application from
 * the policy definition is to allow for optimizations when applying multiple policies of the same
 * type.
 */
public abstract class PlacementPolicyApplication {

    /**
     * We can not set capacity to infinity, because database table can not store string "Infinity".
     */
    public static final float MAX_CAPACITY_VALUE = 1e9f;

    /**
     * Small delta that we are adding to a segmentation commodity capacity, to ensure floating point
     * roundoff error does not accidentally reduce the commodity capacity below the intended value.
     * Another use case for this delta is that (given our standard price function) when you are at
     * the max value that an integer capacity allows, you will not hit an infinite quote.
     * For example, if the capacity is 9.0, you can still accommodate 9 buyers (each one buying 1.0)
     * and the price will not be infinite, but 10 consumers don't fit.
     */
    public static final float SMALL_DELTA_VALUE = 0.1f;

    /**
     * Used value of a segmentation commodity bought
     */
    public static final float SEGM_BOUGHT_USED_VALUE = 1.0f;

    protected Logger logger = LogManager.getLogger(getClass());

    protected final GroupResolver groupResolver;
    protected final TopologyGraph<TopologyEntity> topologyGraph;

    /**
     * Counters for commodities added during this poloicy application.
     * Just for logging/metric/debugging purposes.
     * Populated while running {@link PlacementPolicyApplication#apply(List)}.
     */
    private Map<CommodityDTO.CommodityType, MutableInt> addedCommodities = new HashMap<>();

    protected PlacementPolicyApplication(final GroupResolver groupResolver,
                                         final TopologyGraph<TopologyEntity> topologyGraph) {
        this.groupResolver = groupResolver;
        this.topologyGraph = topologyGraph;
    }

    /**
     * Apply a list of policies. Disabled policies will not be applied.
     *
     * @param policies The policies. Note - the input policies should be the right implementations
     *                 of {@link PlacementPolicy} for the particular implementation of
     *                 {@link PlacementPolicyApplication}.
     * @return The {@link PlacementPolicyApplication} results.
     */
    public PolicyApplicationResults apply(@Nonnull final List<PlacementPolicy> policies) {

        final List<PlacementPolicy> policiesToApply = policies.stream()
            .filter(policy -> {
                if (!policy.isEnabled()) {
                    logger.debug("Skipping application of disabled {} policy.",
                        policy.getPolicyDefinition().getPolicyInfo().getPolicyDetailCase());
                    return false; // Do not apply disabled policies.
                } else {
                    return true;
                }
            })
            .collect(Collectors.toList());

        final Map<PlacementPolicy, PolicyApplicationException> errors = applyInternal(policiesToApply);

        ImmutablePolicyApplicationResults.Builder resBldr = ImmutablePolicyApplicationResults.builder()
            .putAllErrors(errors);

        // The added commodities map gets populated as the application implementation adds
        // commodities during its "applyInternal" method execution.
        addedCommodities.forEach((commType, count) -> {
            resBldr.putAddedCommodities(commType, count.toInteger());
        });
        return resBldr.build();
    }

    /**
     * The results of running a particular {@link PlacementPolicyApplication}.
     */
    @Value.Immutable
    public interface PolicyApplicationResults {
        /**
         * If any input {@link PlacementPolicy}s encountered an error, the error will be in this map.
         */
        Map<PlacementPolicy, PolicyApplicationException> errors();

        /**
         * The counts of commodities added by this {@link PlacementPolicyApplication}, indexed
         * by commodity type. This is just for logging/metric purposes.
         */
        Map<CommodityDTO.CommodityType, Integer> addedCommodities();
    }

    /**
     * Policy-variant-specific implementation.
     */
    protected abstract Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<PlacementPolicy> policies);

    /**
     * Segment the providers from the rest of the topology by having each of them sell a
     * segmentation commodity specific to this policy.
     *
     * @param providers The providers that belong to the segment.
     * @param segmentationCommodity The commodity for use in segmenting the providers.
     */
    protected void addCommoditySold(@Nonnull final Set<Long> providers,
                                    @Nonnull final CommoditySoldDTO segmentationCommodity) {
        providers.forEach(providerId -> topologyGraph.getEntity(providerId)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> {
                recordCommodityAddition(segmentationCommodity.getCommodityType().getType());
                provider.addCommoditySoldList(segmentationCommodity);
                // add segmentation comm on replaced entity
                if (provider.hasEdit() && provider.getEdit().hasReplaced()) {
                    addCommoditySold(provider.getEdit().getReplaced().getReplacementId(),
                            segmentationCommodity);
                }
            }));
    }

    /**
     * Force the provider to sell the commodity passed.
     *
     * @param providerId The provider that needs to sell the commodity.
     * @param commodity The commodity to be sold.
     */
    protected void addCommoditySold(@Nonnull final long providerId,
                                    @Nonnull final CommoditySoldDTO commodity) {
        topologyGraph.getEntity(providerId)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> {
                recordCommodityAddition(commodity.getCommodityType().getType());
                provider.addCommoditySoldList(commodity);
            });
    }

    /**
     * Segment the providers from the rest of the topology by having each of them sell a
     * segmentation commodity specific to this policy. The commodity sold should have
     * the provided capacity.
     *
     * The used value on the commodity sold will be calculated based on the number of
     * consumers in the consumer group currently buying from each provider.
     *
     * @param providers The providers that belong to the segment.
     * @param consumerGroupIds The consumers that belong to the segment.
     * @param commoditySoldCapacity The capacity for the commodity sold by the providers.
     * @param policy The policy that's causing this commodity addition.
     */
    protected void addCommoditySold(@Nonnull final Set<Long> providers,
                                    @Nonnull final Set<Long> consumerGroupIds,
                                    final float commoditySoldCapacity,
                                    final PlacementPolicy policy) {
        providers.forEach(providerId -> topologyGraph.getEntity(providerId)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> {
                final CommoditySoldDTO segmentationCommodity = commoditySold(commoditySoldCapacity,
                    provider.getOid(), consumerGroupIds, policy);
                recordCommodityAddition(segmentationCommodity.getCommodityType().getType());
                provider.addCommoditySoldList(segmentationCommodity);
                // add comm on replaced entity
                if (provider.hasEdit() && provider.getEdit().hasReplaced()) {
                    addCommoditySold(provider.getEdit().getReplaced().getReplacementId(),
                            segmentationCommodity);
                }
            }));
    }

    /**
     * Segment the entities of the same type as the providers that are not actually part of the providers
     * group by having each of them sell a segmentation commodity specific to this policy.
     *
     * @param providers The providers that belong to the segment.
     * @param providerEntityType The entity type of the providers.
     * @param segmentationCommodity The commodity for use in segmenting the providers.
     */
    protected void addCommoditySoldToComplementaryProviders(@Nonnull final Set<Long> providers,
                                                            final long providerEntityType,
                                                            @Nonnull final CommoditySoldDTO segmentationCommodity) {
        topologyGraph.entities()
            .filter(entity -> entity.getEntityType() == providerEntityType)
            .filter(vertex -> !providers.contains(vertex.getOid()))
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .forEach(provider -> {
                recordCommodityAddition(segmentationCommodity.getCommodityType().getType());
                provider.addCommoditySoldList(segmentationCommodity);
            });
    }

    /**
     * Segment the consumers from the rest of the topology by having each of them buy a
     * segmentation commodity specific to this policy.
     *
     * @param consumers The consumers that belong to the segment.
     * @param providerType The type of provider that will be providing the segment commodity
     *                     these consumers must be buying.
     * @param segmentationCommodity The commodity for use in segmenting the consumers.
     * @throws PolicyApplicationException If the consumers cannot be segmented as desired.
     */
    protected void addCommodityBought(@Nonnull final Set<Long> consumers,
                                      final int providerType,
                                      @Nonnull final CommodityBoughtDTO segmentationCommodity)
        throws PolicyApplicationException {
        for (Long consumerId : consumers) {
            final Optional<TopologyEntity> optionalConsumer = topologyGraph.getEntity(consumerId);
            if (optionalConsumer.isPresent()) {
                final TopologyEntityDTO.Builder consumer;
                final Optional<Long> volumeId;
                if (optionalConsumer.get().getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    // if it's volume, the real consumer should be the VM which uses this volume
                    Optional<TopologyEntity> optVM = optionalConsumer.get()
                        .getInboundAssociatedEntities()
                        .stream()
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .findFirst();
                    if (!optVM.isPresent()) {
                        // the volume is not used by any VM, which means it is a wasted volume,
                        // so we can't add segmentation commodity to related VM
                        logger.debug("Skipping applying consumer segment for wasted volume: {}", consumerId);
                        continue;
                    }
                    // consumer should be the VM which is connected to this volume
                    consumer = optVM.get().getTopologyEntityDtoBuilder();
                    volumeId = Optional.of(consumerId);
                } else {
                    consumer = optionalConsumer.get().getTopologyEntityDtoBuilder();
                    volumeId = Optional.empty();
                }

                // Separate commoditiesBoughtFromProvider into two category:
                // Key is True: list of commodityBought group, whose provider entity type matches
                // with given providerType (and volumeId matches if consumer is VirtualVolume)
                // Key is False: list of commodityBought group, whose provider entity type doesn't
                // match with given providerType (or volumeId doesn't match if consumer is VirtualVolume)
                final Map<Boolean, List<CommoditiesBoughtFromProvider>> commodityBoughtsChangeMap =
                    consumer.getCommoditiesBoughtFromProvidersList().stream()
                        .collect(Collectors.partitioningBy(commodityBoughtGroup ->
                            shouldAddSegmentToCommodityBought(commodityBoughtGroup, topologyGraph,
                                providerType, volumeId)));

                // All Commodity Bought list which should be added segmentation commodity
                final List<CommoditiesBoughtFromProvider> commodityBoughtsToAddSegment = commodityBoughtsChangeMap.get(true);
                // All Commodity Bought list which should not be added segmentation commodity
                final List<CommoditiesBoughtFromProvider> commodityBoughtsToNotAddSegment = commodityBoughtsChangeMap.get(false);
                // If there is no matched provider type and volumeId, it means the consumer doesn't
                // buy any commodity from this provider type. For example, VM1 buying ST1, and VM2
                // not buying ST at all, If create a policy to Force VM1 and VM2 to buy ST1,
                // it should throw exception, because VM2 doesn't buy any Storage type.
                if (commodityBoughtsToAddSegment.isEmpty()) {
                    throw new PolicyApplicationException("Unable to apply consumer segment when no " +
                        "provider type " + providerType);
                }
                // For each bundle of commodities bought for the entity type that matches the
                // provider type and volumeId, add the segmentation commodity.
                addCommodityBoughtForProviders(segmentationCommodity, consumer,
                    commodityBoughtsToAddSegment, commodityBoughtsToNotAddSegment);
            }
        }
    }

    /**
     * Check if commodity bought has same provider entity type as providerType parameter. If
     * volumeId is provided, the volumeId in the commodity bought should also match.
     *
     * @param commodityBoughtGrouping Contains a bundle of commodity bought.
     * @param topologyGraph The graph containing the topology.
     * @param providerType The type of provider that will be providing the segment commodity
     *                     these consumers must be buying.
     * @param volumeId the volumeId to match if provided
     * @return boolean type represents if provider entity type matches.
     */
    private boolean shouldAddSegmentToCommodityBought(@Nonnull CommoditiesBoughtFromProvider commodityBoughtGrouping,
                                                      @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                                      final int providerType,
                                                      @Nonnull Optional<Long> volumeId) {
        // TODO: After we guarantee that commodity type always have provider entity type, we will not
        // need to check topology graph to get provider entity type.
        if (commodityBoughtGrouping.hasProviderEntityType()) {
            return commodityBoughtGrouping.getProviderEntityType() == providerType &&
                (!volumeId.isPresent() || volumeId.get() == commodityBoughtGrouping.getVolumeId());
        } else {
            return commodityBoughtGrouping.hasProviderId() &&
                isProviderOfType(commodityBoughtGrouping.getProviderId(), topologyGraph, providerType) &&
                (!volumeId.isPresent() || volumeId.get() == commodityBoughtGrouping.getVolumeId());
        }
    }

    private void addCommodityBoughtForProviders(@Nonnull CommodityBoughtDTO segmentationCommodity,
                                                @Nonnull final TopologyEntityDTO.Builder consumer,
                                                @Nonnull final List<CommoditiesBoughtFromProvider> providersOfType,
                                                @Nonnull final List<CommoditiesBoughtFromProvider> nonProvidersOfType) {
        consumer.clearCommoditiesBoughtFromProviders();
        consumer.addAllCommoditiesBoughtFromProviders(nonProvidersOfType);
        providersOfType.forEach(providerCommodityGrouping -> {
            recordCommodityAddition(segmentationCommodity.getCommodityType().getType());
            consumer.addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder(providerCommodityGrouping)
                    .addCommodityBought(segmentationCommodity)
                    .build()
            );
        });
    }

    /**
     * Construct a commodityType suitable for application with this policy.
     * It specifies a SEGMENTATION commodity with the key based on the policy ID.
     *
     * @param policy The policy that's causing this commodity addition.
     * @return A {@link CommodityType} suitable for application with this policy.
     */
    private CommodityType commodityType(final PlacementPolicy policy) {
        return CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION.getNumber())
            .setKey(Long.toString(policy.getPolicyDefinition().getId()))
            .build();
    }

    /**
     * Create a {@link CommoditySoldDTO} that all members of the consumer group of this policy should buy.
     * Sets the capacity to the specified capacity. The used value is calculated based on the number
     * of consumers in the consumer group currently buying from the provider whose {@code providerId}
     * is supplied.
     *
     * Assumes each consumer is consuming exactly 1 unit of the commoditySold's capacity.
     *
     * @param capacity The capacity for the commodity.
     * @param providerId The ID of the provider
     * @param consumerGroupIds The set of all members of the consumer group for the policy.
     * @return An {@link CommoditySoldDTO} appropriate for the specified provider.
     */
    protected CommoditySoldDTO commoditySold(float capacity,
                                             final long providerId,
                                             final Set<Long> consumerGroupIds,
                                             final PlacementPolicy policy) {
        // Calculate used value.
        final long numConsumersInConsumerGroup = topologyGraph.getConsumers(providerId)
            .map(TopologyEntity::getOid)
            .filter(consumerGroupIds::contains)
            .count();

        return CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setCommodityType(commodityType(policy))
            .setUsed((float)numConsumersInConsumerGroup)
            .build();
    }

    /**
     * Create a {@link CommoditySoldDTO} that all members of the consumer group of this policy should buy.
     * The created commodity has MAX_CAPACITY_VALUE capacity. No used value is provided because the used
     * value is not relevant when the capacity is MAX_CAPACITY_VALUE.
     *
     * @return A {@link CommoditySoldDTO} that all members of the providers group of this policy should buy.
     */
    protected CommoditySoldDTO commoditySold(final PlacementPolicy policy) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(commodityType(policy))
            .setCapacity(MAX_CAPACITY_VALUE)
            .build();
    }

    /**
     * Create a {@link CommodityBoughtDTO} that all members of the consumers group of this policy should buy.
     *
     * @return A {@link CommodityBoughtDTO} that all members of the consumers group of this policy should buy.
     */
    protected CommodityBoughtDTO commodityBought(final PlacementPolicy policy) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(commodityType(policy))
            .setUsed(SEGM_BOUGHT_USED_VALUE)
            .build();
    }

    /**
     * Check if a provider is of the appropriate type.
     *
     * @param providerId The ID of the provider.
     * @param topologyGraph The topology graph of which the provider is a member.
     * @param providerEntityType The entity type to check if the provider matches.
     * @return Whether the provider is of the {@code providerEntityType}.
     */
    protected boolean isProviderOfType(final long providerId,
                                       @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                       final int providerEntityType) {
        return topologyGraph.getEntity(providerId)
            .map(vertex -> vertex.getEntityType() == providerEntityType)
            .orElse(false);
    }

    /**
     * Record the addition of a commodity.
     *
     * Most policy applications add commodities via calls to methods in {@link PlacementPolicyApplication},
     * but some policy applications add additional commodities. In that case they should call
     * this method to ensure the final commodity counts are correct.
     *
     * @param commType The type of the commodity being added.
     */
    protected void recordCommodityAddition(final int commType) {
        addedCommodities.computeIfAbsent(CommodityDTO.CommodityType.forNumber(commType),
            k -> new MutableInt(0)).increment();
    }
}
