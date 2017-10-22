package com.vmturbo.topology.processor.group.policy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * A policy applies a constraint on a topology to restrict the possible options available to the market
 * during market analysis.
 *
 * Policies work by adding, removing, or modifying commodities on a given topology during the policy application
 * process.
 *
 * Where possible, policies that create or modify commodities that require a key should use
 * the policy ID as the key.
 *
 * Subclassed by various specific policy types (ie Merge, BindToGroup, etc.).
 */
public abstract class PlacementPolicy {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The policy definition describing the details of the policy to be applied.
     */
    protected final PolicyDTO.Policy policyDefinition;

    /**
     * Construct a new policy.
     *
     * Note that the Policy ID must be present for a policy to be successfully applied
     * because the ID will be used as the key.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     */
    protected PlacementPolicy(@Nonnull final PolicyDTO.Policy policyDefinition) {
        Preconditions.checkArgument(policyDefinition.hasId());
        this.policyDefinition = policyDefinition;
    }

    protected boolean hasEntityType(PolicyGrouping grouping) {
        return grouping.hasCluster() || grouping.getGroup().getInfo().hasEntityType();
    }

    /**
     * Infer the numeric {@link EntityType} of the members of a policy grouping. If the grouping
     * is by {@link Group} then get the type from the {@link GroupInfo}. If it is a
     * {@link Cluster} then infer it from the {@link ClusterInfo.Type}.
     *
     * @param grouping the grouping for which to infer the entity type
     * @return the numeriucal entity type of the members of the grouping
     */
    protected int entityType(PolicyGrouping grouping) {
        return grouping.hasGroup()
            ? grouping.getGroup().getInfo().getEntityType()
            : grouping.getCluster().getInfo().getClusterType() == ClusterInfo.Type.COMPUTE
                    ? EntityType.PHYSICAL_MACHINE_VALUE
                    : EntityType.STORAGE_VALUE;
    }

    /**
     * Apply the given policy.
     * If the policy is not enabled, it will not be applied.
     *
     * @param groupResolver The group resolver to be used in resolving the consumer and provider groups
     *                      to which the policy applies.
     * @param topologyGraph The {@link TopologyGraph} to which the policy should be applied.
     * @throws GroupResolutionException If one or more group involved in the policy cannot be resolved.
     * @throws PolicyApplicationException If the policy cannot be applied.
     */
    public void apply(@Nonnull final GroupResolver groupResolver,
                               @Nonnull final TopologyGraph topologyGraph)
        throws GroupResolutionException, PolicyApplicationException {

        if (!isEnabled()) {
            logger.debug("Skipping application of disabled {} policy.", policyDefinition.getPolicyDetailCase());
            return; // Do not apply disabled policies.
        }

        applyInternal(groupResolver, topologyGraph);
    }

    /**
     * Policy-variant-specific implementation that applies the policy.
     *
     * @param groupResolver The group resolver to be used in resolving the consumer and provider groups
     *                      to which the policy applies.
     * @param topologyGraph The {@link TopologyGraph} to which the policy should be applied.
     * @throws GroupResolutionException If one or more group involved in the policy cannot be resolved.
     * @throws PolicyApplicationException If the policy cannot be applied.
     */
    protected abstract void applyInternal(@Nonnull final GroupResolver groupResolver,
                                          @Nonnull final TopologyGraph topologyGraph)
        throws GroupResolutionException, PolicyApplicationException;

    /**
     * Get the policy definition describing the details of the policy to be applied.
     *
     * @return The policy definition describing the details of the policy to be applied.
     */
    public PolicyDTO.Policy getPolicyDefinition() {
        return policyDefinition;
    }

    /**
     * Returns whether the policy is currently enabled. Policies that are not enabled should not be applied.
     *
     * @return Whether the policy is currently enabled.
     */
    public boolean isEnabled() {
        return policyDefinition.getEnabled();
    }


    /**
     * Segment the providers from the rest of the topology by having each of them sell a
     * segmentation commodity specific to this policy.
     *
     * @param providers The providers that belong to the segment.
     * @param topologyGraph The graph containing the topology.
     * @param segmentationCommodity The commodity for use in segmenting the providers.
     */
    protected void addCommoditySold(@Nonnull final Set<Long> providers,
                                    @Nonnull final TopologyGraph topologyGraph,
                                    @Nonnull final CommoditySoldDTO segmentationCommodity) {
        providers.forEach(providerId -> topologyGraph.getVertex(providerId)
            .map(Vertex::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> provider.addCommoditySoldList(segmentationCommodity)));
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
     * @param topologyGraph The graph containing the topology.
     * @param commoditySoldCapacity The capacity for the commodity sold by the providers.
     */
    protected void addCommoditySold(@Nonnull final Set<Long> providers,
                                    @Nonnull final Set<Long> consumerGroupIds,
                                    @Nonnull final TopologyGraph topologyGraph,
                                    final float commoditySoldCapacity) {
        providers.forEach(providerId -> topologyGraph.getVertex(providerId)
            .map(Vertex::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> {
                final CommoditySoldDTO segmentationCommodity = commoditySold(commoditySoldCapacity,
                    provider.getOid(), consumerGroupIds, topologyGraph);
                provider.addCommoditySoldList(segmentationCommodity);
            }));
    }

    /**
     * Segment the entities of the same type as the providers that are not actually part of the providers
     * group by having each of them sell a segmentation commodity specific to this policy.
     *
     * @param providers The providers that belong to the segment.
     * @param providerEntityTYpe The entity type of the providers.
     * @param topologyGraph The graph containing the topology.
     * @param segmentationCommodity The commodity for use in segmenting the providers.
     */
    protected void addCommoditySoldToComplementaryProviders(@Nonnull final Set<Long> providers,
                                                            final long providerEntityTYpe,
                                                            @Nonnull final TopologyGraph topologyGraph,
                                                            @Nonnull final CommoditySoldDTO segmentationCommodity) {
        topologyGraph.vertices()
            .filter(vertex -> vertex.getEntityType() == providerEntityTYpe)
            .filter(vertex -> !providers.contains(vertex.getOid()))
            .map(Vertex::getTopologyEntityDtoBuilder)
            .forEach(provider -> provider.addCommoditySoldList(segmentationCommodity));
    }

    /**
     * Segment the consumers from teh rest of the topology by having each of them buy a
     * segmentation commodity specific to this policy.
     *
     * @param consumers The consumers that belong to the segment.
     * @param topologyGraph The graph containing the topology.
     * @param providerType The type of provider that will be providing the segment commodity
     *                     these consumers must be buying.
     * @param segmentationCommodity The commodity for use in segmenting the consumers.
     * @throws PolicyApplicationException If the consumers cannot be segmented as desired.
     */
    protected void addCommodityBought(@Nonnull final Set<Long> consumers,
                                      @Nonnull final TopologyGraph topologyGraph,
                                      final int providerType,
                                      @Nonnull final CommodityBoughtDTO segmentationCommodity)
        throws PolicyApplicationException {
        for (Long consumerId : consumers) {
            final Optional<Builder> optionalConsumer = topologyGraph.getVertex(consumerId)
                .map(Vertex::getTopologyEntityDtoBuilder);
            if (optionalConsumer.isPresent()) {
                final TopologyEntityDTO.Builder consumer = optionalConsumer.get();
                // Separate commoditiesBoughtFromProvider into two category: Key is True: which provider
                // entity type matched with providerType parameter, Key is False: which provider entity
                // type doesn't match with providerType parameter.
                final Map<Boolean, List<CommoditiesBoughtFromProvider>> providersOfTypeMap =
                    consumer.getCommoditiesBoughtFromProvidersList().stream()
                        .collect(Collectors.partitioningBy(commodityBoughtGroup ->
                            checkIfCommodityBoughtEntityType(commodityBoughtGroup, topologyGraph, providerType)));
                // All Commodity Bought which provider entity type is matched with providerType parameter
                final List<CommoditiesBoughtFromProvider> providersOfType = providersOfTypeMap.get(true);
                // All Commodity Bought which provider entity type not matched with providerType parameter
                final List<CommoditiesBoughtFromProvider> nonProvidersOfType = providersOfTypeMap.get(false);
                // If there is no matched provider type, it means the consumer does't buy any commodity
                // from this provider type. For example, VM1 buying ST1, and VM2 not buying ST at all,
                // If create a policy to Force VM1 and VM2 to buy ST1, it should throw exception, because
                // VM2 doesn't buy any Storage type.
                if (providersOfType.isEmpty()) {
                    throw new PolicyApplicationException("Unable to apply consumer segment when no " +
                        "provider type " + providerType);
                }
                // For each bundle of commodities bought for the entity type that matches the provider type,
                // add the segmentation commodity.
                addCommodityBoughtForProviders(segmentationCommodity, consumer, providersOfType, nonProvidersOfType);
            }
        }
    }

    /**
     * Check if commodity bought has same provider entity type as providerType parameter.
     *
     * @param commodityBoughtGrouping Contains a bundle of commodity bought.
     * @param topologyGraph The graph containing the topology.
     * @param providerType The type of provider that will be providing the segment commodity
     *                     these consumers must be buying.
     * @return boolean type represents if provider entity type matches.
     */
    private boolean checkIfCommodityBoughtEntityType(@Nonnull CommoditiesBoughtFromProvider commodityBoughtGrouping,
                                                     @Nonnull final TopologyGraph topologyGraph,
                                                     final int providerType) {
        // TODO: After we guarantee that commodity type always have provider entity type, we will not
        // need to check topology graph to get provider entity type.
        if (commodityBoughtGrouping.hasProviderEntityType()) {
            return commodityBoughtGrouping.getProviderEntityType() == providerType;
        }
        else {
            return commodityBoughtGrouping.hasProviderId() &&
                isProviderOfType(commodityBoughtGrouping.getProviderId(), topologyGraph, providerType);
        }
    }

    private void addCommodityBoughtForProviders(@Nonnull CommodityBoughtDTO segmentationCommodity,
                                                @Nonnull final TopologyEntityDTO.Builder consumer,
                                                @Nonnull final List<CommoditiesBoughtFromProvider> providersOfType,
                                                @Nonnull final List<CommoditiesBoughtFromProvider> nonProvidersOfType) {
        consumer.clearCommoditiesBoughtFromProviders();
        consumer.addAllCommoditiesBoughtFromProviders(nonProvidersOfType);
        providersOfType.forEach(providerCommodityGrouping -> consumer.addCommoditiesBoughtFromProviders(
            CommoditiesBoughtFromProvider.newBuilder(providerCommodityGrouping)
                .addCommodityBought(segmentationCommodity)
                .build()
        ));
    }

    /**
     * Construct a commodityType suitable for application with this policy.
     * It specifies a SEGMENTATION commodity with the key based on the policy ID.
     *
     * @return A {@link CommodityType} suitable for application with this policy.
     */
    private CommodityType commodityType() {
        return CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION.getNumber())
            .setKey(Long.toString(policyDefinition.getId()))
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
     * @param topologyGraph The topology graph that the policy will be applied to.
     * @return An {@link CommoditySoldDTO} appropriate for the specified provider.
     */
    protected CommoditySoldDTO commoditySold(float capacity,
                                             final long providerId,
                                             final Set<Long> consumerGroupIds,
                                             @Nonnull final TopologyGraph topologyGraph) {
        // Calculate used value.
        final long numConsumersInConsumerGroup = topologyGraph.getConsumers(providerId)
            .map(Vertex::getOid)
            .filter(consumerGroupIds::contains)
            .count();

        return CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setCommodityType(commodityType())
            .setUsed((float)numConsumersInConsumerGroup)
            .build();
    }

    /**
     * Create a {@link CommoditySoldDTO} that all members of the consumer group of this policy should buy.
     * The created commodity has POSITIVE_INFINITY capacity. No used value is provided because the used
     * value is not relevant when the capacity is infinite.
     *
     * @return A {@link CommoditySoldDTO} that all members of the providers group of this policy should buy.
     */
    protected CommoditySoldDTO commoditySold() {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(commodityType())
            .setCapacity(Float.POSITIVE_INFINITY)
            .build();
    }

    /**
     * Create a {@link CommodityBoughtDTO} that all members of the consumers group of this policy should buy.
     *
     * @return A {@link CommodityBoughtDTO} that all members of the consumers group of this policy should buy.
     */
    protected CommodityBoughtDTO commodityBought() {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(commodityType())
            .setUsed(1.0)
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
                                       @Nonnull final TopologyGraph topologyGraph,
                                       final int providerEntityType) {
        return topologyGraph.getVertex(providerId)
            .map(vertex -> vertex.getEntityType() == providerEntityType)
            .orElse(false);
    }
}
