package com.vmturbo.topology.processor.group.policy.application;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * The {@link MustNotRunTogetherPolicy} specifies a consumer group and a provider type.
 * To apply the {@link MustNotRunTogetherPolicy} we need to add segmentation commodities
 * to all potential providers of the specified type for the consumers in the group.
 * <p>
 * In the general case, this requires adding segmentation commodities to all providers of the
 * specified type in the topology (since a consumer may potentially move to any provider). This
 * can be very expensive with a large number of policies. Luckily, we can limit the list of potential
 * providers in some common cases.
 * <p>
 * Most notably, if the consumers in the policy's group buy cluster or datacenter commodities from
 * providers of the policy's provider type, the market will only consider other providers that
 * sell matching cluster or datacenter commodities for moves. In this case we only need to add
 * segmentation commodities to those providers, which is a big win.
 * <p>
 * For example, suppose the consumer group is a group of 3 VMs, the provider type is host, and the
 * topology looks like this:
 *
 *               VM1   VM2
 *                |     |
 *   PM0  PM1    PM3   PM4  PM5
 *     |  /       \    /  /
 *   CLSTR1       CLSTR2
 *
 * PM0 and PM1 are selling the CLSTR1 commodity (a commodity with type CLUSTER and key CLSTR1).
 * PM3, PM4, and PM5 are selling the CLSTR2 commodity.
 * VM1 will be buying CLSTR2 from PM3.
 * VM2 will be buying CLSTR2 from PM4.
 *
 * To apply the MustNotRunTogether policy, we only need to add segmentation commodities to
 * PM3, PM4, and PM5. VM1 and VM2 will not move to PM0 or PM1 because of cluster boundaries.
 * (note - there may be a Merge policy for CLSTR1 and CLSTR2. The {@link MergePolicyApplication}
 *         applies the merge policy by modifying the CLUSTER commodity commodity keys, so the logic
 *         of checking commodity keys to find eligible providers will still work as long as we can
 *         rely on {@link PolicyApplicator} to apply
 *         the merge policies before the must not run together policies).
 */
public class MustNotRunTogetherPolicyApplication extends PlacementPolicyApplication<MustNotRunTogetherPolicy> {

    /**
     * These provider types sell either cluster or datacenter commodities, which limits
     * the potential providers for the consumers targeted by a policy.
     * See the {@link MustNotRunTogetherPolicyApplication} documentation.
     */
    private static final Set<Integer> OPTIMIZED_PROVIDER_TYPES = ImmutableSet.of(
        EntityType.PHYSICAL_MACHINE_VALUE,
        EntityType.DATACENTER_VALUE,
        EntityType.STORAGE_VALUE);

    /**
     * This is the capacity value that a segmentation commodity sold by a provider should have,
     * if it want to accommodate only a single provider
     */
    public static final float SEGM_CAPACITY_VALUE_SINGLE_CONSUMER =
        SEGM_BOUGHT_USED_VALUE + SMALL_DELTA_VALUE;

    protected MustNotRunTogetherPolicyApplication(final GroupResolver groupResolver,
            final TopologyGraph<TopologyEntity> topologyGraph,
            final TopologyInvertedIndexFactory invertedIndexFactory) {
        super(groupResolver, topologyGraph, invertedIndexFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<MustNotRunTogetherPolicy> policies) {
        logger.debug("Running {} on {} policies", getClass().getSimpleName(), policies.size());
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        // Arrange the policies by the provider type.
        final Map<Integer, List<MustNotRunTogetherPolicy>> policiesByProviderType =
            policies.stream()
                .collect(Collectors.groupingBy(policy -> policy.getDetails().getProviderEntityType()));

        policiesByProviderType.forEach((providerType, mustNotRunTogether) -> {
            logger.debug("Applying {} must-not-run-together policies for provider type {}",
                mustNotRunTogether.size(), EntityType.forNumber(providerType));
            errors.putAll(applyPoliciesOfProviderType(mustNotRunTogether, providerType));
        });
        return errors;
    }

    /**
     * This applies the policy in the optimized way outlined in the javadoc for
     * {@link MustNotRunTogetherPolicyApplication}.
     *
     * @param policies The policies to apply.
     * @param providerType The provider type.
     * @return A map from policy to error if any policies in the input encountered errors.
     *         An empty map otherwise.
     */
    @Nonnull
    private Map<PlacementPolicy, PolicyApplicationException> applyPoliciesOfProviderType(
            @Nonnull final List<MustNotRunTogetherPolicy> policies,
            final int providerType) {

        // We can decide whether or not to look for a cluster commodity based on the provider type.
        // We always look for the datacenter commodity as a fallback.
        final Optional<CommodityType> optClusterComm;
        if (providerType == EntityType.PHYSICAL_MACHINE_VALUE) {
            optClusterComm = Optional.of(CommodityType.CLUSTER);
        } else if (providerType == EntityType.STORAGE_VALUE) {
            optClusterComm = Optional.of(CommodityType.STORAGE_CLUSTER);
        } else {
            optClusterComm = Optional.empty();
        }

        // Index the providers by cluster key and datacenter key.
        final Map<String, List<TopologyEntity>> providersSellingClusterKey = new HashMap<>();
        final Map<String, List<TopologyEntity>> providersSellingDCKey = new HashMap<>();
        topologyGraph.entitiesOfType(providerType)
            .forEach(provider -> {
                // If we aren't looking for a cluster commodity, pretent clusterFound is true.
                boolean clusterFound = !optClusterComm.isPresent();
                boolean dcFound = false;
                for (CommoditySoldDTO commSold : provider.getTopologyEntityDtoBuilder().getCommoditySoldListList()) {
                    final int commType = commSold.getCommodityType().getType();
                    if (!clusterFound) {
                        if (optClusterComm.get().getNumber() == commType) {
                            providersSellingClusterKey.computeIfAbsent(commSold.getCommodityType().getKey(),
                                k -> new ArrayList<>()).add(provider);
                            clusterFound = true;
                        }
                    }

                    if (!dcFound && commType == CommodityType.DATACENTER_VALUE) {
                        providersSellingDCKey.computeIfAbsent(commSold.getCommodityType().getKey(),
                            k -> new ArrayList<>()).add(provider);
                        dcFound = true;
                    }

                    // Optimization to avoid iterating through the remainder of the
                    if (dcFound && clusterFound) {
                        break;
                    }
                }
            });
        logger.debug("Arranged providers by {} cluster keys, {} DC keys.",
            providersSellingClusterKey.size(), providersSellingDCKey.size());

        final Map<PlacementPolicy, PolicyApplicationException> errs = new HashMap<>();
        policies.forEach(policy -> {
            logger.debug("Applying MUST NOT RUN TOGETHER policy {}",
                policy.getPolicyDefinition().getId());
            final Map<String, Set<Long>> consumersByClusterKey = new HashMap<>();
            final Map<String, Set<Long>> consumersWithoutClusterByDCKey = new HashMap<>();
            final Set<Long> globalConsumers = new HashSet<>();
            resolveConsumers(policy, errs).stream()
                .map(topologyGraph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(entity -> {
                    final Optional<String> clusterBoughtKey = optClusterComm.flatMap(clusterComm ->
                        getCommodityBoughtKey(clusterComm, providerType, entity));
                    if (clusterBoughtKey.isPresent()) {
                        logger.trace("Entity {} buying {} commodity with key {}",
                            entity.getOid(), optClusterComm.get(), clusterBoughtKey.get());
                        consumersByClusterKey.computeIfAbsent(clusterBoughtKey.get(),
                            k -> new HashSet<>()).add(entity.getOid());
                    } else {
                        final Optional<String> dcBoughtKey = getCommodityBoughtKey(CommodityType.DATACENTER,
                                providerType, entity);
                        if (dcBoughtKey.isPresent()) {
                            logger.trace("Entity {} buying DC commodity with key {}",
                                entity.getOid(), dcBoughtKey.get());
                            consumersWithoutClusterByDCKey.computeIfAbsent(dcBoughtKey.get(),
                                k -> new HashSet<>()).add(entity.getOid());
                        } else {
                            logger.trace("Entity {} not buying cluster or DC commodity from provider.",
                                entity.getOid());
                            globalConsumers.add(entity.getOid());
                        }
                    }
                });

            // For each group of consumers that share a cluster key, add the segmentation
            // commodities to all providers that have the same key.
            consumersByClusterKey.forEach((clusterKey, consumers) -> {
                Metrics.APPLICATION_TYPE.labels(Metrics.CLUSTER_TYPE).increment();
                final List<TopologyEntity> providers = providersSellingClusterKey.get(clusterKey);
                if (providers == null) {
                    // This implies some inconsistency in the topology.
                    errs.put(policy,
                        new PolicyApplicationException("No providers selling bought cluster key " +
                            clusterKey));
                } else {
                    logger.debug("Adding commodities for {} consumers and {} providers.",
                        consumers.size(), providers.size());
                    addSegmentationCommodities(consumers, providers.stream(), policy, errs);
                }
            });

            // For each group of consumers that have no cluster key but share a DC key, add the
            // segmentation commodities to all providers that have the same DC key.
            consumersWithoutClusterByDCKey.forEach((dcKey, consumers) -> {
                Metrics.APPLICATION_TYPE.labels(Metrics.DC_TYPE).increment();
                final List<TopologyEntity> providers = providersSellingDCKey.get(dcKey);
                if (providers == null) {
                    // This implies some inconsistency in the topology.
                    errs.put(policy,
                        new PolicyApplicationException("No providers selling bought DC key " +
                            dcKey));
                } else {
                    logger.debug("Adding commodities for {} consumers and {} providers.",
                        consumers.size(), providers.size());
                    addSegmentationCommodities(consumers, providers.stream(), policy, errs);
                }
            });

            // For all remaining consumers, add the segmentation commodities to all providers
            // in the topology.
            if (!globalConsumers.isEmpty()) {
                Metrics.APPLICATION_TYPE.labels(Metrics.GLOBAL_TYPE).increment();
                addSegmentationCommodities(globalConsumers,
                    topologyGraph.entitiesOfType(providerType),
                    policy,
                    errs);
            }
        });
        return errs;
    }

    /**
     * Get the key of the first commodity bought from a specified provider type by a specific entity.
     *
     * @param commType The type of commodity bought to look for.
     * @param providerType The provider type.
     * @param entity The entity doing the buying.
     * @return An optional containing the key of the first commType bought by entity from providerType.
     */
    @Nonnull
    private Optional<String> getCommodityBoughtKey(@Nonnull final CommodityType commType,
                                                   final int providerType,
                                                   @Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
            .filter(commBought -> commBought.getProviderEntityType() == providerType)
            .map(commBoughtFromProvider -> {
                for (CommodityBoughtDTO commBought : commBoughtFromProvider.getCommodityBoughtList()) {
                    if (commBought.getCommodityType().getType() == commType.getNumber()) {
                        return commBought.getCommodityType().getKey();
                    }
                }
                return null;
            })
            .filter(Objects::nonNull)
            .findFirst();
    }

    @Nonnull
    private Set<Long> resolveConsumers(@Nonnull final MustNotRunTogetherPolicy policy,
                                       @Nonnull final Map<PlacementPolicy, PolicyApplicationException> errs) {
        // get group of entities that need to not run together (consumers)
        final Grouping consumerGroup = policy.getPolicyEntities().getGroup();
        GroupProtoUtil.checkEntityTypeForPolicy(consumerGroup);
        // Resolve the relevant groups
        final ApiEntityType consumerEntityType = GroupProtoUtil.getEntityTypes(consumerGroup).iterator().next();
        Set<Long> additionalEntities = policy.getPolicyEntities().getAdditionalEntities();
        try {
            return Sets.union(groupResolver.resolve(consumerGroup, topologyGraph).getEntitiesOfType(consumerEntityType),
                additionalEntities);
        } catch (GroupResolutionException e) {
            errs.put(policy, new PolicyApplicationException(e));
            return Collections.emptySet();
        }
    }

    private void addSegmentationCommodities(@Nonnull final Set<Long> consumers,
                                            @Nonnull final Stream<TopologyEntity> providers,
                                            @Nonnull final MustNotRunTogetherPolicy policy,
                                            @Nonnull final Map<PlacementPolicy, PolicyApplicationException> errs) {
        if (consumers.isEmpty()) {
            return;
        }

        providers.map(TopologyEntity::getTopologyEntityDtoBuilder)
            .forEach(provider -> {
                final CommoditySoldDTO segmentationCommodity = commoditySold(SEGM_CAPACITY_VALUE_SINGLE_CONSUMER,
                    provider.getOid(), consumers, policy);
                addCommoditySold(Collections.singleton(provider.getOid()), segmentationCommodity);
            });

        try {
            addCommodityBought(consumers, policy.getDetails().getProviderEntityType(),
                commodityBought(policy));
        } catch (PolicyApplicationException e) {
            errs.put(policy, e);
        }
    }

    private static class Metrics {

        static final String CLUSTER_TYPE = "cluster";
        static final String DC_TYPE = "dc";
        static final String GLOBAL_TYPE = "global";

        static final DataMetricCounter APPLICATION_TYPE = DataMetricCounter.builder()
            .withName("tp_must_not_run_together_application_type_cont")
            .withHelp("The type of implementation used to apply must not run together policies.")
            .withLabelNames("type")
            .build()
            .register();

    }
}
