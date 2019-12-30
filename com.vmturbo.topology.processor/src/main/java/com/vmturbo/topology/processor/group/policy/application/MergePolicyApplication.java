package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;

/**
 * Applies a collection of {@link MergePolicy} policies.
 */
public class MergePolicyApplication extends PlacementPolicyApplication {

    /**
     * Constructor.
     *
     * @param groupResolver the {@link GroupResolver}
     * @param topologyGraph the {@link TopologyGraph}
     */
    public MergePolicyApplication(@Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<PlacementPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.stream().filter(MergePolicy.class::isInstance).forEach(p -> {
            try {
                apply((MergePolicy)p);
            } catch (GroupResolutionException e) {
                errors.put(p, new PolicyApplicationException(e));
            }
        });
        return errors;
    }

    private void apply(@Nonnull final MergePolicy policy) {
        final long policyId = policy.getPolicyDefinition().getId();
        logger.debug("Applying merge policy {}.", policyId);

        final MergeTypeDefinition mergeTypeDefinition =
                MergeTypeDefinition.valueOf(policy.getDetails().getMergeType());

        final List<Grouping> groups = policy.getMergePolicyEntitiesList()
                .stream()
                .map(PolicyEntities::getGroup)
                .collect(Collectors.toList());

        final int providerEntityTypeNumber =
                mergeTypeDefinition.getProviderEntityType().getNumber();
        final Set<TopologyEntity> providers = resolveRelevantGroups(groups).stream()
                .map(topologyGraph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(entity -> entity.getEntityType() == providerEntityTypeNumber)
                .collect(Collectors.toSet());

        // Iterate through all providers and their consumers
        // and change the key for bought and sold commodities to OID policy.
        if (!providers.isEmpty()) {
            final String commodityKey = Long.toString(policyId);
            changeCommodityKeyForSoldCommodities(providers,
                    mergeTypeDefinition.getAssociatedCommodityType(), commodityKey);

            final int consumerEntityTypeNumber =
                    mergeTypeDefinition.getConsumerEntityType().getNumber();
            final Set<TopologyEntity> consumers = providers.stream()
                    .flatMap(entity -> entity.getConsumers().stream())
                    .filter(entity -> entity.getEntityType() == consumerEntityTypeNumber)
                    .collect(Collectors.toSet());
            changeCommodityKeyForBoughtCommodities(consumers, providers,
                    mergeTypeDefinition.getAssociatedCommodityType(), commodityKey);
        }
    }

    /**
     * Iterate through all providers and change the key of the sold commodity
     * with {@code commodityType} to the {@code newCommodityKey}.
     * Otherwise, if commodity does not exist, create and add it to the list of sold commodities
     * provider.
     *
     * @param commodityType the {@link CommodityType}
     * @param newCommodityKey the new key of commodity
     * @param providers providers
     */
    private void changeCommodityKeyForSoldCommodities(@Nonnull final Set<TopologyEntity> providers,
            @Nonnull final CommodityType commodityType, @Nonnull final String newCommodityKey) {
        for (TopologyEntity provider : providers) {
            final Optional<Builder> commodityToModify = provider.getTopologyEntityDtoBuilder()
                    .getCommoditySoldListBuilderList()
                    .stream()
                    .filter(commodity -> isCommodityTypeEligibleForMerge(
                            commodity.getCommodityType(), commodityType))
                    .findFirst();

            if (commodityToModify.isPresent()) {
                commodityToModify.get().getCommodityTypeBuilder().setKey(newCommodityKey);
            } else {
                final CommoditySoldDTO newSoldCommodity = CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setKey(newCommodityKey)
                                .setType(commodityType.getNumber()))
                        .build();
                recordCommodityAddition(commodityType.getNumber());
                provider.getTopologyEntityDtoBuilder().addCommoditySoldList(newSoldCommodity);
            }
        }
    }

    /**
     * Iterate through all consumers and change the key of the bought commodity
     * with {@code commodityType} to the {@code newCommodityKey}.
     * Otherwise, if commodity does not exist, create and add it to the list of bought commodities
     * сonsumer.
     *
     * @param providers providers
     * @param consumers consumers
     * @param commodityType the {@link CommodityType}
     * @param newCommodityKey the key of commodity
     */
    private void changeCommodityKeyForBoughtCommodities(
            @Nonnull final Set<TopologyEntity> consumers,
            @Nonnull final Set<TopologyEntity> providers,
            @Nonnull final CommodityType commodityType, @Nonnull final String newCommodityKey) {
        for (TopologyEntity consumer : consumers) {
            for (CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider : consumer.getTopologyEntityDtoBuilder()
                    .getCommoditiesBoughtFromProvidersBuilderList()) {
                // The condition is to make sure that the provider id for that bought list
                // is included in the original set of entities that we want to merge.
                // For example: a vm can have multiple storages, and only one of those
                // might be part of the cluster to merge, but not the second one. (so changing the key for the 2nd is wrong).
                if (commoditiesBoughtFromProvider.hasProviderId() && providers.stream()
                        .anyMatch(
                                p -> p.getOid() == commoditiesBoughtFromProvider.getProviderId())) {
                    final Optional<CommodityBoughtDTO.Builder> commodityToModify =
                            commoditiesBoughtFromProvider.getCommodityBoughtBuilderList()
                                    .stream()
                                    .filter(commodity -> isCommodityTypeEligibleForMerge(
                                            commodity.getCommodityType(), commodityType))
                                    .findFirst();

                    if (commodityToModify.isPresent()) {
                        commodityToModify.get().getCommodityTypeBuilder().setKey(newCommodityKey);
                    } else {
                        final CommodityBoughtDTO newBoughtCommodity =
                                CommodityBoughtDTO.newBuilder()
                                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                                .setKey(newCommodityKey)
                                                .setType(commodityType.getNumber()))
                                        .build();
                        recordCommodityAddition(commodityType.getNumber());
                        commoditiesBoughtFromProvider.addCommodityBought(newBoughtCommodity);
                    }
                }
            }
        }
    }

    /**
     * Is commodityType eligible for merge? It is eligible for merge if the commodity type is the
     * same as the commodity type associated with the policy. For a storage cluster commodity type,
     * it is eligible for merge only if it is a real storage cluster commodity.
     *
     * @param commodityType the commodity type to check
     * @param associatedCommodityTypeWithPolicy the commodity type associated with the policy
     * @return true if commodityType is eligible for merge
     */
    private static boolean isCommodityTypeEligibleForMerge(
            @Nonnull TopologyDTO.CommodityType commodityType,
            @Nonnull CommodityType associatedCommodityTypeWithPolicy) {
        return commodityType.getType() == associatedCommodityTypeWithPolicy.getNumber() &&
                (commodityType.getType() != CommodityType.STORAGE_CLUSTER_VALUE ||
                        TopologyDTOUtil.isRealStorageClusterCommodityKey(commodityType.getKey()));
    }

    /**
     * Resolve the relevant groups and return OIDs.
     *
     * @param groups the group referenced by policy
     * @return set of OIDs
     * @throws GroupResolutionException if group resolution failed
     */
    private Set<Long> resolveRelevantGroups(@Nonnull final List<Grouping> groups)
            throws GroupResolutionException {
        final Set<Long> oids = new HashSet<>();
        for (Grouping group : groups) {
            final Set<Long> members = groupResolver.resolve(group, topologyGraph);
            final boolean groupContainsDatacenters = group.getExpectedTypesList()
                    .stream()
                    .filter(MemberType::hasEntity)
                    .map(MemberType::getEntity)
                    .anyMatch(t -> t == EntityType.DATACENTER_VALUE);
            if (groupContainsDatacenters) {
                oids.addAll(getPhysicalMachinesConsumeOnDatacenters(members));
            } else {
                oids.addAll(members);
            }
        }
        return oids;
    }

    /**
     * If merge policy is data center, then the OID list will contains the data center OIDs,
     * we need to convert them into all the physical machine OIDs which belong to the data centers.
     *
     * @param datacenters the data center OIDs.
     * @return the physical machine OIDs that consume on the data centers.
     */
    private Set<Long> getPhysicalMachinesConsumeOnDatacenters(@Nonnull Set<Long> datacenters) {
        return datacenters.stream()
                .flatMap(topologyGraph::getConsumers)
                .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Definition includes provider {@link EntityType}, consumer {@link EntityType},
     * associated {@link CommodityType} for each {@link MergeType}.
     */
    private enum MergeTypeDefinition {
        /**
         * Definition for {@link MergeType#CLUSTER}.
         */
        CLUSTER(MergeType.CLUSTER, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                CommodityType.CLUSTER),

        /**
         * Definition for {@link MergeType#DATACENTER}.
         */
        DATACENTER(MergeType.DATACENTER, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                CommodityType.DATACENTER),

        /**
         * Definition for {@link MergeType#STORAGE_CLUSTER}.
         */
        STORAGE_CLUSTER(MergeType.STORAGE_CLUSTER, EntityType.STORAGE, EntityType.VIRTUAL_MACHINE,
                CommodityType.STORAGE_CLUSTER),

        /**
         * Definition for {@link MergeType#DESKTOP_POOL}.
         */
        DESKTOP_POOL(MergeType.DESKTOP_POOL, EntityType.DESKTOP_POOL, EntityType.BUSINESS_USER,
                CommodityType.ACTIVE_SESSIONS);

        private static final Map<MergeType, MergeTypeDefinition> LOOKUP =
                Stream.of(values()).collect(Collectors.toMap(d -> d.mergeType, d -> d));

        private final MergeType mergeType;
        private final EntityType providerEntityType;
        private final EntityType consumerEntityType;
        private final CommodityType associatedCommodityType;

        /**
         * Constructor.
         *
         * @param mergeType the {@link MergeType}
         * @param providerEntityType provider {@link EntityType}
         * @param consumerEntityType consumer {@link EntityType}
         * @param associatedCommodityType associated {@link CommodityType}
         */
        MergeTypeDefinition(@Nonnull MergeType mergeType, @Nonnull EntityType providerEntityType,
                @Nonnull EntityType consumerEntityType,
                @Nonnull CommodityType associatedCommodityType) {
            this.mergeType = mergeType;
            this.providerEntityType = providerEntityType;
            this.consumerEntityType = consumerEntityType;
            this.associatedCommodityType = associatedCommodityType;
        }

        /**
         * Returns provider {@link EntityType}.
         *
         * @return provider {@link EntityType}
         */
        @Nonnull
        public final EntityType getProviderEntityType() {
            return providerEntityType;
        }

        /**
         * Returns consumer {@link EntityType}.
         *
         * @return consumer {@link EntityType}
         */
        @Nonnull
        public final EntityType getConsumerEntityType() {
            return consumerEntityType;
        }

        /**
         * Returns associated {@link CommodityType} with {@link MergeType}.
         *
         * @return associated {@link CommodityType}
         */
        @Nonnull
        public final CommodityType getAssociatedCommodityType() {
            return associatedCommodityType;
        }

        /**
         * Returns {@link MergeTypeDefinition} by {@link MergeType}.
         *
         * @param mergeType the {@link MergeType}
         * @return the {@link MergeTypeDefinition}
         * @throws InvalidMergePolicyTypeException if invalid {@link MergeType} passed
         */
        @Nonnull
        public static MergeTypeDefinition valueOf(@Nonnull MergeType mergeType) {
            final MergeTypeDefinition mergeTypeDefinition = LOOKUP.get(mergeType);
            if (mergeTypeDefinition != null) {
                return mergeTypeDefinition;
            } else {
                throw new InvalidMergePolicyTypeException(
                        String.format("Invalid merge policy type: %s", mergeType));
            }
        }
    }
}
