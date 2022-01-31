package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * Applies a collection of {@link BindToGroupPolicy}s. No bulk optimizations.
 */
public class BindToGroupPolicyApplication extends PlacementPolicyApplication<BindToGroupPolicy> {

    protected BindToGroupPolicyApplication(final GroupResolver groupResolver,
            final TopologyGraph<TopologyEntity> topologyGraph,
            final TopologyInvertedIndexFactory invertedIndexFactory) {
        super(groupResolver, topologyGraph, invertedIndexFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<BindToGroupPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.forEach(policy -> applyPlacePolicy(errors, null, policy));
        return errors;
    }

    /**
     * Apply place (bind to group) policy.
     *
     * @param errors errors that occurred during the application of the policy
     * @param invertedIndex The {@link InvertedIndex} used to find other potential providers
     *         for the consumers. This index needs to be constructed outside this method so that
     *         multiple calls to the methods can use the same index.
     * @param policy the policy that will be applied.
     */
    protected void applyPlacePolicy(Map<PlacementPolicy, PolicyApplicationException> errors,
            InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> invertedIndex,
            BindToGroupPolicy policy) {
        try {
            final PolicyDetailCase policyDetailCase =
                    policy.getPolicyInfo().getPolicyDetailCase();
            logger.debug("Applying {} policy.", policyDetailCase);
            final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
            final Grouping consumerGroup = policy.getConsumerPolicyEntities().getGroup();
            GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
            GroupProtoUtil.checkEntityTypeForPolicy(consumerGroup);
            // Resolve the relevant groups
            final ApiEntityType providerEntityType = GroupProtoUtil.getEntityTypes(providerGroup).iterator().next();
            final ApiEntityType consumerEntityType = GroupProtoUtil.getEntityTypes(consumerGroup).iterator().next();
            final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph).getEntitiesOfType(providerEntityType),
                    policy.getProviderPolicyEntities().getAdditionalEntities());
            final Set<Long> consumers = Sets.union(
                    groupResolver.resolve(consumerGroup, topologyGraph)
                            .getEntitiesOfType(consumerEntityType),
                    policy.getConsumerPolicyEntities().getAdditionalEntities()).stream().filter(
                    id -> topologyGraph.getEntity(id)
                            .filter(e -> !e.hasReservationOrigin())
                            .isPresent()).collect(Collectors.toSet());



            // Add the commodity to the appropriate entities.
            addCommoditySold(providers, commoditySold(policy));
            //checkEntityType logic makes sure that the group only has only one entity type here
            final int providerType = providerEntityType.typeNumber();
            addCommodityBought(consumers, providerType, commodityBought(policy));
            applyExclusivePolicy(invertedIndex, policy, policyDetailCase, consumerEntityType, providers,
                    consumers, providerType);
        } catch (GroupResolutionException e) {
            errors.put(policy, new PolicyApplicationException(e));
        } catch (PolicyApplicationException e2) {
            errors.put(policy, e2);
        }
    }

    /**
     * Apply exclusive place (exclusive bind to group) policy.
     *
     * @param invertedIndex The {@link InvertedIndex} used to find other potential providers
     *         for the consumers. This index needs to be constructed outside this method so that
     *         multiple calls to the methods can use the same index.
     * @param policy the policy that will be applied.
     * @param policyDetailCase policy detail case
     * @param consumerEntityType consumer type
     * @param providers providers
     * @param consumers consumers
     * @param providerType provider type
     * @throws PolicyApplicationException If the consumers cannot be segmented as desired.
     */
    protected void applyExclusivePolicy(
            InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> invertedIndex,
            BindToGroupPolicy policy, PolicyDetailCase policyDetailCase,
            ApiEntityType consumerEntityType, Set<Long> providers, Set<Long> consumers,
            int providerType) throws PolicyApplicationException {
    }
}
