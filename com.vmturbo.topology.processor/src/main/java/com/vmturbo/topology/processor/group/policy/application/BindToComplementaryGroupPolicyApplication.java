package com.vmturbo.topology.processor.group.policy.application;

import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

/**
 * Applies a collection of {@link BindToComplementaryGroupPolicy}s. No bulk optimizations.
 */
public class BindToComplementaryGroupPolicyApplication extends PlacementPolicyApplication {
    protected BindToComplementaryGroupPolicyApplication(final GroupResolver groupResolver,
                                                        final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<PlacementPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.stream()
            .filter(policy -> policy instanceof BindToComplementaryGroupPolicy)
            .map(policy -> (BindToComplementaryGroupPolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying bindToComplementaryGroup policy.");
                    final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
                    final Grouping consumerGroup = policy.getConsumerPolicyEntities().getGroup();
                    GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
                    Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup,
                        topologyGraph), policy.getProviderPolicyEntities().getAdditionalEntities());
                    final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup,
                        topologyGraph), policy.getConsumerPolicyEntities().getAdditionalEntities());
                    //checkEntityType logic makes sure that the group only has only one entity type here
                    final int providerType = GroupProtoUtil.getEntityTypes(providerGroup).iterator().next().typeNumber();
                    // if providers have been replaced, add them to the list of providers so as to skip them
                    Set<Long> replacedProviders = new HashSet<>();
                    providers.forEach(providerId -> topologyGraph.getEntity(providerId)
                        .map(TopologyEntity::getTopologyEntityDtoBuilder)
                        .ifPresent(provider -> {
                            if (provider.hasEdit() && provider.getEdit().hasReplaced()) {
                                replacedProviders.add(provider.getEditBuilder().getReplaced().getReplacementId());
                            }
                        }));

                    // Add the commodity to the appropriate entities
                    addCommoditySoldToComplementaryProviders(Sets.union(providers, replacedProviders),
                            providerType, commoditySold(policy));
                    addCommodityBought(consumers, providerType, commodityBought(policy));
                } catch (GroupResolutionException e) {
                    errors.put(policy, new PolicyApplicationException(e));
                } catch (PolicyApplicationException e2) {
                    errors.put(policy, e2);
                }
            });
        return errors;
    }
}
