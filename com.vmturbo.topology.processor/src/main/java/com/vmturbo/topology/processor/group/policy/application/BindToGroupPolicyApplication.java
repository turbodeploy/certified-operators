package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Applies a collection of {@link BindToGroupPolicy}s. No bulk optimizations.
 */
public class BindToGroupPolicyApplication extends PlacementPolicyApplication {
    protected BindToGroupPolicyApplication(final GroupResolver groupResolver,
                                           final TopologyGraph topologyGraph) {
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
            .filter(policy -> policy instanceof BindToGroupPolicy)
            .map(policy -> (BindToGroupPolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying bindToGroup policy.");
                    final Group providerGroup = policy.getProviderPolicyEntities().getGroup();
                    final Group consumerGroup = policy.getConsumerPolicyEntities().getGroup();
                    // Resolve the relevant groups
                    final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph),
                        policy.getProviderPolicyEntities().getAdditionalEntities());
                    final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                        policy.getConsumerPolicyEntities().getAdditionalEntities());

                    // Add the commodity to the appropriate entities.
                    addCommoditySold(providers, commoditySold(policy));
                    addCommodityBought(consumers, GroupProtoUtil.getEntityType(providerGroup),
                        commodityBought(policy));
                } catch (GroupResolutionException e) {
                    errors.put(policy, new PolicyApplicationException(e));
                } catch (PolicyApplicationException e2) {
                    errors.put(policy, e2);
                }
            });
        return errors;
    }
}
