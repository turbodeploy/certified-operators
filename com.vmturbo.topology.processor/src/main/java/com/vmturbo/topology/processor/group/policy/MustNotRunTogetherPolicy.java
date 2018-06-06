package com.vmturbo.topology.processor.group.policy;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Requires that all entities in the consumer group must run in separate providers of the
 * specified type.
 * Common use case: VM->VM anti-affinity, keep VMs in separate hosts (we can also use it to keep VMs
 * in separate storages).
 *
 * Please note that this is different from the AtMostN and AtMostNBound policies. Those last two
 * require you to also limit on which providers the entities should run; and also limit how many
 * of them to run, per provider.
 *
 * Note also that the consumer group might span over multiple clusters (and be used in combination
 * with a cluster merge policy)
 */
public class MustNotRunTogetherPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherPolicy;

    private final PolicyEntities policyEntities;

    /**
     * Create a new {@link MustNotRunTogetherPolicy}, the policy should be of type
     * {@link PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy}.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param policyEntities consumer entities of current policy.
     */
    public MustNotRunTogetherPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                 @Nonnull final PolicyEntities policyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasMustNotRunTogether());
        this.mustNotRunTogetherPolicy = Objects.requireNonNull(policyDefinition.getPolicyInfo().getMustNotRunTogether());
        this.policyEntities = Objects.requireNonNull(policyEntities);
    }

    @Override
    protected void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying MustNotRunTogether policy.");

        // get group of entities that need to not run together (consumers)
        final Group consumerGroup = policyEntities.getGroup();
        Set<Long> additionalEntities = policyEntities.getAdditionalEntities();
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                additionalEntities);

        // if there are no consumers, we don't need to do any changes
        if (!consumers.isEmpty()) {
            // add the commodity sold to all the entities of a particular type.
            addCommoditySoldToSpecificEntityTypeProviders(mustNotRunTogetherPolicy.getProviderEntityType(),
                    ImmutableSet.of(consumerGroup.getId()),
                    topologyGraph,
                    SEGM_CAPACITY_VALUE_SINGLE_CONSUMER);

            // add the commodity bought to the entities that need to run separate
            addCommodityBought(consumers, topologyGraph, mustNotRunTogetherPolicy.getProviderEntityType(),
                    commodityBought());
        }
    }
}
