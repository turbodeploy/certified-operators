package com.vmturbo.topology.processor.group.policy;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Requires that all entities in the consumer group must run together on a single provider in
 * the provider group.
 * Common use case: VM->VM affinity.
 */
public class MustRunTogetherPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogetherPolicy;

    private final PolicyEntities consumerPolicyEntities;
    private final PolicyEntities providerPolicyEntities;

    private static final Set<Integer> HOST_AND_STORAGE_TYPES = ImmutableSet.of(
            EntityType.PHYSICAL_MACHINE_VALUE, EntityType.STORAGE_VALUE);

    /**
     * Create a new MustRunTogetherPolicy, the policy should be of type MustRunTogether.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param consumerPolicyEntities consumer entities of current policy.
     * @param providerPolicyEntities provider entities of current policy.
     */
    public MustRunTogetherPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                 @Nonnull final PolicyEntities consumerPolicyEntities,
                                 @Nonnull final PolicyEntities providerPolicyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasMustRunTogether());
        this.mustRunTogetherPolicy = Objects.requireNonNull(policyDefinition.getMustRunTogether());
        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
    }

    /**
     * Constrain entities in the consumer group to be forced to reside on only one entity (we choose
     * the entity which has max number of consumers on it to avoid disruptive ping-ponging of
     * consumers in customer's environment, if there are two providers sell same number of consumers,
     * use it's id to break tie) in the providers group by creating a segmentation commodity bought
     * by the consumers and sold ONLY by that one entity.
     *
     * {@inheritDoc}
     */
    @Override
    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying mustRunTogether policy.");
        final Group providerGroup = providerPolicyEntities.getGroup();
        final Group consumerGroup = consumerPolicyEntities.getGroup();
        final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph),
                providerPolicyEntities.getAdditionalEntities());
        /* We do filtering by entity types because VC cluster groups may contain Virtual
           Datacenter as member. It is used by UI but it shouldn't be for DRS groups.
           As it comes from probe we may want to change probe itself in the future.
         */
        final Predicate<Long> isHostOrStorage = id -> topologyGraph.getEntity(id).isPresent()
                && HOST_AND_STORAGE_TYPES.contains(topologyGraph.getEntity(id).get().getEntityType());
        final Set<Long> onlyHostsOrStoragesProviders = providers.stream()
                .filter(isHostOrStorage)
                .collect(Collectors.toSet());
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                consumerPolicyEntities.getAdditionalEntities());

        final int providerType = GroupProtoUtil.getEntityType(providerGroup);

        addCommoditySold(onlyHostsOrStoragesProviders, consumers, topologyGraph);
        addCommodityBought(consumers, topologyGraph, providerType, commodityBought());
    }

    /**
     * Add commoditySold to provider which contains max number of consumers in group, if there are two
     * providers sell same number of consumers, use it's id to break tie.
     *
     * @param providers The providers that belong to the segment.
     * @param consumers The consumers that belong to the segment.
     * @param topologyGraph The graph containing the topology.
     */
    private void addCommoditySold(@Nonnull final Set<Long> providers,
                                  @Nonnull final Set<Long> consumers,
                                  @Nonnull final TopologyGraph topologyGraph) {
        final Map<Long, Long> providerMatchCountMap = providers.stream()
            .collect(Collectors.toMap(Function.identity(),
                provider -> topologyGraph.getConsumers(provider)
                    .map(TopologyEntity::getOid)
                    .filter(consumers::contains)
                    .count())
            );

        Comparator<Long> providerCompare = Comparator.comparingLong(providerMatchCountMap::get);
        providers.stream()
            .max(providerCompare.thenComparing(Long::compare))
            .flatMap(topologyGraph::getEntity)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> provider.addCommoditySoldList(commoditySold()));
    }
}
