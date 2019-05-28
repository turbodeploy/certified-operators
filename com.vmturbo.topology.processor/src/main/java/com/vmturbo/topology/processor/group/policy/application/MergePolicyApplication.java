package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;

/**
 * Applies a collection of {@link MergePolicy}s. No bulk optimizations.
 */
public class MergePolicyApplication extends PlacementPolicyApplication {
    protected MergePolicyApplication(final GroupResolver groupResolver, final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<PlacementPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.stream()
            .filter(policy -> policy instanceof MergePolicy)
            .map(policy -> (MergePolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying mergePolicy policy.");
                    List<Group> groups = policy.getMergePolicyEntitiesList()
                        .stream()
                        .map(PolicyEntities::getGroup)
                        .collect(Collectors.toList());

                    // Resolve the relevant groups and return List of OIDs.
                    List<Long> oidList = getListOfOids(groupResolver, topologyGraph, groups);

                    applyClusterPolicy(policy, oidList);
                } catch (GroupResolutionException e) {
                    errors.put(policy, new PolicyApplicationException(e));
                }
            });
        return errors;
    }

    /**
     * Going through all the PMs (or Storage) and their attached VMs in the clusters and
     * change the key of the cluster commodity to the policy OID.
     * It applies to both computation or storage cluster.
     *
     * @param policy {@link MergePolicy} we're applying.
     * @param oidList list of OIDs from the relevant groups
     */
    private void applyClusterPolicy(MergePolicy policy,
                                    @Nonnull final List<Long> oidList) {
        final Set<TopologyEntity> entitySet = oidList.stream()
            .map(topologyGraph::getEntity)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(entity -> entity.getEntityType() == getCurrentEntityType(policy))
            .collect(Collectors.toSet());

        // PM (or Storage)
        changeClusterKey(policy, entitySet);

        // change the key of the cluster commodity for all VMs that attached to the PMs (or Storage) in the clusters
        final Set<TopologyEntity> vmSet = entitySet
            .stream()
            .flatMap(entity -> entity.getConsumers().stream())
            .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .collect(Collectors.toSet());
        changeClusterKeyForVM(policy, vmSet, entitySet);
    }

    /**
     * Going through all the PMs (or Storage) and change the key of the 'sold' cluster commodity
     * to the policy OID.
     *
     * @param policy The {@link MergePolicy} we're applying.
     * @param entitySet Set with either PM (or Storage) OIDs
     */

    private void changeClusterKey(final MergePolicy policy,
                                  @Nonnull final Set<TopologyEntity> entitySet) {
        entitySet.forEach(topologyEntity -> {
            // get the commodity sold list
            final List<CommoditySoldDTO.Builder> commoditySoldList = topologyEntity
                .getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList();

            final Optional<Builder> commodityToModify = commoditySoldList.stream()
                .filter(commodity -> commodity.getCommodityType().getType() == getCommodityType(policy))
                .findFirst();

            final String key = commKey(policy);
            if (commodityToModify.isPresent()) {
                commodityToModify.get().getCommodityTypeBuilder()
                    .setKey(key);
            } else {
                final CommoditySoldDTO soldComm = CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO
                        .CommodityType.newBuilder()
                        .setKey(key) /* set the key to merge policy id*/
                        .setType(getCommodityType(policy)))
                    .build();

                recordCommodityAddition(soldComm.getCommodityType().getType());
                topologyEntity.getTopologyEntityDtoBuilder()
                    .addCommoditySoldList(soldComm);
            }
        });
    }

    @Nonnull
    private String commKey(@Nonnull final MergePolicy policy) {
        return Long.toString(policy.getPolicyDefinition().getId());
    }

    /**
     * Going through all the attached VMs (of PMs or Storage) in the clusters and change the key of
     * the 'bought' cluster commodity to the policy OID.
     * <p>
     * Summary:
     * VMid -> TopologyEntity -> commoditiesBoughtFromProvidersList
     * for (provider in List) {
     * get commodityBoughtDTOList
     * set CLUSTER commodity bought
     * }
     *
     * @param policy {@link MergePolicy} we're applying.
     * @param hostSet   The set of hosts, could be PMs or Storages
     * @param vmSet     Set with VM OIDs
     */
    private void changeClusterKeyForVM(final MergePolicy policy,
                                       @Nonnull final Set<TopologyEntity> vmSet,
                                       @Nonnull final Set<TopologyEntity> hostSet) {
        vmSet.forEach(topologyEntity -> {
            // get the commodity bought provider list
            List<CommoditiesBoughtFromProvider.Builder> boughtFromProviderBuilderList = topologyEntity
                .getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersBuilderList();
            boughtFromProviderBuilderList.forEach(commoditiesBoughtFromProvider -> {
                // the condition is to make sure that the providerID for that bought list is included in
                // the original set of entities that we want to merge.
                // This is because a vm can have multiple storages, and only one of those might be part
                // of the cluster to merge, but not the second one. (so changing the key for the 2nd is wrong).
                if (commoditiesBoughtFromProvider.hasProviderId() &&
                    hostSet.stream()
                        .anyMatch(host -> host.getOid() == commoditiesBoughtFromProvider.getProviderId())) {
                    final List<CommodityBoughtDTO.Builder> commodityBoughtDTOBuilderList = commoditiesBoughtFromProvider
                        .getCommodityBoughtBuilderList();
                    final Optional<CommodityBoughtDTO.Builder> commodityToModify = commodityBoughtDTOBuilderList
                        .stream()
                        .filter(commodity -> commodity.getCommodityType().getType() == getCommodityType(policy))
                        .findFirst();

                    final String key = commKey(policy);
                    if (commodityToModify.isPresent()) {
                        commodityToModify.get().getCommodityTypeBuilder()
                            .setKey(key);
                    } else {
                        final CommodityBoughtDTO commBought = CommodityBoughtDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setKey(key)
                                .setType(getCommodityType(policy)))
                            .build();
                        recordCommodityAddition(commBought.getCommodityType().getType());
                        commoditiesBoughtFromProvider.addCommodityBought(commBought);
                    }
                }
            });
        });
    }


    /**
     * Get list of OIDs.
     *
     * @param groupResolver the group resolver to be used in resolving the groups
     *                      to which the policy applies.
     * @param topologyGraph the {@link TopologyGraph<TopologyEntity>} to which the policy should be applied.
     * @param groups        the group referenced by Policy.
     * @return list of OIDs.
     * @throws GroupResolutionException if group resolution failed.
     */
    private List<Long> getListOfOids(@Nonnull final GroupResolver groupResolver,
                                     @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                     @Nonnull final List<Group> groups) throws GroupResolutionException {
        // Not using lambda here, for loop is easier to throw GroupResolutionException to caller
        List<Set<Long>> listOfOids = Lists.newArrayList();
        for (Group group : groups) {
            Set<Long> entityOids = groupResolver.resolve(group, topologyGraph);
            // If the group is data center group, then retrieve the physical machine OIDs by data
            // center OIDs.
            if (group.getGroup().getEntityType() == EntityType.DATACENTER_VALUE) {
                entityOids = getConsumedPMOids(entityOids, topologyGraph);
            }
            listOfOids.add(entityOids);
        }
        return listOfOids
            .stream()
            .flatMap(ids -> ids.stream())
            .collect(Collectors.toList());
    }

    /**
     * If merge policy is data center, then the OID list will contains the data center OIDs,
     * we need to convert them into all the physical machine OIDs which belong to the data centers.
     *
     * @param dataCenterOids The data center OIDs set.
     * @param topologyGraph the {@link TopologyGraph<TopologyEntity>} to which the policy should be applied.
     * @return The physical machine OIDs that consume on the data centers.
     */
    private Set<Long> getConsumedPMOids(@Nonnull Set<Long> dataCenterOids,
                                        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        return dataCenterOids.stream()
            .flatMap(topologyGraph::getConsumers)
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .map(TopologyEntity::getOid)
            .collect(Collectors.toSet());
    }

    /**
     * Get entity type based on current merge policy type.
     *
     * @return entity type
     */
    private int getCurrentEntityType(MergePolicy policy) {
        final PolicyDTO.PolicyInfo.MergePolicy mergePolicy = policy.getDetails();
        switch (mergePolicy.getMergeType()) {
            case CLUSTER:
                return EntityType.PHYSICAL_MACHINE_VALUE;
            case STORAGE_CLUSTER:
                return EntityType.STORAGE_VALUE;
            case DATACENTER:
                return EntityType.PHYSICAL_MACHINE_VALUE;
            default:
                throw new InvalidMergePolicyTypeException("Invalid merge policy type: "
                    + mergePolicy.getMergeType());
        }
    }

    /**
     * Get the commodity type based on the current merge policy type
     *
     * @return
     */
    private int getCommodityType(MergePolicy policy) {
        switch (policy.getDetails().getMergeType()) {
            case CLUSTER:
                return CommodityType.CLUSTER_VALUE;
            case STORAGE_CLUSTER:
                return CommodityType.STORAGE_CLUSTER_VALUE;
            case DATACENTER:
                return CommodityType.DATACENTER_VALUE;
            default:
                throw new InvalidMergePolicyTypeException("Invalid merge policy type: "
                    + policy.getDetails().getMergeType());
        }
    }
}
