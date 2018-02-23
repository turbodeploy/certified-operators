package com.vmturbo.topology.processor.group.policy;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * A policy that removes cluster boundaries. Merge policies merge multiple clusters
 * into a single logical group for the purpose of workload placement.
 * <p>
 */
public class MergePolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();
    private final PolicyDTO.Policy.MergePolicy mergePolicy;
    // list of PolicyEntities which is wrapper for Group and additional entities
    private final List<PolicyEntities> mergePolicyEntitiesList;
    private final PolicyDTO.Policy policyDefinition;

    /**
     * Create a new MergePolicy.
     * The policy should be MergePolicy
     *
     * @param policyDefinition        The policy definition describing the details of the policy to be applied.
     * @param mergePolicyEntitiesList list of entities in merge policy.
     */
    public MergePolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                       @Nonnull final List<PolicyEntities> mergePolicyEntitiesList) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasMerge(), "Must be MergePolicy");
        this.policyDefinition = Objects.requireNonNull(policyDefinition);
        this.mergePolicy = Objects.requireNonNull(policyDefinition.getMerge());
        this.mergePolicyEntitiesList = Objects.requireNonNull(mergePolicyEntitiesList);
        mergePolicyEntitiesList
                .forEach(mergePolicyEntities -> GroupProtoUtil.checkEntityType(mergePolicyEntities.getGroup()));
    }

    /**
     * Update entities to buy/sell with the same key.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying mergePolicy policy.");
        List<Group> groups = mergePolicyEntitiesList
                .stream()
                .map(mergePolicyEntities -> mergePolicyEntities.getGroup())
                .collect(Collectors.toList());

        // Resolve the relevant groups and return List of OIDs.
        List<Long> oidList = getListOfOids(groupResolver, topologyGraph, groups);

        // get the policy OID
        long policyOid = policyDefinition.getId();
        applyClusterPolicy(oidList, policyOid, groupResolver, topologyGraph);
    }

    /**
     * Going through all the PMs (or Storage) and their attached VMs in the clusters and
     * change the key of the cluster commodity to the policy OID.
     * It applies to both computation or storage cluster.
     *
     * @param oidList       list of OIDs from the relevant groups
     * @param policyOid     merge policy OID
     * @param groupResolver The group resolver to be used in resolving the groups
     *                      to which the policy applies.
     * @param topologyGraph The {@link TopologyGraph} to which the policy should be applied.
     */
    private void applyClusterPolicy(@Nonnull final List<Long> oidList,
                                    final long policyOid,
                                    @Nonnull final GroupResolver groupResolver,
                                    @Nonnull final TopologyGraph topologyGraph) {
        // predicate for either PM or Storage type
        final Predicate<Long> isMatchingPolicyType = id -> topologyGraph.getEntity(id).isPresent()
                && getCurrentEntityType() == (topologyGraph.getEntity(id).get().getEntityType());
        Set<TopologyEntity> entitySet = oidList
                .stream()
                .filter(isMatchingPolicyType)
                .map(id -> topologyGraph.getEntity(id).get())
                .collect(Collectors.toSet());
        // PM (or Storage)
        changeClusterKey(policyOid, topologyGraph, entitySet);

        // change the key of the cluster commodity for all VMs that attached to the PMs (or Storage) in the clusters
        Set<TopologyEntity> vmSet = entitySet
                .stream()
                .flatMap(entity -> entity.getConsumers().stream())
                .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toSet());
        changeClusterKeyForVM(policyOid, topologyGraph, vmSet, entitySet);
    }

    /**
     * Going through all the PMs (or Storage) and change the key of the 'sold' cluster commodity
     * to the policy OID.
     *
     * @param policyOid     merge policy OID
     * @param topologyGraph The {@link TopologyGraph} to which the policy should be applied.
     * @param entitySet     Set with either PM (or Storage) OIDs
     */

    private void changeClusterKey(final long policyOid,
                                  @Nonnull final TopologyGraph topologyGraph,
                                  @Nonnull final Set<TopologyEntity> entitySet) {
        entitySet.stream().forEach(topologyEntity -> {
            // get the commodity sold list
            List<CommoditySoldDTO> commoditySoldList = topologyEntity
                    .getTopologyEntityDtoBuilder()
                    .getCommoditySoldListList();

            // check if we have cluster, storage cluster or datacenter commodity, if yes store and reuse later
            List<CommoditySoldDTO> newComoditySoldList = Lists.newArrayList();
            CommoditySoldDTO oldSoldDTO = null;
            for (CommoditySoldDTO soldDTO : commoditySoldList) {
                if (soldDTO.getCommodityType().getType() == getCommodityType()) {
                    oldSoldDTO = soldDTO;
                } else {
                    newComoditySoldList.add(soldDTO);
                }
            }

            // build Cluster commodity type
            TopologyDTO.CommodityType newClusterType = TopologyDTO
                    .CommodityType.newBuilder()
                    .setKey(Long.toString(policyOid)) // see the key to merge policy id
                    .setType(getCommodityType())
                    .build();

            // create cluster commodity.
            CommoditySoldDTO newClusterDTO;
            if (oldSoldDTO != null) {
                newClusterDTO = CommoditySoldDTO.newBuilder(oldSoldDTO)
                        .setCommodityType(newClusterType)
                        .build();
            } else {
                newClusterDTO = CommoditySoldDTO.newBuilder()
                        .setCommodityType(newClusterType)
                        .build();
            }
            // add the new Cluster DTO to list
            newComoditySoldList.add(newClusterDTO);

            // remove the old list and add the new one
            topologyEntity
                    .getTopologyEntityDtoBuilder()
                    .clearCommoditySoldList()
                    .addAllCommoditySoldList(newComoditySoldList);
        });
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
     * @param policyOid     merge policy OID
     * @param topologyGraph The {@link TopologyGraph} to which the policy should be applied.
     * @param hostSet       The set of hosts, could be PMs or Storages
     * @param vmSet         Set with VM OIDs
     */
    private void changeClusterKeyForVM(final long policyOid,
                                       @Nonnull final TopologyGraph topologyGraph,
                                       @Nonnull final Set<TopologyEntity> vmSet,
                                       @Nonnull final Set<TopologyEntity> hostSet) {
        vmSet.stream().forEach(topologyEntity -> {
            // get the commodity bought provider list
            List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvidersList = topologyEntity
                    .getTopologyEntityDtoBuilder()
                    .getCommoditiesBoughtFromProvidersList();

            List<CommoditiesBoughtFromProvider> newCommodityBoughtFromProviderList = Lists.newArrayList();


            commoditiesBoughtFromProvidersList
                    .stream()
                    .forEach(commoditiesBoughtFromProvider -> {
                        // the condition is to make sure that the providerID for that bought list is included in
                        // the original set of entities that we want to merge.
                        // This is because a vm can have multiple storages, and only one of those might be part
                        // of the cluster to merge, but not the second one. (so changing the key for the 2nd is wrong).
                        if (commoditiesBoughtFromProvider.hasProviderId() &&
                                hostSet.stream()
                                        .anyMatch(host -> host.getOid() == commoditiesBoughtFromProvider.getProviderId())) {
                            List<CommodityBoughtDTO> commodityBoughtDTOList = commoditiesBoughtFromProvider
                                    .getCommodityBoughtList();
                            List<CommodityBoughtDTO> newCommodityBoughtList = Lists.newArrayList();
                            CommodityBoughtDTO oldBoughtDto = null;
                            for (CommodityBoughtDTO boughtDTO : commodityBoughtDTOList) {
                                if (boughtDTO.getCommodityType().getType() == getCommodityType()) {
                                    oldBoughtDto = boughtDTO;
                                } else {
                                    newCommodityBoughtList.add(boughtDTO);
                                }
                            }

                            TopologyDTO.CommodityType newClusterType = TopologyDTO.CommodityType.newBuilder()
                                    .setKey(Long.toString(policyOid))
                                    .setType(getCommodityType())
                                    .build();

                            // create cluster commodity.
                            CommodityBoughtDTO newClusterDTO;
                            if (oldBoughtDto != null) {
                                newClusterDTO = CommodityBoughtDTO.newBuilder(oldBoughtDto)
                                        .setCommodityType(newClusterType)
                                        .build();
                            } else {
                                newClusterDTO = CommodityBoughtDTO.newBuilder()
                                        .setCommodityType(newClusterType)
                                        .build();
                            }
                            newCommodityBoughtList.add(newClusterDTO);
                            CommoditiesBoughtFromProvider newCommodityBoughtFromProvider = CommoditiesBoughtFromProvider
                                    .newBuilder()
                                    .addAllCommodityBought(newCommodityBoughtList)
                                    .build();
                            newCommodityBoughtFromProviderList.add(newCommodityBoughtFromProvider);
                        } else { // keep the original provider
                            newCommodityBoughtFromProviderList.add(commoditiesBoughtFromProvider);
                        }

                    });

            topologyEntity
                    .getTopologyEntityDtoBuilder()
                    .clearCommoditiesBoughtFromProviders()
                    .addAllCommoditiesBoughtFromProviders(newCommodityBoughtFromProviderList);
        });
    }


    /**
     * Get list of OIDs.
     *
     * @param groupResolver the group resolver to be used in resolving the groups
     *                      to which the policy applies.
     * @param topologyGraph the {@link TopologyGraph} to which the policy should be applied.
     * @param groups        the group referenced by Policy.
     * @return list of OIDs.
     * @throws GroupResolutionException if group resolution failed.
     */
    private List<Long> getListOfOids(@Nonnull final GroupResolver groupResolver,
                                     @Nonnull final TopologyGraph topologyGraph,
                                     @Nonnull final List<Group> groups) throws GroupResolutionException {
        // Not using lambda here, for loop is easier to throw GroupResolutionException to caller
        List<Set<Long>> listOfOids = Lists.newArrayList();
        for (Group group : groups) {
            listOfOids.add(groupResolver.resolve(group, topologyGraph));
        }
        return listOfOids
                .stream()
                .flatMap(ids -> ids.stream())
                .collect(Collectors.toList());
    }

    /**
     * Get entity type based on current merge policy type.
     *
     * @return entity type
     */
    private int getCurrentEntityType() {
        switch (this.mergePolicy.getMergeType()) {
            case CLUSTER:
                return EntityType.PHYSICAL_MACHINE_VALUE;
            case STORAGE_CLUSTER:
                return EntityType.STORAGE_VALUE;
            case DATACENTER:
                return EntityType.PHYSICAL_MACHINE_VALUE;
            default:
                throw new InvalidMergePolicyTypeException("Invalid merge policy type: "
                        + this.mergePolicy.getMergeType());
        }
    }

    /**
     * Get the commodity type based on the current merge policy type
     *
     * @return
     */
    private int getCommodityType() {
        switch (this.mergePolicy.getMergeType()) {
            case CLUSTER:
                return CommodityType.CLUSTER_VALUE;
            case STORAGE_CLUSTER:
                return CommodityType.STORAGE_CLUSTER_VALUE;
            case DATACENTER:
                return CommodityType.DATACENTER_VALUE;
            default:
                throw new InvalidMergePolicyTypeException("Invalid merge policy type: "
                        + this.mergePolicy.getMergeType());
        }

    }
}