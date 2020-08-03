package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateConstraintMapRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Status;

/**
 * Responsible for generating a map from constraints to commodities.
 */
public class GenerateConstraintMap {
    private final PolicyManager policyManager;
    private final GroupServiceBlockingStub groupServiceClient;
    private final ReservationServiceBlockingStub reservationService;

    /**
     * constructor for GenerateConstraintMap.
     *
     * @param policyManager      policy manager to get policy details
     * @param groupServiceClient group service to get cluster and datacenter information
     * @param reservationService reservation service to update the PO with the constraint map.
     */
    public GenerateConstraintMap(
            @Nonnull final PolicyManager policyManager,
            @Nonnull final GroupServiceBlockingStub groupServiceClient,
            @Nonnull final ReservationServiceBlockingStub reservationService
    ) {
        this.policyManager = policyManager;
        this.groupServiceClient = groupServiceClient;
        this.reservationService = reservationService;
    }

    /**
     * Identifies the commodities that the reservation instance has to buy if the user selects constraints.
     * The constraints taken care of are cluster, datacenter and placement policy.
     * @param topologyGraph the input topology graph.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @return success if the map is successfully loaded in the plan orchestrator.
     */
    public Status createMap(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph, GroupResolver groupResolver) {

        UpdateConstraintMapRequest.Builder updateConstraintMapRequest =
                UpdateConstraintMapRequest.newBuilder();

        //go over all the clusters..Find all members(hosts) of the cluster. Pick a random
        //host. Find the cluster commodity sold by the host. Find the key associated with the
        //commodity.
        final List<Grouping> allGroups = new ArrayList<>();
        groupServiceClient
                .getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()).build()).forEachRemaining(allGroups::add);
        final List<Long> allClusterIds = new ArrayList<>();
        for (Grouping group : allGroups) {
            if (GroupProtoUtil.CLUSTER_GROUP_TYPES.contains(group.getDefinition().getType())) {
                allClusterIds.add(group.getId());
            }
        }
        final Iterator<GetMembersResponse> membersResponseIterator = groupServiceClient.getMembers(GetMembersRequest.newBuilder()
                .addAllId(allClusterIds).build());
        while (membersResponseIterator.hasNext()) {
            GetMembersResponse membersResponse = membersResponseIterator.next();
            if (!membersResponse.getMemberIdList().isEmpty()) {
                Optional<Long> providerOidOptional = membersResponse.getMemberIdList()
                        .stream().findFirst();
                if (!providerOidOptional.isPresent()) {
                    continue;
                }
                Long providerOid = providerOidOptional.get();
                Optional<TopologyEntity> providerOptional = topologyGraph.getEntity(providerOid);
                if (!providerOptional.isPresent()) {
                    continue;
                }
                TopologyEntity provider = providerOptional.get();
                if (provider.getTypeSpecificInfo().hasPhysicalMachine()) {
                    String key = provider.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                            .filter(a -> a.getCommodityType().getType()
                                    == CommodityType.CLUSTER_VALUE).findFirst().get()
                            .getCommodityType().getKey();
                    updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                            .newBuilder()
                            .setKey(key)
                            .setConstraintId(membersResponse.getGroupId())
                            .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setType(Type.CLUSTER).build());
                } else if (provider.getTypeSpecificInfo().hasStorage()) {
                    String key = provider.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                            .filter(a -> a.getCommodityType().getType()
                                    == CommodityType.STORAGE_CLUSTER_VALUE).findFirst().get()
                            .getCommodityType().getKey();
                    updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                            .newBuilder()
                            .setKey(key)
                            .setConstraintId(membersResponse.getGroupId())
                            .setProviderType(EntityType.STORAGE_VALUE)
                            .setType(Type.STORAGE_CLUSTER).build());
                }
            }
        }


        // go over all the datacenter. Find a host in the datacenter. Find the
        // data center commodity sold by the host. Find the key associated with the
        // commodity.
        final List<TopologyEntity> allDatacenters = topologyGraph
                .entitiesOfType(EntityType.DATACENTER).collect(Collectors.toList());
        for (TopologyEntity dataCenter : allDatacenters) {
            Optional<TopologyEntity> hostOptional = dataCenter.getConsumers().stream().findFirst();
            if (!hostOptional.isPresent()) {
                continue;
            }
            TopologyEntity host = hostOptional.get();
            String key = host.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .filter(a -> a.getCommodityType().getType()
                            == CommodityType.DATACENTER_VALUE).findFirst().get()
                    .getCommodityType().getKey();
            updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                    .newBuilder()
                    .setKey(key)
                    .setConstraintId(dataCenter.getOid())
                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setType(Type.DATA_CENTER).build());
        }

        // go over all the policies and find the key of the segmentaion commodity
        // associated with the placement policy and also the provider type of the entity.
        Table<Long, Integer, TopologyDTO.CommodityType> placementPolicyIdToCommodityType = policyManager
                .getPlacementPolicyIdToCommodityType(topologyGraph,
                        groupResolver);
        for (Cell<Long, Integer, TopologyDTO.CommodityType> cell : placementPolicyIdToCommodityType.cellSet()) {
            updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                    .newBuilder()
                    .setKey(cell.getValue().getKey())
                    .setConstraintId(cell.getRowKey())
                    .setProviderType(cell.getColumnKey())
                    .setType(Type.POLICY).build());
        }

        // TODO handle VDC, storage clusters and Network constraints

        reservationService.updateConstraintMap(updateConstraintMapRequest.build());
        return Status.success();
    }

}