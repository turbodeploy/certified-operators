package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateConstraintMapRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateConstraintMapResponse;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyManager;

/**
 * Responsible for generating a map from constraints to commodities.
 */
public class GenerateConstraintMap {
    private final PolicyManager policyManager;
    private final GroupServiceBlockingStub groupServiceClient;
    private final ReservationServiceStub reservationService;
    // Logger
    private static final Logger logger = LogManager.getLogger();
    /**
     * prefix for initial placement log messages.
     */
    private final String logPrefix = "FindInitialPlacement: ";

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
            @Nonnull final ReservationServiceStub reservationService
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
     * @return the constraint map send to the plan orchestrator.
     */
    public UpdateConstraintMapRequest createMap(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph, GroupResolver groupResolver) {

        UpdateConstraintMapRequest.Builder updateConstraintMapRequest =
                UpdateConstraintMapRequest.newBuilder();

        updateClusters(topologyGraph, updateConstraintMapRequest);
        updateNetworks(topologyGraph, updateConstraintMapRequest);
        updateDatacenter(topologyGraph, updateConstraintMapRequest);
        updatePolicies(topologyGraph, groupResolver, updateConstraintMapRequest);

        UpdateConstraintMapRequest constraintMapRequest = updateConstraintMapRequest.build();
        StreamObserver<UpdateConstraintMapResponse> response =
                new StreamObserver<UpdateConstraintMapResponse>() {

                    @Override
                    public void onNext(UpdateConstraintMapResponse
                                               updateConstraintMapResponse) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error(logPrefix + "Failed to update PO with ConstraintMap");
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
        reservationService.updateConstraintMap(constraintMapRequest, response);
        return constraintMapRequest;
    }

    /**
     * Identifies the commodities that the reservation instance has to buy if the user selects cluster.
     * @param topologyGraph the input topology graph.
     * @param updateConstraintMapRequest constraint map send to the plan orchestrator.
     */
    private void updateClusters(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                UpdateConstraintMapRequest.Builder updateConstraintMapRequest) {
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

        if (!allClusterIds.isEmpty()) {
            final Iterator<GetMembersResponse> membersResponseIterator = groupServiceClient.getMembers(GetMembersRequest.newBuilder()
                    .addAllId(allClusterIds).build());
            while (membersResponseIterator.hasNext()) {
                GetMembersResponse membersResponse = membersResponseIterator.next();
                boolean foundCommodityKey = false;
                List<String> storageClusterCommodities = new ArrayList<>();
                for (long providerOid : membersResponse.getMemberIdList()) {
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
                        // Storage can belong to multiple storage cluster.
                        // Keep intersecting the storage
                        // cluster commodities sold by the storage in this
                        // cluster until we get to a single entry
                        List<String> currentStorageClusterCommodities = provider
                                .getTopologyEntityDtoBuilder()
                                .getCommoditySoldListList().stream()
                                .filter(a -> a.getCommodityType().getType()
                                        == CommodityType.STORAGE_CLUSTER_VALUE)
                                .map(b -> b.getCommodityType().getKey())
                                .collect(Collectors.toList());
                        if (storageClusterCommodities.isEmpty()) {
                            storageClusterCommodities.addAll(currentStorageClusterCommodities);
                        } else {
                            storageClusterCommodities.retainAll(currentStorageClusterCommodities);
                        }
                        if (storageClusterCommodities.size() != 1) {
                            continue;
                        }
                        String key = storageClusterCommodities.get(0);
                        updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                                .newBuilder()
                                .setKey(key)
                                .setConstraintId(membersResponse.getGroupId())
                                .setProviderType(EntityType.STORAGE_VALUE)
                                .setType(Type.STORAGE_CLUSTER).build());
                    }
                    foundCommodityKey = true;
                    break;
                }
                if (!foundCommodityKey) {
                    logger.error(logPrefix + "Failed to find the commodity key for cluster with ID: "
                            + membersResponse.getGroupId());
                }
            }
        }
    }

    /**
     * Identifies the commodities that the reservation instance has to buy if the user selects network.
     * @param topologyGraph the input topology graph.
     * @param updateConstraintMapRequest constraint map send to the plan orchestrator.
     */
    private void updateNetworks(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                UpdateConstraintMapRequest.Builder updateConstraintMapRequest) {
        // go over all the networks.
        final List<TopologyEntity> allNetworks = topologyGraph
                .entitiesOfType(EntityType.NETWORK).collect(Collectors.toList());
        for (TopologyEntity network : allNetworks) {
            String key = "Network::" + network.getDisplayName();
            updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                    .newBuilder()
                    .setKey(key)
                    .setConstraintId(network.getOid())
                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setType(Type.NETWORK).build());
        }

    }

    /**
     * Identifies the commodities that the reservation instance has to buy if the user selects dataCenter.
     * @param topologyGraph the input topology graph.
     * @param updateConstraintMapRequest constraint map send to the plan orchestrator.
     */
    private void updateDatacenter(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                UpdateConstraintMapRequest.Builder updateConstraintMapRequest) {
        // go over all the datacenter. Find a host in the datacenter. Find the
        // data center commodity sold by the host. Find the key associated with the
        // commodity.
        final List<TopologyEntity> allDatacenters = topologyGraph
                .entitiesOfType(EntityType.DATACENTER).collect(Collectors.toList());
        for (TopologyEntity dataCenter : allDatacenters) {
            List<TopologyEntity> hostList = dataCenter.getConsumers();
            for (TopologyEntity host : hostList) {
                Optional<CommoditySoldDTO> dataCenterCommoditySold =
                        host.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                                .filter(a -> a.getCommodityType().getType()
                                        == CommodityType.DATACENTER_VALUE).findFirst();
                if (dataCenterCommoditySold.isPresent()) {
                    String key = dataCenterCommoditySold.get()
                            .getCommodityType().getKey();
                    updateConstraintMapRequest.addReservationContraintInfo(ReservationConstraintInfo
                            .newBuilder()
                            .setKey(key)
                            .setConstraintId(dataCenter.getOid())
                            .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setType(Type.DATA_CENTER).build());
                    break;
                }
            }
        }
    }

    /**
     * Identifies the commodities that the reservation instance has to buy if the user selects placement policy.
     * @param topologyGraph the input topology graph.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @param updateConstraintMapRequest constraint map send to the plan orchestrator.
     */
    private void updatePolicies(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                GroupResolver groupResolver,
                                UpdateConstraintMapRequest.Builder updateConstraintMapRequest) {
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
    }

}