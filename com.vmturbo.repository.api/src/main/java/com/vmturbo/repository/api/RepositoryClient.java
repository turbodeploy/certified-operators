package com.vmturbo.repository.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Sends commands to the repository using gRPC.
 */
public class RepositoryClient {

    private static final Logger logger = LogManager.getLogger(RepositoryClient.class);

    private final RepositoryServiceBlockingStub repositoryService;

    /**
     * nonSupplyChainEntityTypesToInclude Set of non-supplychain entities to include by querying
     * repository for instance, each would require its implementation to retrieve.
     */
    protected static final Set<EntityType> supportedNonSupplyChainEntitiesByType =
                    ImmutableSet.of(EntityType.BUSINESS_ACCOUNT);

    public RepositoryClient(@Nonnull Channel repositoryChannel) {
        repositoryService = RepositoryServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
    }

    public Iterator<RetrieveTopologyResponse> retrieveTopology(long topologyId) {
        RetrieveTopologyRequest request = RetrieveTopologyRequest.newBuilder()
                .setTopologyId(topologyId)
                .build();
        return repositoryService.retrieveTopology(request);
    }

    /**
     * Retrieve real time topology entities with provided OIDs.
     *
     * @param oids OIDs to retrieve topology entities
     * @param realtimeContextId real time context id
     * @return a stream of {@link TopologyEntityDTO} objects
     */
    public Stream<TopologyEntityDTO> retrieveTopologyEntities(@Nonnull final List<Long> oids,
                                                              final long realtimeContextId) {
        // If we ever need to retrieve ALL topology entities, create a new call with no oids input.
        // It's too error-prone to pass in an empty list to indicate "get all" - we end up
        // getting the entire topology in places we don't need it.
        if (oids.isEmpty()) {
            return Stream.empty();
        }
        RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
            .addAllEntityOids(oids)
            .setTopologyContextId(realtimeContextId)
            .setTopologyType(TopologyType.SOURCE)
            .setReturnType(Type.FULL)
            .build();

        return RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(request))
            .map(PartialEntity::getFullEntity);
    }

    public RepositoryOperationResponse deleteTopology(long topologyId,
                                                      long topologyContextId,
                                                      TopologyType topologyType) {
        DeleteTopologyRequest request = DeleteTopologyRequest.newBuilder()
            .setTopologyId(topologyId)
            .setTopologyContextId(topologyContextId)
            .setTopologyType(topologyType)
            .build();
        try {
            return repositoryService.deleteTopology(request);
        } catch (StatusRuntimeException sre) {

            RepositoryOperationResponse.Builder responseBuilder =
                RepositoryOperationResponse.newBuilder();

            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // If topology doesn't exist, return success
                logger.info("Topology with Id:{}, contextId:{} and type:{} not found",
                    topologyId, topologyContextId, topologyType);
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.OK);
            } else {
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.FAILED)
                    .setError(sre.toString());
            }

            return responseBuilder.build();
        }
    }

    /**
     * Add the business family (master account OID to the cloud account OIDs for the scope of the plan).
     *
     * @param scopeIds The seed plan scopes OIDs.
     * @param realtimeTopologyContextId The real-time topology context id used to get all accounts in the
     * environment, and figure out relevant accounts for plans and real-time.
     * @return OIDs of business families in scope - master and sub-accounts.
     * TODO: This may need to be revisited.  Check with Vasile and team.
     */
    public List<Long> getRelatedBusinessAccountOrSubscriptionOids(final List<Long> scopeIds,
                                                                  final Long realtimeTopologyContextId) {
        // If business account scope, add all sibling accounts under the same business family
        // This is only needed in cloud OCP plans.
        List<TopologyEntityDTO> allBusinessAccounts =
                          RepositoryDTOUtil.topologyEntityStream(
                         repositoryService
                         .retrieveTopologyEntities(RetrieveTopologyEntitiesRequest
                                         .newBuilder()
                                         .setTopologyContextId(realtimeTopologyContextId)
                                         .setReturnType(Type.FULL)
                                         .setTopologyType(TopologyType.SOURCE)
                                         .addEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                         .build()))
                                          // It has to be FULL here, or something more than MINIMAL,
                                          // as the TopologyEntityDTO is needed.
                                          .map(PartialEntity::getFullEntity)
                                      .collect(Collectors.toList());
        return parseRelatedBusinessAccountOrSubscriptionOids(scopeIds, allBusinessAccounts);
    }

    /**
     * Get the related business accounts/ subscriptions in a billing family by parsing the
     * TopologyEntityDTO's associated with all business account/subscriptions in real-time.
     *
     * @param scopeIds The seed plan scopes OIDs.
     * @param allBusinessAccounts All real-time Business accounts/subscriptions.
     * @return
     */
    public List<Long> parseRelatedBusinessAccountOrSubscriptionOids(@Nonnull final List<Long> scopeIds,
                                      @Nonnull final List<TopologyEntityDTO> allBusinessAccounts) {

        Set<Long> relatedBusinessAccountsOrSubscriptions = new HashSet<>();
        // Get the business families in scope and the sub-accounts under them.
        for (long scopeId : scopeIds) {
           for(TopologyEntityDTO ba : allBusinessAccounts) {
               List<ConnectedEntity> subAccountsList = ba.getConnectedEntityListList()
                           .stream()
                           .filter(v -> v.getConnectedEntityType()
                                  == EntityType.BUSINESS_ACCOUNT_VALUE)
                           .collect(Collectors.toList());
               List<Long> connectedOidsList = ba.getConnectedEntityListList()
                                                  .stream()
                                                  .map(ConnectedEntity::getConnectedEntityId)
                                                  .collect(Collectors.toList());
                // if scope is a sub-account
                if (connectedOidsList.contains(scopeId)
                    // scope is a master account
                    || ba.getOid() == scopeId
                    // account ba is a master account, and scope is another entity e.g. VM
                    || !subAccountsList.isEmpty()) {
                    relatedBusinessAccountsOrSubscriptions.addAll(subAccountsList.stream()
                                                  .map(ConnectedEntity::getConnectedEntityId)
                                                  .collect(Collectors.toList()));
                    relatedBusinessAccountsOrSubscriptions.add(ba.getOid());
                }
            };
        }
        // real-time or plan global scope, return all Business Accounts/ Substriptions.
        if (scopeIds.isEmpty()) {
            List<Long> allBaOids =  allBusinessAccounts.stream().map(TopologyEntityDTO::getOid)
                            .collect(Collectors.toList());
            return allBaOids;
        }
        return relatedBusinessAccountsOrSubscriptions.stream().collect(Collectors.toList());
    }

    /**
     * Get the entities map associated with a scoped or global topology (cloud plans or real-time).
     *
     * @param scopeIds  The topology scope seed IDs.
     * @param realtimeTopologyContextId The real-time context id.
     * @param supplyChainServiceBlockingStub the Supply Chain Service to make calls to to get the topology
     * nodes associated with the scopeIds.
     * @return A Map containing the relevant cloud scopes, keyed by scope type and mapped to scope OIDs.
     */
    @Nonnull
    public Map<EntityType, Set<Long>> getEntityOidsByType(@Nonnull final List<Long> scopeIds,
                          final Long realtimeTopologyContextId,
                          @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub) {
        try {
            final GetSupplyChainRequest.Builder requestBuilder = GetSupplyChainRequest.newBuilder();
            GetSupplyChainRequest request = requestBuilder
                .setContextId(realtimeTopologyContextId)
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(scopeIds)
                    .setEnvironmentType(EnvironmentType.CLOUD))
                .build();

            final GetSupplyChainResponse response = supplyChainServiceBlockingStub
                            .getSupplyChain(request);
            if (!response.getSupplyChain().getMissingStartingEntitiesList().isEmpty()) {
                logger.warn("{} of {} seed entities were not found for the supply chain: {}.",
                            response.getSupplyChain().getMissingStartingEntitiesCount(),
                            scopeIds.size(),
                            response.getSupplyChain().getMissingStartingEntitiesList());
            }
            Map<EntityType, Set<Long>> topologyMap =
                            parseSupplyChainResponseToEntityOidsMap(response,
                                                     realtimeTopologyContextId);
            // Include supported non-supplychain entities in response.
            for (EntityType entityType : supportedNonSupplyChainEntitiesByType) {
                switch (entityType) {
                    case BUSINESS_ACCOUNT:
                        // Make adjustment for Business Accounts/Subscriptions.  Get all related
                        // accounts in the family.
                        List<Long> allRelatedBaOids = getRelatedBusinessAccountOrSubscriptionOids(
                            scopeIds.stream().collect(Collectors.toList()),
                            realtimeTopologyContextId);
                        topologyMap.put(EntityType.BUSINESS_ACCOUNT, allRelatedBaOids.stream()
                                        .collect(Collectors.toSet()));
                        break;
                    default:
                        break;
                }
            }
            return topologyMap;
        } catch (Exception e) {
            StringBuilder errMsg = new StringBuilder();
            for (Long scopeId : scopeIds) {
                errMsg.append(scopeId);
                errMsg.append(" ");
            }
            logger.error("Error getting cloud scopes, this could result in the wrong set of RIs"
                            + "retrieved in scoped plans with scope ids: " + errMsg.toString(),
                         e);
            return Collections.emptyMap();
        }
    }

    /**
     * Parse the supply chain nodes from response and group the entities by type.
     *
     * @param response The response to be parsed.
     * @param realtimeTopologyContextId Real-time context id.
     * @return The Map of topology entities of interest, grouped by type.
     */
    @Nonnull
    public Map<EntityType, Set<Long>>
           parseSupplyChainResponseToEntityOidsMap(@Nonnull final GetSupplyChainResponse response,
                                                   final Long realtimeTopologyContextId) {
        if (response == null) {
            logger.warn("No Supply Chain retrieved to parse");
            return Collections.emptyMap();
        }
        try {
            List<SupplyChainNode> supplyChainNodes = response.getSupplyChain()
                            .getSupplyChainNodesList();
            Map<EntityType, Set<Long>> entitiesMap = new HashMap<>();
            for (SupplyChainNode node : supplyChainNodes) {
                Map<Integer, SupplyChainNode.MemberList> relatedEntitiesByType = node
                                .getMembersByStateMap();
                for (SupplyChainNode.MemberList members : relatedEntitiesByType.values()) {
                    String entityTypeName = node.getEntityType();
                    EntityType entityType = UIEntityType.fromString(entityTypeName).sdkType();
                    List<Long> memberOids = members.getMemberOidsList();
                    entitiesMap.put(entityType, memberOids.stream()
                                            .collect(Collectors.toSet()));
                }
            }
            return entitiesMap;
        } catch (Exception e) {
            logger.error("Error parsing cloud scopes, this could result in the wrong set of RIs"
                            + " retrieved in scoped plans",
                         e);
            return Collections.emptyMap();
        }
    }
}
