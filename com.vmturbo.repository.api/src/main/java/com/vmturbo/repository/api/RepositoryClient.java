package com.vmturbo.repository.api;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Sends commands to the repository using gRPC.
 */
public class RepositoryClient {

    private static final Logger logger = LogManager.getLogger(RepositoryClient.class);

    private final RepositoryServiceBlockingStub repositoryService;

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
            long topologyContextId) {

        DeleteTopologyRequest request = DeleteTopologyRequest.newBuilder()
                                            .setTopologyId(topologyId)
                                            .setTopologyContextId(topologyContextId)
                                            .build();
        try {
            return repositoryService.deleteTopology(request);
        } catch (StatusRuntimeException sre) {

            RepositoryOperationResponse.Builder responseBuilder =
                RepositoryOperationResponse.newBuilder();

            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // If topology doesn't exist, return success
                logger.info("Topology with Id:{} and contextId:{} not found",
                    topologyId, topologyContextId);
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.OK);
            } else {
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.FAILED)
                    .setError(sre.toString());
            }

            return responseBuilder.build();
        }
    }

    /**
     * Add the business family (master account oid to the cloud account oids for the scope of the plan).
     *
     * @param scopeIds The seed plan scopes OIDs.
     * @param realtimeTopologyContextId The real-time topology context id used to get all accounts in the
     * environment, and figure out relevant accounts for plans and real-time.
     * @param onlyMasterAccounts if true return only master accounts, else sub-accounts as well.
     * @return Oids of business families in scope - master and sub-accounts.
     * TODO: This may need to be revisited.  Check with Vasile and team.
     */
    public List<Long> getRelatedBusinessAccountOrSubscriptionOids(final List<Long> scopeIds,
                                                                  final Long realtimeTopologyContextId,
                                                                  final boolean onlyMasterAccounts) {

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
                                          .map(PartialEntity::getFullEntity)
                                      .collect(Collectors.toList());

        Set<Long> relatedBusinessAccountsOrSubscriptions = new HashSet<>();
        // Get the business families in scope and the sub-accounts under them.
        for (long scopeId : scopeIds) {
           for(TopologyEntityDTO ba : allBusinessAccounts) {
                List<Long> subAccountOidsList =
                      ba.getConnectedEntityListList()
                          .stream()
                          .filter(v -> v.getConnectedEntityType()
                                  == EntityType.BUSINESS_ACCOUNT_VALUE)
                          .map(ConnectedEntity::getConnectedEntityId)
                          .collect(Collectors.toList());
                // if scope is a sub-account
                if (subAccountOidsList.contains(scopeId)
                    // scope is a master account
                    || (ba.getOid() == scopeId &&
                        !subAccountOidsList.isEmpty())) {
                    if (!onlyMasterAccounts) {
                        relatedBusinessAccountsOrSubscriptions.addAll(subAccountOidsList);
                    }
                    relatedBusinessAccountsOrSubscriptions.add(ba.getOid());
                }
            };
        }
        // real-time or plan global scope, return all Business Accounts/ Substriptions.
        // TODO:  OM-50904 For real-time, no scopes need be returned and global RIs will be fetched.
        if (scopeIds.isEmpty()) {
            List<Long> allBaOids =  allBusinessAccounts.stream().map(TopologyEntityDTO::getOid)
                            .collect(Collectors.toList());
            return allBaOids;
        }
        return relatedBusinessAccountsOrSubscriptions.stream().collect(Collectors.toList());
    }
}
