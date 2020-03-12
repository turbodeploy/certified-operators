package com.vmturbo.repository.api;

import java.util.Collection;
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
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.springframework.util.CollectionUtils;

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
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;

/**
 * Sends commands to the repository using gRPC.
 */
public class RepositoryClient {

    private static final Logger logger = LogManager.getLogger(RepositoryClient.class);

    private final RepositoryServiceBlockingStub repositoryService;

    private Long realtimeTopologyContextId;

    /**
     * nonSupplyChainEntityTypesToInclude Set of non-supplychain entities to include by querying
     * repository for instance, each would require its implementation to retrieve.
     */
    protected static final Set<EntityType> supportedNonSupplyChainEntitiesByType =
                    ImmutableSet.of(EntityType.BUSINESS_ACCOUNT);

    /**
     * RepositoryClient constructor.
     *
     * @param repositoryChannel Repository channel.
     * @param realtimeTopologyContextId Real-time context id.
     */
    public RepositoryClient(@Nonnull Channel repositoryChannel, final Long realtimeTopologyContextId) {
        repositoryService = RepositoryServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
        this.realtimeTopologyContextId = realtimeTopologyContextId;
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
     * Get all Business Account {@link TopologyEntityDTO} in the current repository.
     *
     * @param realtimeTopologyContextId the realtimeTopologyContextId used to request accounts
     * @return list of all Business Account {@link TopologyEntityDTO} in the current repository
     */
    public List<TopologyEntityDTO> getAllBusinessAccounts(final long realtimeTopologyContextId) {
        // If business account scope, add all sibling accounts under the same billing family
        // This is only needed in cloud OCP plans.
        return RepositoryDTOUtil.topologyEntityStream(
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
    }

    /**
     * Retrieves the Azure EA enrollment number associated with a given business account, or
     * returns null if there is none.
     *
     * @param businessAccount a {@link TopologyEntityDTO} representing a Business Account object
     * @return the Azure EA enrollment number corresponding to the given {@param businessAccount}
     */
    public static String getEnrollmentNumber(TopologyEntityDTO businessAccount) {
        // Prevent this function from returning null- if null is returned,
        // Collectors.groupingBy in getBaOidToEaSiblingAccounts will fail with NPE
        String enrollmentNumber = "";
        if (!businessAccount.hasTypeSpecificInfo() ||
            !businessAccount.getTypeSpecificInfo().hasBusinessAccount()) {
            return enrollmentNumber;
        }
        List<PricingIdentifier> pricingIdentifiers = businessAccount
            .getTypeSpecificInfo()
            .getBusinessAccount()
            .getPricingIdentifiersList();

        if (CollectionUtils.isEmpty(pricingIdentifiers)) {
            return enrollmentNumber;
        }
        for (PricingIdentifier pricingIdentifier : pricingIdentifiers) {
            if (pricingIdentifier.getIdentifierName() == PricingIdentifierName.ENROLLMENT_NUMBER) {
                enrollmentNumber = pricingIdentifier.getIdentifierValue();
            }
        }
        return enrollmentNumber;
    }

    /**
     * Build a map of all Business Account oid -> EA sibling oids (including key oid). Used in place of AWS
     * BA -> connectedTo -> BA representing billing family relationships.
     *
     * @param allBusinessAccounts all business accounts in the current real-time topology
     * @return map of all Business Account oid -> EA sibling oids (including key oid)
     */
    public static Map<Long, Set<Long>> getBaOidToEaSiblingAccounts(@Nonnull final List<TopologyEntityDTO> allBusinessAccounts) {
        Map<String, Set<Long>> enrollmentNumberToAccountOids = allBusinessAccounts.stream()
            .collect(Collectors.groupingBy(RepositoryClient::getEnrollmentNumber,
                    Collectors.mapping(TopologyEntityDTO::getOid, Collectors.toSet())));

        Map<Long, Set<Long>> baOidToEaAccounts = Maps.newHashMap();
        for (Map.Entry<String, Set<Long>> entry : enrollmentNumberToAccountOids.entrySet()) {
            String enrollmentNumber = entry.getKey();
            if (Strings.isNotBlank(enrollmentNumber)) {
                Set<Long> eaAccountOids = entry.getValue();
                eaAccountOids.forEach(eaAccoundOid -> baOidToEaAccounts.put(eaAccoundOid, eaAccountOids));
            }
        }
        return baOidToEaAccounts;
    }

    /**
     * Get the business accounts in the scope dictated by {@param scopeEntityOids}. The result of this function
     * does not include Azure EA sibling accounts related to EA accounts in the scope specified.
     *
     * @param scopeEntityOids The seed plan scope- set of all entity OIDs in scope
     * @param allBusinessAccounts All real-time Business accounts/subscriptions.
     * @return a set of all business account OIDs in the scope dictated by {@param scopeEntityOids}
     */
    public static Set<Long> getFilteredScopeBusinessAccountOids(
            @Nonnull final Set<Long> scopeEntityOids,
            @Nonnull final List<TopologyEntityDTO> allBusinessAccounts) {
        // real-time or plan global scope, return all Business Accounts/Subscriptions.
        if (scopeEntityOids.isEmpty()) {
            return allBusinessAccounts.stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());
        }
        Set<Long> scopeBusinessAccounts = Sets.newHashSet();
        for (TopologyEntityDTO ba : allBusinessAccounts) {
            long baOid = ba.getOid();
            Set<Long> subAccounts = Sets.newHashSet();
            boolean baConnectedToScopeId = false;
            for (ConnectedEntity baConnectedEntity : ba.getConnectedEntityListList()) {
                long connectedEntityOid = baConnectedEntity.getConnectedEntityId();
                if (scopeEntityOids.contains(connectedEntityOid)) {
                    baConnectedToScopeId = true;
                }
                if (baConnectedEntity.getConnectedEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                    subAccounts.add(connectedEntityOid);
                }
            }
            // scope is an account or connected entity
            if (scopeEntityOids.contains(baOid) || baConnectedToScopeId) {
                // add business account
                scopeBusinessAccounts.add(baOid);
                // add potential AWS master sub-accounts
                scopeBusinessAccounts.addAll(subAccounts);
            }
        }
        return scopeBusinessAccounts;
    }

    /**
     * Returns a Stream of Maps, one per element present in the startingOidsPerScope. Each Map
     * represents entities by type in a scope, the starting entities of which are defined in
     * startingOidsPerScope.
     *
     * @param startingOidsPerScope Collection of Lists where each list represents starting oids
     *                             for a given scope. Therefore the collection of lists
     *                             represents all the scopes for which the entities are requested.
     * @param supplyChainServiceBlockingStub service endpoint to make the request.
     * @return stream of Maps per scope, each map representing entities in a given scope.
     */
    public Stream<Map<EntityType, Set<Long>>> getEntitiesByTypePerScope(
            final Collection<List<Long>> startingOidsPerScope,
            final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub) {
        final Collection<SupplyChainSeed> seeds = startingOidsPerScope.stream()
                .map(startingOids -> SupplyChainSeed.newBuilder()
                        .setScope(SupplyChainScope.newBuilder()
                                .addAllStartingEntityOid(startingOids)
                                .build())
                        .build())
                .collect(Collectors.toSet());
        final GetMultiSupplyChainsRequest request = GetMultiSupplyChainsRequest.newBuilder()
                .addAllSeeds(seeds)
                .build();
        final Collection<Map<EntityType, Set<Long>>> entitiesPerScope = new HashSet<>();
        final Iterator<GetMultiSupplyChainsResponse> responses =
                supplyChainServiceBlockingStub.getMultiSupplyChains(request);
        responses.forEachRemaining(response -> entitiesPerScope.add(
                parseSupplyChainResponseToEntityOidsMap(response.getSupplyChain())
        ));
        return entitiesPerScope.stream();
    }

    /**
     * A helper method to combine the results of getFilteredScopeBusinessAccountOids and getBaOidToEaSiblingAccounts
     *
     * @param scopeBusinessAccountsOids a set of all business account OIDs in the scope
     * @param baOidToEaSiblingAccounts a map of all Business Account oid -> EA sibling oids (including key oid)
     * @return a set of all scope-related account OIDs (includes
     */
    public static Set<Long> getAllBusinessAccountOidsInScope(
        @Nonnull Set<Long> scopeBusinessAccountsOids,
        @Nonnull Map<Long, Set<Long>> baOidToEaSiblingAccounts) {
        return new HashSet<Long>() {{
            addAll(scopeBusinessAccountsOids);
            addAll(scopeBusinessAccountsOids.stream()
                .map(scopeBusinessAccountsOid -> baOidToEaSiblingAccounts.getOrDefault(scopeBusinessAccountsOid, Sets.newHashSet()))
                .flatMap(Set::stream)
                .collect(Collectors.toSet()));
        }};
    }

    /**
     * Get a set of all Business Account OIDs in scope from a set of scope OIDs.
     *
     * @param scopeEntityOids set of all scope OIDs
     * @return a set of Business Account OIDs in scope
     */
    public Set<Long> getAllBusinessAccountOidsInScope(@Nonnull Set<Long> scopeEntityOids) {
        List<TopologyEntityDTO> allBusinessAccounts = getAllBusinessAccounts(realtimeTopologyContextId);
        Map<Long, Set<Long>> baOidToEaSiblingAccounts = getBaOidToEaSiblingAccounts(allBusinessAccounts);
        Set<Long> scopeBusinessAccountsOids = RepositoryClient.getFilteredScopeBusinessAccountOids(scopeEntityOids, allBusinessAccounts);
        return getAllBusinessAccountOidsInScope(scopeBusinessAccountsOids, baOidToEaSiblingAccounts);
    }

    /**
     * Get the entities map associated with a scoped or global topology (cloud plans or real-time).
     *
     * @param scopeIds  The topology scope seed IDs.
     * @param topologyContextId The topology context id.
     * @param supplyChainServiceBlockingStub the Supply Chain Service to make calls to to get the topology
     * nodes associated with the scopeIds.
     * @return A Map containing the relevant cloud scopes, keyed by scope type and mapped to scope OIDs.
     */
    @Nonnull
    public Map<EntityType, Set<Long>> getEntityOidsByTypeForRIQuery(
            @Nonnull final List<Long> scopeIds,
            final long topologyContextId,
            @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub) {
        return getEntityOidsByTypeForRIQuery(scopeIds, topologyContextId, supplyChainServiceBlockingStub, null);
    }

    /**
     * Get the entities map associated with a scoped or global topology (cloud plans or real-time).
     *
     * @param scopeIds  The topology scope seed IDs.
     * @param topologyContextId The topology context id.
     * @param supplyChainServiceBlockingStub the Supply Chain Service to make calls to to get the topology
     * nodes associated with the scopeIds.
     * @param accountIds the optional set of business account IDs to be integrated into the returned map
     * @return A Map containing the relevant cloud scopes, keyed by scope type and mapped to scope OIDs.
     */
    @Nonnull
    public Map<EntityType, Set<Long>> getEntityOidsByTypeForRIQuery(
            @Nonnull final List<Long> scopeIds,
            final Long topologyContextId,
            @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
            @Nullable final Set<Long> accountIds) {
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
            Map<EntityType, Set<Long>> topologyMap = parseSupplyChainResponseToEntityOidsMap(response.getSupplyChain());

            Set<Integer> typesRelevantToAccountRIScope = Sets.newHashSet(
                EntityType.BUSINESS_ACCOUNT_VALUE,
                EntityType.DATABASE_VALUE,
                EntityType.DATABASE_SERVER_VALUE,
                EntityType.VIRTUAL_MACHINE_VALUE);

            Set<Integer> topologyTypesRelevantToAccountRIScope = retrieveTopologyEntities(scopeIds, topologyContextId)
                .map(scopeEntity -> scopeEntity.getEntityType())
                .distinct()
                .filter(typesRelevantToAccountRIScope::contains)
                .collect(Collectors.toSet());

            // Include supported non-supplychain entities in response.
            for (EntityType entityType : supportedNonSupplyChainEntitiesByType) {
                switch (entityType) {
                    case BUSINESS_ACCOUNT:
                        // Make adjustment for Business Accounts/Subscriptions.  Get all related
                        // accounts in the family.
                        if (topologyContextId != realtimeTopologyContextId) {
                            if (topologyTypesRelevantToAccountRIScope.isEmpty()) {
                                break;
                            }
                            topologyMap.put(EntityType.BUSINESS_ACCOUNT,
                                Objects.isNull(accountIds)
                                    ? getAllBusinessAccountOidsInScope(Sets.newHashSet(scopeIds))
                                    : accountIds);
                        }
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
     * @param supplyChain to be parsed.
     * @return The Map of topology entities of interest, grouped by type.
     */
    @Nonnull
    public Map<EntityType, Set<Long>>
           parseSupplyChainResponseToEntityOidsMap(@Nonnull final SupplyChain supplyChain) {
        try {
            List<SupplyChainNode> supplyChainNodes = supplyChain.getSupplyChainNodesList();
            Map<EntityType, Set<Long>> entitiesMap = new HashMap<>();
            for (SupplyChainNode node : supplyChainNodes) {
                final Map<Integer, SupplyChainNode.MemberList> relatedEntitiesByType = node
                                .getMembersByStateMap();
                final String entityTypeName = node.getEntityType();
                final EntityType entityType = UIEntityType.fromString(entityTypeName).sdkType();
                for (SupplyChainNode.MemberList members : relatedEntitiesByType.values()) {
                    final List<Long> memberOids = members.getMemberOidsList();
                    entitiesMap.computeIfAbsent(entityType, (key) -> new HashSet<>())
                            .addAll(memberOids);
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
