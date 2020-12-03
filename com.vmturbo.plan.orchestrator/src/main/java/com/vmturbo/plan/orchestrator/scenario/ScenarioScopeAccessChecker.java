package com.vmturbo.plan.orchestrator.scenario;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * A Utility class for checking user scope
 */
public class ScenarioScopeAccessChecker {
    private static final Logger logger = LogManager.getLogger();

    // the set of group class types that may appear in a scenario scope.
    static private final Set<String> GROUP_SCOPE_ENTRY_TYPES = ImmutableSet.of(StringConstants.GROUP,
            StringConstants.CLUSTER, StringConstants.STORAGE_CLUSTER,
            StringConstants.VIRTUAL_MACHINE_CLUSTER, StringConstants.RESOURCE_GROUP,
            StringConstants.BILLING_FAMILY);

    private final UserSessionContext userSessionContext;
    private final GroupServiceBlockingStub groupServiceClient;
    private final SearchServiceBlockingStub searchServiceClient;
    private final SupplyChainServiceBlockingStub supplyChainServiceClient;
    private final RepositoryServiceBlockingStub repositoryServiceBlockingStub;

    public ScenarioScopeAccessChecker(UserSessionContext userSessionContext,
                                      GroupServiceBlockingStub groupServiceClient,
                                      SearchServiceBlockingStub searchServiceClient,
                                      SupplyChainServiceBlockingStub supplyChainServiceClient,
                                      RepositoryServiceBlockingStub repositoryServiceBlockingStub) {
        this.userSessionContext = userSessionContext;
        this.groupServiceClient = groupServiceClient;
        this.searchServiceClient = searchServiceClient;
        this.supplyChainServiceClient = supplyChainServiceClient;
        this.repositoryServiceBlockingStub = repositoryServiceBlockingStub;
    }

    /**
     * Given an entity access scope and scenario scope, check that the scenario scope is covered
     * by the entity access scope. This does NOT check plan ownership. Also it checks if all the
     * given scopes actually exist, if any one of them does not exist, then this scenario scope is
     * considered invalid and IllegalArgumentException is thrown.
     *
     * @param scenarioInfo the scenario info containing scope entries
     * @return if the user is scoped, the plan scope is updated with the entities that are accessible,
     *          otherwise it will be the same input {@link ScenarioInfo}
     * @throws ScenarioScopeNotFoundException exception thrown if scope not found
     */
    ScenarioInfo checkScenarioAccessAndValidateScopes(ScenarioInfo scenarioInfo)
            throws ScenarioScopeNotFoundException {

        // if no scope, this is a "market" plan and a scoped user does not have access to it.
        if (userSessionContext.isUserScoped() && !scenarioInfo.hasScope()) {
            throw new UserAccessScopeException("Scoped User doesn't have access to all entities" +
                " since we treat empty scenario scope as whole market.");
        }

        // if there IS a scope, we need to verify user access to all of the entities in the scope.
        final EntityAccessScope accessScope = userSessionContext.getUserAccessScope();
        final Set<Long> scopeGroupIds = new HashSet<>();
        final Set<Long> scopeEntityIds = new HashSet<>();

        for (PlanScopeEntry scopeEntry : scenarioInfo.getScope().getScopeEntriesList()) {
            if (GROUP_SCOPE_ENTRY_TYPES.contains(scopeEntry.getClassName())) {
                // if it's a group type, add it to the list of groups to expand and check.
                // If Cloud Billing Family scope, add to scopeGroupIds as well, as internally this is
                // a group of business accounts and will be treated as such.
                scopeGroupIds.add(scopeEntry.getScopeObjectOid());
            } else {
                // this is an entity -- check directly
                if (!userSessionContext.isUserScoped()
                        || accessScope.contains(scopeEntry.getScopeObjectOid())) {
                    scopeEntityIds.add(scopeEntry.getScopeObjectOid());
                }
            }
        }

        // check if all the scope entities exist in the repository
        if (!scopeEntityIds.isEmpty()) {
            List<Long> responseEntities = searchServiceClient.searchEntityOids(
                    Search.SearchEntityOidsRequest.newBuilder().addAllEntityOid(scopeEntityIds).build())
                    .getEntitiesList();
            // if any of the entities does not exist, then the whole scenario scopes are invalid
            if (responseEntities.size() != scopeEntityIds.size()) {
                throw new ScenarioScopeNotFoundException(Sets.difference(scopeEntityIds,
                        new HashSet<>(responseEntities)));
            }
        }

        // check if all the scope groups exist in the repository, if yes, resolve the members and add to the entity list
        if (!scopeGroupIds.isEmpty()) {
            final Set<Long> resultGroups = Sets.newHashSet();
            final GroupDTO.GetMembersRequest getGroupMembersReq = GroupDTO.GetMembersRequest.newBuilder()
                    .addAllId(scopeGroupIds)
                    .build();
            final Iterator<GroupDTO.GetMembersResponse> groupMembersResp =
                    groupServiceClient.getMembers(getGroupMembersReq);
            while (groupMembersResp.hasNext()) {
                GroupDTO.GetMembersResponse membersResponse = groupMembersResp.next();
                resultGroups.add(membersResponse.getGroupId());
                scopeEntityIds.addAll(membersResponse.getMemberIdList());
            }

            // check if all the groups exist, if any of the groups does not exist, then the whole
            // scenario scopes are invalid
            if (resultGroups.size() != scopeGroupIds.size()) {
                throw new ScenarioScopeNotFoundException(Sets.difference(scopeGroupIds, resultGroups));
            }
        }

        if (userSessionContext.isUserScoped()) {
            // make a SupplyChain request to intersect the Plan scope workloads with the User scope workloads
            final GetSupplyChainRequest.Builder supplyChainRequestBuilder = GetSupplyChainRequest.newBuilder()
                    .setScope(SupplyChainScope.newBuilder()
                            .addAllStartingEntityOid(scopeEntityIds))
                    .setFilterForDisplay(false);
            final GetSupplyChainRequest supplyChainRequest = supplyChainRequestBuilder.build();
            final SupplyChain response = supplyChainServiceClient.getSupplyChain(supplyChainRequest)
                    .getSupplyChain();

            if (scenarioInfo.getType().equals(StringConstants.OPTIMIZE_CLOUD_PLAN)) {
                // For OCP, we need to make sure user has access to all workloads in the plan scope.
                // Get the set of all workloads in the plan scope
                final Set<Long> planScope = response.getSupplyChainNodesList().stream()
                        .filter(node -> node.getEntityType() == ApiEntityType.VIRTUAL_MACHINE.typeNumber()
                                || node.getEntityType() == ApiEntityType.DATABASE.typeNumber()
                                || node.getEntityType() == ApiEntityType.DATABASE_SERVER.typeNumber())
                        .flatMap(node -> node.getMembersByStateMap().values().stream())
                        .flatMap(memberList -> memberList.getMemberOidsList().stream())
                        .collect(Collectors.toSet());

                // Check if user has access to all workloads in the plan scope.
                final boolean planScopeSubsetOfUserScope = accessScope.contains(planScope);
                if (!planScopeSubsetOfUserScope) {
                    // Don't allow user to run the plan if some of the workloads are not accessible.
                    // RI analysis and RI related APIs only support certain entity type as scopes.
                    // We can't do RI analysis on an arbitrary set of entities.
                    throw new UserAccessScopeException("User doesn't have access to some entities in plan scope.");
                }
            } else {
                final Set<Long> workloadOidsInPlanScope = response.getSupplyChainNodesList().stream()
                        .filter(node -> node.getEntityType() == ApiEntityType.VIRTUAL_MACHINE.typeNumber()
                                || node.getEntityType() == ApiEntityType.DATABASE.typeNumber()
                                || node.getEntityType() == ApiEntityType.DATABASE_SERVER.typeNumber())
                        .flatMap(node -> node.getMembersByStateMap().values().stream())
                        .flatMap(memberList -> memberList.getMemberOidsList().stream())
                        .collect(Collectors.toSet());

                final long numWorkloadsInPlanScope = workloadOidsInPlanScope.size();

                final Set<Long> accessibleWorkloadOidsInPlanScope = workloadOidsInPlanScope.stream()
                        .filter(accessScope::contains)
                        .collect(Collectors.toSet());

                // Update the Plan scope to allow only what the User can access to
                if (accessibleWorkloadOidsInPlanScope.size() == 0) {
                    throw new UserAccessScopeException("User doesn't have access to any entities in scenario.");
                } else if (accessibleWorkloadOidsInPlanScope.size() != numWorkloadsInPlanScope) {
                    // If user has access to a subset of the plan scope, rewrite the scope to the
                    // list of entities he has access to.
                    RetrieveTopologyEntitiesRequest.Builder requestBuilder = RetrieveTopologyEntitiesRequest.newBuilder()
                            .addAllEntityOids(accessibleWorkloadOidsInPlanScope)
                            .setTopologyType(TopologyType.SOURCE);
                    Set<PlanScopeEntry> accessibleEntitiesInPlanScope = RepositoryDTOUtil.topologyEntityStream(
                            repositoryServiceBlockingStub.retrieveTopologyEntities(requestBuilder.setReturnType(Type.MINIMAL).build()))
                            .map(PartialEntity::getMinimal)
                            .map(minimalEntity -> PlanScopeEntry.newBuilder()
                                    .setScopeObjectOid(minimalEntity.getOid())
                                    .setClassName(ApiEntityType.fromType(minimalEntity.getEntityType()).apiStr())
                                    .setDisplayName(minimalEntity.getDisplayName())
                                    .build())
                            .collect(Collectors.toSet());

                    PlanScope.Builder scopeBuilder = scenarioInfo.getScope().toBuilder();
                    scopeBuilder.clearScopeEntries();
                    scopeBuilder.addAllScopeEntries(accessibleEntitiesInPlanScope);
                    scenarioInfo = scenarioInfo.toBuilder()
                            .setScope(scopeBuilder.build())
                            .build();
                }
                // If user has access to all entities in the plan scope, there is no need to change
                // the scenarioInfo.
            }
        }

        return scenarioInfo;
    }
}
