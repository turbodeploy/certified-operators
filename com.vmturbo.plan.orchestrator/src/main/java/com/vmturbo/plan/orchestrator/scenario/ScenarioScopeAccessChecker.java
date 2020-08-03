package com.vmturbo.plan.orchestrator.scenario;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
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

    public ScenarioScopeAccessChecker(UserSessionContext userSessionContext,
                                      GroupServiceBlockingStub groupServiceClient,
                                      SearchServiceBlockingStub searchServiceClient,
                                      SupplyChainServiceBlockingStub supplyChainServiceClient) {
        this.userSessionContext = userSessionContext;
        this.groupServiceClient = groupServiceClient;
        this.searchServiceClient = searchServiceClient;
        this.supplyChainServiceClient = supplyChainServiceClient;
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

        final List<PlanScopeEntry> scopeEntriesToAdd = Lists.newArrayList();

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

            // We compare only VMs, DBs and DBSs entity types because they are the main focus for Plan results
            response.getSupplyChainNodesList().stream()
                    .filter(node -> node.getEntityType().equals(StringConstants.VIRTUAL_MACHINE)
                            || node.getEntityType().equals(StringConstants.DATABASE)
                            || node.getEntityType().equals(StringConstants.DATABASE_SERVER))
                    .forEach(node -> {
                        node.getMembersByStateMap().values().stream()
                                .forEach(memberList -> memberList.getMemberOidsList().stream()
                                        .filter(accessScope::contains)
                                        .forEach(memberOid -> {
                                            scopeEntriesToAdd.add(PlanScopeEntry.newBuilder()
                                                    .setScopeObjectOid(memberOid)
                                                    .setClassName(node.getEntityType())
                                                    .build());
                                        }));
                    });

            // Update the Plan scope to allow only what the User can access to
            if (scopeEntriesToAdd.isEmpty()) {
                throw new UserAccessScopeException("User doesn't have access to all entities in scenario.");
            } else {
                PlanScope.Builder scopeBuilder = scenarioInfo.getScope().toBuilder();
                scopeBuilder.clearScopeEntries();
                scopeBuilder.addAllScopeEntries(scopeEntriesToAdd);
                scenarioInfo = scenarioInfo.toBuilder()
                        .setScope(scopeBuilder.build())
                        .build();
            }
        }

        return scenarioInfo;
    }
}
