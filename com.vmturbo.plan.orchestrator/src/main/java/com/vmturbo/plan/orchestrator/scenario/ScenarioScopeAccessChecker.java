package com.vmturbo.plan.orchestrator.scenario;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
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

    public ScenarioScopeAccessChecker(UserSessionContext userSessionContext,
                                      GroupServiceBlockingStub groupServiceClient,
                                      SearchServiceBlockingStub searchServiceClient) {
        this.userSessionContext = userSessionContext;
        this.groupServiceClient = groupServiceClient;
        this.searchServiceClient = searchServiceClient;
    }

    /**
     * Given an entity access scope and scenario scope, check that the scenario scope is covered
     * by the entity access scope. This does NOT check plan ownership. Also it checks if all the
     * given scopes actually exist, if any one of them does not exist, then this scenario scope is
     * considered invalid and IllegalArgumentException is thrown.
     *
     * @param scenarioInfo the scenario info containing scope entries
     * @throws ScenarioScopeNotFoundException exception thrown if scope not found
     */
    void checkScenarioAccessAndValidateScopes(ScenarioInfo scenarioInfo)
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
        List<Long> groupMembers =  Collections.EMPTY_LIST;
        for (PlanScopeEntry scopeEntry : scenarioInfo.getScope().getScopeEntriesList()) {
            if (GROUP_SCOPE_ENTRY_TYPES.contains(scopeEntry.getClassName())) {
                // if it's a group type, add it to the list of groups to expand and check.
                // If Cloud Billing Family scope, add to scopeGroupIds as well, as internally this is
                // a group of business accounts and will be treated as such.
                scopeGroupIds.add(scopeEntry.getScopeObjectOid());
            } else {
                // this is an entity -- check directly
                if (userSessionContext.isUserScoped() &&
                        !accessScope.contains(scopeEntry.getScopeObjectOid())) {
                    throw new UserAccessScopeException("User doesn't have access to all entities in scenario.");
                } else {
                    scopeEntityIds.add(scopeEntry.getScopeObjectOid());
                }
            }
        }

        // check access on any groups that were contained in the group list.
        // the groupService.getGroups() will return an error if any groups in the request are out
        // of scope, so we don't need to check them individually.
        if (!scopeGroupIds.isEmpty()) {
            Iterator<Grouping> groups = groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                            .setGroupFilter(GroupFilter.newBuilder()
                                            .addAllId(scopeGroupIds))
                            .build());
            final Set<Long> resultGroups = new HashSet<>(scopeGroupIds.size());
            while (groups.hasNext()) {
                resultGroups.add(groups.next().getId());
            }

            // check if all the groups exist, if any of the groups does not exist, then the whole
            // scenario scopes are invalid
            if (resultGroups.size() != scopeGroupIds.size()) {
                throw new ScenarioScopeNotFoundException(Sets.difference(scopeGroupIds, resultGroups));
            }
        }

        // check if all the scope entities exist in the repository
        if (!scopeEntityIds.isEmpty()) {
            List<Long> responseEntities = searchServiceClient.searchEntityOids(
                SearchEntityOidsRequest.newBuilder().addAllEntityOid(scopeEntityIds).build())
                .getEntitiesList();
            // if any of the entities does not exist, then the whole scenario scopes are invalid
            if (responseEntities.size() != scopeEntityIds.size()) {
                throw new ScenarioScopeNotFoundException(Sets.difference(scopeEntityIds,
                        new HashSet<>(responseEntities)));
            }
        }
    }
}
