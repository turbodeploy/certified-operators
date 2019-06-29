package com.vmturbo.plan.orchestrator.scenario;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * A Utility class for checking user scope
 */
public class ScenarioScopeAccessChecker {
    private static final Logger logger = LogManager.getLogger();

    // the set of group class types that may appear in a scenario scope.
    static private final Set<String> GROUP_SCOPE_ENTRY_TYPES = ImmutableSet.of(StringConstants.GROUP,
            StringConstants.CLUSTER, StringConstants.STORAGE_CLUSTER,
            StringConstants.VIRTUAL_MACHINE_CLUSTER);

    private final UserSessionContext userSessionContext;
    private final GroupServiceBlockingStub groupServiceClient;

    public ScenarioScopeAccessChecker(UserSessionContext userSessionContext,
                                      GroupServiceBlockingStub groupServiceClient) {
        this.userSessionContext = userSessionContext;
        this.groupServiceClient = groupServiceClient;
    }

    /**
     * Given an entity access scope and scenario scope, check that the scenario scope is covered
     * by the entity access scope. This does NOT check plan ownership.
     *
     * @param scenarioInfo
     * @return
     */
    public boolean checkScenarioAccess(ScenarioInfo scenarioInfo) {
        // non-scoped users have access by default.
        if (! userSessionContext.isUserScoped()) {
            return true;
        }

        // if no scope, this is a "market" plan and a scoped user does not have access to it.
        if (!scenarioInfo.hasScope()) {
            return false;
        }

        // if there IS a scope, we need to verify user access to all of the entities in the scope.
        EntityAccessScope accessScope = userSessionContext.getUserAccessScope();
        PlanScope planScope = scenarioInfo.getScope();
        Set<Long> scopeGroupIds = new HashSet();
        for (PlanScopeEntry scopeEntry : planScope.getScopeEntriesList()) {
            if (GROUP_SCOPE_ENTRY_TYPES.contains(scopeEntry.getClassName())) {
                // if it's a group type, add it to the list of groups to expand and check.
                scopeGroupIds.add(scopeEntry.getScopeObjectOid());
            } else {
                // this is an entity -- check directly
                if (!accessScope.contains(scopeEntry.getScopeObjectOid())) {
                    throw new UserAccessScopeException("User doesn't have access to all entities in scenario.");
                }
            }
        }
        // check access on any groups that were contained in the group list.
        // the groupService.getGroups() will return an error if any groups in the request are out
        // of scope, so we don't need to check them individually.
        if (scopeGroupIds.size() > 0) {
            Iterator<Group> groups = groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(scopeGroupIds)
                    .build());
            // we'll just count then number of returned groups and match against the request group
            // as a sanity check.
            int numResultGroups = Iterators.size(groups);
            if (numResultGroups != scopeGroupIds.size()) {
                logger.warn("Only {} of {} groups in the plan scenario actually exist.",
                        numResultGroups, scopeGroupIds.size());
            }
        }
        return true;
    }
}
