package com.vmturbo.plan.orchestrator.api;

import java.util.Optional;

import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

/**
 * Utilities for working with plans
 */
public class PlanUtils {
    /**
     * Check if the currently-logged-in user can access the specified plan. Access is allowed if:
     *   * There is no user in the calling context. (this implies a system user)
     *   * The current user is an administrator. Administrators can access all plans.
     *   * The current user is a site administrator. Site administrators can access all plans.
     *   * The current user is the creator of the plan, as per the {@link PlanInstance#getCreatedByUser()} field.
     * @param planInstance the plan instance to check.
     * @return true, if access should be allowed. false if not.
     */
    static public boolean canCurrentUserAccessPlan(PlanInstance planInstance) {
        // an anonymous plan can be accessed by anyone.
        if (! planInstance.hasCreatedByUser()) {
            return true;
        }
        // if we have a user id in the context, we may prevent access to the plan supply chain
        // if the user is either not an admin user or does not own the plan
        // if a user id is NOT in the context, we assume the current user is the "system" and allow
        // access to all plans. This is not ideal, and is something we should revisit when we
        // address OM-44445 (more comprehensive treatment of "System" users)
        Optional<String> userId = UserContextUtils.getCurrentUserId();
        Optional<Boolean> isAdmin = UserContextUtils.currentUserHasRole(SecurityConstant.ADMINISTRATOR);
        Optional<Boolean> isSiteAdmin =
            UserContextUtils.currentUserHasRole(SecurityConstant.SITE_ADMIN);

        boolean isDefinitelyNotAdmin = ! isAdmin.orElse(true) && ! isSiteAdmin.orElse(true);
        if (isDefinitelyNotAdmin && userId.isPresent()) {
            // check access
            if (! planInstance.getCreatedByUser().equalsIgnoreCase(userId.get())) {
                return false;
            }
        }
        return true;
    }
}
