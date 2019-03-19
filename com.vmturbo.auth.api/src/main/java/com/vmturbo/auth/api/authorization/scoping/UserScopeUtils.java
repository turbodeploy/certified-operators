package com.vmturbo.auth.api.authorization.scoping;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.security.access.AccessDeniedException;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.identity.OidSet.AllOidsSet;
import com.vmturbo.components.common.identity.RoaringBitmapOidSet;

/**
 * Static utility methods for finding user scope info from the current security / grpc context
 */
public class UserScopeUtils {

    public static boolean isUserScoped() {
        // first check if there is a security context user
        Optional<AuthUserDTO> user = getAuthUser();
        if (user.isPresent()) {
            return (user.get().getScopeGroups().size() > 0);
        }
        // if no user, check if there is something in the grpc context.
        List<Long> grpcContextScopeGroups = SecurityConstant.USER_SCOPE_GROUPS_KEY.get();
        return (CollectionUtils.isNotEmpty(grpcContextScopeGroups));
    }

    private static Optional<AuthUserDTO> getAuthUser() {
        return SAMLUserUtils.getAuthUserDTO();
    }


    public static List<Long> getUserScopeGroups() {
        // first check if there is a security context user
        Optional<AuthUserDTO> user = getAuthUser();
        if (user.isPresent()) {
            return user.get().getScopeGroups();
        }
        // if no user, check if there is something in the grpc context.
        List<Long> grpcScopeGroups = SecurityConstant.USER_SCOPE_GROUPS_KEY.get();
        return (grpcScopeGroups == null) ? Collections.EMPTY_LIST : grpcScopeGroups;
    }

    /**
     * Convenience method for checking access on a set of entity ids.
     *
     * This method is effectively the same thing as "contains(...)", but instead of returning false
     * if any oid is not "contained", this method will throw an {@link AccessDeniedException}, which
     * the caller can catch or propagate normally. The idea of this method is to reduce
     * boilerplate generation of {@link AccessDeniedException} objects since a "check access and
     * throw exception on failure" pattern may be common.
     *
     * @param scope an {@link EntityAccessScope} to check access on
     * @param oids a collection of oids to check acess for.
     * @return true, if all oids are considered accessible by the user.
     * @throws AccessDeniedException if any of the oids are not in the set of accessible entities
     * for the user.
     */
    public static boolean checkAccess(@Nonnull final EntityAccessScope scope,
                               @Nonnull final Collection<Long> oids) throws UserAccessScopeException {
        if (scope.contains(oids)) {
            return true;
        }
        throw new UserAccessScopeException("User doesn't have access to all entities in set.");
    }

    /**
     * Another convenience method for checking access on a set of entity id's. This one will work
     * off of the {@link UserSessionContext} rather than the scope directly, and will return true
     * if the user is not scoped.
     *
     * @param context The {@link UserSessionContext} instance to check access using.
     * @param oids the collection of oids to check access on.
     * @return true, if the oids in the collection are all accessible.
     * @throws AccessDeniedException
     */
    public static boolean checkAccess(@Nonnull final UserSessionContext context,
                                      @Nonnull final Collection<Long> oids) throws UserAccessScopeException {
        if (!context.isUserScoped()) {
            return true;
        }
        return checkAccess(context.getUserAccessScope(), oids);
    }


    /**
     * Convenience method for translating a collection of Longs into a filtered-down OidSet, in the
     * context of service request parameters. We usually treat an empty entity id collection as a
     * request for "the whole market", e.g. all entities.
     *
     * Because inspecting this set of requested entities and choosing a path based on whether the
     * request is for "the market" or not, we are encapsulating some of that logic here.
     *
     * This decision on handling requests for "the market" vs specific entities is one with a lot of
     * ramifications that are component-specific, because components handle entity-related requests
     * so differently. A generic framework for managing and scoping these requests would be great to
     * have. Unfortunately, we're going to have to analyze these case-by-case until something better
     * evolves.
     *
     * TODO: I ended up not using this method yet. But keeping it "on probation" until the scope
     * checks are done. If it's still not necessary by then I'll remove it.
     *
     * @param oids a collection of the oids in the request. If this collection is empty, it's assumed
     *             the collection represents the whole market.
     * @return an {@link OidSet} representing the potentially filtered-down oid set.
     */
    public static OidSet filterEntityRequest(@Nonnull final EntityAccessScope scope,
                                             final Collection<Long> oids) throws UserAccessScopeException {
        // an empty collection means a request for the whole market.
        // if the request is for "the market", return the "all oids set" if the user is not
        // scoped. If the user IS scoped, then return the members of the users scope groups. This
        // should be consistent with the behavior in classic, where a "market" request for a scoped
        // user is converted to a request for the user's scope group members.
        if (oids == null || oids.isEmpty()) {
            // if the user does not have any scope restrictions, return everything.
            if (scope.containsAll()) {
                return AllOidsSet.ALL_OIDS_SET;
            } else {
                // return their scope group members.
                return scope.getScopeGroupMembers();
            }
        }
        // if the "request" set is not-empty, then we need to perform an intersection.
        // if the requested set is a subset of the accessible set, then we will return the request
        // set. If the requested set is NOT a subset of the accessible set, we will throw an
        // AccessDeniedException. This is in keeping with the behavior in classic, where AXDenied
        // exceptions are only thrown when a specific set of entities are requested, and never from
        // "full market" requests.
        if (scope.contains(oids)) {
            return new RoaringBitmapOidSet(oids);
        } else {
            throw new UserAccessScopeException("User doesn't have access to all entities in request.");
        }
    }

}
