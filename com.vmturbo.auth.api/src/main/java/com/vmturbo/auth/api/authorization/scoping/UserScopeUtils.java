package com.vmturbo.auth.api.authorization.scoping;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OPERATIONAL_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_OBSERVER;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.access.AccessDeniedException;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.identity.OidSet.AllOidsSet;
import com.vmturbo.components.common.identity.RoaringBitmapOidSet;

/**
 * Static utility methods for finding user scope info from the current security / grpc context.
 */
public class UserScopeUtils {
    private static Logger logger = LogManager.getLogger();

    /**
     * shared roles.
     */
    public static final Set<String> SHARED_ROLES = ImmutableSet.of(SHARED_OBSERVER, SHARED_ADVISOR);

    /**
     * observer roles.
     */
    public static final Set<String> OBSERVER_ROLES = ImmutableSet.of(OBSERVER, SHARED_OBSERVER, OPERATIONAL_OBSERVER);

    // entity types available to "shared" roles. Modeled after SHARED_USER_ENTITIES_LIST in classic's
    // ScopedUserUtil.java.
    // Please sync changes to this list with identical const in ux-app user.form.component.ts
    public static final Set<String> SHARED_USER_ENTITY_TYPES = ImmutableSet.of(
            ApiEntityType.APPLICATION_COMPONENT.apiStr(),
            ApiEntityType.BUSINESS_APPLICATION.apiStr(),
            ApiEntityType.BUSINESS_TRANSACTION.apiStr(),
            ApiEntityType.SERVICE.apiStr(),
            ApiEntityType.VIRTUAL_MACHINE.apiStr(),
            ApiEntityType.DATABASE_SERVER.apiStr(),
            ApiEntityType.DATABASE.apiStr(),
            ApiEntityType.CONTAINER.apiStr(),
            ApiEntityType.CONTAINER_POD.apiStr(),
            ApiEntityType.VIRTUAL_DATACENTER.apiStr()
            );

    /**
     * Cloud static infrastructure EntityTypes.
     */
    public static final Collection<String> STATIC_CLOUD_ENTITY_TYPES = ImmutableList.of(
            ApiEntityType.REGION.apiStr(),
            ApiEntityType.AVAILABILITY_ZONE.apiStr(),
            ApiEntityType.COMPUTE_TIER.apiStr(),
            ApiEntityType.STORAGE_TIER.apiStr(),
            ApiEntityType.DATABASE_SERVER_TIER.apiStr(),
            ApiEntityType.DATABASE_TIER.apiStr());


    public static boolean isUserScoped() {
        // first check if there is a security context user
        Optional<AuthUserDTO> user = getAuthUser();
        if (user.isPresent()) {
            return (user.get().getScopeGroups().size() > 0);
        }
        // if no user, check if there is something in the grpc context.
        List<Long> grpcContextScopeGroups = SecurityConstant.USER_SCOPE_GROUPS_KEY.get();
        return (grpcContextScopeGroups != null && !grpcContextScopeGroups.isEmpty());
    }

    /**
     * Does the list of roles contain any "shared" roles? These get special treatment... for the
     * time being. (we'll expect to replace this method when we add support for "custom user roles",
     * and the "shared roles" will stop being a special hard-coded case)
     *
     * @param roles the list of roles to check.
     * @return true, if any of the roles are considered "shared". false, otherwise.
     */
    public static boolean containsSharedRole(List<String> roles) {

        if (roles == null || roles.isEmpty()) {
            // no roles found -- assuming not shared.
            // TODO: Once we have a reliable "system user" as part of OM-44445, we should consider
            // treating this as a security errors, since at that point all users should have some
            // kind of role.
            logger.debug("No roles found in calling context -- assuming user is not shared");
            return false;
        }
        logger.debug("Found roles {} in calling context", roles);
        for (String role : roles) {
            if (SHARED_ROLES.contains(role.toUpperCase())) {
                logger.debug("User is 'shared' because it has role {}", role);
                return true;
            }
        }
        return false;
    }

    /**
     * To check if an auth user is of type observer. See {@link #OBSERVER_ROLES}.
     *
     * @return true if observer user.
     */
    public static boolean isUserObserver() {
        Optional<AuthUserDTO> user = getAuthUser();
        List<String> roles;
        if (user.isPresent()) {
            roles = user.get().getRoles();
        } else {
            // check grpc context
            roles = SecurityConstant.USER_ROLES_KEY.get();
        }
        return roles != null && (roles.stream().anyMatch(OBSERVER_ROLES::contains));
    }

    /**
     * Is the current user in a "shared role"? ("Shared Observer", "Shared Advisor"). This will be
     * based on the user's role.
     *
     * @return
     */
    public static boolean isUserShared() {
        // first check if there is a security context user
        List<String> roles;
        Optional<AuthUserDTO> user = getAuthUser();
        if (user.isPresent()) {
            roles = user.get().getRoles();
        } else {
            // check grpc context
            roles = SecurityConstant.USER_ROLES_KEY.get();
        }

        return containsSharedRole(roles);
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
        return (grpcScopeGroups == null) ? Collections.emptyList() : grpcScopeGroups;
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
