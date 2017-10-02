package com.vmturbo.auth.api.authorization.spring;

import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.springframework.security.access.expression.method.MethodSecurityExpressionOperations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

/**
 * The SpringMethodSecurityExpressionRoot implements custom SecurityExpressionRoot.
 */
public class SpringMethodSecurityExpressionRoot implements MethodSecurityExpressionOperations {
    /**
     * The role prefix.
     */
    private final String DEFAULT_ROLEPREFIX = "ROLE_";

    /**
     * The role prefix length.
     */
    private final int DEFAULT_ROLEPREFIX_LENGTH = DEFAULT_ROLEPREFIX.length();

    /**
     * The set of roles supplied as part of the authentication object.
     */
    private final Set<String> roles_;

    /**
     * The authentication object.
     */
    private final Authentication authentication_;

    /**
     * The filter object.
     */
    private Object filterObject_;

    /**
     * The return object.
     */
    private Object returnObject_;

    /**
     * The target.
     */
    private Object target_;

    /**
     * Constructs the expression root.
     *
     * @param authentication The authentication object.
     */
    public SpringMethodSecurityExpressionRoot(final @Nonnull Authentication authentication) {
        authentication_ = Objects.requireNonNull(authentication);
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        for (GrantedAuthority auth : authentication_.getAuthorities()) {
            String role = auth.getAuthority();
            if (role.startsWith(DEFAULT_ROLEPREFIX)) {
                role = role.substring(DEFAULT_ROLEPREFIX_LENGTH);
            }
            set.add(role);
        }
        roles_ = set.build();
    }

    /**
     * Sets the filter object.
     *
     * @param filterObject The filter object.
     */
    @Override
    public void setFilterObject(Object filterObject) {
        filterObject_ = filterObject;
    }

    /**
     * Returns the filter object.
     *
     * @return The filter object.
     */
    @Override
    public Object getFilterObject() {
        return filterObject_;
    }

    /**
     * Sets the return object.
     *
     * @param returnObject The return object.
     */
    @Override
    public void setReturnObject(Object returnObject) {
        returnObject_ = returnObject;
    }

    /**
     * Returns the return object.
     *
     * @return The return object.
     */
    @Override
    public Object getReturnObject() {
        return returnObject_;
    }

    /**
     * Sets the "this" property for use in expressions. Typically this will be the "this"
     * property of the {@code JoinPoint} representing the method invocation which is being
     * protected.
     *
     * @param target the target object on which the method in is being invoked.
     */
    void setThis(Object target) {
        target_ = target;
    }

    /**
     * Returns the target object.
     *
     * @return The target object.
     */
    @Override
    public Object getThis() {
        return target_;
    }

    /**
     * Gets the {@link Authentication} used for evaluating the expressions
     *
     * @return the {@link Authentication} for evaluating the expressions
     */
    @Override public Authentication getAuthentication() {
        return authentication_;
    }

    /**
     * Determines if the {@link #getAuthentication()} has a particular authority within
     * {@link Authentication#getAuthorities()}.
     *
     * @param authority the authority to test (i.e. "ROLE_USER")
     * @return true if the authority is found, else false
     */
    @Override public boolean hasAuthority(String authority) {
        return false;
    }

    /**
     * Determines if the {@link #getAuthentication()} has any of the specified authorities
     * within {@link Authentication#getAuthorities()}.
     *
     * @param authorities the authorities to test (i.e. "ROLE_USER", "ROLE_ADMIN")
     * @return true if any of the authorities is found, else false
     */
    @Override public boolean hasAnyAuthority(String... authorities) {
        return false;
    }

    /**
     * <p>
     * Determines if the {@link #getAuthentication()} has a particular authority within
     * {@link Authentication#getAuthorities()}.
     * </p>
     * <p>
     * This is similar to {@link #hasAuthority(String)} except that this method implies
     * that the String passed in is a role. For example, if "USER" is passed in the
     * implementation may convert it to use "ROLE_USER" instead. The way in which the role
     * is converted may depend on the implementation settings.
     * </p>
     *
     * @param role the authority to test (i.e. "USER")
     * @return true if the authority is found, else false
     */
    @Override public boolean hasRole(String role) {
        if (role.startsWith(DEFAULT_ROLEPREFIX)) {
            role = role.substring(DEFAULT_ROLEPREFIX_LENGTH);
        }
        return roles_.contains(role);
    }

    /**
     * <p>
     * Determines if the {@link #getAuthentication()} has any of the specified authorities
     * within {@link Authentication#getAuthorities()}.
     * </p>
     * <p>
     * This is a similar to hasAnyAuthority except that this method implies
     * that the String passed in is a role. For example, if "USER" is passed in the
     * implementation may convert it to use "ROLE_USER" instead. The way in which the role
     * is converted may depend on the implementation settings.
     * </p>
     *
     * @param roles the authorities to test (i.e. "USER", "ADMIN")
     * @return true if any of the authorities is found, else false
     */
    @Override public boolean hasAnyRole(String... roles) {
        for (String role : roles) {
            if (hasRole(role)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Always grants access.
     *
     * @return true
     */
    @Override public boolean permitAll() {
        return false;
    }

    /**
     * Always denies access
     *
     * @return false
     */
    @Override public boolean denyAll() {
        return false;
    }

    /**
     * Determines if the {@link #getAuthentication()} is anonymous
     *
     * @return true if the user is anonymous, else false
     */
    @Override public boolean isAnonymous() {
        return false;
    }

    /**
     * Determines ifthe {@link #getAuthentication()} is authenticated
     *
     * @return true if the {@link #getAuthentication()} is authenticated, else false
     */
    @Override public boolean isAuthenticated() {
        return authentication_.isAuthenticated();
    }

    /**
     * Determines if the {@link #getAuthentication()} was authenticated using remember me
     *
     * @return true if the {@link #getAuthentication()} authenticated using remember me,
     * else false
     */
    @Override public boolean isRememberMe() {
        return false;
    }

    /**
     * Determines if the {@link #getAuthentication()} authenticated without the use of
     * remember me
     *
     * @return true if the {@link #getAuthentication()} authenticated without the use of
     * remember me, else false
     */
    @Override public boolean isFullyAuthenticated() {
        return isAuthenticated();
    }

    /**
     * Determines if the {@link #getAuthentication()} has permission to access the target
     * given the permission
     *
     * @param target     the target domain object to check permission on
     * @param permission the permission to check on the domain object (i.e. "read",
     *                   "write", etc).
     * @return true if permission is granted to the {@link #getAuthentication()}, else
     * false
     */
    @Override public boolean hasPermission(Object target, Object permission) {
        // TODO:
        return false;
    }

    /**
     * Determines if the {@link #getAuthentication()} has permission to access the domain
     * object with a given id, type, and permission.
     *
     * @param targetId   the identifier of the domain object to determine access
     * @param targetType the type (i.e. com.example.domain.Message)
     * @param permission the perission to check on the domain object (i.e. "read",
     *                   "write", etc)
     * @return true if permission is granted to the {@link #getAuthentication()}, else
     * false
     */
    @Override public boolean hasPermission(Object targetId, String targetType, Object permission) {
        // TODO:
        return false;
    }
}
