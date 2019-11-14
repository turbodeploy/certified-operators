package com.vmturbo.api.component.security;

import javax.annotation.Nullable;

/**
 * A interface to map headers from external requests to XL, e.g.:
 * <ul>
 * <li> barracuda_account -> user name
 * <li> barracuda_roles -> user role -> external user group
 * e.g. Service Administrator -> Administrator -> CWOM_Administrator (group)
 * </ul>
 */
public interface HeaderMapper {
    /**
     * Get external vendor username.
     *
     * @return external vendor username.
     */
    String getUserName();

    /**
     * Get external vendeor role.
     *
     * @return external vendeor role.
     */
    String getRole();

    /**
     * Get XL role.
     *
     * @param externalRole external vendor role
     * @return get xl role.
     */
    String getAuthRole(@Nullable String externalRole);
}
