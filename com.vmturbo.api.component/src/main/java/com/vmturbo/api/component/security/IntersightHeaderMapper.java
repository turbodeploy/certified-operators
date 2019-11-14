package com.vmturbo.api.component.security;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A class to map between Cisco Intersight and XL:
 * <ul>
 * <li> barracuda_account -> user name
 * <li> barracuda_roles -> user role -> external user group
 * e.g. Service Administrator -> Administrator -> CWOM_Administrator (group)
 * </ul>
 */
public class IntersightHeaderMapper implements HeaderMapper {

    private final String userName;
    private final String role;
    private final Map<String, String> intersightToCwomRoleMap;

    /**
     * Constructor for the mapper.
     *
     * @param intersightToCwomGroupMap map Cisco intersight role to CWOM group.
     * @param xBarracudaAccount Cisco intersight user account header.
     * @param xBarracudaRoles Cisco intersight user role header.
     */
    public IntersightHeaderMapper(@Nonnull Map<String, String> intersightToCwomGroupMap,
            @Nonnull String xBarracudaAccount, @Nonnull String xBarracudaRoles) {
        this.userName = xBarracudaAccount;
        this.role = xBarracudaRoles;
        this.intersightToCwomRoleMap = intersightToCwomGroupMap;
    }

    /**
     * Get user name.
     *
     * @return user name.
     */
    @Override
    public String getUserName() {
        return userName;
    }

    /**
     * Get user role.
     *
     * @return user role.
     */
    @Override
    public String getRole() {
        return role;
    }

    /**
     * Get internal Auth role by intersight role.
     *
     * @param intersigtRole intersight role.
     * @return CWOM role if exists.
     */
    @Override
    public String getAuthRole(@Nullable String intersigtRole) {
        return intersightToCwomRoleMap.get(intersigtRole);
    }
}
