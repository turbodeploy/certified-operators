package com.vmturbo.auth.api.usermgmt;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An {@link AuthorizeUserInGroupsInputDTO} represents the User object to be authorized in auth component with multiple external groups.
 */
public class AuthorizeUserInGroupsInputDTO implements Serializable {

    /**
     * The user name.
     */
    private String user;

    /**
     * The user external groups.
     */
    private String[] group;
    /**
     * The user's IP address.
     */
    private String ipAddress;

    /**
     * Constructs the AuthUserDTO. We keep it non-public, so that only the JSON deserialization
     * could use it.
     */
    protected AuthorizeUserInGroupsInputDTO() {
    }

    /**
     * Constructs the AuthUserDTO.
     *
     * @param user      The user name.
     * @param group     external group the user belonged to.
     * @param ipAddress The user's IP address
     */
    public AuthorizeUserInGroupsInputDTO(final @Nonnull String user, final @Nullable String[] group,
            final @Nonnull String ipAddress) {
        this.user = user;
        this.ipAddress = ipAddress;
        this.group = group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthorizeUserInGroupsInputDTO that = (AuthorizeUserInGroupsInputDTO)o;
        return user.equals(that.user)
                && Arrays.equals(group, that.group)
                && ipAddress.equals(that.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, group, ipAddress);
    }

    /**
     * /** Returns the user name.
     *
     * @return The user name.
     */
    public String getUser() {
        return user;
    }

    /**
     * Returns the user's IP address.
     *
     * @return The user's IP address.
     */
    public String getIpAddress() {
        return ipAddress;
    }

    /**
     * Returns external group.
     *
     * @return external group.
     */
    public String[] getGroup() {
        return group;
    }
}
